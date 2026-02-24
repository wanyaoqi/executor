package server

import (
	"context"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"yunion.io/x/executor/apis"
	"yunion.io/x/log"
)

var globalSn uint32

func NewSN() uint32 {
	return atomic.AddUint32(&globalSn, 1)
}

func Len(sm *sync.Map) int {
	lengh := 0
	f := func(key, value interface{}) bool {
		lengh++
		return true
	}
	sm.Range(f)
	return lengh
}

var cmds = &sync.Map{}

type Commander struct {
	c      *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	wg         *sync.WaitGroup
	stdoutCh   chan struct{}
	stderrCh   chan struct{}
	stdoutData chan []byte // filled by background reader, consumed by FetchStdout
	stderrData chan []byte
	stdinFile  *os.File // /dev/null when no stdin, closed in Wait
}

// readPipeToChan reads from r until EOF and sends each chunk to ch, then closes ch.
// When the process exits very quickly, the first Read may return (0, io.EOF) before
// data is visible; we retry a few times so output is not lost.
func readPipeToChan(r io.Reader, ch chan<- []byte, bufSize int, label string, sn uint32) {
	buf := make([]byte, bufSize)
	firstRead := true
	var totalSent int
	for {
		n, err := r.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			ch <- chunk
			totalSent += len(chunk)
			firstRead = false
		}
		if err == io.EOF {
			if firstRead {
				log.Warningf("[executor stdout/stderr] sn=%d %s: first Read returned (0, EOF), retrying", sn, label)
				const maxRetries = 8
				for retry := 0; retry < maxRetries; retry++ {
					time.Sleep(time.Duration(5+retry*5) * time.Millisecond)
					n, err = r.Read(buf)
					if n > 0 {
						chunk := append([]byte(nil), buf[:n]...)
						ch <- chunk
						totalSent += len(chunk)
						firstRead = false
						log.Infof("[executor stdout/stderr] sn=%d %s: retry %d got %d bytes", sn, label, retry, n)
						break
					}
					if err != nil && err != io.EOF {
						log.Warningf("[executor stdout/stderr] sn=%d %s: retry %d read err=%v", sn, label, retry, err)
						break
					}
					if err == io.EOF {
						break
					}
				}
				if firstRead {
					log.Warningf("[executor stdout/stderr] sn=%d %s: closed channel with 0 bytes after %d retries", sn, label, maxRetries)
					close(ch)
					return
				}
				continue
			}
			log.Infof("[executor stdout/stderr] sn=%d %s: EOF after %d bytes total", sn, label, totalSent)
			close(ch)
			return
		}
		if err != nil {
			log.Warningf("[executor stdout/stderr] sn=%d %s: read err=%v, totalSent=%d", sn, label, err, totalSent)
			close(ch)
			return
		}
	}
}

func BytesArrayToStrArray(ba [][]byte) []string {
	if len(ba) == 0 {
		return nil
	}
	res := make([]string, len(ba))
	for i := 0; i < len(ba); i++ {
		res[i] = string(ba[i])
	}
	return res
}

func NewCommander(in *apis.Command) *Commander {
	cmd := exec.Command(string(in.Path), BytesArrayToStrArray(in.Args)...)
	if len(in.Env) > 0 {
		cmd.Env = BytesArrayToStrArray(in.Env)
	}
	if len(in.Dir) > 0 {
		cmd.Dir = string(in.Dir)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return &Commander{
		c:  cmd,
		wg: new(sync.WaitGroup),
	}
}

type Executor struct{}

func (e *Executor) ExecCommand(ctx context.Context, req *apis.Command) (*apis.Sn, error) {
	cm := NewCommander(req)
	sn := NewSN()
	log.Infof("%d/%d Exec %s", sn, Len(cmds), req.String())
	cmds.Store(sn, cm)
	return &apis.Sn{Sn: sn}, nil
}

func (e *Executor) Start(ctx context.Context, req *apis.StartInput) (*apis.StartResponse, error) {
	icm, ok := cmds.Load(req.Sn)
	if !ok {
		return nil, errors.Errorf("unknown sn %d", req.Sn)
	}
	var (
		m   = icm.(*Commander)
		err error
	)
	if req.HasStdin {
		m.stdin, err = m.c.StdinPipe()
		if err != nil {
			return &apis.StartResponse{
				Success: false,
				Error:   []byte(err.Error()),
			}, nil
		}
	} else {
		// Give child a valid stdin that returns EOF; avoids "read |0: file already closed"
		if m.stdinFile, err = os.Open(os.DevNull); err != nil {
			return &apis.StartResponse{
				Success: false,
				Error:   []byte(err.Error()),
			}, nil
		}
		m.c.Stdin = m.stdinFile
	}
	if req.HasStdout {
		m.stdout, err = m.c.StdoutPipe()
		if err != nil {
			if m.stdinFile != nil {
				m.stdinFile.Close()
				m.stdinFile = nil
			}
			return &apis.StartResponse{
				Success: false,
				Error:   []byte(err.Error()),
			}, nil
		}
		m.stdoutCh = make(chan struct{})
		m.stdoutData = make(chan []byte, 32)
	}
	if req.HasStderr {
		m.stderr, err = m.c.StderrPipe()
		if err != nil {
			if m.stdinFile != nil {
				m.stdinFile.Close()
				m.stdinFile = nil
			}
			return &apis.StartResponse{
				Success: false,
				Error:   []byte(err.Error()),
			}, nil
		}
		m.stderrCh = make(chan struct{})
		m.stderrData = make(chan []byte, 32)
	}

	if err := m.c.Start(); err != nil {
		if m.stdinFile != nil {
			m.stdinFile.Close()
			m.stdinFile = nil
		}
		return &apis.StartResponse{
			Success: false,
			Error:   []byte(err.Error()),
		}, nil
	}

	// Start reading stdout/stderr immediately so we never miss data (avoids race with fast-exit commands).
	// When process exits very fast, first Read() can return (0, EOF) before kernel delivers data; retry on that.
	if m.stdout != nil {
		log.Infof("[executor] sn=%d Start: spawning stdout reader", req.Sn)
		go readPipeToChan(m.stdout, m.stdoutData, 4096, "stdout", req.Sn)
	}
	if m.stderr != nil {
		go readPipeToChan(m.stderr, m.stderrData, 4096, "stderr", req.Sn)
	}

	return &apis.StartResponse{
		Success: true,
		Error:   nil,
	}, nil
}

func (e *Executor) Wait(ctx context.Context, in *apis.Sn) (*apis.WaitResponse, error) {
	icm, ok := cmds.Load(in.Sn)
	if !ok {
		return nil, errors.Errorf("unknown sn %d", in.Sn)
	}
	m := icm.(*Commander)

	// Must wait for stdout/stderr to be fully read BEFORE calling m.c.Wait().
	// Once m.c.Wait() returns, exec.Cmd may close the pipe FDs; our reader
	// goroutines would then get "read |0: file already closed" and miss data.
	if m.stdout != nil {
		<-m.stdoutCh
	}
	if m.stderr != nil {
		<-m.stderrCh
	}
	m.wg.Wait()

	err := m.c.Wait()
	var (
		exitStatus uint32
		errContent string
	)
	if err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			exitStatus = uint32(exiterr.Sys().(syscall.WaitStatus))
		} else {
			errContent = err.Error()
		}
	} else {
		exitStatus = 0
	}
	if m.stdinFile != nil {
		m.stdinFile.Close()
		m.stdinFile = nil
	}
	cmds.Delete(in.Sn)
	return &apis.WaitResponse{
		ExitStatus: exitStatus,
		ErrContent: []byte(errContent),
	}, nil
}

func (e *Executor) Kill(ctx context.Context, req *apis.Sn) (*apis.Error, error) {
	icm, ok := cmds.Load(req.Sn)
	if !ok {
		return nil, errors.Errorf("unknown sn %d", req.Sn)
	}

	m := icm.(*Commander)
	err := m.c.Process.Kill()
	if err != nil {
		return &apis.Error{Error: []byte(err.Error())}, nil
	}
	return &apis.Error{}, nil
}

func (e *Executor) SendInput(s apis.Executor_SendInputServer) error {
	var m *Commander
	for {
		input, err := s.Recv()
		if err == io.EOF {
			if input != nil && m == nil {
				icm, ok := cmds.Load(input.Sn)
				if !ok {
					return errors.Errorf("unknown sn %d", input.Sn)
				}
				m = icm.(*Commander)
				if m.stdin == nil {
					return errors.New("Process stdin not init")
				}
			}
			if m != nil {
				if e := m.stdin.Close(); e != nil {
					return errors.Wrap(e, "close stdin")
				}
			}
			return s.SendAndClose(&apis.Error{})
		} else if err != nil {
			return s.SendAndClose(&apis.Error{
				Error: []byte(err.Error()),
			})
		}
		if m == nil {
			icm, ok := cmds.Load(input.Sn)
			if !ok {
				return errors.Errorf("unknown sn %d", input.Sn)
			}
			m = icm.(*Commander)
			if m.stdin == nil {
				return errors.New("Process stdin not init")
			}
		}
		_, err = m.stdin.Write(input.Input)
		if err != nil {
			return s.SendAndClose(&apis.Error{
				Error: []byte(err.Error()),
			})
		}
	}
}

func (e *Executor) FetchStdout(sn *apis.Sn, s apis.Executor_FetchStdoutServer) error {
	icm, ok := cmds.Load(sn.Sn)
	if !ok {
		return errors.Errorf("unknown sn %d", sn.Sn)
	}
	m := icm.(*Commander)

	if m.stdout == nil {
		return errors.New("Process stdout not init")
	}
	close(m.stdoutCh)

	log.Infof("[executor] sn=%d FetchStdout: stream opened, reading from channel", sn.Sn)
	m.wg.Add(1)
	defer m.wg.Done()
	if err := s.Send(&apis.Stdout{Start: true}); err != nil {
		return err
	}
	var chunks int
	var totalBytes int
	for b := range m.stdoutData {
		chunks++
		totalBytes += len(b)
		if err := s.Send(&apis.Stdout{Stdout: b}); err != nil {
			log.Warningf("[executor] sn=%d FetchStdout: Send err=%v after %d chunks %d bytes", sn.Sn, err, chunks, totalBytes)
			return err
		}
	}
	log.Infof("[executor] sn=%d FetchStdout: channel closed, sent %d chunks %d bytes, sending Closed", sn.Sn, chunks, totalBytes)
	return s.Send(&apis.Stdout{Closed: true})
}

func (e *Executor) FetchStderr(sn *apis.Sn, s apis.Executor_FetchStderrServer) error {
	icm, ok := cmds.Load(sn.Sn)
	if !ok {
		return errors.Errorf("unknown sn %d", sn.Sn)
	}
	m := icm.(*Commander)

	if m.stderr == nil {
		return errors.New("Process stderr not init")
	}
	close(m.stderrCh)

	m.wg.Add(1)
	defer m.wg.Done()
	if err := s.Send(&apis.Stderr{Start: true}); err != nil {
		return err
	}
	for b := range m.stderrData {
		if err := s.Send(&apis.Stderr{Stderr: b}); err != nil {
			return err
		}
	}
	return s.Send(&apis.Stderr{Closed: true})
}
