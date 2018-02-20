package cmd

import (
	"sync"
	"os/exec"
	"time"
	"syscall"
	"os"
	"context"
	"fmt"
	"bytes"
)

type AsyncCommandWaitGroup struct {
	commands []*AsyncCommand
}

func (wg AsyncCommandWaitGroup) WaitTimeout(to time.Duration) {
	var swg sync.WaitGroup
	for _, c := range wg.commands {
		c.WaitGroup(&swg)
	}
	ch := make(chan struct{})
	go func() {
		swg.Wait()
		close(ch)
	}()
	select {
	case <- ch:
		return
	case <- time.After(to):
		for _, c := range wg.commands {
			c.Signal(syscall.SIGKILL)
		}
	}
}

type AsyncCommand struct {
	mtx sync.Mutex
	cmd *exec.Cmd
	sig os.Signal
	wait chan struct{}
	waitErr error
	buf bytes.Buffer
	exited bool
}

func NewAsyncCommand(ctx context.Context, cmdline, env []string) *AsyncCommand {
	cmd := exec.CommandContext(ctx, cmdline[0], cmdline[1:]...)
	cmd.Env = env
	c := &AsyncCommand{
		wait: make(chan struct{}),
		cmd: cmd,
	}
	cmd.Stdout = &c.buf
	cmd.Stderr = &c.buf
	return c
}

func (c *AsyncCommand) Signal(sig os.Signal) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.exited {
		return
	}
	c.cmd.Process.Signal(sig)
	c.sig = sig
}

func (c *AsyncCommand) Wait() <- chan error {
	ch := make(chan error)
	go func() {
		<-c.wait
		ch <- c.waitErr
	}()
	return ch
}

func (c *AsyncCommand) WaitGroup(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		<-c.wait
		wg.Done()
	}()
}

func (c *AsyncCommand) Start() {
	if err := c.cmd.Start(); err != nil {
		c.waitErr = err
		close(c.wait)
		return
	}
	go func() {
		defer close(c.wait)
		err := c.cmd.Wait()
		c.mtx.Lock()
		defer c.mtx.Unlock()
		c.exited = true
		if _, ok := err.(*exec.ExitError); ok {
			c.waitErr = fmt.Errorf("%s\n%s", err, c.buf.String())
		} else {
			c.waitErr = err
		}
	}()
}

