package cmd

import (
	"sync"
	"os/exec"
	"time"
	"context"
	"fmt"
	"bytes"
	"github.com/pkg/errors"
	"log"
	"syscall"
)

type AsyncCommand struct {
	mtx sync.Mutex
	cmd *exec.Cmd
	wait chan struct{}
	waitErr error
	buf bytes.Buffer
	exited bool
	sig syscall.Signal
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

func (c *AsyncCommand) Signal(sig syscall.Signal) {
	if c.exited {
		log.Printf("signalling exited process %s", c.cmd.Args)
		return
	} else {
		log.Printf("signalling process %s with %s", c.cmd.Args, sig)
	}
	c.sig = sig
	c.cmd.Process.Signal(sig)
}

func (c *AsyncCommand) Wait() <- chan error {
	ch := make(chan error)
	go func() {
		<-c.wait
		ch <- c.waitErr
	}()
	return ch
}

func (c *AsyncCommand) SignalAndWaitTimeout(sig syscall.Signal, to time.Duration) error {
	c.Signal(sig)
	select {
	case err := <-c.Wait():
		return err
	case <- time.After(to):
		return errors.New("did not exit after timeout")
	}
	panic("impl")
	return nil
}

func (c *AsyncCommand) Start() {
	if err := c.cmd.Start(); err != nil {
		c.waitErr = err
		c.exited = true
		close(c.wait)
		return
	}
	go func() {
		defer close(c.wait)
		err := c.cmd.Wait()
		log.Printf("process %s exited", c.cmd.Args)
		c.exited = true
		if ee, ok := err.(*exec.ExitError); ok {
			ws := ee.Sys().(syscall.WaitStatus)
			if ws.Signaled() && ws.Signal() == c.sig {
				c.waitErr = nil
			} else {
				c.waitErr = fmt.Errorf("%s\n%s", err, c.buf.String())
			}
		} else {
			c.waitErr = err
		}
	}()
}

