package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/guycanella/weir/internal/awsclient"
)

const (
	defaultMaxMessages   int32 = 10
	defaultWaitTime      int32 = 20
	defaultShutdownGrace       = 30 * time.Second
)

var ErrShutdownTimeout = errors.New("worker: shutdown grace period expired")

type ProcessFunc func(context.Context, awsclient.Message) error

type Worker struct {
	SQSClient     awsclient.SQSClient
	QueueURL      string
	Process       ProcessFunc
	ShutdownGrace time.Duration
}

func New(w Worker) Worker {
	if w.ShutdownGrace == 0 {
		w.ShutdownGrace = defaultShutdownGrace
	}
	return w
}

// Validate reports an error when a required dependency or configuration
// field is missing, so Run can fail cleanly before starting the receive
// loop instead of panicking or nil-dereferencing on first use.
func (w Worker) Validate() error {
	if w.SQSClient == nil {
		return fmt.Errorf("worker: SQSClient is required")
	}
	if w.QueueURL == "" {
		return fmt.Errorf("worker: QueueURL is required")
	}
	if w.Process == nil {
		return fmt.Errorf("worker: Process is required")
	}
	return nil
}

func (w Worker) Run(recvCtx context.Context) error {
	if err := w.Validate(); err != nil {
		return err
	}

	workCtx, cancelWork := context.WithCancelCause(context.Background())
	defer cancelWork(nil)

	graceACK := make(chan struct{})
	var watchdogWG sync.WaitGroup
	watchdogWG.Add(1)
	defer func() {
		close(graceACK)
		watchdogWG.Wait()
	}()

	go func() {
		defer watchdogWG.Done()
		select {
		case <-recvCtx.Done():
			timer := time.NewTimer(w.ShutdownGrace)
			defer timer.Stop()
			select {
			case <-timer.C:
				cancelWork(ErrShutdownTimeout)
			case <-graceACK:
			}
		case <-graceACK:
		}
	}()

	for {
		if recvCtx.Err() != nil {
			return nil
		}

		out, err := w.SQSClient.ReceiveMessage(recvCtx, awsclient.ReceiveMessageInput{
			QueueUrl:            w.QueueURL,
			MaxNumberOfMessages: defaultMaxMessages,
			WaitTimeSeconds:     defaultWaitTime,
		})
		if err != nil {
			if recvCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive messages: %w", err)
		}

		for i, msg := range out.Messages {
			if err := shutdownTimeoutErr(workCtx, out.Messages, i); err != nil {
				return err
			}

			if err := w.Process(workCtx, msg); err != nil {
				if err := shutdownTimeoutErr(workCtx, out.Messages, i); err != nil {
					return err
				}
				continue
			}

			if _, err := w.SQSClient.DeleteMessage(workCtx, awsclient.DeleteMessageInput{
				QueueUrl:      w.QueueURL,
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				if err := shutdownTimeoutErr(workCtx, out.Messages, i); err != nil {
					return err
				}
				continue
			}
		}

		if recvCtx.Err() != nil {
			return nil
		}
	}
}

// shutdownTimeoutErr reports a non-nil error wrapping ErrShutdownTimeout when
// workCtx was canceled by the shutdown-grace watchdog, naming how many
// messages at or after index i in batch were left unprocessed. It reports
// nil when workCtx is not in that state, so callers can use it both as a
// pre-iteration guard and as a post-operation check.
func shutdownTimeoutErr(workCtx context.Context, batch []awsclient.Message, i int) error {
	if !errors.Is(context.Cause(workCtx), ErrShutdownTimeout) {
		return nil
	}
	remaining := len(batch) - i
	return fmt.Errorf("%w: %d of %d messages left unprocessed", ErrShutdownTimeout, remaining, len(batch))
}
