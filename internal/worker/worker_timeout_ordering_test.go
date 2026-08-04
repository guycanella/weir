// Regression test for the shutdown-timeout ordering bug found in review of
// WR-021: Run must check shutdownTimeoutErr right after Process returns,
// BEFORE branching on whether Process itself errored. Otherwise a processor
// that observes workCtx cancellation but still returns nil (a "successful"
// finish that merely happens to land after the grace period expired) falls
// through to DeleteMessage and, for the last message in a batch, makes Run
// return nil instead of an error wrapping ErrShutdownTimeout.
//
// This file is deliberately isolated from worker_test.go per review
// instructions, reusing that file's fixtures/helpers since both live in
// package worker_test.
package worker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/worker"
)

// TestProcessReturnsNilAfterGraceExpiry: the second message's processor
// blocks until the grace timer fires (observed via workCtx.Done()), then
// returns nil — success — rather than propagating workCtx.Err(). Even so,
// Run must treat the batch as timed out: the message must NOT be deleted,
// the third message must never be started, and Run must return an error
// wrapping ErrShutdownTimeout.
func TestProcessReturnsNilAfterGraceExpiry(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 3)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed []string

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: shortGrace,
		Process: func(workCtx context.Context, msg awsclient.Message) error {
			processed = append(processed, msg.Body)

			switch len(processed) {
			case 1:
				// First message completes normally, then shutdown is
				// requested — this starts the single grace timer.
				cancel()
				return nil
			case 2:
				// Block until the grace period expires, then report
				// success (nil) instead of propagating the cancellation —
				// this is the exact case the ordering bug missed.
				<-workCtx.Done()
				return nil
			default:
				return nil
			}
		},
	})

	err := runWorker(t, w, ctx, cancel)

	if err == nil {
		t.Fatal("Run returned nil, want an error wrapping ErrShutdownTimeout — a nil return from Process must not bypass the shutdown-timeout check")
	}
	if !errors.Is(err, worker.ErrShutdownTimeout) {
		t.Fatalf("Run returned %v, want an error wrapping worker.ErrShutdownTimeout", err)
	}
	if !mentionsCount(err.Error(), 2) {
		t.Errorf("shutdown-timeout error %q does not report the remaining message count (2 of the 3-message batch were left unprocessed)", err.Error())
	}

	if want := []string{"m1", "m2"}; !equalStrings(processed, want) {
		t.Errorf("processed %v, want %v — no message may be started after the grace period expires", processed, want)
	}
	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1"}) {
		t.Errorf("deleted %v, want [m1] — the message that returned nil only after grace expiry must NOT be deleted", got)
	}
	if got := rec.receiveCount(); got != 1 {
		t.Errorf("ReceiveMessage called %d time(s), want exactly 1", got)
	}
}
