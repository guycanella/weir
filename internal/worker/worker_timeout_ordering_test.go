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
	"time"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/worker"
)

// blockingDeleteSQS wraps an SQSClient so that DeleteMessage blocks on
// workCtx.Done() before delegating to the underlying client. It models a
// delete that is genuinely in flight when the shutdown-grace timer expires:
// the delegate still reports success (nil), which is the exact case the
// ordering bug missed — shutdownTimeoutErr was only ever checked on the
// DeleteMessage *error* path, never unconditionally after a successful
// delete.
type blockingDeleteSQS struct {
	awsclient.SQSClient
}

func (b *blockingDeleteSQS) DeleteMessage(
	ctx context.Context,
	in awsclient.DeleteMessageInput,
) (awsclient.DeleteMessageOutput, error) {
	<-ctx.Done()
	return b.SQSClient.DeleteMessage(ctx, in)
}

// TestProcessReturnsNilAfterGraceExpiry: the second message's processor
// blocks until the grace timer fires (observed via workCtx.Done()), then
// returns nil — success — rather than propagating workCtx.Err(). Even so,
// Run must treat the run as timed out: the message must NOT be deleted, no
// further message may be started, and Run must return an error wrapping
// ErrShutdownTimeout.
//
// Concurrency stays at the default 1, so under WR-022's capacity-capped
// receive each ReceiveMessage asks for exactly one message: m1 and m2 arrive
// from two separate calls and m3 is never checked out at all. Shutdown is
// requested from the test goroutine once m2 is provably holding the only
// slot — see TestShutdownGraceExpiresMidBatch's comment for why cancelling
// from inside a processor would make the counts scheduling-dependent now that
// free capacity gates the receive call.
func TestProcessReturnsNilAfterGraceExpiry(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 3)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		processed []string
		blocked   = make(chan struct{})
	)

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: shortGrace,
		Process: func(workCtx context.Context, msg awsclient.Message) error {
			processed = append(processed, msg.Body)

			switch len(processed) {
			case 1:
				// First message completes normally and is deleted, before
				// shutdown is requested.
				return nil
			case 2:
				// Block until the grace period expires, then report
				// success (nil) instead of propagating the cancellation —
				// this is the exact case the ordering bug missed.
				close(blocked)
				<-workCtx.Done()
				return nil
			default:
				return nil
			}
		},
	})

	done := startWorker(w, ctx)

	select {
	case <-blocked:
	case <-time.After(runTimeout):
		t.Error("the second message was never started, so the late-nil case was never exercised")
		abandonRun(done, cancel)
		return
	}
	cancel() // the single grace timer starts here and can only expire

	err := awaitRun(t, done, cancel)

	if err == nil {
		t.Fatal("Run returned nil, want an error wrapping ErrShutdownTimeout — a nil return from Process must not bypass the shutdown-timeout check")
	}
	if !errors.Is(err, worker.ErrShutdownTimeout) {
		t.Fatalf("Run returned %v, want an error wrapping worker.ErrShutdownTimeout", err)
	}
	// Two messages checked out, only m1 deleted: the late nil must leave its
	// message counted as unprocessed.
	if !mentionsCounts(err.Error(), 1, 2) {
		t.Errorf("shutdown-timeout error %q does not report 1 of the 2 received messages left unprocessed — a nil that arrived after the budget was gone is not a completion", err.Error())
	}

	if want := []string{"m1", "m2"}; !equalStrings(processed, want) {
		t.Errorf("processed %v, want %v — no message may be started after the grace period expires", processed, want)
	}
	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1"}) {
		t.Errorf("deleted %v, want [m1] — the message that returned nil only after grace expiry must NOT be deleted", got)
	}
	if got := len(f.Received[queueURL]); got != 2 {
		t.Errorf("%d message(s) were checked out of the queue, want exactly 2 — the third must never be claimed once the budget is gone", got)
	}
	if got := rec.receiveCount(); got != 2 {
		t.Errorf("ReceiveMessage called %d time(s), want exactly 2 — one free slot means one message per call, and no call may follow grace expiry", got)
	}
}

// TestDeleteReturnsNilAfterGraceExpiry is the delete-side counterpart to
// TestProcessReturnsNilAfterGraceExpiry: DeleteMessage for the first message
// blocks until the grace timer fires, then reports success (nil) — a delete
// that completes right as/after the deadline — rather than an error. Even
// so, Run must treat the run as timed out: the second message must never be
// started, and Run must return an error wrapping ErrShutdownTimeout instead
// of falling through to the next iteration.
//
// The delete that lands late still counts as a delete — the message really is
// gone from the queue, so calling it unprocessed would over-report
// redeliveries — which is why the reported figure here is "0 of 1" rather
// than "1 of 1". The load-bearing claim is that a successful delete does not
// suppress the timeout error: everything received happened to be deleted, and
// Run must STILL say the budget expired.
//
// Under WR-022's capacity-capped receive that single message is the only one
// ever checked out: Concurrency is the default 1, and the one token stays held
// by the processor blocked inside DeleteMessage until the watchdog fires, at
// which point the receive loop's acquire fails for good. m2 and m3 are
// therefore never claimed, which is why "1 of 3 messages received" became "1
// of 1" without weakening what the test proves.
func TestDeleteReturnsNilAfterGraceExpiry(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 3)

	rec := &recordingSQS{SQSClient: &blockingDeleteSQS{SQSClient: f}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed []string

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: shortGrace,
		Process: func(_ context.Context, msg awsclient.Message) error {
			processed = append(processed, msg.Body)

			if len(processed) == 1 {
				// Completing normally starts the single grace timer.
				// DeleteMessage for this same message will then block on
				// blockingDeleteSQS until that timer fires.
				cancel()
			}
			return nil
		},
	})

	err := runWorker(t, w, ctx, cancel)

	if err == nil {
		t.Fatal("Run returned nil, want an error wrapping ErrShutdownTimeout — a nil return from DeleteMessage must not bypass the shutdown-timeout check")
	}
	if !errors.Is(err, worker.ErrShutdownTimeout) {
		t.Fatalf("Run returned %v, want an error wrapping worker.ErrShutdownTimeout", err)
	}
	if !mentionsCounts(err.Error(), 0, 1) {
		t.Errorf("shutdown-timeout error %q does not report 0 of the 1 received message left unprocessed — the late delete did succeed, so nothing is awaiting redelivery, and the timeout must be reported anyway", err.Error())
	}

	if want := []string{"m1"}; !equalStrings(processed, want) {
		t.Errorf("processed %v, want %v — no message may be started after the grace period expires", processed, want)
	}
	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1"}) {
		t.Errorf("deleted %v, want [m1] — the delete that only completed after grace expiry still succeeded and must be recorded", got)
	}
	if got := len(f.Received[queueURL]); got != 1 {
		t.Errorf("%d message(s) were checked out of the queue, want exactly 1 — the slot is held by the blocked delete until the budget is gone, so nothing else may be claimed", got)
	}
	if got := rec.receiveCount(); got != 1 {
		t.Errorf("ReceiveMessage called %d time(s), want exactly 1", got)
	}
}
