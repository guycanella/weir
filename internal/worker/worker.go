package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guycanella/weir/internal/awsclient"
)

const (
	defaultMaxMessages   int32 = 10
	defaultWaitTime      int32 = 20
	defaultShutdownGrace       = 30 * time.Second
	defaultConcurrency         = 1
)

var ErrShutdownTimeout = errors.New("worker: shutdown grace period expired")

// ProcessFunc handles a single received message. Its contract, in the
// presence of a shutdown-grace timeout, is:
//
//  1. A job is admitted only while shutdown has not been observed.
//     "Admitted" means the dispatch loop in Run decided to hand the message
//     off — i.e. it acquired a concurrency slot for it before cancellation
//     fired.
//  2. Once the shutdown-grace timeout cancels the run's work context, no
//     further jobs are admitted. This is deterministic, not merely
//     unlikely: every acquire() call made after cancellation is observed
//     returns false, unconditionally.
//  3. A job admitted immediately before cancellation may still have its
//     ProcessFunc invoked after cancellation fires. This is allowed, not a
//     bug: Process must itself behave cooperatively with the passed
//     context being canceled (e.g. by returning promptly once ctx.Done()
//     fires), and Run guarantees such a call's result is never treated as
//     complete — it will not be counted as processed and its message will
//     not be deleted from the queue, regardless of what Process returns.
type ProcessFunc func(context.Context, awsclient.Message) error

type Worker struct {
	SQSClient     awsclient.SQSClient
	QueueURL      string
	Process       ProcessFunc
	ShutdownGrace time.Duration
	// Concurrency bounds how many Process calls may be in flight at once,
	// across the whole Run invocation (not per received batch). A
	// non-positive value means sequential processing (1).
	Concurrency int
}

func New(w Worker) Worker {
	if w.ShutdownGrace == 0 {
		w.ShutdownGrace = defaultShutdownGrace
	}
	if w.Concurrency <= 0 {
		w.Concurrency = defaultConcurrency
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

func (w Worker) Run(recvCtx context.Context) (err error) {
	if err := w.Validate(); err != nil {
		return err
	}

	concurrency := w.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
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

	// tokens is a counting semaphore of size concurrency: it is the single
	// gate for both capacity to run a message AND capacity to fetch a new
	// batch. A token is acquired BEFORE the receive loop calls
	// ReceiveMessage — spent immediately on the batch's first message, or
	// released right away if the batch turns out empty or erroring — and
	// one more token is acquired per additional message in that same batch
	// before it is handed to its own goroutine. Requiring a free token
	// before ReceiveMessage is what closes the cross-batch over-receive
	// window: the loop cannot check out a new batch until some previously
	// dispatched message has actually FINISHED and released its token, not
	// merely been started (see TestNoReceiveWhileFullyDispatchedBatchStillProcessing).
	tokens := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		tokens <- struct{}{}
	}
	var wg sync.WaitGroup

	// acquire blocks until a slot is free, or reports false if the work
	// context is canceled first (grace expired while waiting for capacity).
	//
	// Admission is deterministic once cancellation has been observed: the
	// first select below checks workCtx.Done() alone, with a default
	// branch. A select with exactly one case plus default is never
	// randomized by the runtime — it takes the ready case over default
	// whenever that one case can proceed. So once workCtx is canceled,
	// every subsequent call to acquire() takes this branch and returns
	// false unconditionally, no matter whether a token happens to be free
	// at the same instant. Only a call that races the exact moment
	// cancellation fires can still fall through to the second, multi-way
	// select and go either way — that is the one boundary case rule 3 of
	// Worker.Process's contract (see its doc comment) explicitly allows.
	acquire := func() bool {
		select {
		case <-workCtx.Done():
			return false
		default:
		}
		select {
		case <-tokens:
			return true
		case <-workCtx.Done():
			return false
		}
	}
	release := func() {
		tokens <- struct{}{}
	}

	// received and deleted are run-wide (not per-batch): received grows by
	// the size of every non-empty ReceiveMessage response over the whole
	// Run, and deleted grows the moment a DeleteMessage succeeds, no matter
	// which receive batch either came from. A message counted in received
	// already has its SQS visibility timeout running, so it will be
	// redelivered unless deleted — that is true of the whole batch SQS
	// handed back, not only of the messages that got around to starting.
	// Tracking both run-wide is what lets the final "left unprocessed"
	// count stay correct when grace expires while an earlier batch's slow
	// message is still in flight alongside a later, fully-completed one
	// (see TestSlowMessageHoldsSlotAcrossBatches) — a per-batch counter
	// would only see whichever batch happened to be current.
	var received, deleted atomic.Int64

	var runErr error

	// Registered before the wg-join defer below so it runs AFTER it: defers
	// unwind LIFO, and the wg-join defer (registered later, right after the
	// semaphore is set up) therefore fires first on return. That guarantees
	// every dispatched Process/DeleteMessage call has finished before this
	// reads received/deleted, so the reported count is the true run-wide
	// total, not a snapshot taken while goroutines were still in flight.
	defer func() {
		if runErr != nil {
			err = runErr
			return
		}
		err = shutdownTimeoutErr(workCtx, deleted.Load(), received.Load())
	}()
	// Registered last, so it is the first defer to run on any return from
	// here on — structurally guaranteeing every in-flight goroutine is
	// drained before work-context cancellation, the watchdog join, or the
	// result above ever observe its outcome, even if a future change adds
	// an early return between here and the end of the function.
	defer wg.Wait()

	for recvCtx.Err() == nil {
		if !acquire() {
			// Grace expired while waiting for a free slot: no capacity
			// will ever come back for this run.
			break
		}

		if recvCtx.Err() != nil {
			// The recv context was canceled while we were acquiring (or
			// right after): don't spend the token we just reserved on a
			// ReceiveMessage call that should never go out.
			release()
			break
		}

		// One token is already held (the acquire() above). Opportunistically
		// reserve as many ADDITIONAL tokens as are immediately available —
		// never blocking, since blocking here would defeat the point of
		// already holding one free slot — up to SQS's hard maximum. The
		// resulting count is the number of slots that are ACTUALLY free at
		// this instant, so capping MaxNumberOfMessages to it is what closes
		// the request-level over-receive window: SQS can never hand back
		// more messages than the worker can immediately start.
		reserved := 1
	reserveLoop:
		for reserved < int(defaultMaxMessages) {
			select {
			case <-tokens:
				reserved++
			default:
				break reserveLoop
			}
		}

		out, recvErr := w.SQSClient.ReceiveMessage(recvCtx, awsclient.ReceiveMessageInput{
			QueueUrl:            w.QueueURL,
			MaxNumberOfMessages: int32(reserved),
			WaitTimeSeconds:     defaultWaitTime,
		})
		if recvErr != nil {
			for i := 0; i < reserved; i++ {
				release()
			}
			if recvCtx.Err() == nil {
				runErr = fmt.Errorf("receive messages: %w", recvErr)
			}
			break
		}

		if len(out.Messages) == 0 {
			for i := 0; i < reserved; i++ {
				release()
			}
			if recvCtx.Err() != nil {
				break
			}
			continue
		}

		received.Add(int64(len(out.Messages)))

		// Release any tokens reserved but not used — SQS may return fewer
		// messages than the cap we requested.
		for i := len(out.Messages); i < reserved; i++ {
			release()
		}

		for _, msg := range out.Messages {
			// Every message here already has a reserved token:
			// MaxNumberOfMessages was capped to `reserved`, so SQS cannot
			// return more messages than that — no per-message acquire()
			// needed inside this loop.
			wg.Add(1)
			go func(msg awsclient.Message) {
				defer wg.Done()
				defer release()

				// This job was admitted (its acquire() call above returned
				// true) before cancellation was observed there, so Process
				// runs unconditionally here. It may still receive an
				// already-canceled workCtx if cancellation fires in the
				// brief gap between admission and this line — that is
				// allowed by rule 3 of Worker.Process's contract (see its
				// doc comment): Process must itself behave cooperatively
				// with workCtx cancellation, and the post-Process check
				// below ensures such a call is never counted as complete.
				processErr := w.Process(workCtx, msg)

				// A processor that observes cancellation but still
				// reports success (nil) after the grace period has
				// already expired must not be treated as done: the
				// success merely arrived too late. Once past this point
				// (delete genuinely in flight when grace expires) a
				// subsequent successful delete still counts — see
				// TestDeleteReturnsNilAfterGraceExpiry.
				if errors.Is(context.Cause(workCtx), ErrShutdownTimeout) {
					return
				}
				if processErr != nil {
					return
				}
				if _, delErr := w.SQSClient.DeleteMessage(workCtx, awsclient.DeleteMessageInput{
					QueueUrl:      w.QueueURL,
					ReceiptHandle: msg.ReceiptHandle,
				}); delErr == nil {
					deleted.Add(1)
				}
			}(msg)
		}

		if recvCtx.Err() != nil {
			break
		}
	}

	return
}

// shutdownTimeoutErr reports a non-nil error wrapping ErrShutdownTimeout
// when workCtx was canceled by the shutdown-grace watchdog, naming how many
// messages received from SQS over the whole Run were left unprocessed.
// "Unprocessed" means "not successfully deleted" — the messages SQS will
// redeliver, since every received message's visibility timeout is already
// running whether or not the pool got around to starting it — evaluated
// once every dispatched goroutine, across every receive batch, has joined.
// It reports nil when workCtx is not in that state.
func shutdownTimeoutErr(workCtx context.Context, deleted, received int64) error {
	if !errors.Is(context.Cause(workCtx), ErrShutdownTimeout) {
		return nil
	}
	unprocessed := received - deleted
	return fmt.Errorf("%w: %d of %d messages left unprocessed", ErrShutdownTimeout, unprocessed, received)
}
