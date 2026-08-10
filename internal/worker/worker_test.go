// Package worker_test drives WR-021's consume/shutdown skeleton from
// OUTSIDE the package: every call goes through the exported surface
// cmd/worker uses (worker.New, Worker.Run, worker.ErrShutdownTimeout), so
// nothing passes only because a test could reach unexported state.
//
// These are the fast, deterministic proofs of the plan's state machine
// (ROADMAP_WR-021.md, "Unit tests"). None of them sleeps to win a race:
// every ordering constraint is expressed either as "the processing
// callback does X" (the callbacks run serially inside Run, so ordering is
// established by the loop itself) or as a channel handshake. The only
// timer any test depends on is Worker.ShutdownGrace, and the single test
// that depends on it blocks until it fires rather than racing it.
//
// Two things force test-only wrappers around fake.SQS:
//
//   - The fake returns an empty batch immediately instead of long-polling,
//     so a test that does not cancel recvCtx would spin forever. Every
//     test therefore cancels deterministically from a callback, and
//     runWorker() bounds the wait.
//   - The fake ignores ctx entirely, so it can neither report how many
//     times ReceiveMessage was called nor model a cancel arriving during
//     an in-flight receive. recordingSQS covers the former, blockingSQS
//     the latter.
package worker_test

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
	"github.com/guycanella/weir/internal/worker"
)

const (
	// longGrace is used by every test that must NOT observe a shutdown
	// timeout: it is long enough that a correct implementation always
	// finishes its batch first, and short enough that a buggy one still
	// ends the test run.
	longGrace = time.Minute

	// shortGrace is used only by TestShutdownGraceExpiresMidBatch, where
	// grace expiry is the behavior under test.
	shortGrace = 20 * time.Millisecond

	// runTimeout bounds Run. A correct implementation returns well inside
	// it; exceeding it means Run failed to observe cancellation (or is
	// busy-looping on the fake's empty batches), which is a failure, not a
	// hang.
	runTimeout = 10 * time.Second

	// wantMaxMessages is SQS's hard ceiling on MaxNumberOfMessages, and so
	// the largest value the worker may ever request. It is NOT the value it
	// always requests: since WR-022 the request is capped to the number of
	// concurrency slots that are actually free (see
	// TestReceiveRequestsOnlyFreeSlots), so a worker at the default
	// Concurrency of 1 asks for exactly one message. wantWaitTime is the
	// long-poll duration, which concurrency does not touch.
	wantMaxMessages int32 = 10
	wantWaitTime    int32 = 20
)

// ── fixtures ────────────────────────────────────────────────────────────

// newFakeQueue returns a fake SQS with one created queue, plus its URL.
// The fake rejects operations on an unknown queue URL, so the queue must
// exist before the worker touches it.
func newFakeQueue(t *testing.T) (*fake.SQS, string) {
	t.Helper()

	f := fake.NewSQS()
	out, err := f.CreateQueue(context.Background(), awsclient.CreateQueueInput{Name: "weir-worker-test"})
	if err != nil {
		t.Fatalf("fixture CreateQueue: %v", err)
	}
	return f, out.QueueUrl
}

// seed sends n messages with predictable bodies ("m1".."mN") so a test can
// identify a message by body regardless of the fake's ID/handle scheme.
func seed(t *testing.T, f *fake.SQS, queueURL string, n int) {
	t.Helper()

	for i := 1; i <= n; i++ {
		if _, err := f.SendMessage(context.Background(), awsclient.SendMessageInput{
			QueueUrl: queueURL,
			Body:     "m" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("fixture SendMessage(%d): %v", i, err)
		}
	}
}

// recordingSQS counts ReceiveMessage calls and keeps the inputs it was
// called with, which fake.SQS deliberately does not expose (an empty
// receive leaves no trace in fake.Received). "No second receive after
// cancellation" is a central WR-021 claim, so it needs an observer.
type recordingSQS struct {
	awsclient.SQSClient

	mu       sync.Mutex
	receives []awsclient.ReceiveMessageInput
}

func (r *recordingSQS) ReceiveMessage(
	ctx context.Context,
	in awsclient.ReceiveMessageInput,
) (awsclient.ReceiveMessageOutput, error) {
	r.mu.Lock()
	r.receives = append(r.receives, in)
	r.mu.Unlock()

	return r.SQSClient.ReceiveMessage(ctx, in)
}

func (r *recordingSQS) receiveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.receives)
}

func (r *recordingSQS) receiveInputs() []awsclient.ReceiveMessageInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]awsclient.ReceiveMessageInput(nil), r.receives...)
}

// blockingSQS models a receive that is genuinely in flight when
// cancellation arrives: it announces that it started, blocks until the
// context is canceled, and reports ctx.Err(). fake.SQS ignores ctx, so it
// cannot express this; injecting a synthetic context error instead would
// only test error classification, not propagation of recvCtx into the call.
type blockingSQS struct {
	awsclient.SQSClient

	started chan struct{}
}

func (b *blockingSQS) ReceiveMessage(
	ctx context.Context,
	_ awsclient.ReceiveMessageInput,
) (awsclient.ReceiveMessageOutput, error) {
	close(b.started)
	<-ctx.Done()
	return awsclient.ReceiveMessageOutput{}, ctx.Err()
}

// runWorker runs w.Run(ctx) on its own goroutine and returns its error,
// failing the test if Run does not return within runTimeout. On timeout it
// cancels ctx first, so a worker stuck in a long poll (or spinning on the
// fake's empty batches) is given the chance to unwind instead of being
// leaked spinning for the rest of the package's run.
func runWorker(t *testing.T, w worker.Worker, ctx context.Context, cancel context.CancelFunc) error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case err := <-done:
		return err
	case <-time.After(runTimeout):
		cancel()
		select {
		case <-done:
		case <-time.After(runTimeout):
		}
		t.Fatal("Run did not return within the test timeout — it never observed receive-context cancellation")
		return nil
	}
}

// mentionsCount reports whether msg contains want as a standalone number.
// The shutdown-timeout error must report how many messages were left
// unprocessed (ROADMAP_WR-021.md: "len(out.Messages) - currentIndex"), and
// this is the loosest assertion that still pins that number without
// pinning the wording.
func mentionsCount(msg string, want int) bool {
	return regexp.MustCompile(`(^|[^0-9])` + strconv.Itoa(want) + `([^0-9]|$)`).MatchString(msg)
}

// mentionsCounts is mentionsCount for the "<unprocessed> of <received>" pair
// the shutdown-timeout error reports. Asserting both numbers separately is
// far too weak once they are small: "1 of 2" satisfies mentionsCount(1) and
// mentionsCount(2) — and so does "2 of 1". This pins the two numbers AND
// their order without pinning the wording between them.
func mentionsCounts(msg string, first, second int) bool {
	return regexp.MustCompile(
		`(^|[^0-9])` + strconv.Itoa(first) + `\D+` + strconv.Itoa(second) + `([^0-9]|$)`,
	).MatchString(msg)
}

// ── tests ───────────────────────────────────────────────────────────────

// TestRunConsumesAllMessages is the happy path: every message in the batch
// is processed and then deleted, and a receive-context cancellation is a
// clean (nil) shutdown rather than a worker failure.
//
// Cancelling from the last callback is what makes this terminate: the fake
// answers the next receive with an empty batch instead of long-polling.
func TestRunConsumesAllMessages(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed []string
	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, msg awsclient.Message) error {
			processed = append(processed, msg.Body)
			if len(processed) == 3 {
				cancel()
			}
			return nil
		},
	})

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil (cancellation is a clean shutdown)", err)
	}

	if want := []string{"m1", "m2", "m3"}; !equalStrings(processed, want) {
		t.Errorf("processed %v, want %v (serially, in receive order)", processed, want)
	}
	if got := len(f.Deleted[queueURL]); got != 3 {
		t.Errorf("deleted %d message(s), want 3: %v", got, f.Deleted[queueURL])
	}
	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1", "m2", "m3"}) {
		t.Errorf("deleted bodies %v, want [m1 m2 m3]", got)
	}
}

// TestReceiveUsesLongPollSettings pins the receive configuration: a
// 20-second long poll, the configured queue URL, and — deliberately — no
// VisibilityTimeout override, which belongs to WR-024. Without this, the
// worker could silently degrade to short polling (burning API calls and
// defeating scale-to-zero economics) with every other test still green.
//
// The batch size is no longer a fixed 10: WR-022's review established that
// the worker must never ask for messages it has no free slot to start, so
// the request is capped to the free-slot count. This worker leaves
// Concurrency unset, i.e. the default of 1, so it must ask for exactly one
// message. The cap is exercised across concurrency levels in
// TestReceiveRequestsOnlyFreeSlots; what this test pins is that capping the
// batch size did not disturb the long-poll settings around it.
func TestReceiveUsesLongPollSettings(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, _ awsclient.Message) error {
			cancel()
			return nil
		},
	})

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	inputs := rec.receiveInputs()
	if len(inputs) == 0 {
		t.Fatal("ReceiveMessage was never called")
	}
	got := inputs[0]
	if got.QueueUrl != queueURL {
		t.Errorf("ReceiveMessage QueueUrl = %q, want %q", got.QueueUrl, queueURL)
	}
	// One free slot (the default Concurrency), therefore one message.
	if got.MaxNumberOfMessages != 1 {
		t.Errorf("ReceiveMessage MaxNumberOfMessages = %d, want 1 — at the default Concurrency of 1 exactly one slot is free, and a bigger request would check out messages the worker cannot start", got.MaxNumberOfMessages)
	}
	if got.WaitTimeSeconds != wantWaitTime {
		t.Errorf("ReceiveMessage WaitTimeSeconds = %d, want %d (long polling)", got.WaitTimeSeconds, wantWaitTime)
	}
	if got.VisibilityTimeout != 0 {
		t.Errorf("ReceiveMessage VisibilityTimeout = %d, want 0 — WR-021 must not override the queue's configured value (that is WR-024)", got.VisibilityTimeout)
	}
}

// TestCancelBeforeReceive: a worker handed an already-canceled receive
// context must not open a long poll at all. Calling ReceiveMessage with a
// dead context would, against real SQS, be a wasted round trip on every
// restart-during-shutdown; the state machine's first step is an explicit
// pre-receive cancellation check.
func TestCancelBeforeReceive(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var processed int
	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, _ awsclient.Message) error {
			processed++
			return nil
		},
	})

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil for an already-canceled receive context", err)
	}

	if got := rec.receiveCount(); got != 0 {
		t.Errorf("ReceiveMessage called %d time(s) with an already-canceled receive context, want 0", got)
	}
	if processed != 0 {
		t.Errorf("Process called %d time(s), want 0", processed)
	}
	if got := len(f.Deleted[queueURL]); got != 0 {
		t.Errorf("deleted %d message(s), want 0", got)
	}
}

// TestCancelDuringReceive: cancelling while a long poll is genuinely in
// flight must interrupt it and be reported as a clean shutdown, not as a
// receive failure. This is the SIGTERM-on-an-idle-worker case, which is
// the common one for a scale-to-zero deployment.
func TestCancelDuringReceive(t *testing.T) {
	f, queueURL := newFakeQueue(t)

	blocking := &blockingSQS{SQSClient: f, started: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed int
	w := worker.New(worker.Worker{
		SQSClient:     blocking,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, _ awsclient.Message) error {
			processed++
			return nil
		},
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Handshake rather than a sleep: cancel strictly after the receive is
	// in flight.
	select {
	case <-blocking.started:
	case <-time.After(runTimeout):
		t.Fatal("ReceiveMessage was never called")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil — a receive interrupted by shutdown is not a worker failure", err)
		}
	case <-time.After(runTimeout):
		t.Fatal("Run did not return after the in-flight receive was canceled")
	}

	if processed != 0 {
		t.Errorf("Process called %d time(s) although no message was received, want 0", processed)
	}
}

// TestCancelAfterReceiveDrainsCurrentBatch is the primary proof of the
// two-context model: SIGTERM stops NEW receives without abandoning the
// batch SQS already handed over. Abandoning it would be visible in
// production as duplicate work after the visibility timeout elapsed.
//
// Concurrency is set to 5 — matching the seeded message count — so that the
// whole batch still arrives from a SINGLE ReceiveMessage under WR-022's
// capacity-capped request (the request is bounded by the free-slot count, so
// at Concurrency=1 the same five messages would take five receive calls and
// the "one batch handed over, then cancellation" framing this test exists to
// pin would dissolve into five unrelated one-message batches).
//
// Above Concurrency=1 the five processors run in parallel, so the ORDER in
// which bodies are seen is scheduling, not behavior: the drain claim is
// therefore asserted as a multiset. What stays exact is the count — all five
// messages of the handed-over batch complete and are deleted — and the claim
// that cancellation stopped further work from being CHECKED OUT, which is
// asserted on the messages SQS actually handed back rather than on the
// number of calls: an extra receive may still be issued in the instant
// before cancellation becomes visible to the loop, but with the queue now
// empty it can hand back nothing, and against real SQS a receive on a
// canceled context fails without checking anything out.
func TestCancelAfterReceiveDrainsCurrentBatch(t *testing.T) {
	const batch = 5

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, batch)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		processed bodyLog // several goroutines: a plain slice would race
		entered   atomic.Int64
	)
	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   batch,
		Process: func(workCtx context.Context, msg awsclient.Message) error {
			processed.add(msg.Body)
			if entered.Add(1) == 1 {
				// Shutdown requested with four messages still to go.
				cancel()
			}
			// The work context must stay alive for the rest of the batch.
			if err := workCtx.Err(); err != nil {
				t.Errorf("work context canceled (%v) while draining %q inside the grace period", err, msg.Body)
			}
			return nil
		},
	})

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got, want := processed.snapshot(), bodySet(batch); !sortedEqual(got, want) {
		t.Errorf("processed %v, want %v — the in-flight batch must be drained after cancellation", got, want)
	}
	if got := len(f.Deleted[queueURL]); got != batch {
		t.Errorf("deleted %d message(s), want %d: %v", got, batch, f.Deleted[queueURL])
	}
	if got := len(f.Received[queueURL]); got != batch {
		t.Errorf("%d message(s) were checked out of the queue over the whole run, want exactly the %d of the in-flight batch — cancellation must not let a further message be claimed", got, batch)
	}
	if got := rec.receiveCount(); got < 1 {
		t.Errorf("ReceiveMessage called %d time(s), want at least the one that handed over the batch", got)
	}
}

// TestShutdownGraceExpiresMidBatch: the drain budget is bounded and
// shared. When it expires, cooperative processing must observe
// cancellation whose Cause is ErrShutdownTimeout (NOT
// context.DeadlineExceeded — WithCancelCause reports context.Canceled and
// preserves the cause separately), no further message may be started, and
// Run must report a distinct timeout error carrying the number of messages
// left. Without the sentinel, a caller could not tell "we ran out of time"
// from "processing failed".
//
// Concurrency stays at the default 1, which keeps the message ordering this
// test reasons about deterministic. Under WR-022's capacity-capped receive
// that means one message per ReceiveMessage: m1 (which completes and is
// deleted) and m2 (the cooperative long-running one) therefore arrive from
// two separate calls, and m3 is never even checked out — which is the
// stronger form of "not started", since an unreceived message has no
// visibility timeout running.
//
// Shutdown is requested from the TEST goroutine, once m2 has announced that
// it holds the only slot, rather than from inside m1's processor as before.
// That is what makes the expected counts exact: with capacity now gating the
// receive call, a cancel issued from inside a processor also frees that
// processor's slot moments later, so the loop can legitimately check out one
// more message before it observes the cancellation — the counts would be
// scheduling-dependent. Cancelling while the single slot is held by work that
// only returns on cancellation leaves the loop parked in acquire with nothing
// to claim, so the grace period can only expire.
func TestShutdownGraceExpiresMidBatch(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 3)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		processed  []string
		observed   error // the cause the blocked processor saw
		blockedCtx context.Context
		blocked    = make(chan struct{})
	)

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: shortGrace,
		Process: func(workCtx context.Context, msg awsclient.Message) error {
			processed = append(processed, msg.Body)

			switch len(processed) {
			case 1:
				// First message completes normally and is deleted, well
				// before shutdown is requested.
				return nil
			case 2:
				// Cooperative long-running work: block until the grace
				// period cancels the work context. No sleep, no race —
				// this returns only once the timer has fired.
				blockedCtx = workCtx
				close(blocked)
				<-workCtx.Done()
				observed = context.Cause(workCtx)
				return workCtx.Err()
			default:
				return nil
			}
		},
	})

	done := startWorker(w, ctx)

	// Handshake, not a sleep: the only slot is provably occupied by work that
	// will not return until the watchdog fires.
	select {
	case <-blocked:
	case <-time.After(runTimeout):
		t.Error("the second message was never started, so grace expiry was never exercised")
		abandonRun(done, cancel)
		return
	}
	cancel() // shutdown requested; the single grace timer starts here

	err := awaitRun(t, done, cancel)

	if err == nil {
		t.Fatal("Run returned nil, want an error wrapping ErrShutdownTimeout")
	}
	if !errors.Is(err, worker.ErrShutdownTimeout) {
		t.Fatalf("Run returned %v, want an error wrapping worker.ErrShutdownTimeout", err)
	}
	// Two messages were checked out over the run; only m1 was deleted, so
	// exactly one is left for redelivery.
	if !mentionsCounts(err.Error(), 1, 2) {
		t.Errorf("shutdown-timeout error %q does not report 1 of the 2 received messages left unprocessed — the blocked message is undeleted and will be redelivered", err.Error())
	}

	if !errors.Is(observed, worker.ErrShutdownTimeout) {
		t.Errorf("processor observed context.Cause = %v, want worker.ErrShutdownTimeout", observed)
	}
	if blockedCtx != nil && !errors.Is(blockedCtx.Err(), context.Canceled) {
		t.Errorf("work context Err = %v, want context.Canceled (WithCancelCause never reports DeadlineExceeded)", blockedCtx.Err())
	}

	if want := []string{"m1", "m2"}; !equalStrings(processed, want) {
		t.Errorf("processed %v, want %v — no message may be started after the grace period expires", processed, want)
	}
	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1"}) {
		t.Errorf("deleted %v, want [m1] — only the message that finished before the deadline", got)
	}
	if got := len(f.Received[queueURL]); got != 2 {
		t.Errorf("%d message(s) were checked out of the queue, want exactly 2 — the third must never be claimed once the budget is gone", got)
	}
	if got := rec.receiveCount(); got != 2 {
		t.Errorf("ReceiveMessage called %d time(s), want exactly 2 — one free slot means one message per call, and no call may follow grace expiry", got)
	}
}

// TestReceiveError: a receive failure that is NOT shutdown must be wrapped
// and returned, not swallowed. WR-021 has no backoff policy, so the
// process exits and the platform restarts it, rather than hot-looping on a
// failing API.
func TestReceiveError(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 2)

	boom := errors.New("sqs unavailable")
	f.InjectError(fake.SQSMethodReceiveMessage, boom, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed int
	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, _ awsclient.Message) error {
			processed++
			return nil
		},
	})

	err := runWorker(t, w, ctx, cancel)

	if err == nil {
		t.Fatal("Run returned nil, want the receive error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want an error wrapping %v", err, boom)
	}
	if err.Error() == boom.Error() {
		t.Errorf("Run returned the receive error unwrapped (%q); it should add context about the failing operation", err.Error())
	}
	if errors.Is(err, worker.ErrShutdownTimeout) {
		t.Errorf("Run returned %v, which wrongly reports itself as a shutdown timeout", err)
	}
	if processed != 0 {
		t.Errorf("Process called %d time(s) after a failed receive, want 0", processed)
	}
	if got := len(f.Deleted[queueURL]); got != 0 {
		t.Errorf("deleted %d message(s) after a failed receive, want 0", got)
	}
}

// TestProcessErrorSkipsDelete: a failed message must be left undeleted so
// SQS redelivers it (and eventually redrives it to the DLQ), and it must
// not take the rest of the batch down with it.
func TestProcessErrorSkipsDelete(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 3)

	failed := errors.New("processing failed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed []string
	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, msg awsclient.Message) error {
			processed = append(processed, msg.Body)
			if len(processed) == 3 {
				cancel()
			}
			if msg.Body == "m2" {
				return failed
			}
			return nil
		},
	})

	err := runWorker(t, w, ctx, cancel)

	if err != nil {
		t.Fatalf("Run returned %v, want nil — a single processing failure is not a worker failure", err)
	}
	if want := []string{"m1", "m2", "m3"}; !equalStrings(processed, want) {
		t.Errorf("processed %v, want %v — the batch must continue past a failure", processed, want)
	}
	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1", "m3"}) {
		t.Errorf("deleted %v, want [m1 m3] — the failed message must stay undeleted for redelivery", got)
	}
}

// TestDeleteErrorLeavesMessageUndeleted: a delete failure after successful
// processing is also non-fatal. The message will be redelivered (at-least-
// once is the contract; WR-010's idempotency key is what makes that safe),
// and the rest of the batch still gets attempted.
func TestDeleteErrorLeavesMessageUndeleted(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 3)

	// One-shot: only the first DeleteMessage fails.
	deleteBoom := errors.New("delete rejected")
	f.InjectError(fake.SQSMethodDeleteMessage, deleteBoom, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed []string
	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, msg awsclient.Message) error {
			processed = append(processed, msg.Body)
			if len(processed) == 3 {
				cancel()
			}
			return nil
		},
	})

	err := runWorker(t, w, ctx, cancel)

	if err != nil {
		t.Fatalf("Run returned %v, want nil — a single delete failure is not a worker failure", err)
	}
	if want := []string{"m1", "m2", "m3"}; !equalStrings(processed, want) {
		t.Errorf("processed %v, want %v — the batch must continue past a delete failure", processed, want)
	}
	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m2", "m3"}) {
		t.Errorf("deleted %v, want [m2 m3] — the message whose delete failed must be absent from the deletion record", got)
	}
}

// ── assertion helpers ───────────────────────────────────────────────────

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// deletedBodies maps the receipt handles the fake recorded as deleted back
// to the message bodies they belonged to, so assertions read in terms of
// "m1, m3" rather than opaque handles. Order is preserved.
func deletedBodies(f *fake.SQS, queueURL string) []string {
	handleToBody := make(map[string]string, len(f.Received[queueURL]))
	for _, msg := range f.Received[queueURL] {
		handleToBody[msg.ReceiptHandle] = msg.Body
	}

	bodies := make([]string, 0, len(f.Deleted[queueURL]))
	for _, handle := range f.Deleted[queueURL] {
		body, ok := handleToBody[handle]
		if !ok {
			body = "<unknown handle " + handle + ">"
		}
		bodies = append(bodies, body)
	}
	return bodies
}
