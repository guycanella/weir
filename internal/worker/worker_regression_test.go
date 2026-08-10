// Package worker_test — WR-022 review-driven regression tests.
//
// Each test in this file pins ONE finding from the external review of
// WR-022's first implementation. They are kept apart from
// worker_concurrency_test.go's twelve tests deliberately: those describe the
// feature, these describe two specific correctness holes that a plausible
// implementation of the feature can still have. If either of these ever goes
// red again, the diagnosis is in the test name.
//
// Finding 1 (HIGH) — a batch may be fully DISPATCHED while no worker has
// finished, and the receive loop must not treat "every message handed to
// some worker" as "there is capacity for another batch". See
// TestNoReceiveWhileFullyDispatchedBatchStillProcessing.
//
// Finding 2 (MEDIUM) — the shutdown contract is about ADMISSION, not about
// Process invocation: once the grace-period expiry has been observed, the
// dispatch loop must admit no further message, and it must do so
// deterministically rather than "usually". See
// TestNoAdmissionAfterGraceExpiryObserved.
//
// The first attempt at finding 2 pinned a stronger claim — "Process is never
// invoked with an already-canceled work context" — and that claim was
// retired, not weakened by convenience: it is unachievable. Admission and
// invocation are separated by a goroutine scheduling gap that no observer
// can close, so any admitted message can have cancellation land inside that
// gap. Worker.Process's doc comment now states the achievable contract
// (rules 1-3), and this file pins the part of it that IS deterministic:
// nothing new is admitted once cancellation has been observed, and anything
// admitted at the boundary is never counted or deleted.
//
// Both reuse the fixtures and synchronization helpers already defined in
// worker_test.go and worker_concurrency_test.go (newFakeQueue, seed,
// recordingSQS, tracker, startWorker, awaitRun, abandonRun, holdsFor,
// mustAwait, deletedBodies, bodySet, bodyLog, mentionsCount, longGrace,
// runTimeout, stabilityWindow). Nothing here sleeps to win a race.
package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/worker"
)

// ── finding 1: no receive while dispatched work is still in flight ───────

// TestNoReceiveWhileFullyDispatchedBatchStillProcessing pins that the
// receive loop's gate is FREE CAPACITY, not "the current batch has been
// handed out".
//
// TestBurstNeverExceedsConcurrencyCap already covers the easier half of
// this: while a batch still has messages that no worker has accepted, the
// dispatch send itself blocks, so no further ReceiveMessage can be issued.
// That leaves the harder half untested — the batch is EXHAUSTED (every
// message in it was accepted by some worker) but no worker has FINISHED. A
// dispatcher that only blocks on the handoff sails past that point and
// checks out a brand-new batch that it cannot start.
//
// Why that matters against real SQS rather than against the fake: a
// received message's visibility timeout starts ticking the instant SQS hands
// it back, not when Process is called. Messages checked out into a worker
// that is still busy therefore burn their whole visibility budget waiting in
// memory, and SQS redelivers them to another consumer — duplicate
// processing, caused by the consumer itself, precisely under the saturation
// where it hurts most.
//
// The shape: Concurrency is 1 and the queue holds exactly one message, so
// the first ReceiveMessage returns a batch of one, and dispatching it
// EMPTIES the batch — the dispatch loop has nothing left to block on. The
// test then synchronizes on Process being ENTERED (not merely dispatched)
// and asserts that no second ReceiveMessage has been issued while the only
// worker is inside it.
func TestNoReceiveWhileFullyDispatchedBatchStillProcessing(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr tracker
	// inside is closed from Process's first statement, so waiting on it
	// proves the worker is genuinely executing the message — the distinction
	// this whole test rests on. A dispatched-but-not-started message would
	// leave it open.
	inside := make(chan struct{})
	release := make(chan struct{})

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace, // grace must not be what ends this test
		Concurrency:   1,
		Process: func(_ context.Context, _ awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			close(inside)
			<-release
			return nil
		},
	})

	done := startWorker(w, ctx)

	select {
	case <-inside:
	case <-time.After(runTimeout):
		t.Error("the only message was never started")
		close(release)
		abandonRun(done, cancel)
		return
	}

	// Negative claim, held under observation (see worker_concurrency_test.go's
	// note on one-sided flakiness): a correct worker can never issue a second
	// receive here no matter how long the window is, because its only slot is
	// occupied for as long as this test chooses. A worker that gates on
	// dispatch instead of on capacity has already issued it — and, against the
	// fake, will keep issuing empty receives in a tight loop.
	if !holdsFor(func() bool { return rec.receiveCount() == 1 }, stabilityWindow) {
		t.Errorf("ReceiveMessage called %d time(s) while the only worker was still inside Process, want exactly 1 — a fully dispatched batch is not free capacity: checking out more messages than the pool can start burns their SQS visibility timeout in memory and causes redelivery",
			rec.receiveCount())
		close(release)
		abandonRun(done, cancel)
		return
	}
	if got := tr.started.Load(); got != 1 {
		t.Errorf("%d Process call(s) started, want exactly 1 with a single slot", got)
	}

	// Capacity comes back; the run must then proceed normally.
	close(release)

	mustAwait(t, "the message to finish", func() bool { return tr.finished.Load() == 1 })
	cancel()

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if got, want := deletedBodies(f, queueURL), bodySet(1); !sortedEqual(got, want) {
		t.Errorf("deleted %v, want %v — gating the receive loop on capacity must not stop the message from completing", got, want)
	}
}

// ── finding 2: nothing may be ADMITTED after grace expiry is observed ────

// graceValues are the shutdown-grace budgets
// TestNoAdmissionAfterGraceExpiryObserved cycles through. The outcome must
// not depend on any of them — that is the point of cycling: 0 fires the
// watchdog's timer the instant the receive context is canceled (so
// cancellation lands as early as it possibly can), the larger values let the
// dispatch loop settle first. A contract that only holds for one of these
// budgets is a coincidence, not a contract.
var graceValues = []time.Duration{
	0,
	10 * time.Microsecond,
	50 * time.Microsecond,
	100 * time.Microsecond,
	250 * time.Microsecond,
	500 * time.Microsecond,
}

// noAdmissionAttempts is how many independent runs
// TestNoAdmissionAfterGraceExpiryObserved performs. The claim under test is
// DETERMINISTIC, not merely likely, so the high repeat count is the evidence
// for that word: the abandoned "Process is never invoked after expiry" test
// failed on its very first attempt, every time it ran. This one must record
// zero violations across every attempt, at every -cpu setting, for the claim
// to mean anything.
const noAdmissionAttempts = 60

// admissionWindow is how long "no further message has been admitted" is held
// under observation once expiry has been observed first-hand. It only needs
// to be long enough for the dispatch loop's goroutine to be scheduled at all
// — every other goroutine in the run is parked at that moment, and holdsFor
// yields between polls — and the assertion after the gate opens catches a
// dispatcher that had not exited yet even so.
const admissionWindow = 5 * time.Millisecond

// TestNoAdmissionAfterGraceExpiryObserved pins rule 2 of Worker.Process's
// contract: once the shutdown-grace timeout has cancelled the work context,
// the dispatch loop admits nothing further — deterministically.
//
// # WHY ADMISSION, AND NOT "PROCESS WAS INVOKED"
//
// This test replaces an earlier one that asserted Process is never INVOKED
// with an already-canceled work context. That is unachievable and the
// contract no longer claims it: between the dispatch loop admitting a
// message (acquiring its slot) and the pool goroutine reaching
// w.Process(...) there is a goroutine scheduling gap with no observable
// event inside it, so cancellation can always land there. Rule 3 therefore
// allows a boundary-admitted message to enter Process already canceled, and
// guarantees instead that its result is discarded (no delete, not counted).
//
// What IS deterministic is admission, and that is what is measured here.
//
// # HOW A NON-EVENT IS MEASURED
//
// "No message was admitted" cannot be observed directly — admission happens
// inside Run. The test makes Process ENTRY a faithful proxy for it by
// freezing the semaphore: all `concurrency` slots are taken by processors
// that are blocked on a gate the test controls and that no cancellation
// releases, so not one token can come back while the assertion is being
// made. Under that condition a Process entry is possible ONLY via a fresh
// admission, which makes the proxy exact rather than approximate — the
// boundary case rule 3 tolerates (a slot freed at the same instant
// cancellation fires) is structurally excluded, so any violation seen here
// is a real one.
//
// The synchronization point is first-hand: the sentinel processor (m1, always
// the batch's first message and therefore always admitted well before
// cancellation) blocks on the work context, and only once it has itself read
// context.Cause == ErrShutdownTimeout does it announce expiry to the test.
// From that announcement onward the watchdog has provably already fired, so
// there is no clock reasoning anywhere in the assertion.
//
// The six messages left in the QUEUE beyond the four that fill the slots are
// the bait. Since WR-022's review the receive request is capped to the
// free-slot count, so those six are never checked out in the first place —
// they can only be reached by a receive issued after expiry. A dispatch loop
// that waits on capacity without watching the work context (or that watches
// it only some of the time) takes the four tokens the moment the gate opens
// and goes back for them; a correct one has already broken out of the receive
// loop and never touches them, leaving them untouched in the queue, which is
// what the reported "4 of 4 left unprocessed" then confirms — four checked
// out, none deleted, and nothing beyond them ever claimed.
func TestNoAdmissionAfterGraceExpiryObserved(t *testing.T) {
	for attempt := 0; attempt < noAdmissionAttempts; attempt++ {
		grace := graceValues[attempt%len(graceValues)]
		if !admissionStopsAfterExpiry(t, attempt, grace) {
			// Stop at the first violation: sixty copies of the same
			// diagnosis is noise, and the interesting number (which attempt,
			// which grace budget) is already reported.
			return
		}
	}
}

// admissionStopsAfterExpiry performs one attempt of
// TestNoAdmissionAfterGraceExpiryObserved and reports whether it held.
func admissionStopsAfterExpiry(t *testing.T, attempt int, grace time.Duration) bool {
	t.Helper()

	const (
		concurrency = 4
		// 4 fill the slots and so are the only ones the capped receive can
		// claim; the other 6 stay in the queue as bait for a receive that
		// must never happen.
		queued = 10
	)

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, queued)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		tr         tracker
		wrongCause atomic.Int64
		lateEntry  bodyLog // messages that entered Process already canceled
		arrived    = make(chan string, queued)
		release    = make(chan struct{})
		expired    = make(chan struct{})
	)

	// Deliberately NOT worker.New: New would replace a sub-millisecond grace
	// with the 30s default. Run clamps Concurrency itself either way
	// (TestRunClampsNonPositiveConcurrency).
	w := worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: grace,
		Concurrency:   concurrency,
		Process: func(workCtx context.Context, msg awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			// Recorded, not failed on: with every token frozen this cannot
			// happen, but if it ever does it is the rule-3 boundary case and
			// the post-run assertions below hold it to rule 3's guarantee
			// (never deleted).
			if errors.Is(context.Cause(workCtx), worker.ErrShutdownTimeout) {
				lateEntry.add(msg.Body)
			}

			arrived <- msg.Body

			if msg.Body == "m1" {
				// The sentinel. It reports expiry only after observing the
				// cause itself, so "expired" means the watchdog has already
				// fired — not that some duration has elapsed.
				<-workCtx.Done()
				if !errors.Is(context.Cause(workCtx), worker.ErrShutdownTimeout) {
					wrongCause.Add(1)
				}
				close(expired)
			}

			// Every processor holds its slot until the test says otherwise,
			// including past cancellation. This is what freezes the semaphore.
			<-release

			// A late nil: reported success that arrived after the budget was
			// gone. Rule 3 requires Run to discard it.
			return nil
		},
	}

	done := startWorker(w, ctx)

	for i := 0; i < concurrency; i++ {
		select {
		case <-arrived:
		case <-time.After(runTimeout):
			t.Errorf("attempt %d (grace %v): only %d of %d slots were filled, so the semaphore was never frozen and the attempt proves nothing", attempt, grace, i, concurrency)
			close(release)
			abandonRun(done, cancel)
			return false
		}
	}

	// Every slot is now held by a blocked processor, so the dispatch loop is
	// parked in (or about to call) acquire before it may request anything
	// more — the queue still holds six messages it must never claim.
	// Cancelling the receive context starts the grace timer; it can only
	// expire, because nothing will complete until this test allows it.
	cancel()

	select {
	case <-expired:
	case <-time.After(runTimeout):
		t.Errorf("attempt %d (grace %v): the work context was never canceled with ErrShutdownTimeout, so grace expiry was not exercised", attempt, grace)
		close(release)
		abandonRun(done, cancel)
		return false
	}

	// The claim, held under observation from a point ordered strictly after
	// expiry: no fifth message is admitted. A dispatcher parked on capacity
	// must wake on the work context instead and abandon the rest of the queue.
	if !holdsFor(func() bool { return tr.started.Load() == concurrency }, admissionWindow) {
		t.Errorf("attempt %d (grace %v): %d Process call(s) started, want exactly %d — a message was ADMITTED after the grace expiry had already been observed (in-flight: %v); no slot can have come free, so the dispatch loop is not treating work-context cancellation as final",
			attempt, grace, tr.started.Load(), concurrency, lateEntry.snapshot())
		close(release)
		abandonRun(done, cancel)
		return false
	}

	// The gate opens: four tokens come back at once. A dispatch loop still
	// waiting on capacity would take them and start the six bait messages —
	// long after cancellation, which is the failure this catches.
	close(release)

	err := awaitRun(t, done, cancel)

	ok := true
	fail := func(format string, args ...any) {
		t.Errorf("attempt %d (grace %v): "+format, append([]any{attempt, grace}, args...)...)
		ok = false
	}

	if !errors.Is(err, worker.ErrShutdownTimeout) {
		fail("Run returned %v, want an error wrapping worker.ErrShutdownTimeout", err)
	} else if !mentionsCounts(err.Error(), concurrency, concurrency) {
		fail("shutdown-timeout error %q does not report %d of %d — the capped receive claims exactly the %d messages that fit the free slots, and nothing was deleted, so every one of them is left for redelivery", err.Error(), concurrency, concurrency, concurrency)
	}
	if got := tr.started.Load(); got != concurrency {
		fail("%d Process call(s) started over the whole run, want exactly %d — the messages still queued at expiry must never be admitted, not even once capacity returns", got, concurrency)
	}
	if got := tr.finished.Load(); got != tr.started.Load() {
		fail("%d of %d started processors had finished when Run returned, want all of them — Run must join every goroutine it admitted, including one admitted at the cancellation boundary", got, tr.started.Load())
	}
	if got := tr.cur.Load(); got != 0 {
		fail("%d Process call(s) still in flight after Run returned, want 0", got)
	}
	if wrongCause.Load() != 0 {
		fail("the sentinel saw a context.Cause other than worker.ErrShutdownTimeout — the watchdog must cancel with that exact cause, or the whole expiry contract is untestable")
	}
	if got := deletedBodies(f, queueURL); len(got) != 0 {
		fail("deleted %v, want none — every processor returned nil only after the budget was gone, so all messages stay for redelivery (rule 3: a late success is not a success)", got)
	}

	return ok
}
