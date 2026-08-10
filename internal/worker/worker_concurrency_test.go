// Package worker_test — WR-022 (bounded concurrency).
//
// These tests drive the concurrency cap from OUTSIDE the package, through
// the same exported surface cmd/worker uses (worker.New, Worker.Run,
// worker.ErrShutdownTimeout, the new Worker.Concurrency field). They reuse
// worker_test.go's fixtures and helpers (newFakeQueue, seed, recordingSQS,
// runWorker, deletedBodies, mentionsCount, longGrace, shortGrace,
// runTimeout) rather than duplicating them.
//
// # WHAT CHANGES FOR THE EXISTING WR-021 TESTS
//
// Nothing, deliberately: worker.New must default Concurrency to 1 when it
// is <= 0, so every WR-021 test keeps running fully sequentially and keeps
// its ordering assertions (processed == [m1 m2 m3], deleted in the same
// order, "N of M messages left unprocessed" counted by batch index). Those
// files are the Concurrency=1 regression net; this file never re-pins what
// they already pin.
//
// WHAT NO LONGER HOLDS ABOVE Concurrency=1
//
//   - Per-message ORDER. With more than one slot, neither the order of
//     Process calls nor the order of DeleteMessage calls is defined, so
//     assertions here compare SETS of bodies (sortedEqual), never
//     sequences. worker_test.go's equalStrings-on-order assertions are
//     valid only because it runs at the default Concurrency of 1.
//
//   - "the callbacks run serially inside Run, so ordering is established by
//     the loop itself" (worker_test.go's header). Above 1 slot that is
//     false, so every shared variable a callback touches here is an
//     atomic or mutex-guarded — a plain `processed = append(...)` from a
//     callback would be a data race under -race.
//
//   - The batch-index framing of the shutdown-timeout count. Sequentially,
//     "left unprocessed" is len(batch)-i for the single current index.
//     Concurrently there is no single index. This file pins the semantics
//     it considers the only defensible generalization:
//     UNPROCESSED == NOT SUCCESSFULLY DELETED, i.e. the messages the grace
//     expiry leaves for SQS to redeliver. See
//     TestGraceExpiryWithConcurrentInFlightReportsUndeletedCount.
//
// # ON DETERMINISM
//
// Two negative claims here ("never more than N in flight", "no unbounded
// goroutine growth") cannot be proven by an event, only observed over a
// window. holdsFor() supplies that window, and the flakiness is
// deliberately one-sided: a CORRECT implementation can never exceed the cap
// no matter how long the window runs, so these tests cannot fail
// spuriously; only a broken implementation depends on the window being long
// enough, and a broken one saturates within microseconds. Every positive
// claim ("the cap is actually reached", "all N finished") is a bounded poll
// on an atomic or a channel handshake, never a fixed sleep.
package worker_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/worker"
)

const (
	// stabilityWindow is how long a "never exceeds the cap" claim is held
	// under observation. See the package comment on one-sided flakiness.
	stabilityWindow = 100 * time.Millisecond

	// pollInterval paces bounded polling. Small enough that tests stay
	// fast, large enough not to starve the worker goroutines it observes.
	pollInterval = time.Millisecond
)

// ── concurrency instrumentation ─────────────────────────────────────────

// tracker records, across a whole Run invocation, how many Process calls
// are in flight, the high-water mark, and how many have started/finished.
// The high-water mark is the central measurement of WR-022: it must equal
// the configured Concurrency exactly — no more (the cap holds) and no less
// (the work really did run in parallel).
type tracker struct {
	cur      atomic.Int64
	max      atomic.Int64
	started  atomic.Int64
	finished atomic.Int64
}

func (t *tracker) enter() {
	t.started.Add(1)
	c := t.cur.Add(1)
	for {
		m := t.max.Load()
		if c <= m || t.max.CompareAndSwap(m, c) {
			return
		}
	}
}

func (t *tracker) leave() {
	t.cur.Add(-1)
	t.finished.Add(1)
}

// ── synchronization helpers ─────────────────────────────────────────────

// awaitCond polls cond until it holds or the timeout elapses, reporting
// whether it held. It is goroutine-safe (no t.Fatal), so it can be used
// from a callback or a helper goroutine as well as from the test goroutine.
func awaitCond(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		runtime.Gosched()
		time.Sleep(pollInterval)
	}
}

// mustAwait is awaitCond plus a failure on timeout, for the test goroutine.
func mustAwait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if !awaitCond(cond, runTimeout) {
		t.Fatalf("timed out after %v waiting for %s", runTimeout, what)
	}
}

// holdsFor reports whether cond holds continuously for d. Used only for
// negative claims (a cap not being exceeded), where no event exists to
// synchronize on.
func holdsFor(cond func() bool, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			return false
		}
		runtime.Gosched()
		time.Sleep(pollInterval)
	}
	return cond()
}

// startWorker launches w.Run(ctx) and returns the channel its error will
// arrive on. Unlike runWorker it does not block, so a test can drive a
// rendezvous with the in-flight processors before Run returns.
func startWorker(w worker.Worker, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	return done
}

// awaitRun waits for an already-started Run to return, bounded by
// runTimeout. On timeout it cancels first so a stuck worker can unwind
// instead of being leaked for the rest of the package's run.
func awaitRun(t *testing.T, done <-chan error, cancel context.CancelFunc) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(runTimeout):
		cancel()
		select {
		case <-done:
		case <-time.After(runTimeout):
		}
		t.Fatal("Run did not return within the test timeout — it is deadlocked or never observed receive-context cancellation")
		return nil
	}
}

// abandonRun cancels and waits for a Run the test has given up on, WITHOUT
// reporting a failure of its own. Cleanup on a failure path must not mask
// the diagnosis that led to it: a t.Fatal from awaitRun would replace "the
// cap was exceeded" with the far less useful "Run did not return".
func abandonRun(done <-chan error, cancel context.CancelFunc) {
	cancel()
	select {
	case <-done:
	case <-time.After(runTimeout):
	}
}

// hasReturned reports whether Run has already delivered its error, without
// consuming it. Used to prove Run is still running (i.e. still waiting on
// its in-flight processors) at a moment when it must not have returned yet.
func hasReturned(done <-chan error) bool {
	return len(done) > 0
}

// ── assertion helpers ───────────────────────────────────────────────────

// bodySet returns "m1".."mN", the bodies seed() produces.
func bodySet(n int) []string {
	out := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, "m"+strconv.Itoa(i))
	}
	return out
}

// sortedEqual compares got and want as multisets. Above Concurrency=1 the
// order of Process and DeleteMessage calls is undefined, so order-sensitive
// comparison would be asserting a coincidence of scheduling.
func sortedEqual(got, want []string) bool {
	g := slices.Clone(got)
	w := slices.Clone(want)
	slices.Sort(g)
	slices.Sort(w)
	return slices.Equal(g, w)
}

// bodyLog is a mutex-guarded []string. Callbacks running on several
// goroutines cannot append to a plain slice.
type bodyLog struct {
	mu     sync.Mutex
	bodies []string
}

func (b *bodyLog) add(s string) {
	b.mu.Lock()
	b.bodies = append(b.bodies, s)
	b.mu.Unlock()
}

func (b *bodyLog) snapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.bodies)
}

// ── New: defaulting ─────────────────────────────────────────────────────

// TestNewDefaultsConcurrency pins the configuration contract: a missing or
// nonsensical Concurrency means 1 (WR-021's sequential behavior), and a
// valid value is passed through untouched. Defaulting to 1 rather than to
// some "sensible" number is what keeps every WR-021 test honest, and
// clamping <= 0 instead of accepting it is what prevents a zero-capacity
// semaphore — a worker that consumes nothing forever while looking healthy.
func TestNewDefaultsConcurrency(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "unset means sequential", in: 0, want: 1},
		{name: "negative is clamped to sequential", in: -1, want: 1},
		{name: "large negative is clamped to sequential", in: -1000, want: 1},
		{name: "explicit one is preserved", in: 1, want: 1},
		{name: "explicit value is preserved", in: 8, want: 8},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := worker.New(worker.Worker{Concurrency: tc.in})
			if got.Concurrency != tc.want {
				t.Errorf("New(Worker{Concurrency: %d}).Concurrency = %d, want %d", tc.in, got.Concurrency, tc.want)
			}
			// Defaulting Concurrency must not disturb the other default.
			if got.ShutdownGrace <= 0 {
				t.Errorf("New left ShutdownGrace = %v, want the package default to still be applied", got.ShutdownGrace)
			}
		})
	}
}

// ── Concurrency == 1: regression net ────────────────────────────────────

// TestConcurrencyOneProcessesSequentially is the safety net for WR-021: one
// slot must mean strictly one Process call at a time, in receive order,
// with deletes in the same order. If WR-022's refactor accidentally makes
// the single-slot path concurrent, this fails here rather than as a
// mysterious flake in worker_test.go.
func TestConcurrencyOneProcessesSequentially(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		tr        tracker
		processed bodyLog
	)

	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   1,
		Process: func(_ context.Context, msg awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			processed.add(msg.Body)
			if tr.finished.Load() == 4 { // this call is the 5th
				cancel()
			}
			return nil
		},
	})

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := tr.max.Load(); got != 1 {
		t.Errorf("peak in-flight Process calls = %d, want 1 — Concurrency=1 must stay strictly sequential", got)
	}
	if got, want := processed.snapshot(), bodySet(5); !equalStrings(got, want) {
		t.Errorf("processed %v, want %v in receive order", got, want)
	}
	if got, want := deletedBodies(f, queueURL), bodySet(5); !equalStrings(got, want) {
		t.Errorf("deleted %v, want %v in receive order", got, want)
	}
}

// TestRunClampsNonPositiveConcurrency covers the worker built by a struct
// literal instead of by New — the CRD path defaults Concurrency, but a
// programming slip should degrade to sequential, not hang. A semaphore
// sized 0 would make Run block forever on its first acquire: a worker that
// long-polls, receives, and silently processes nothing. Run must clamp too,
// so <= 0 means 1 everywhere, not only in New.
func TestRunClampsNonPositiveConcurrency(t *testing.T) {
	for _, concurrency := range []int{0, -4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			f, queueURL := newFakeQueue(t)
			seed(t, f, queueURL, 3)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var (
				tr        tracker
				processed bodyLog
			)

			// Deliberately NOT worker.New: this is the un-defaulted struct.
			w := worker.Worker{
				SQSClient:     f,
				QueueURL:      queueURL,
				ShutdownGrace: longGrace,
				Concurrency:   concurrency,
				Process: func(_ context.Context, msg awsclient.Message) error {
					tr.enter()
					defer tr.leave()

					processed.add(msg.Body)
					if tr.started.Load() == 3 {
						cancel()
					}
					return nil
				},
			}

			if err := runWorker(t, w, ctx, cancel); err != nil {
				t.Fatalf("Run returned %v, want nil — Concurrency=%d must be clamped to 1, not rejected", err, concurrency)
			}
			if got := tr.max.Load(); got != 1 {
				t.Errorf("peak in-flight Process calls = %d, want 1", got)
			}
			if got, want := processed.snapshot(), bodySet(3); !sortedEqual(got, want) {
				t.Errorf("processed %v, want %v — a non-positive Concurrency must not stall the consumer", got, want)
			}
		})
	}
}

// ── the cap: reached, and never exceeded ────────────────────────────────

// TestProcessRunsConcurrentlyUpToCap proves the cap is genuinely reached,
// not merely respected: three processors must be alive at the same instant.
// The rendezvous is what makes this a proof rather than an observation — a
// sequential implementation can never collect three arrivals, because
// nobody returns until all three have arrived, so it fails by timeout
// instead of by luck.
func TestProcessRunsConcurrentlyUpToCap(t *testing.T) {
	const concurrency = 3

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, concurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr tracker
	arrived := make(chan string, concurrency)
	release := make(chan struct{})

	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   concurrency,
		Process: func(_ context.Context, msg awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			arrived <- msg.Body
			<-release
			return nil
		},
	})

	done := startWorker(w, ctx)

	for i := 0; i < concurrency; i++ {
		select {
		case <-arrived:
		case <-time.After(runTimeout):
			t.Errorf("only %d of %d processors were alive simultaneously — messages are not being processed concurrently", i, concurrency)
			close(release)
			abandonRun(done, cancel)
			return
		}
	}
	close(release)

	mustAwait(t, "all processors to finish", func() bool { return tr.finished.Load() == concurrency })
	cancel()

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if got := tr.max.Load(); got != concurrency {
		t.Errorf("peak in-flight Process calls = %d, want %d", got, concurrency)
	}
	if got, want := deletedBodies(f, queueURL), bodySet(concurrency); !sortedEqual(got, want) {
		t.Errorf("deleted %v, want %v (any order)", got, want)
	}
}

// TestBurstNeverExceedsConcurrencyCap is WR-022's Done-when ("concurrency
// cap verified under a burst"). Forty messages are queued at once, so an
// unbounded implementation races straight to forty in-flight calls while a
// bounded one parks at two. The capacity-capped receive means the burst
// arrives in batches of at most `concurrency` messages, not in fixed batches
// of ten.
//
// It also pins a consequence that matters against real SQS: while every
// slot is busy and the current batch still has unstarted messages, no
// FURTHER ReceiveMessage may be issued. Pulling batches the worker cannot
// start would burn the messages' visibility timeout in a queue instead of
// in a processor, producing duplicate deliveries under load.
func TestBurstNeverExceedsConcurrencyCap(t *testing.T) {
	const (
		concurrency = 2
		total       = 40
	)

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, total)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		tr        tracker
		processed bodyLog
	)
	gate := make(chan struct{})

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   concurrency,
		Process: func(_ context.Context, msg awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			processed.add(msg.Body)
			<-gate
			return nil
		},
	})

	done := startWorker(w, ctx)

	// Positive claim, bounded poll: the cap is reached.
	if !awaitCond(func() bool { return tr.cur.Load() >= concurrency }, runTimeout) {
		t.Errorf("in-flight Process calls never reached %d (peak %d) — the burst is not being processed concurrently", concurrency, tr.max.Load())
		close(gate)
		abandonRun(done, cancel)
		return
	}

	// Negative claim, held under observation: it is never exceeded, and no
	// extra batch is checked out while every slot is blocked.
	saturated := func() bool {
		return tr.cur.Load() <= concurrency && tr.started.Load() <= concurrency && rec.receiveCount() == 1
	}
	if !holdsFor(saturated, stabilityWindow) {
		inFlight, started, receives := tr.cur.Load(), tr.started.Load(), rec.receiveCount()
		t.Errorf("with all %d slots blocked: in-flight=%d started=%d ReceiveMessage calls=%d; want in-flight and started capped at %d and exactly 1 receive — the worker is not bounding its concurrency (or is checking out messages it cannot start)",
			concurrency, inFlight, started, receives, concurrency)
		close(gate)
		abandonRun(done, cancel)
		return
	}

	close(gate)

	mustAwait(t, "the whole burst to drain", func() bool { return tr.finished.Load() == total })
	cancel()

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if got := tr.max.Load(); got != concurrency {
		t.Errorf("peak in-flight Process calls = %d, want exactly %d over the whole burst", got, concurrency)
	}
	if got, want := processed.snapshot(), bodySet(total); !sortedEqual(got, want) {
		t.Errorf("processed %d message(s), want all %d exactly once: %v", len(got), total, got)
	}
	if got, want := deletedBodies(f, queueURL), bodySet(total); !sortedEqual(got, want) {
		t.Errorf("deleted %d message(s), want all %d: %v", len(got), total, got)
	}
}

// TestBurstDoesNotGrowGoroutinesUnbounded is the other half of the
// Done-when ("no unbounded goroutine growth"). The cap on in-flight Process
// calls is not by itself enough: spawning one goroutine per received
// message and having each block on a semaphore acquire would keep every
// Process call bounded while still growing the goroutine count with the
// backlog — a worker whose memory footprint tracks queue depth, which is
// exactly what scale-from-zero bursts would blow up. Live goroutines must
// be bounded by Concurrency, not by the number of messages received.
func TestBurstDoesNotGrowGoroutinesUnbounded(t *testing.T) {
	const (
		concurrency = 2
		total       = 40

		// Run itself, the watchdog, the test's startWorker goroutine, an
		// optional dispatcher, plus slack for runtime-owned goroutines.
		overhead = 5
	)

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, total)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr tracker
	gate := make(chan struct{})

	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   concurrency,
		Process: func(_ context.Context, _ awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			<-gate
			return nil
		},
	})

	baseline := runtime.NumGoroutine()
	done := startWorker(w, ctx)

	if !awaitCond(func() bool { return tr.cur.Load() >= concurrency }, runTimeout) {
		t.Errorf("in-flight Process calls never reached %d — cannot assess goroutine growth", concurrency)
		close(gate)
		abandonRun(done, cancel)
		return
	}

	limit := baseline + concurrency + overhead
	if !holdsFor(func() bool { return runtime.NumGoroutine() <= limit }, stabilityWindow) {
		got := runtime.NumGoroutine()
		t.Errorf("goroutine count reached %d with %d messages queued and %d slots (baseline %d, limit %d) — live goroutines must scale with Concurrency, not with the backlog",
			got, total, concurrency, baseline, limit)
		close(gate)
		abandonRun(done, cancel)
		return
	}

	close(gate)
	mustAwait(t, "the whole burst to drain", func() bool { return tr.finished.Load() == total })
	cancel()

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	// And nothing spawned per message may outlive Run.
	if !awaitCond(func() bool { return runtime.NumGoroutine() <= baseline }, 2*time.Second) {
		t.Fatalf("goroutine count settled at %d, want <= baseline %d — processing goroutines outlived Run", runtime.NumGoroutine(), baseline)
	}
}

// TestSlowMessageHoldsSlotAcrossBatches pins that the cap is a property of
// Run, not of one batch. A bounded-per-batch implementation (spawn N,
// wait for all, receive again) would pass every test above and still be
// wrong: the slow tail of batch 1 would be joined by a full complement of
// batch 2, doubling real concurrency exactly when the queue is deep.
//
// One message blocks for the whole run, so a correct worker keeps exactly
// one free slot and drains the remaining eleven — across at least two
// receive calls — one at a time.
//
// The fast processors block on slowInside before completing. Without that
// rendezvous the peak in-flight count is a scheduling coincidence rather
// than a measurement: nothing would force m1's goroutine to be running while
// a fast message is, so all eleven could enter and leave through the other
// slot before m1's Process ever started, and a correct worker would be
// reported as having peaked at 1. Gating the fast side on "m1 is provably
// inside Process, holding its slot" makes an overlap of two unavoidable while
// still leaving the cap free to be exceeded if the implementation is wrong.
func TestSlowMessageHoldsSlotAcrossBatches(t *testing.T) {
	const (
		concurrency = 2
		// More messages than the free slot can hold at once, so draining
		// them provably crosses receive-call boundaries while m1 is stuck.
		total = 12
	)

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, total)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		tr         tracker
		fastDone   atomic.Int64
		slowGate   = make(chan struct{})
		slowInside = make(chan struct{})
	)

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   concurrency,
		Process: func(_ context.Context, msg awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			if msg.Body == "m1" {
				close(slowInside)
				<-slowGate
				return nil
			}
			// Rendezvous, not a sleep: a fast message may only finish once
			// m1 is inside its own Process call and holding a slot, so the
			// two are provably in flight together. m1 is the queue's first
			// message and the run has two slots, so it is always admitted in
			// the first batch — this can never deadlock waiting for a
			// message that has not been dispatched. The slowGate arm is the
			// give-up path only: every failure branch below closes slowGate,
			// and on the happy path it is closed only after all eleven fast
			// messages have already passed this point.
			select {
			case <-slowInside:
			case <-slowGate:
			}
			fastDone.Add(1)
			return nil
		},
	})

	done := startWorker(w, ctx)

	select {
	case <-slowInside:
	case <-time.After(runTimeout):
		t.Error("the slow message was never started")
		close(slowGate)
		abandonRun(done, cancel)
		return
	}

	// The other eleven must drain while m1 still holds its slot, which
	// forces at least a second ReceiveMessage during that time.
	if !awaitCond(func() bool { return fastDone.Load() == total-1 }, runTimeout) {
		got, receives := fastDone.Load(), rec.receiveCount()
		t.Errorf("only %d of %d fast messages drained (ReceiveMessage calls: %d) while one slow message held a slot — a blocked message must not stall the free slot", got, total-1, receives)
		close(slowGate)
		abandonRun(done, cancel)
		return
	}
	if got := rec.receiveCount(); got < 2 {
		t.Errorf("ReceiveMessage called %d time(s), want >= 2 — the run must cross a batch boundary while the slow message is still in flight", got)
	}
	if got := tr.max.Load(); got > concurrency {
		t.Errorf("peak in-flight Process calls = %d, want <= %d — a new batch must not add slots on top of an in-flight message", got, concurrency)
	}

	close(slowGate)
	mustAwait(t, "the slow message to finish", func() bool { return tr.finished.Load() == total })
	cancel()

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if got := tr.max.Load(); got != concurrency {
		t.Errorf("peak in-flight Process calls = %d, want exactly %d", got, concurrency)
	}
	if got, want := deletedBodies(f, queueURL), bodySet(total); !sortedEqual(got, want) {
		t.Errorf("deleted %d message(s), want all %d: %v", len(got), total, got)
	}
}

// ── per-message contract under concurrency ──────────────────────────────

// TestProcessErrorDoesNotAffectConcurrentSiblings: the WR-021 per-message
// contract (failure => no delete => redelivery, and the batch carries on)
// must survive concurrency. The rendezvous guarantees the failing message
// is in flight AT THE SAME TIME as its siblings, which is the case a
// sequential test cannot reach: a shared errgroup-style cancellation, or a
// single error short-circuiting the group, would tear the siblings down and
// silently lose their deletes.
func TestProcessErrorDoesNotAffectConcurrentSiblings(t *testing.T) {
	const concurrency = 3

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 3)

	failed := errors.New("processing failed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		tr        tracker
		processed bodyLog
	)
	arrived := make(chan struct{}, concurrency)
	release := make(chan struct{})

	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   concurrency,
		Process: func(workCtx context.Context, msg awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			processed.add(msg.Body)
			arrived <- struct{}{}
			<-release

			if msg.Body == "m2" {
				return failed
			}
			// A sibling's failure must not cancel this message's context.
			if err := workCtx.Err(); err != nil {
				t.Errorf("work context canceled (%v) while %q was still running alongside a failing sibling", err, msg.Body)
			}
			return nil
		},
	})

	done := startWorker(w, ctx)

	for i := 0; i < concurrency; i++ {
		select {
		case <-arrived:
		case <-time.After(runTimeout):
			t.Errorf("only %d of %d messages were in flight together", i, concurrency)
			close(release)
			abandonRun(done, cancel)
			return
		}
	}
	close(release)

	mustAwait(t, "all three messages to finish", func() bool { return tr.finished.Load() == 3 })
	cancel()

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil — one message's failure is not a worker failure", err)
	}
	if got, want := processed.snapshot(), bodySet(3); !sortedEqual(got, want) {
		t.Errorf("processed %v, want all of %v", got, want)
	}
	if got := deletedBodies(f, queueURL); !sortedEqual(got, []string{"m1", "m3"}) {
		t.Errorf("deleted %v, want [m1 m3] in any order — the failed message must stay undeleted for redelivery, and its siblings must still be deleted", got)
	}
}

// ── shutdown with concurrent work in flight ─────────────────────────────

// TestRunWaitsForInFlightProcessorsBeforeReturning: Run owns the lifetime
// of every goroutine it spawns. On cancellation it must not return while
// processors are still inside Process — that would let main() exit (and the
// container die) mid-write, and would report a clean shutdown while work
// was actually abandoned. This is the concurrent counterpart of WR-021's
// "drain the in-flight batch", and the reason the new per-message
// goroutines need the same WaitGroup discipline the watchdog already has.
func TestRunWaitsForInFlightProcessorsBeforeReturning(t *testing.T) {
	const concurrency = 4

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, concurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr tracker
	arrived := make(chan struct{}, concurrency)
	release := make(chan struct{})

	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace, // grace must NOT be the thing that ends this test
		Concurrency:   concurrency,
		Process: func(_ context.Context, _ awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			arrived <- struct{}{}
			<-release
			return nil
		},
	})

	done := startWorker(w, ctx)

	for i := 0; i < concurrency; i++ {
		select {
		case <-arrived:
		case <-time.After(runTimeout):
			t.Errorf("only %d of %d processors started", i, concurrency)
			close(release)
			abandonRun(done, cancel)
			return
		}
	}

	// Shutdown requested with all four processors still inside Process.
	cancel()

	if !holdsFor(func() bool { return !hasReturned(done) }, stabilityWindow) {
		close(release)
		t.Fatal("Run returned while processors were still in flight — it must wait for every goroutine it spawned")
	}

	close(release)

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil — draining in-flight work inside the grace period is a clean shutdown", err)
	}
	if got := tr.finished.Load(); got != concurrency {
		t.Errorf("%d of %d processors had finished when Run returned, want all of them", got, concurrency)
	}
	if got := tr.cur.Load(); got != 0 {
		t.Errorf("%d Process call(s) still in flight after Run returned, want 0", got)
	}
	if got, want := deletedBodies(f, queueURL), bodySet(concurrency); !sortedEqual(got, want) {
		t.Errorf("deleted %v, want %v — every drained message must be deleted", got, want)
	}
}

// TestGraceExpiryWithConcurrentInFlightReportsUndeletedCount is the
// concurrent translation of TestProcessReturnsNilAfterGraceExpiry (and of
// TestShutdownGraceExpiresMidBatch). All three messages are in flight when
// the grace period expires, and each then returns nil — a "success" that
// merely landed too late. Run must:
//
//   - hand every processor a work context whose Cause is ErrShutdownTimeout
//     (not DeadlineExceeded: WithCancelCause reports Canceled),
//   - delete NOTHING, since no message completed inside the budget,
//   - still wait for all three goroutines before returning,
//   - return an error wrapping ErrShutdownTimeout that reports 3.
//
// The count is the one WR-021 assumption that does not survive
// concurrency: sequentially it was len(batch)-i for a single cursor, and
// concurrently there is no cursor. The semantics pinned here are
// UNPROCESSED == NOT SUCCESSFULLY DELETED, i.e. what SQS will redeliver —
// the only reading that stays operationally meaningful (and that still
// yields WR-021's "2 of 3" for the sequential cases those tests cover).
func TestGraceExpiryWithConcurrentInFlightReportsUndeletedCount(t *testing.T) {
	const batch = 3

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, batch)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		tr           tracker
		wrongCause   atomic.Int64
		wrongErr     atomic.Int64
		arrived      = make(chan struct{}, batch)
		causeChecked atomic.Int64
	)

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: shortGrace,
		Concurrency:   batch,
		Process: func(workCtx context.Context, _ awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			arrived <- struct{}{}

			// Cooperative long-running work: returns only once the shared
			// grace budget has expired. No sleep, no race.
			<-workCtx.Done()
			if !errors.Is(context.Cause(workCtx), worker.ErrShutdownTimeout) {
				wrongCause.Add(1)
			}
			if !errors.Is(workCtx.Err(), context.Canceled) {
				wrongErr.Add(1)
			}
			causeChecked.Add(1)

			// Report success despite being late — the ordering trap WR-021
			// already fixed sequentially, re-armed concurrently.
			return nil
		},
	})

	done := startWorker(w, ctx)

	for i := 0; i < batch; i++ {
		select {
		case <-arrived:
		case <-time.After(runTimeout):
			t.Errorf("only %d of %d messages were in flight together", i, batch)
			cancel()
			abandonRun(done, cancel)
			return
		}
	}

	// Shutdown requested with the whole batch in flight; the single shared
	// grace timer starts here and every processor is released by its expiry.
	cancel()

	err := awaitRun(t, done, cancel)

	if err == nil {
		t.Fatal("Run returned nil, want an error wrapping ErrShutdownTimeout — a late nil from Process must not be treated as success")
	}
	if !errors.Is(err, worker.ErrShutdownTimeout) {
		t.Fatalf("Run returned %v, want an error wrapping worker.ErrShutdownTimeout", err)
	}
	if !mentionsCount(err.Error(), batch) {
		t.Errorf("shutdown-timeout error %q does not report %d — all %d in-flight messages missed the deadline and stay undeleted for redelivery", err.Error(), batch, batch)
	}

	if got := causeChecked.Load(); got != batch {
		t.Errorf("%d of %d processors observed work-context cancellation, want all of them — the grace budget is shared, not per message", got, batch)
	}
	if got := wrongCause.Load(); got != 0 {
		t.Errorf("%d processor(s) saw a context.Cause other than worker.ErrShutdownTimeout", got)
	}
	if got := wrongErr.Load(); got != 0 {
		t.Errorf("%d processor(s) saw a ctx.Err() other than context.Canceled (WithCancelCause never reports DeadlineExceeded)", got)
	}
	if got := tr.finished.Load(); got != batch {
		t.Errorf("%d of %d processors had finished when Run returned, want all of them — a timed-out Run must still not leak goroutines", got, batch)
	}
	if got := deletedBodies(f, queueURL); len(got) != 0 {
		t.Errorf("deleted %v, want none — no message completed inside the grace budget, so all must be left for redelivery", got)
	}
	// Deliberately NOT "ReceiveMessage was called exactly once", the way the
	// sequential WR-021 tests assert it. There, cancel() came from inside a
	// callback, so it was ordered before the loop's next receive. Here the
	// whole batch is already dispatched when the test goroutine cancels, so
	// the receive loop legitimately issues further (empty) receives in the
	// interval — against real SQS each of those is a 20s long poll, against
	// the fake they return instantly. What must hold is the claim that
	// actually matters: no message beyond the in-flight batch is ever
	// STARTED once the budget is gone.
	if got := tr.started.Load(); got != batch {
		t.Errorf("%d Process call(s) started, want exactly %d — no message may be started after the grace period expires", got, batch)
	}
	if got := rec.receiveCount(); got < 1 {
		t.Error("ReceiveMessage was never called")
	}
}

// deleteCountingSQS counts SUCCESSFUL DeleteMessage calls. fake.SQS already
// records them in f.Deleted, but that map is only safe to read once Run has
// returned (it is mutated from the pool goroutines), and
// TestGraceExpiryCountsUndeletedAcrossBatches needs to know, WHILE Run is
// still going, that every message it expects to succeed has already been
// deleted. Synchronizing on the deletes rather than on the Process calls is
// what keeps the expected count exact instead of racing the grace timer:
// Process returning is not the same event as its delete landing.
type deleteCountingSQS struct {
	awsclient.SQSClient

	deleted atomic.Int64
}

func (d *deleteCountingSQS) DeleteMessage(
	ctx context.Context,
	in awsclient.DeleteMessageInput,
) (awsclient.DeleteMessageOutput, error) {
	out, err := d.SQSClient.DeleteMessage(ctx, in)
	if err == nil {
		d.deleted.Add(1)
	}
	return out, err
}

func (d *deleteCountingSQS) deleteCount() int64 { return d.deleted.Load() }

// TestGraceExpiryCountsUndeletedAcrossBatches pins that the shutdown-timeout
// count is RUN-WIDE, not a property of whichever receive batch happened to be
// current when the grace period expired.
//
// Every other grace-expiry assertion in this package (and in worker_test.go
// and worker_timeout_ordering_test.go) uses a single batch, where run-wide and
// per-batch accounting are numerically indistinguishable, and
// TestSlowMessageHoldsSlotAcrossBatches crosses a batch boundary but never
// expires grace. So without this test a regression back to per-batch counting
// would pass the entire suite.
//
// The shape that makes the distinction provable:
//
//   - 12 messages arrive across several batches. Since WR-022's review the
//     batch size is the free-slot count, so with Concurrency 2 the first
//     receive takes two messages and — while m1 stays stuck below — every
//     later one takes a single message: many batches, none of them able to
//     account for the whole run on its own.
//   - Concurrency is 2, and m1 — from the FIRST batch — never completes: it
//     blocks until the work context is canceled. It therefore still occupies a
//     slot while every later batch is received and processed through the other
//     one.
//   - The other 11, spanning all those batches, are deleted before shutdown is
//     even requested (awaited on the delete count, not on Process).
//   - Only then is the receive context canceled, so the grace period expires
//     with exactly one undeleted message — one belonging to a batch that is no
//     longer "current".
//
// The reported total must be 12. That is the load-bearing assertion: no batch
// in this run ever held more than 2 messages, and the last one held a single
// message that WAS deleted — so per-batch accounting could only produce "0 of
// 1". "1 of 12" is unreachable from any last-batch-only bookkeeping.
func TestGraceExpiryCountsUndeletedAcrossBatches(t *testing.T) {
	const (
		concurrency = 2
		total       = 12
		fast        = total - 1 // everything except the deliberately stuck m1
	)

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, total)

	counting := &deleteCountingSQS{SQSClient: f}
	rec := &recordingSQS{SQSClient: counting}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		tr          tracker
		wrongCause  atomic.Int64
		stuckDone   atomic.Int64
		stuckInside = make(chan struct{})
	)

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: shortGrace,
		Concurrency:   concurrency,
		Process: func(workCtx context.Context, msg awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			if msg.Body != "m1" {
				return nil
			}

			// Cooperative work that outlives the whole drain budget. No
			// sleep: it returns only once the watchdog has canceled.
			close(stuckInside)
			<-workCtx.Done()
			if !errors.Is(context.Cause(workCtx), worker.ErrShutdownTimeout) {
				wrongCause.Add(1)
			}
			stuckDone.Add(1)
			// A late nil, as in the single-batch cases: reported success that
			// missed the deadline must neither be deleted nor counted as done.
			return nil
		},
	})

	done := startWorker(w, ctx)

	select {
	case <-stuckInside:
	case <-time.After(runTimeout):
		t.Error("the message held back from batch 1 was never started")
		abandonRun(done, cancel)
		return
	}

	// The rest of batch 1 AND all of batch 2 must be deleted while m1 still
	// holds its slot — that is what puts the eventual undeleted message in a
	// batch other than the last one received.
	if !awaitCond(func() bool { return counting.deleteCount() == fast }, runTimeout) {
		t.Errorf("only %d of %d messages were deleted (ReceiveMessage calls: %d, Process calls started: %d) while one batch-1 message held a slot — the free slot must drain both batches",
			counting.deleteCount(), fast, rec.receiveCount(), tr.started.Load())
		abandonRun(done, cancel)
		return
	}
	if got := rec.receiveCount(); got < 2 {
		t.Errorf("ReceiveMessage called %d time(s), want >= 2 — the run must cross a batch boundary before the grace period starts, or this test is not exercising run-wide accounting at all", got)
		abandonRun(done, cancel)
		return
	}

	// Shutdown requested. The grace timer starts here, and it can only expire:
	// the stuck message returns nothing until the work context is canceled.
	cancel()

	err := awaitRun(t, done, cancel)

	if err == nil {
		t.Fatal("Run returned nil, want an error wrapping ErrShutdownTimeout — one message was still in flight when the grace period expired")
	}
	if !errors.Is(err, worker.ErrShutdownTimeout) {
		t.Fatalf("Run returned %v, want an error wrapping worker.ErrShutdownTimeout", err)
	}
	if !mentionsCounts(err.Error(), 1, total) {
		t.Errorf("shutdown-timeout error %q does not report 1 of %d — the total must span every message received over the whole Run, and the one message left unprocessed is the stuck first-batch one; a count taken from the last batch alone could only say 0 of 1",
			err.Error(), total)
	}

	if got := stuckDone.Load(); got != 1 {
		t.Errorf("the stuck processor finished %d time(s), want exactly 1 — Run must wait for it before reporting", got)
	}
	if got := wrongCause.Load(); got != 0 {
		t.Errorf("%d processor(s) saw a context.Cause other than worker.ErrShutdownTimeout", got)
	}
	if got := tr.finished.Load(); got != total {
		t.Errorf("%d of %d processors had finished when Run returned, want all of them", got, total)
	}
	if got := tr.started.Load(); got != total {
		t.Errorf("%d Process call(s) started, want exactly %d — every message was handed out before shutdown, and none may start after it", got, total)
	}
	if got := tr.max.Load(); got > concurrency {
		t.Errorf("peak in-flight Process calls = %d, want <= %d", got, concurrency)
	}
	if got, want := deletedBodies(f, queueURL), bodySet(total)[1:]; !sortedEqual(got, want) {
		t.Errorf("deleted %v, want %v — everything except the message stuck past the deadline, which stays for redelivery", got, want)
	}
}

// ── concurrency changes the batch size and nothing else ─────────────────

// TestConcurrencyChangesBatchSizeOnlyNotPollPolicy pins where Concurrency's
// influence over the receive call stops.
//
// It DOES set the batch size: WR-022's review established that requesting
// more messages than there are free slots checks out work the worker cannot
// start, with the messages' visibility timeouts already running while they
// wait in memory — so MaxNumberOfMessages is the free-slot count, clamped to
// SQS's maximum of ten. (An earlier version of this test asserted the
// opposite, a fixed ten regardless of Concurrency. That premise was wrong and
// is now the disproven one; TestReceiveRequestsOnlyFreeSlots covers the whole
// free-slot relationship across concurrency levels.)
//
// It must NOT reach anything else about the poll: shortening
// WaitTimeSeconds would silently degrade the worker to short polling —
// burning API calls and defeating scale-to-zero economics — and setting a
// VisibilityTimeout override is WR-024's decision, not a side effect of a
// concurrency knob. That is what this test still exists to catch, at a
// concurrency level (4) where the batch size is genuinely derived rather
// than coincidentally equal to a default.
func TestConcurrencyChangesBatchSizeOnlyNotPollPolicy(t *testing.T) {
	const concurrency = 4 // below the SQS maximum, so the clamp is not what is measured

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	rec := &recordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   concurrency,
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
	// The first call is made with every slot free, so the batch size is the
	// full concurrency.
	if got.MaxNumberOfMessages != int32(concurrency) {
		t.Errorf("ReceiveMessage MaxNumberOfMessages = %d, want %d — with all %d slots free the batch size is the free-slot count, never more (surplus messages would be checked out unstartable) and never a fixed %d",
			got.MaxNumberOfMessages, concurrency, concurrency, wantMaxMessages)
	}
	if got.WaitTimeSeconds != wantWaitTime {
		t.Errorf("ReceiveMessage WaitTimeSeconds = %d, want %d (long polling)", got.WaitTimeSeconds, wantWaitTime)
	}
	if got.VisibilityTimeout != 0 {
		t.Errorf("ReceiveMessage VisibilityTimeout = %d, want 0 — that is WR-024's concern", got.VisibilityTimeout)
	}
}
