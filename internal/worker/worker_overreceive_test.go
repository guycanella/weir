// Package worker_test — WR-022 review-driven regression tests, round 2.
//
// Finding 3 (HIGH, narrower than finding 1) — the receive loop acquires ONE
// token before calling ReceiveMessage, which proves at least one slot is
// free, but then asks SQS for a FIXED batch of ten regardless of how much
// capacity actually exists. So the over-receive window finding 1 closed at
// the batch level is still open at the message level:
//
//	Concurrency=1, two messages available. One token is held, the request
//	still says MaxNumberOfMessages=10, and SQS legitimately answers with
//	BOTH messages. The first starts; the second is already RECEIVED — its
//	visibility timeout is running — while it sits in the dispatch loop
//	waiting for a token that only frees when the first message completes.
//	At Concurrency=4 a full ten-message answer strands six the same way.
//
// This is the same operational harm as finding 1 (visibility budget burned
// in memory instead of in a processor => SQS redelivers => duplicate
// processing under exactly the saturation where it hurts), reached through
// the request parameter rather than through the loop's gating. The invariant
// pinned here is therefore about the REQUEST, which is the only thing the
// worker controls:
//
//	MaxNumberOfMessages must never exceed the number of concurrency slots
//	free at the moment of the call — i.e. min(defaultMaxMessages,
//	Concurrency - in-flight).
//
// Why the previous round's test did not catch it: it seeded exactly one
// message, so a request for ten and a request for one produce an identical
// response. Every case here makes MORE messages available than the free-slot
// count, so the two diverge.
//
// The slot arithmetic counts the token the loop is holding for the batch's
// first message. With nothing in flight all Concurrency slots are usable, so
// the request is Concurrency (clamped to SQS's maximum of ten); with one
// message in flight at Concurrency=4, three are usable. Asking for LESS than
// that is not a correctness bug but is still pinned exactly: it would trade
// the over-receive bug for wasted round trips, and a test that accepts any
// number "at most N" would let the worker silently degrade to
// one-message-per-poll.
//
// Fixtures and helpers come from worker_test.go and
// worker_concurrency_test.go (newFakeQueue, seed, tracker, startWorker,
// awaitCond, mustAwait, awaitRun, abandonRun, holdsFor, deletedBodies,
// bodySet, sortedEqual, longGrace, runTimeout, stabilityWindow,
// wantMaxMessages). Nothing here sleeps to win a race.
//
// NOTE ON THE FAKE: fake.SQS already honors MaxNumberOfMessages structurally
// (it truncates pending to the requested count, and treats <= 0 as 1, like
// real SQS). So the response size is a faithful consequence of the request,
// and the end-to-end claim in TestReceiveWithInFlightMessagesRequestsOnlyFreeSlots
// ("no more messages were ever claimed from the queue than could be started")
// is a real measurement rather than a property of the double.
package worker_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
	"github.com/guycanella/weir/internal/worker"
)

// ── instrumentation ─────────────────────────────────────────────────────

// receiveCall pairs what a ReceiveMessage call ASKED for with what it got
// back. recordingSQS records inputs only; the returned count is what turns
// "the request was too large" into the observable harm — messages claimed
// out of the queue that no slot can start.
type receiveCall struct {
	requested int32
	returned  int
}

// maxMessagesCapturingSQS records every ReceiveMessage request/response pair
// and the running total of messages claimed from the queue, all readable
// WHILE Run is still going (fake.SQS's Received map is only safe to read
// after Run returns).
type maxMessagesCapturingSQS struct {
	awsclient.SQSClient

	mu      sync.Mutex
	calls   []receiveCall
	claimed int64
}

func (c *maxMessagesCapturingSQS) ReceiveMessage(
	ctx context.Context,
	in awsclient.ReceiveMessageInput,
) (awsclient.ReceiveMessageOutput, error) {
	out, err := c.SQSClient.ReceiveMessage(ctx, in)

	c.mu.Lock()
	c.calls = append(c.calls, receiveCall{requested: in.MaxNumberOfMessages, returned: len(out.Messages)})
	c.claimed += int64(len(out.Messages))
	c.mu.Unlock()

	return out, err
}

func (c *maxMessagesCapturingSQS) snapshot() []receiveCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]receiveCall(nil), c.calls...)
}

func (c *maxMessagesCapturingSQS) claimedCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.claimed
}

// maxRequested returns the largest MaxNumberOfMessages seen so far, or -1 if
// ReceiveMessage was never called.
func (c *maxMessagesCapturingSQS) maxRequested() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	got := int32(-1)
	for _, call := range c.calls {
		if call.requested > got {
			got = call.requested
		}
	}
	return got
}

// ── idle worker: the request is bounded by Concurrency ───────────────────

// TestReceiveRequestsOnlyFreeSlots pins the invariant in its simplest form:
// with no message in flight, a worker with N slots may ask for N messages —
// never for the fixed ten, unless it happens to have ten slots.
//
// The queue deliberately holds MORE messages than any case's slot count, so
// an unbounded request is visible both in the parameter and in the response:
// at Concurrency=1 the fake answers a request for ten with three messages,
// two of which are stranded with their visibility timeout already running.
//
// The 12-slot case pins the other side of the clamp: SQS rejects
// MaxNumberOfMessages above ten, so free capacity beyond that must not be
// requested either. Deriving the batch size from Concurrency without
// clamping would turn a legitimate Concurrency=12 pipeline into a worker
// whose every receive call is an InvalidParameterValue error.
func TestReceiveRequestsOnlyFreeSlots(t *testing.T) {
	const available = 12 // more than any case's free-slot count

	tests := []struct {
		name        string
		concurrency int
		want        int32
	}{
		{name: "one slot asks for one", concurrency: 1, want: 1},
		{name: "two slots ask for two", concurrency: 2, want: 2},
		{name: "four slots ask for four", concurrency: 4, want: 4},
		{name: "ten slots ask for ten", concurrency: 10, want: wantMaxMessages},
		{name: "twelve slots are clamped to the SQS maximum", concurrency: 12, want: wantMaxMessages},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, queueURL := newFakeQueue(t)
			seed(t, f, queueURL, available)

			capturing := &maxMessagesCapturingSQS{SQSClient: f}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var tr tracker
			gate := make(chan struct{})

			w := worker.New(worker.Worker{
				SQSClient:     capturing,
				QueueURL:      queueURL,
				ShutdownGrace: longGrace, // grace must not be what ends this test
				Concurrency:   tc.concurrency,
				Process: func(_ context.Context, _ awsclient.Message) error {
					tr.enter()
					defer tr.leave()

					// Every processor holds its slot until the assertion is
					// made, so no token can come back and change the free-slot
					// count under the measurement.
					<-gate
					return nil
				},
			})

			done := startWorker(w, ctx)

			// A Process entry proves the first ReceiveMessage has returned, so
			// the call being asserted on is complete and recorded. No clock.
			if !awaitCond(func() bool { return tr.started.Load() >= 1 }, runTimeout) {
				t.Error("no message was ever started, so no ReceiveMessage call can be inspected")
				close(gate)
				abandonRun(done, cancel)
				return
			}

			calls := capturing.snapshot()
			if len(calls) == 0 {
				t.Error("ReceiveMessage was never called")
				close(gate)
				abandonRun(done, cancel)
				return
			}

			first := calls[0]
			if first.requested != tc.want {
				t.Errorf("first ReceiveMessage requested MaxNumberOfMessages = %d with %d idle slot(s) and %d message(s) available, want %d — asking for more than the free-slot count claims messages the worker cannot start, and their SQS visibility timeout is already running while they wait in memory",
					first.requested, tc.concurrency, available, tc.want)
			}
			// The consequence, measured rather than inferred: the fake honors
			// the request, so an oversized request really does check out
			// messages that no slot can accept.
			if int32(first.returned) > tc.want {
				t.Errorf("first ReceiveMessage returned %d message(s) with only %d slot(s) free, want at most %d — the surplus is received (visibility timeout ticking) but unstartable",
					first.returned, tc.concurrency, tc.want)
			}

			close(gate)

			mustAwait(t, "the queue to drain", func() bool { return tr.finished.Load() == available })
			cancel()

			if err := awaitRun(t, done, cancel); err != nil {
				t.Fatalf("Run returned %v, want nil — bounding the request must not stop the queue from draining", err)
			}
			if got := capturing.maxRequested(); got > wantMaxMessages {
				t.Errorf("some ReceiveMessage requested MaxNumberOfMessages = %d over the whole run, want at most %d (SQS's hard maximum)", got, wantMaxMessages)
			}
			if got, want := deletedBodies(f, queueURL), bodySet(available); !sortedEqual(got, want) {
				t.Errorf("deleted %d message(s), want all %d — a smaller batch size means more receive calls, not fewer messages processed: %v", len(got), available, got)
			}
		})
	}
}

// ── busy worker: the request shrinks with in-flight work ─────────────────

// TestReceiveWithInFlightMessagesRequestsOnlyFreeSlots is the reviewer's
// exact scenario, and the one no existing test reaches: capacity is
// PARTIALLY consumed when the receive call is made.
//
// Concurrency is 4 and exactly one message (m1) is in flight, blocked on a
// gate the test controls, so three slots are free and stay free for as long
// as the assertion needs. Six further messages are then made available —
// twice the free capacity — so a fixed request for ten claims all six and
// strands three of them, while a correct request for three claims exactly
// what it can start.
//
// Two independent claims are pinned, because either alone is satisfiable by
// a wrong implementation:
//
//   - the REQUEST parameter is 3, not 10 (a worker could otherwise ask for
//     ten and rely on a busy queue happening to be shallow); and
//   - the RUN-WIDE count of messages claimed out of the queue never exceeds
//     what has been started (a worker could otherwise compute a correct
//     number and then ignore it).
//
// The second is a negative claim, so it is held under observation
// (stabilityWindow) rather than sampled: see worker_concurrency_test.go on
// one-sided flakiness — a correct worker is parked in acquire at that point
// and can never claim more, however long the window runs.
func TestReceiveWithInFlightMessagesRequestsOnlyFreeSlots(t *testing.T) {
	const (
		concurrency = 4
		inFlight    = 1
		wantFree    = concurrency - inFlight // 3
		later       = 6                      // twice the free capacity
		total       = 1 + later
	)

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1) // m1 only: the message that will hold a slot

	capturing := &maxMessagesCapturingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr tracker
	// inside is closed from m1's Process body, so waiting on it proves one
	// slot is genuinely occupied — not merely that a message was dispatched.
	inside := make(chan struct{})
	gate := make(chan struct{})

	w := worker.New(worker.Worker{
		SQSClient:     capturing,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Concurrency:   concurrency,
		Process: func(_ context.Context, msg awsclient.Message) error {
			tr.enter()
			defer tr.leave()

			if msg.Body == "m1" {
				close(inside)
			}
			<-gate
			return nil
		},
	})

	done := startWorker(w, ctx)

	if !awaitCond(func() bool {
		select {
		case <-inside:
			return true
		default:
			return false
		}
	}, runTimeout) {
		t.Error("m1 was never started, so no slot is occupied and the test proves nothing")
		close(gate)
		abandonRun(done, cancel)
		return
	}

	// One slot occupied, three free. Now make twice the free capacity
	// available: from here on every receive is made against a queue deeper
	// than the worker's remaining capacity, which is the condition the
	// previous round's single-message test could not create.
	sendBodies(t, f, queueURL, 2, later)

	// The worker fills its remaining slots. Both a correct and a broken
	// implementation reach four started messages, so this is only the
	// synchronization point, not the assertion.
	if !awaitCond(func() bool { return tr.started.Load() == concurrency }, runTimeout) {
		t.Errorf("%d of %d slots were filled (messages claimed from the queue: %d), want all of them", tr.started.Load(), concurrency, capturing.claimedCount())
		close(gate)
		abandonRun(done, cancel)
		return
	}

	// Claim 1, on the two calls whose free-slot count is EXACT.
	//
	// Call #1 is made with every slot idle: the queue holds only m1 at that
	// point, so it can return at most one message and admit at most one.
	//
	// Call #2 is therefore made with exactly one message admitted and none
	// finished (the gate is shut), and no further message can be admitted
	// until it returns — so three slots are free, exactly. This is the
	// reviewer's scenario, and it is deterministic rather than sampled.
	//
	// Calls #3 onward are made as the worker fills its remaining slots, so
	// their exact free-slot count depends on how the sends interleave with
	// the polls. Those get the INEQUALITY instead: in-flight only grows while
	// the gate is shut (nothing completes), so at least one slot is busy from
	// call #2 onward and no request may exceed three. Asserting equality
	// there would be pinning a scheduling coincidence.
	calls := capturing.snapshot()
	if len(calls) < 2 {
		t.Errorf("only %d ReceiveMessage call(s) were made, want at least 2 — the second is the one issued with a message already in flight, which is the case under test", len(calls))
		close(gate)
		abandonRun(done, cancel)
		return
	}
	if got := calls[0].requested; got != int32(concurrency) {
		t.Errorf("ReceiveMessage call #1 requested MaxNumberOfMessages = %d with all %d slots idle, want %d", got, concurrency, concurrency)
	}
	if got := calls[1].requested; got != wantFree {
		t.Errorf("ReceiveMessage call #2 requested MaxNumberOfMessages = %d, want %d — exactly %d of %d slots were free at that moment (m1 was in flight and nothing else could have been admitted), so anything larger claims messages that must then sit in memory with their SQS visibility timeout already running",
			got, wantFree, wantFree, concurrency)
	}
	bad := 0
	for i := 1; i < len(calls); i++ {
		if calls[i].requested > wantFree {
			bad++
			if bad <= 3 { // enough to diagnose; not one line per empty poll
				t.Errorf("ReceiveMessage call #%d requested MaxNumberOfMessages = %d, want at most %d — at least one of the %d slots was occupied at that moment, and in-flight work only grows while the gate is shut",
					i+1, calls[i].requested, wantFree, concurrency)
			}
		}
	}
	if bad > 3 {
		t.Errorf("(%d further over-sized ReceiveMessage requests suppressed)", bad-3)
	}

	// Claim 2: nothing beyond the four startable messages was ever taken out
	// of the queue, so the other three are still there for a consumer that
	// can actually run them — rather than stranded in this worker's memory.
	overReceived := func() bool { return capturing.claimedCount() <= int64(concurrency) }
	if !holdsFor(overReceived, stabilityWindow) {
		t.Errorf("%d message(s) claimed from the queue while only %d could be started (in flight: %d) — the surplus is received but unstartable: its visibility timeout expires in memory and SQS redelivers it to another consumer",
			capturing.claimedCount(), concurrency, tr.cur.Load())
	}

	if t.Failed() {
		close(gate)
		abandonRun(done, cancel)
		return
	}

	// Capacity returns: the run must then drain the rest normally, which is
	// what distinguishes "bounded the request" from "stopped fetching".
	close(gate)

	mustAwait(t, "the whole queue to drain", func() bool { return tr.finished.Load() == total })
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

// sendBodies makes n further messages available on queueURL with bodies
// "m<first>".."m<first+n-1>", continuing seed's numbering. seed always
// restarts at m1, so a test that must deepen the queue AFTER the worker has
// started (the only way to create "more messages available than free slots"
// at a chosen moment) cannot reuse it without duplicating bodies and
// breaking every body-set assertion.
func sendBodies(t *testing.T, f *fake.SQS, queueURL string, first, n int) {
	t.Helper()

	for i := first; i < first+n; i++ {
		if _, err := f.SendMessage(context.Background(), awsclient.SendMessageInput{
			QueueUrl: queueURL,
			Body:     "m" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("fixture SendMessage(m%d): %v", i, err)
		}
	}
}
