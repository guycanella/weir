// WR-024's deterministic half: the redelivery/visibility-timeout semantics
// that surround "delete on success only".
//
// worker_test.go already proves the STATIC half of WR-024 — a failed Process
// skips the delete (TestProcessErrorSkipsDelete), a failed delete leaves no
// deletion record (TestDeleteErrorLeavesMessageUndeleted), and the worker
// never overrides the queue's configured VisibilityTimeout on receive
// (TestReceiveUsesLongPollSettings). What none of those can show is the
// DYNAMIC consequence: what SQS would actually do next. "Undeleted" only
// matters because the message comes back once its visibility timeout
// elapses, and "deleted" only matters because it then cannot.
//
// That consequence is modeled here with fake.SQS.ExpireInFlight, which
// requeues every message currently in flight on a queue exactly as a
// wall-clock visibility-timeout expiry would, and invalidates the receipt
// handle the caller was holding. Firing it explicitly is what keeps these
// tests deterministic: the assertions read "IF the timeout had elapsed at
// this precise instant, would the right thing happen?" rather than racing a
// real timer. No test in this file sleeps.
//
// The DLQ redrive itself is deliberately NOT modeled here — the fake
// implements no redrive policy, and reimplementing SQS's threshold logic in a
// test double would only prove the double agrees with itself. What is proven
// here is the worker-visible PRECONDITION for redrive: across genuine
// retries, ApproximateReceiveCount climbs, which is the counter a real
// queue's maxReceiveCount is compared against. That a real queue then really
// moves the message is worker_dlq_integration_test.go's job, against
// LocalStack.
package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
	"github.com/guycanella/weir/internal/worker"
)

// receiveCountAttr is the SQS message attribute carrying how many times a
// message has been delivered. It is the value a queue's redrive policy
// compares against maxReceiveCount, so every assertion about "this is a
// redelivery, not a first delivery" is expressed through it.
const receiveCountAttr = "ApproximateReceiveCount"

// ── fixtures ────────────────────────────────────────────────────────────

// receiveOne takes a single message off the queue directly, bypassing the
// worker, and reports whether one was available. It is the oracle for
// "would SQS hand this message to a worker again?": a message still in
// flight (received, not yet deleted, timeout not yet elapsed) is invisible
// and yields false, and so does one that was deleted.
//
// It goes through the fake rather than through a second Worker on purpose:
// asking the queue directly keeps the observation independent of the code
// under test.
func receiveOne(t *testing.T, f *fake.SQS, queueURL string) (awsclient.Message, bool) {
	t.Helper()

	out, err := f.ReceiveMessage(context.Background(), awsclient.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	if err != nil {
		t.Fatalf("oracle ReceiveMessage(%q): %v", queueURL, err)
	}
	if len(out.Messages) == 0 {
		return awsclient.Message{}, false
	}
	if len(out.Messages) > 1 {
		t.Fatalf("oracle ReceiveMessage returned %d messages for MaxNumberOfMessages=1", len(out.Messages))
	}
	return out.Messages[0], true
}

// assertNothingAvailable fails unless the queue hands back nothing —
// including recording WHAT it wrongly handed back, since "which message came
// back" is the whole diagnosis.
func assertNothingAvailable(t *testing.T, f *fake.SQS, queueURL, why string) {
	t.Helper()

	if msg, ok := receiveOne(t, f, queueURL); ok {
		t.Errorf("queue handed back message %q (receive count %q), want nothing — %s",
			msg.Body, msg.Attributes[receiveCountAttr], why)
	}
}

// deleteRecordingSQS records every DeleteMessage call AND the error it
// returned. fake.SQS.Deleted only records the deletes that SUCCEEDED, so it
// cannot distinguish "the worker never tried to delete" from "the worker
// tried and the receipt handle had already expired" — which is exactly the
// distinction the too-short-visibility-timeout test turns on.
type deleteRecordingSQS struct {
	awsclient.SQSClient

	mu       sync.Mutex
	attempts []deleteAttempt
}

type deleteAttempt struct {
	receiptHandle string
	err           error
}

func (d *deleteRecordingSQS) DeleteMessage(
	ctx context.Context,
	in awsclient.DeleteMessageInput,
) (awsclient.DeleteMessageOutput, error) {
	out, err := d.SQSClient.DeleteMessage(ctx, in)

	d.mu.Lock()
	d.attempts = append(d.attempts, deleteAttempt{receiptHandle: in.ReceiptHandle, err: err})
	d.mu.Unlock()

	return out, err
}

func (d *deleteRecordingSQS) deleteAttempts() []deleteAttempt {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]deleteAttempt(nil), d.attempts...)
}

// ── property 1: a deleted message can never come back ───────────────────

// TestDeletedMessageIsNotRedeliveredAfterVisibilityTimeoutExpiry is the
// positive half of delete-on-success: once the delete lands, the message is
// gone from the queue's world, so a visibility-timeout expiry has nothing
// left to requeue.
//
// This is the assertion that makes "delete on success" mean something. A
// worker that deleted correctly and a worker whose delete silently did
// nothing look identical in fake.Deleted terms if nobody ever asks the queue
// what it still holds; firing the expiry afterwards is what asks.
func TestDeletedMessageIsNotRedeliveredAfterVisibilityTimeoutExpiry(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := worker.New(worker.Worker{
		SQSClient:     f,
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

	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1"}) {
		t.Fatalf("deleted %v, want [m1] — the rest of this test is vacuous unless the delete landed", got)
	}

	// The visibility timeout elapses. A deleted message is not in flight, so
	// there is nothing to requeue.
	if got := f.ExpireInFlight(queueURL); got != 0 {
		t.Errorf("ExpireInFlight requeued %d message(s), want 0 — a successfully deleted message must not be in flight any more", got)
	}
	assertNothingAvailable(t, f, queueURL,
		"a message deleted after successful processing must never be redelivered, no matter how much later its visibility timeout would have elapsed")
}

// TestExpireInFlightRequeuesOnlyUndeletedMessages is the discriminating form
// of the same claim: with one deleted and one failed message on the same
// queue, the expiry must requeue exactly one of them.
//
// Separating this from the single-message case above matters because "0
// requeued" is also what a broken ExpireInFlight that requeues nothing at all
// would report. Here the same call must return 1 AND hand back the FAILED
// body specifically, so neither an over-eager nor an inert expiry can pass.
//
// Concurrency stays at the default 1, so m1 is processed and deleted strictly
// before m2 is even received, and no ordering is left to scheduling.
func TestExpireInFlightRequeuesOnlyUndeletedMessages(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 2)

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
			if msg.Body == "m2" {
				cancel()
				return failed
			}
			return nil
		},
	})

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil — a single processing failure is not a worker failure", err)
	}
	if want := []string{"m1", "m2"}; !equalStrings(processed, want) {
		t.Fatalf("processed %v, want %v", processed, want)
	}
	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1"}) {
		t.Fatalf("deleted %v, want [m1] — only the successful message", got)
	}

	if got := f.ExpireInFlight(queueURL); got != 1 {
		t.Fatalf("ExpireInFlight requeued %d message(s), want exactly 1 — the failed message is still in flight and the successful one is gone", got)
	}

	msg, ok := receiveOne(t, f, queueURL)
	if !ok {
		t.Fatal("queue handed back nothing after the visibility timeout elapsed, want the failed message m2")
	}
	if msg.Body != "m2" {
		t.Errorf("redelivered %q, want %q — only the message that was NOT deleted may come back", msg.Body, "m2")
	}
	assertNothingAvailable(t, f, queueURL, "only one message was undeleted, so only one may be redelivered")
}

// ── property 2: a failed message comes back, with a higher count ────────

// TestFailedMessageIsRedeliveredWithIncrementedReceiveCount pins the
// redelivery contract failure depends on: the message SQS hands back is the
// same message (same id and body), and it is marked as a second delivery.
//
// The receive count is the load-bearing part. Redelivering the message with
// the count reset to 1 would look identical to this test's body assertion,
// yet no redrive policy could ever fire — a poison message would loop
// forever instead of reaching the DLQ.
func TestFailedMessageIsRedeliveredWithIncrementedReceiveCount(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	failed := errors.New("processing failed")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var firstDelivery awsclient.Message
	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, msg awsclient.Message) error {
			firstDelivery = msg
			cancel()
			return failed
		},
	})

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if len(f.Deleted[queueURL]) != 0 {
		t.Fatalf("deleted %v, want nothing — a failed message must not be deleted", f.Deleted[queueURL])
	}
	if got := firstDelivery.Attributes[receiveCountAttr]; got != "1" {
		t.Fatalf("first delivery reported %s=%q, want %q", receiveCountAttr, got, "1")
	}

	// Still in flight before the timeout elapses: a failed message is not
	// instantly back on the queue, it waits out its visibility timeout.
	assertNothingAvailable(t, f, queueURL,
		"a failed message stays invisible until its visibility timeout elapses; becoming available immediately would mean a hot retry loop")

	if got := f.ExpireInFlight(queueURL); got != 1 {
		t.Fatalf("ExpireInFlight requeued %d message(s), want 1 — the undeleted message must still be in flight", got)
	}

	redelivered, ok := receiveOne(t, f, queueURL)
	if !ok {
		t.Fatal("queue handed back nothing after the visibility timeout elapsed, want the undeleted message redelivered")
	}
	if redelivered.Body != firstDelivery.Body {
		t.Errorf("redelivered body %q, want %q — the SAME message must come back", redelivered.Body, firstDelivery.Body)
	}
	if redelivered.MessageId != firstDelivery.MessageId {
		t.Errorf("redelivered MessageId %q, want %q — a redelivery keeps the message's identity", redelivered.MessageId, firstDelivery.MessageId)
	}
	if redelivered.ReceiptHandle == firstDelivery.ReceiptHandle {
		t.Errorf("redelivered ReceiptHandle %q is the same as the first delivery's — each delivery gets its own handle, and the expired one is dead",
			redelivered.ReceiptHandle)
	}
	if got := redelivered.Attributes[receiveCountAttr]; got != "2" {
		t.Errorf("redelivered %s=%q, want %q — without a climbing count no maxReceiveCount can ever be exhausted and a poison message never reaches the DLQ",
			receiveCountAttr, got, "2")
	}
}

// ── property 3: the count climbs across repeated failures ───────────────

// TestRepeatedFailureKeepsIncrementingReceiveCount is the worker-visible
// precondition for DLQ redrive: a message that fails every single time is
// redelivered every single time, and each delivery is marked one higher than
// the last. A real queue with maxReceiveCount = N moves the message to the
// DLQ precisely when this counter passes N, so a counter that stalled at 1
// (or reset per delivery) would mean poison messages circulate forever.
//
// The expiry is fired from inside Process, which is what makes the loop
// deterministic without a timer: at the default Concurrency of 1 the worker
// holds its only slot for the duration of the call, so nothing else can
// touch the queue, and requeueing there is indistinguishable (to the fake's
// state) from requeueing the instant Process returned — a failed Process
// performs no delete, so there is no receipt handle whose invalidation could
// be observed differently.
func TestRepeatedFailureKeepsIncrementingReceiveCount(t *testing.T) {
	const deliveries = 4

	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	failed := errors.New("poison message: fails every time")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var counts, bodies bodyLog
	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, msg awsclient.Message) error {
			counts.add(msg.Attributes[receiveCountAttr])
			bodies.add(msg.Body)

			if len(counts.snapshot()) >= deliveries {
				cancel()
				return failed
			}
			// The visibility timeout elapses on this failing delivery, so the
			// message goes back on the queue and the loop picks it up again.
			f.ExpireInFlight(queueURL)
			return failed
		},
	})

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil — repeated processing failures are not a worker failure", err)
	}

	wantCounts := []string{"1", "2", "3", "4"}
	if got := counts.snapshot(); !equalStrings(got, wantCounts) {
		t.Errorf("%s across deliveries = %v, want %v — the counter a redrive policy compares against maxReceiveCount must climb by one per genuine retry",
			receiveCountAttr, got, wantCounts)
	}
	if got := bodies.snapshot(); !equalStrings(got, []string{"m1", "m1", "m1", "m1"}) {
		t.Errorf("delivered bodies = %v, want m1 four times — every retry must be the same message, not a new one", got)
	}
	if len(f.Deleted[queueURL]) != 0 {
		t.Errorf("deleted %v, want nothing — a message that never processes successfully must never be deleted", f.Deleted[queueURL])
	}
}

// ── property 4: slow-but-successful is not redelivered ──────────────────

// TestSlowSuccessfulMessageIsNotRedelivered is WR-024's second Done-when:
// "a slow-but-successful message is not redelivered". A message whose
// processing takes a long time is safe as long as the queue's visibility
// timeout is tuned to exceed that duration — which, expressed in this
// fake's terms, is exactly "no expiry fires between the receive and the
// delete". This test therefore never fires one, and proves the message is
// unavailable throughout AND gone afterwards.
//
// "Slow" is modeled with a blocking channel rather than a sleep: the pause
// lasts exactly as long as the test wants and adds nothing to the suite's
// runtime. Wall-clock duration is irrelevant to the property — what matters
// is only the ORDER of the delete and the (never fired) expiry.
func TestSlowSuccessfulMessageIsNotRedelivered(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	release := make(chan struct{})

	w := worker.New(worker.Worker{
		SQSClient:     f,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, _ awsclient.Message) error {
			close(started)
			// Arbitrarily long work, under the test's control.
			<-release
			cancel()
			return nil
		},
	})

	done := startWorker(w, ctx)

	select {
	case <-started:
	case <-done:
		t.Fatal("Run returned before the message was ever processed")
	}

	// Mid-processing, with the visibility timeout NOT elapsed: the message is
	// checked out and invisible, so no other consumer can pick it up. This is
	// the tuned-correctly case — long work is not duplicated work.
	assertNothingAvailable(t, f, queueURL,
		"while a message is in flight and its visibility timeout has not elapsed, it must not be available to anyone else")

	close(release)

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if got := deletedBodies(f, queueURL); !equalStrings(got, []string{"m1"}) {
		t.Fatalf("deleted %v, want [m1] — slow processing still ends in a delete", got)
	}
	if got := len(f.Received[queueURL]); got != 1 {
		t.Errorf("the message was handed out %d time(s) over the whole run, want exactly 1 — slow-but-successful work must be delivered once", got)
	}
	// And it stays gone: the delete beat the timeout, so a later expiry finds
	// nothing to requeue.
	if got := f.ExpireInFlight(queueURL); got != 0 {
		t.Errorf("ExpireInFlight requeued %d message(s) after the delete, want 0", got)
	}
	assertNothingAvailable(t, f, queueURL,
		"a slow message that finished and was deleted must never be redelivered")
}

// ── property 5: why the timeout must be tuned ───────────────────────────

// TestVisibilityTimeoutElapsingMidProcessingCausesDuplicateDelivery is the
// negative counterpart, and the reason WR-024 asks for the timeout to be
// tuned to the work duration at all. If the timeout elapses while processing
// is still running, SQS hands the message to a second consumer even though
// the first one is about to succeed; the first one's receipt handle is dead
// by then, so its delete fails and the work is genuinely duplicated.
//
// The scope is deliberately internal/worker + the fake. WR-023's idempotency
// layer (internal/processing) is what stops such a duplicate from writing a
// second RESULT, and it has its own tests for that; what is proven here is
// the raw SQS mechanic underneath, which no amount of idempotency removes —
// the duplicate delivery, and the wasted second Process call, still happen.
//
// Note what this test does NOT claim: it is not a bug in Worker.Run. Run
// behaves correctly throughout — it processes, it attempts the delete, and it
// treats the delete failure as non-fatal. The defect being demonstrated is a
// CONFIGURATION one, a VisibilityTimeout shorter than the work takes.
func TestVisibilityTimeoutElapsingMidProcessingCausesDuplicateDelivery(t *testing.T) {
	f, queueURL := newFakeQueue(t)
	seed(t, f, queueURL, 1)

	rec := &deleteRecordingSQS{SQSClient: f}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		counts, handles bodyLog
		firstStarted    = make(chan struct{})
		releaseFirst    = make(chan struct{})
		calls           int
		callsMu         sync.Mutex
	)

	w := worker.New(worker.Worker{
		SQSClient:     rec,
		QueueURL:      queueURL,
		ShutdownGrace: longGrace,
		Process: func(_ context.Context, msg awsclient.Message) error {
			counts.add(msg.Attributes[receiveCountAttr])
			handles.add(msg.ReceiptHandle)

			callsMu.Lock()
			calls++
			first := calls == 1
			callsMu.Unlock()

			if first {
				close(firstStarted)
				<-releaseFirst
				// Succeeds — but too late: the handle it holds has expired.
				return nil
			}
			// The duplicate delivery. It succeeds and, holding a live handle,
			// really does delete the message.
			cancel()
			return nil
		},
	})

	done := startWorker(w, ctx)

	select {
	case <-firstStarted:
	case <-done:
		t.Fatal("Run returned before the message was ever processed")
	}

	// The visibility timeout elapses while the first Process call is still
	// running. The message goes straight back on the queue.
	if got := f.ExpireInFlight(queueURL); got != 1 {
		t.Fatalf("ExpireInFlight requeued %d message(s) mid-processing, want 1", got)
	}

	close(releaseFirst)

	if err := awaitRun(t, done, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil — a delete rejected for an expired receipt handle is not a worker failure", err)
	}

	// Two deliveries of ONE message: the duplicate work a too-short timeout
	// buys you.
	if got, want := counts.snapshot(), []string{"1", "2"}; !equalStrings(got, want) {
		t.Fatalf("%s across Process calls = %v, want %v — the timeout elapsing mid-processing must produce a second, redundant delivery",
			receiveCountAttr, got, want)
	}

	attempts := rec.deleteAttempts()
	if len(attempts) != 2 {
		t.Fatalf("DeleteMessage called %d time(s), want 2 (one per successful Process call): %+v", len(attempts), attempts)
	}
	// The first delete is rejected: the expiry killed that handle, so the
	// worker cannot delete a message that now belongs to a later delivery.
	// This is the assertion fake.Deleted alone cannot make — it records only
	// successes, so a delete that was never attempted and one that was
	// attempted and rejected look identical there.
	if !errors.Is(attempts[0].err, fake.ErrReceiptHandleNotFound) {
		t.Errorf("first DeleteMessage returned %v, want an error wrapping fake.ErrReceiptHandleNotFound — a receipt handle whose visibility timeout elapsed must no longer delete anything",
			attempts[0].err)
	}
	if handleSnapshot := handles.snapshot(); len(handleSnapshot) == 2 && attempts[0].receiptHandle != handleSnapshot[0] {
		t.Errorf("first DeleteMessage used receipt handle %q, want the first delivery's %q", attempts[0].receiptHandle, handleSnapshot[0])
	}
	// The second delete succeeds, so the message really does leave the queue
	// in the end — the duplicate cost work, not correctness of the drain.
	if attempts[1].err != nil {
		t.Errorf("second DeleteMessage returned %v, want nil — the redelivery's handle is live", attempts[1].err)
	}
	if got := len(f.Deleted[queueURL]); got != 1 {
		t.Errorf("%d successful delete(s) recorded, want exactly 1 — only the delivery holding a live handle may delete", got)
	}
	if got := f.ExpireInFlight(queueURL); got != 0 {
		t.Errorf("ExpireInFlight requeued %d message(s) after the successful delete, want 0", got)
	}
	assertNothingAvailable(t, f, queueURL, "the message was ultimately deleted by the second delivery")
}
