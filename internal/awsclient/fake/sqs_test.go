package fake_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
)

// newSQSWithQueue is the setup every SQS test starts from: a fake with one
// queue, plus that queue's URL.
func newSQSWithQueue(t *testing.T, name string, attrs map[string]string) (*fake.SQS, string) {
	t.Helper()
	f := fake.NewSQS()
	out, err := f.CreateQueue(context.Background(), awsclient.CreateQueueInput{Name: name, Attributes: attrs})
	if err != nil {
		t.Fatalf("setup: CreateQueue(%q) returned error %v, want nil", name, err)
	}
	return f, out.QueueUrl
}

// --- CreateQueue / GetQueueUrl --------------------------------------------

// TestSQSCreateQueueIsIdempotentOnName mirrors the SNS topic case, for the same
// reason: WR-018 ensures the DLQ and then the main queue on every reconcile, so
// repeated creates must converge on one queue with a stable URL. A fake that
// minted a new URL per call would break every subsequent send/receive in a
// multi-reconcile test.
func TestSQSCreateQueueIsIdempotentOnName(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSQS()

	first, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work"})
	if err != nil {
		t.Fatalf("first CreateQueue returned error %v, want nil", err)
	}
	if first.QueueUrl == "" {
		t.Fatal("CreateQueue returned an empty QueueUrl, want a non-empty URL")
	}

	for i := 2; i <= 4; i++ {
		again, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work"})
		if err != nil {
			t.Fatalf("CreateQueue call #%d returned error %v, want nil", i, err)
		}
		if again.QueueUrl != first.QueueUrl {
			t.Errorf("CreateQueue call #%d returned URL %q, want the existing %q",
				i, again.QueueUrl, first.QueueUrl)
		}
	}

	if got := len(f.Queues); got != 1 {
		t.Errorf("Queues holds %d entries after four calls with the same name, want 1: %v", got, f.Queues)
	}
	if got := f.Queues["weir-work"]; got != first.QueueUrl {
		t.Errorf("Queues[%q] = %q, want %q", "weir-work", got, first.QueueUrl)
	}
}

// TestSQSCreateQueueDistinctNamesGetDistinctURLs: the main queue and its DLQ
// coexist in every pipeline (WR-018), so they must not alias.
func TestSQSCreateQueueDistinctNamesGetDistinctURLs(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSQS()

	urls := map[string]string{}
	for _, name := range []string{"weir-work", "weir-work-dlq"} {
		out, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: name})
		if err != nil {
			t.Fatalf("CreateQueue(%q) returned error %v, want nil", name, err)
		}
		if prev, dup := urls[out.QueueUrl]; dup {
			t.Fatalf("CreateQueue(%q) reused the URL %q already issued to %q", name, out.QueueUrl, prev)
		}
		urls[out.QueueUrl] = name
	}
}

// TestSQSCreateQueueStoresTheInitialAttributesAndSynthesizesTheARN covers the
// two things WR-018 needs from a create: the attributes it passed (visibility
// timeout, redrive policy) are readable afterwards, and the queue has a
// QueueArn — which is the value the redrive policy on the main queue and the
// SNS subscription both need, and which real SQS synthesizes server-side.
func TestSQSCreateQueueStoresTheInitialAttributesAndSynthesizesTheARN(t *testing.T) {
	ctx := context.Background()
	attrs := map[string]string{
		"VisibilityTimeout":      "45",
		"MessageRetentionPeriod": "1209600",
	}
	f, url := newSQSWithQueue(t, "weir-work", attrs)

	got, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	for k, want := range attrs {
		if got.Attributes[k] != want {
			t.Errorf("attribute %q = %q, want %q", k, got.Attributes[k], want)
		}
	}

	arn := got.Attributes["QueueArn"]
	if arn == "" {
		t.Error("QueueArn is empty after CreateQueue; the fake must synthesize one, since WR-018's " +
			"redrive policy and the SNS subscription are both built from it")
	}
	if want := "arn:aws:sqs:local:000000000000:weir-work"; arn != want {
		t.Errorf("QueueArn = %q, want %q (derived from the queue name)", arn, want)
	}
}

// TestSQSCreateQueueCopiesTheCallerAttributes: a reconciler that builds one
// attribute map, creates the DLQ, then mutates the map to add the redrive
// policy for the main queue is entirely reasonable. If the fake retained the
// caller's map, the DLQ would retroactively "have" a redrive policy pointing at
// itself, and the resulting test failure would be baffling.
func TestSQSCreateQueueCopiesTheCallerAttributes(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSQS()

	attrs := map[string]string{"VisibilityTimeout": "30"}
	out, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work", Attributes: attrs})
	if err != nil {
		t.Fatalf("CreateQueue returned error %v, want nil", err)
	}

	attrs["VisibilityTimeout"] = "999"
	attrs["RedrivePolicy"] = `{"deadLetterTargetArn":"arn:oops"}`

	got, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: out.QueueUrl})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	if got.Attributes["VisibilityTimeout"] != "30" {
		t.Errorf("VisibilityTimeout = %q after the caller mutated its map, want %q: CreateQueue must "+
			"copy the attributes", got.Attributes["VisibilityTimeout"], "30")
	}
	if _, present := got.Attributes["RedrivePolicy"]; present {
		t.Error("RedrivePolicy appeared after the caller mutated its map; CreateQueue must copy the attributes")
	}
}

// TestSQSCreateQueueOnAnExistingNameIgnoresNewAttributes pins deliberate fake
// behavior that is easy to trip over: the second create is a pure no-op, so
// attributes passed to it are DISCARDED, not applied.
//
// This is the faithful choice. Real SQS returns QueueAlreadyExists when a create
// names an existing queue with different attributes — it certainly does not
// quietly update them — so any code that expects "create again to change an
// attribute" is wrong against AWS too. The correct call is SetQueueAttributes,
// and this test is what steers WR-018 there.
func TestSQSCreateQueueOnAnExistingNameIgnoresNewAttributes(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", map[string]string{"VisibilityTimeout": "30"})

	if _, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{
		Name: "weir-work", Attributes: map[string]string{"VisibilityTimeout": "600"},
	}); err != nil {
		t.Fatalf("second CreateQueue returned error %v, want nil", err)
	}

	got, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	if got.Attributes["VisibilityTimeout"] != "30" {
		t.Errorf("VisibilityTimeout = %q after re-creating the queue with a new value, want the "+
			"original %q: re-creating an existing queue is a no-op, use SetQueueAttributes to change "+
			"attributes", got.Attributes["VisibilityTimeout"], "30")
	}
}

// TestSQSGetQueueUrl covers both halves of WR-018's lookup-by-name: the queue
// exists, and — the branch that decides whether to create — it does not, which
// must report the ErrQueueNotFound sentinel so a caller can tell "absent" apart
// from "call failed" with errors.Is.
func TestSQSGetQueueUrl(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	t.Run("existing queue", func(t *testing.T) {
		out, err := f.GetQueueUrl(ctx, awsclient.GetQueueUrlInput{Name: "weir-work"})
		if err != nil {
			t.Fatalf("GetQueueUrl returned error %v, want nil", err)
		}
		if out.QueueUrl != url {
			t.Errorf("GetQueueUrl = %q, want %q", out.QueueUrl, url)
		}
	})

	t.Run("unknown queue reports ErrQueueNotFound", func(t *testing.T) {
		out, err := f.GetQueueUrl(ctx, awsclient.GetQueueUrlInput{Name: "no-such-queue"})
		if !errors.Is(err, fake.ErrQueueNotFound) {
			t.Fatalf("GetQueueUrl error = %v, want one matching fake.ErrQueueNotFound", err)
		}
		if out.QueueUrl != "" {
			t.Errorf("failed GetQueueUrl reported URL %q, want the zero value", out.QueueUrl)
		}
	})

	t.Run("lookup is by name, not by URL", func(t *testing.T) {
		if _, err := f.GetQueueUrl(ctx, awsclient.GetQueueUrlInput{Name: url}); !errors.Is(err, fake.ErrQueueNotFound) {
			t.Errorf("GetQueueUrl(<the URL>) error = %v, want ErrQueueNotFound: the input is a queue "+
				"name", err)
		}
	})
}

// TestSQSOperationsOnAnUnknownQueueReportErrQueueNotFound sweeps every method
// that takes a queue URL. Getting this wrong in a fake (silently succeeding, or
// auto-creating the queue) would hide the most common integration mistake there
// is: passing a queue name where a URL is expected, or using a stale URL.
//
// DeleteMessage is deliberately absent — see
// TestSQSDeleteMessageValidatesTheReceiptHandleNotTheQueue.
func TestSQSOperationsOnAnUnknownQueueReportErrQueueNotFound(t *testing.T) {
	ctx := context.Background()
	const unknown = "https://queue.local/000000000000/never-created"

	cases := []struct {
		name string
		call func(f *fake.SQS) error
	}{
		{"GetQueueAttributes", func(f *fake.SQS) error {
			_, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: unknown})
			return err
		}},
		{"SetQueueAttributes", func(f *fake.SQS) error {
			_, err := f.SetQueueAttributes(ctx, awsclient.SetQueueAttributesInput{
				QueueUrl: unknown, Attributes: map[string]string{"VisibilityTimeout": "30"},
			})
			return err
		}},
		{"SendMessage", func(f *fake.SQS) error {
			_, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: unknown, Body: "hi"})
			return err
		}},
		{"ReceiveMessage", func(f *fake.SQS) error {
			_, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: unknown})
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A fake that does have another queue, to prove the check is per URL.
			f, _ := newSQSWithQueue(t, "weir-work", nil)
			if err := tc.call(f); !errors.Is(err, fake.ErrQueueNotFound) {
				t.Errorf("%s on an unknown queue URL error = %v, want one matching fake.ErrQueueNotFound",
					tc.name, err)
			}
		})
	}
}

// --- GetQueueAttributes / SetQueueAttributes ------------------------------

// TestSQSSetQueueAttributesMergesRatherThanReplaces pins the documented
// semantics: attributes not mentioned survive. WR-018 sets the redrive policy in
// a step of its own, after the queue already carries a visibility timeout, so a
// replace-everything fake would silently drop the timeout and make the reconcile
// look convergent when it is not.
func TestSQSSetQueueAttributesMergesRatherThanReplaces(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", map[string]string{"VisibilityTimeout": "30"})

	const redrive = `{"deadLetterTargetArn":"arn:aws:sqs:local:000000000000:weir-work-dlq","maxReceiveCount":"5"}`
	if _, err := f.SetQueueAttributes(ctx, awsclient.SetQueueAttributesInput{
		QueueUrl: url, Attributes: map[string]string{"RedrivePolicy": redrive},
	}); err != nil {
		t.Fatalf("SetQueueAttributes returned error %v, want nil", err)
	}

	got, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	if got.Attributes["RedrivePolicy"] != redrive {
		t.Errorf("RedrivePolicy = %q, want %q", got.Attributes["RedrivePolicy"], redrive)
	}
	if got.Attributes["VisibilityTimeout"] != "30" {
		t.Errorf("VisibilityTimeout = %q after setting an unrelated attribute, want the original %q: "+
			"SetQueueAttributes merges", got.Attributes["VisibilityTimeout"], "30")
	}
	if got.Attributes["QueueArn"] == "" {
		t.Error("QueueArn was lost by SetQueueAttributes, want it preserved")
	}

	// An overwrite of an existing attribute does replace that one value.
	if _, err := f.SetQueueAttributes(ctx, awsclient.SetQueueAttributesInput{
		QueueUrl: url, Attributes: map[string]string{"VisibilityTimeout": "120"},
	}); err != nil {
		t.Fatalf("second SetQueueAttributes returned error %v, want nil", err)
	}
	got, err = f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	if got.Attributes["VisibilityTimeout"] != "120" {
		t.Errorf("VisibilityTimeout = %q after an explicit overwrite, want %q",
			got.Attributes["VisibilityTimeout"], "120")
	}
}

// TestSQSGetQueueAttributesFiltersByName covers the AttributeNames contract in
// one table, because WR-031's backlog poll will ask for exactly one attribute
// and a fake that ignored the filter would let it accidentally depend on
// attributes it never requested.
func TestSQSGetQueueAttributesFiltersByName(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", map[string]string{
		"VisibilityTimeout": "30",
		"RedrivePolicy":     "{}",
	})
	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "one"}); err != nil {
		t.Fatalf("setup: SendMessage returned error %v, want nil", err)
	}

	// Every attribute the queue has at this point.
	allNames := []string{"VisibilityTimeout", "RedrivePolicy", "QueueArn", "ApproximateNumberOfMessages"}

	cases := []struct {
		name  string
		names []string
		want  []string // attribute names expected in the result, exactly
	}{
		{
			name:  "empty AttributeNames requests everything",
			names: nil,
			want:  allNames,
		},
		{
			name:  `"All" requests everything`,
			names: []string{"All"},
			want:  allNames,
		},
		{
			name:  `"All" alongside other names still requests everything`,
			names: []string{"QueueArn", "All"},
			want:  allNames,
		},
		{
			name:  "a single name returns only that attribute",
			names: []string{"ApproximateNumberOfMessages"},
			want:  []string{"ApproximateNumberOfMessages"},
		},
		{
			name:  "several names return exactly those",
			names: []string{"QueueArn", "VisibilityTimeout"},
			want:  []string{"QueueArn", "VisibilityTimeout"},
		},
		{
			name:  "an unset attribute name is omitted rather than returned empty",
			names: []string{"QueueArn", "DelaySeconds"},
			want:  []string{"QueueArn"},
		},
		{
			name:  "only unset names yield an empty, non-nil map",
			names: []string{"DelaySeconds"},
			want:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
				QueueUrl: url, AttributeNames: tc.names,
			})
			if err != nil {
				t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
			}
			if got.Attributes == nil {
				t.Fatal("Attributes is nil, want a non-nil (possibly empty) map so callers can index it safely")
			}
			if len(got.Attributes) != len(tc.want) {
				t.Errorf("got %d attributes %v, want %d %v",
					len(got.Attributes), keysOf(got.Attributes), len(tc.want), tc.want)
			}
			for _, name := range tc.want {
				if _, ok := got.Attributes[name]; !ok {
					t.Errorf("attribute %q missing from the result %v", name, keysOf(got.Attributes))
				}
			}
		})
	}
}

// TestSQSApproximateNumberOfMessagesCountsOnlyPendingMessages is the most
// load-bearing SQS behavior in this file, because WR-031 derives replica counts
// from it (ADR-002, scale from backlog).
//
// The semantics, verified against the implementation and matching real SQS:
// ApproximateNumberOfMessages counts messages VISIBLE in the queue. Receiving a
// message removes it from that count immediately (it is in flight — real SQS
// would report it under ApproximateNumberOfMessagesNotVisible, which this fake
// does not model), so the count drops on RECEIVE, and deleting an already
// received message does not change it further.
//
// Consequence worth stating for WR-031: this number is backlog waiting to be
// picked up, NOT total outstanding work. A queue whose messages are all in
// flight reports 0.
func TestSQSApproximateNumberOfMessagesCountsOnlyPendingMessages(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	count := func() string {
		t.Helper()
		out, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
			QueueUrl: url, AttributeNames: []string{"ApproximateNumberOfMessages"},
		})
		if err != nil {
			t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
		}
		return out.Attributes["ApproximateNumberOfMessages"]
	}

	if got := count(); got != "0" {
		t.Fatalf("a fresh queue reports ApproximateNumberOfMessages = %q, want %q", got, "0")
	}

	for i := 1; i <= 3; i++ {
		if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{
			QueueUrl: url, Body: "body-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("SendMessage #%d returned error %v, want nil", i, err)
		}
		if got, want := count(), strconv.Itoa(i); got != want {
			t.Errorf("after %d sends the count is %q, want %q", i, got, want)
		}
	}

	rcv, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 1})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(rcv.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1", len(rcv.Messages))
	}
	if got := count(); got != "2" {
		t.Errorf("after receiving 1 of 3 the count is %q, want %q: an in-flight message is not visible "+
			"in the queue, so the count drops on receive", got, "2")
	}

	if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
		QueueUrl: url, ReceiptHandle: rcv.Messages[0].ReceiptHandle,
	}); err != nil {
		t.Fatalf("DeleteMessage returned error %v, want nil", err)
	}
	if got := count(); got != "2" {
		t.Errorf("after deleting the received message the count is %q, want it unchanged at %q: the "+
			"message already left the visible count when it was received", got, "2")
	}

	rest, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 10})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(rest.Messages) != 2 {
		t.Fatalf("ReceiveMessage returned %d messages, want the remaining 2", len(rest.Messages))
	}
	if got := count(); got != "0" {
		t.Errorf("with every message in flight the count is %q, want %q", got, "0")
	}
}

// TestSQSApproximateNumberOfMessagesIsAlwaysComputedNotStored: the count is
// derived state, so a caller cannot seed or override it. Without this, a test
// could "helpfully" set ApproximateNumberOfMessages to fake a backlog and then
// silently diverge from what send/receive actually did — the worst kind of
// green test for WR-031's scaling math.
func TestSQSApproximateNumberOfMessagesIsAlwaysComputedNotStored(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", map[string]string{"ApproximateNumberOfMessages": "999"})

	read := func(when string) string {
		t.Helper()
		out, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: url})
		if err != nil {
			t.Fatalf("GetQueueAttributes (%s) returned error %v, want nil", when, err)
		}
		return out.Attributes["ApproximateNumberOfMessages"]
	}

	if got := read("seeded via CreateQueue"); got != "0" {
		t.Errorf("ApproximateNumberOfMessages = %q on an empty queue seeded with 999, want %q: the "+
			"count is computed from the queue's real contents, never taken from stored attributes", got, "0")
	}

	if _, err := f.SetQueueAttributes(ctx, awsclient.SetQueueAttributesInput{
		QueueUrl: url, Attributes: map[string]string{"ApproximateNumberOfMessages": "42"},
	}); err != nil {
		t.Fatalf("SetQueueAttributes returned error %v, want nil", err)
	}
	if got := read("seeded via SetQueueAttributes"); got != "0" {
		t.Errorf("ApproximateNumberOfMessages = %q after setting it to 42 on an empty queue, want %q",
			got, "0")
	}

	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "real"}); err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}
	if got := read("after a real send"); got != "1" {
		t.Errorf("ApproximateNumberOfMessages = %q after one send, want %q", got, "1")
	}
}

// TestSQSGetQueueAttributesReturnsACopy: attributes are handed out as a fresh
// map, so a caller that adds or edits entries in the result is not editing the
// queue. Returning the internal map would turn a read into a write.
func TestSQSGetQueueAttributesReturnsACopy(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", map[string]string{"VisibilityTimeout": "30"})

	first, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	first.Attributes["VisibilityTimeout"] = "clobbered"
	first.Attributes["Injected"] = "nope"

	second, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("second GetQueueAttributes returned error %v, want nil", err)
	}
	if second.Attributes["VisibilityTimeout"] != "30" {
		t.Errorf("VisibilityTimeout = %q after the caller mutated the first result, want %q",
			second.Attributes["VisibilityTimeout"], "30")
	}
	if _, present := second.Attributes["Injected"]; present {
		t.Error("an attribute added to the first result leaked into the queue; GetQueueAttributes must " +
			"return a copy")
	}
}

// --- SendMessage / ReceiveMessage / DeleteMessage --------------------------

// TestSQSSendThenReceiveDeliversTheMessage is the worker's happy path (WR-021):
// what was sent comes back with its body intact, a message id, a receipt handle
// to delete it with, and an ApproximateReceiveCount a retry-aware consumer
// (WR-024) can read.
func TestSQSSendThenReceiveDeliversTheMessage(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	const body = `{"Type":"Notification","Message":"{}"}`
	sent, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: body})
	if err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}
	if sent.MessageId == "" {
		t.Fatal("SendMessage returned an empty MessageId, want a non-empty id")
	}

	out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
		QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 20,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(out.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1", len(out.Messages))
	}

	got := out.Messages[0]
	if got.Body != body {
		t.Errorf("received body = %q, want %q", got.Body, body)
	}
	if got.MessageId != sent.MessageId {
		t.Errorf("received MessageId = %q, want the %q SendMessage reported", got.MessageId, sent.MessageId)
	}
	if got.ReceiptHandle == "" {
		t.Error("received message has an empty ReceiptHandle; without one it cannot be deleted")
	}
	if got.ReceiptHandle == got.MessageId {
		t.Error("ReceiptHandle equals MessageId; they are distinct identifiers in SQS and conflating " +
			"them would let a caller delete by message id, which real SQS rejects")
	}
	if got.Attributes["ApproximateReceiveCount"] != "1" {
		t.Errorf("ApproximateReceiveCount = %q on first delivery, want %q",
			got.Attributes["ApproximateReceiveCount"], "1")
	}

	// The spy records mirror the calls.
	if len(f.Sent[url]) != 1 || f.Sent[url][0].Body != body {
		t.Errorf("Sent[%q] = %+v, want one entry with the sent body", url, f.Sent[url])
	}
	if len(f.Received[url]) != 1 || f.Received[url][0].ReceiptHandle != got.ReceiptHandle {
		t.Errorf("Received[%q] = %+v, want one entry matching the returned message", url, f.Received[url])
	}
}

// TestSQSReceiveMessageOnAnEmptyQueueReturnsNothing: an empty queue is not an
// error, and — the part that matters for a unit test — the fake does not
// actually long-poll. WaitTimeSeconds is accepted and ignored, so a worker loop
// under test spins fast and deterministically instead of sleeping.
func TestSQSReceiveMessageOnAnEmptyQueueReturnsNothing(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
		QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 20,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage on an empty queue returned error %v, want nil", err)
	}
	if len(out.Messages) != 0 {
		t.Errorf("ReceiveMessage on an empty queue returned %+v, want no messages", out.Messages)
	}
}

// TestSQSReceiveMessageIsFIFOAndRespectsMaxNumberOfMessages: the batching rules
// the worker's consume loop is written against. The MaxNumberOfMessages <= 0
// case is the one worth pinning — it means 1, matching real SQS's default, so a
// caller that forgets to set it gets one message rather than the whole queue.
func TestSQSReceiveMessageIsFIFOAndRespectsMaxNumberOfMessages(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name  string
		max   int32
		want  []string // bodies expected from the first receive, in order
		after []string // bodies still pending afterwards
	}{
		{"max unset means one message", 0, []string{"a"}, []string{"b", "c", "d"}},
		{"negative max means one message", -5, []string{"a"}, []string{"b", "c", "d"}},
		{"max 1", 1, []string{"a"}, []string{"b", "c", "d"}},
		{"max 2 returns the two oldest, in order", 2, []string{"a", "b"}, []string{"c", "d"}},
		{"max equal to the depth drains the queue", 4, []string{"a", "b", "c", "d"}, nil},
		{"max above the depth returns what is there", 10, []string{"a", "b", "c", "d"}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, url := newSQSWithQueue(t, "weir-work", nil)
			for _, body := range []string{"a", "b", "c", "d"} {
				if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: body}); err != nil {
					t.Fatalf("SendMessage(%q) returned error %v, want nil", body, err)
				}
			}

			out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
				QueueUrl: url, MaxNumberOfMessages: tc.max,
			})
			if err != nil {
				t.Fatalf("ReceiveMessage returned error %v, want nil", err)
			}
			if got := bodiesOf(out.Messages); !equalStrings(got, tc.want) {
				t.Errorf("ReceiveMessage(max=%d) returned bodies %v, want %v (oldest first)",
					tc.max, got, tc.want)
			}

			rest, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
				QueueUrl: url, MaxNumberOfMessages: 10,
			})
			if err != nil {
				t.Fatalf("second ReceiveMessage returned error %v, want nil", err)
			}
			if got := bodiesOf(rest.Messages); !equalStrings(got, tc.after) {
				t.Errorf("the second ReceiveMessage returned %v, want %v: a message already in flight "+
					"must not be handed out again", got, tc.after)
			}
		})
	}
}

// TestSQSReceiveMessageDoesNotRedeliverAnInFlightMessage states the fake's
// central simplification explicitly, since a reader could otherwise mistake it
// for a bug: the fake has no wall clock, so a received message that is never
// deleted stays in flight indefinitely and no amount of receiving will return
// it.
//
// That is a deliberate design choice, not a limitation: time never leaks into
// the fake (ADR-003), so a test that needs the visibility timeout to elapse
// says so explicitly by calling ExpireInFlight instead of sleeping — see the
// ExpireInFlight section below, which is what makes WR-024's
// redelivery-on-failure path testable here rather than only on LocalStack. What
// this test pins is the other half: nothing becomes visible again on its own.
func TestSQSReceiveMessageDoesNotRedeliverAnInFlightMessage(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "only"}); err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}

	first, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(first.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1", len(first.Messages))
	}

	// Never deleted, and received again several times: still nothing.
	for i := range 3 {
		again, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
			QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 20,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage #%d returned error %v, want nil", i+2, err)
		}
		if len(again.Messages) != 0 {
			t.Fatalf("ReceiveMessage #%d returned %+v, want nothing: the message is in flight and this "+
				"fake never expires a visibility timeout", i+2, again.Messages)
		}
	}
}

// --- ExpireInFlight (simulated visibility-timeout expiry) ------------------

// depthOf reads a queue's ApproximateNumberOfMessages, the number WR-031 scales
// on. It is the externally visible proof that a message really did return to the
// backlog rather than merely becoming receivable.
func depthOf(t *testing.T, f *fake.SQS, url string) string {
	t.Helper()
	out, err := f.GetQueueAttributes(context.Background(), awsclient.GetQueueAttributesInput{
		QueueUrl: url, AttributeNames: []string{"ApproximateNumberOfMessages"},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes(%q) returned error %v, want nil", url, err)
	}
	return out.Attributes["ApproximateNumberOfMessages"]
}

// receiveOne receives exactly one message, failing the test if the queue does
// not hand one over.
func receiveOne(t *testing.T, f *fake.SQS, url string) awsclient.Message {
	t.Helper()
	out, err := f.ReceiveMessage(context.Background(), awsclient.ReceiveMessageInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(out.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1", len(out.Messages))
	}
	return out.Messages[0]
}

// TestSQSExpireInFlightRedeliversTheMessage is the redelivery path WR-024 needs:
// a worker that receives a message and fails to process it must eventually see
// that message again. ExpireInFlight is how a test says "the visibility timeout
// elapsed" without sleeping, which keeps the fake deterministic (ADR-003) — the
// alternative, a real timer, would make every such test slow and flaky.
//
// The redelivered message must be the SAME message (id and body) with a HIGHER
// ApproximateReceiveCount: that counter is what a retry-aware consumer reads to
// decide "this one keeps failing, let it go to the DLQ", so a fake that reset it
// to 1 on every redelivery would make a give-up-after-N-attempts policy
// untestable and apparently correct.
func TestSQSExpireInFlightRedeliversTheMessage(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", map[string]string{"VisibilityTimeout": "30"})

	sent, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "process-me"})
	if err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}

	first := receiveOne(t, f, url)
	if got := first.Attributes["ApproximateReceiveCount"]; got != "1" {
		t.Fatalf("ApproximateReceiveCount on first delivery = %q, want %q", got, "1")
	}
	if got := depthOf(t, f, url); got != "0" {
		t.Fatalf("queue depth with the message in flight = %q, want %q", got, "0")
	}

	if got := f.ExpireInFlight(url); got != 1 {
		t.Fatalf("ExpireInFlight returned %d, want 1 (the one in-flight message was requeued)", got)
	}
	if got := depthOf(t, f, url); got != "1" {
		t.Errorf("queue depth after the visibility timeout elapsed = %q, want %q: an expired message "+
			"returns to the visible backlog WR-031 scales on", got, "1")
	}

	second := receiveOne(t, f, url)
	if second.MessageId != sent.MessageId {
		t.Errorf("redelivered MessageId = %q, want the original %q: ExpireInFlight must requeue the same "+
			"message, not a new one", second.MessageId, sent.MessageId)
	}
	if second.Body != "process-me" {
		t.Errorf("redelivered body = %q, want %q", second.Body, "process-me")
	}
	if got := second.Attributes["ApproximateReceiveCount"]; got != "2" {
		t.Errorf("ApproximateReceiveCount on redelivery = %q, want %q: the count must carry over from the "+
			"previous delivery, since that is what a retry policy reads",
			got, "2")
	}
	if second.ReceiptHandle == first.ReceiptHandle {
		t.Error("the redelivery reused the first receipt handle, want a fresh one: real SQS issues a new " +
			"handle per delivery, and reusing it would hide a worker that held on to a stale handle")
	}

	// The spy record shows both deliveries, so a test can assert how many
	// attempts a message took.
	if got := len(f.Received[url]); got != 2 {
		t.Errorf("Received[%q] holds %d entries after two deliveries, want 2", url, got)
	}
}

// TestSQSExpireInFlightWithNothingInFlightReturnsZero: the return value is the
// number of messages requeued, so it is how a test asserts "nothing was
// outstanding". Each subtest is a state where the answer must be zero, and
// crucially where nothing else may move either — a pending message must NOT be
// swept up (it was never received, so no timeout is running on it), and a
// deleted one must not come back from the dead.
func TestSQSExpireInFlightWithNothingInFlightReturnsZero(t *testing.T) {
	ctx := context.Background()

	t.Run("a fresh queue", func(t *testing.T) {
		f, url := newSQSWithQueue(t, "weir-work", nil)
		if got := f.ExpireInFlight(url); got != 0 {
			t.Errorf("ExpireInFlight on an empty queue returned %d, want 0", got)
		}
	})

	t.Run("a queue whose messages were never received", func(t *testing.T) {
		f, url := newSQSWithQueue(t, "weir-work", nil)
		for _, body := range []string{"a", "b"} {
			if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: body}); err != nil {
				t.Fatalf("SendMessage(%q) returned error %v, want nil", body, err)
			}
		}

		if got := f.ExpireInFlight(url); got != 0 {
			t.Errorf("ExpireInFlight returned %d, want 0: a pending message has no visibility timeout "+
				"running, so there is nothing to expire", got)
		}
		if got := depthOf(t, f, url); got != "2" {
			t.Errorf("queue depth = %q, want it unchanged at %q", got, "2")
		}

		out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 10})
		if err != nil {
			t.Fatalf("ReceiveMessage returned error %v, want nil", err)
		}
		if got := bodiesOf(out.Messages); !equalStrings(got, []string{"a", "b"}) {
			t.Errorf("ReceiveMessage returned %v, want [a b] still in order: ExpireInFlight must not "+
				"reorder or duplicate untouched pending messages", got)
		}
		for _, m := range out.Messages {
			if got := m.Attributes["ApproximateReceiveCount"]; got != "1" {
				t.Errorf("ApproximateReceiveCount for %q = %q, want %q: the message was never delivered "+
					"before", m.Body, got, "1")
			}
		}
	})

	t.Run("a queue whose only message was already deleted", func(t *testing.T) {
		f, url := newSQSWithQueue(t, "weir-work", nil)
		if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "acked"}); err != nil {
			t.Fatalf("SendMessage returned error %v, want nil", err)
		}
		msg := receiveOne(t, f, url)
		if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
			QueueUrl: url, ReceiptHandle: msg.ReceiptHandle,
		}); err != nil {
			t.Fatalf("DeleteMessage returned error %v, want nil", err)
		}

		if got := f.ExpireInFlight(url); got != 0 {
			t.Errorf("ExpireInFlight returned %d, want 0: a deleted message is gone and must not be "+
				"resurrected by a later timeout", got)
		}
		if got := depthOf(t, f, url); got != "0" {
			t.Errorf("queue depth = %q, want %q", got, "0")
		}
	})

	t.Run("a queue that does not exist", func(t *testing.T) {
		f, _ := newSQSWithQueue(t, "weir-work", nil)
		if got := f.ExpireInFlight("https://queue.local/000000000000/never-created"); got != 0 {
			t.Errorf("ExpireInFlight on an unknown queue URL returned %d, want 0", got)
		}
	})
}

// TestSQSExpireInFlightInvalidatesTheOldReceiptHandle is the hazard a worker
// actually hits in production: it received a message, took too long, and by the
// time it calls DeleteMessage the visibility timeout has expired and its handle
// is dead — real SQS rejects it, and the message is already being processed by
// somebody else. A fake that kept the stale handle working would let a worker
// with no timeout discipline look correct, and would silently mask the
// double-processing window WR-024 has to reason about.
func TestSQSExpireInFlightInvalidatesTheOldReceiptHandle(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "slow"}); err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}
	stale := receiveOne(t, f, url).ReceiptHandle

	if got := f.ExpireInFlight(url); got != 1 {
		t.Fatalf("ExpireInFlight returned %d, want 1", got)
	}

	if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
		QueueUrl: url, ReceiptHandle: stale,
	}); !errors.Is(err, fake.ErrReceiptHandleNotFound) {
		t.Errorf("DeleteMessage with the pre-expiry handle error = %v, want one matching "+
			"fake.ErrReceiptHandleNotFound: an expired receipt handle must stop working immediately", err)
	}
	if got := f.Deleted[url]; len(got) != 0 {
		t.Errorf("Deleted[%q] = %v after a delete with a stale handle, want empty", url, got)
	}
	if got := depthOf(t, f, url); got != "1" {
		t.Errorf("queue depth = %q after the rejected delete, want %q: the message must still be there", got, "1")
	}

	// The handle from the redelivery is the one that works.
	fresh := receiveOne(t, f, url)
	if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
		QueueUrl: url, ReceiptHandle: fresh.ReceiptHandle,
	}); err != nil {
		t.Errorf("DeleteMessage with the redelivery's handle returned error %v, want nil", err)
	}
	if got := depthOf(t, f, url); got != "0" {
		t.Errorf("queue depth after the successful delete = %q, want %q", got, "0")
	}
}

// TestSQSApproximateReceiveCountClimbsAcrossRepeatedRedeliveries is the counter
// a give-up policy is written against: "after N attempts, stop retrying and let
// it go to the DLQ" (WR-024). One redelivery could pass with an implementation
// that hardcoded 2, so this walks several cycles and pins the whole sequence.
func TestSQSApproximateReceiveCountClimbsAcrossRepeatedRedeliveries(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "keeps-failing"}); err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}

	const attempts = 4
	handles := make(map[string]bool, attempts)

	for attempt := 1; attempt <= attempts; attempt++ {
		msg := receiveOne(t, f, url)
		if got, want := msg.Attributes["ApproximateReceiveCount"], strconv.Itoa(attempt); got != want {
			t.Fatalf("ApproximateReceiveCount on delivery #%d = %q, want %q", attempt, got, want)
		}
		if handles[msg.ReceiptHandle] {
			t.Errorf("delivery #%d reused receipt handle %q, want a fresh one per delivery",
				attempt, msg.ReceiptHandle)
		}
		handles[msg.ReceiptHandle] = true

		if attempt == attempts {
			// The last attempt succeeds and acks the message.
			if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
				QueueUrl: url, ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				t.Fatalf("DeleteMessage on the final attempt returned error %v, want nil", err)
			}
			break
		}
		// Processing "failed": the worker does not delete, and the timeout runs out.
		if got := f.ExpireInFlight(url); got != 1 {
			t.Fatalf("ExpireInFlight after attempt #%d returned %d, want 1", attempt, got)
		}
	}

	if got := depthOf(t, f, url); got != "0" {
		t.Errorf("queue depth after the message was finally acked = %q, want %q", got, "0")
	}
	if got := f.ExpireInFlight(url); got != 0 {
		t.Errorf("ExpireInFlight after the ack returned %d, want 0", got)
	}
	if got := len(f.Received[url]); got != attempts {
		t.Errorf("Received[%q] holds %d entries, want one per delivery (%d)", url, got, attempts)
	}
}

// TestSQSExpireInFlightRequeuesEveryInFlightMessageAheadOfNewerOnes covers the
// batch case and the documented ordering: expired messages go to the FRONT of
// the queue, ahead of anything sent while they were in flight. That is what
// makes a redelivered message the next thing a worker picks up, so a
// backlog-draining test does not have to consume the entire queue before seeing
// its retry.
//
// The relative order AMONG messages expired by one call is explicitly
// unspecified (the fake walks a Go map), so this asserts it as a set — a test
// that depended on that order would be flaky by construction.
func TestSQSExpireInFlightRequeuesEveryInFlightMessageAheadOfNewerOnes(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	send := func(body string) {
		t.Helper()
		if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: body}); err != nil {
			t.Fatalf("SendMessage(%q) returned error %v, want nil", body, err)
		}
	}

	send("a")
	send("b")

	inFlight, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 2})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(inFlight.Messages) != 2 {
		t.Fatalf("ReceiveMessage returned %d messages, want 2", len(inFlight.Messages))
	}

	// Arrived while a and b were being processed.
	send("c")

	if got := f.ExpireInFlight(url); got != 2 {
		t.Fatalf("ExpireInFlight returned %d, want 2 (both in-flight messages)", got)
	}
	if got := depthOf(t, f, url); got != "3" {
		t.Errorf("queue depth after expiring 2 with 1 newer message pending = %q, want %q", got, "3")
	}

	out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 10})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	bodies := bodiesOf(out.Messages)
	if len(bodies) != 3 {
		t.Fatalf("ReceiveMessage returned %v, want all three messages", bodies)
	}
	if bodies[2] != "c" {
		t.Errorf("the drained order was %v, want the two expired messages first and %q last: expired "+
			"messages are requeued at the front", bodies, "c")
	}
	if requeued := []string{bodies[0], bodies[1]}; !equalStrings(sortedCopy(requeued), []string{"a", "b"}) {
		t.Errorf("the first two messages were %v, want a and b in either order", requeued)
	}

	for _, m := range out.Messages {
		want := "2"
		if m.Body == "c" {
			want = "1"
		}
		if got := m.Attributes["ApproximateReceiveCount"]; got != want {
			t.Errorf("ApproximateReceiveCount for %q = %q, want %q", m.Body, got, want)
		}
	}
}

// TestSQSExpireInFlightIsScopedToOneQueue: every pipeline has at least two
// queues (main + DLQ, WR-018), and a fake keyed only by receipt handle could
// easily sweep both. Expiring one queue's timeout must leave the other's
// in-flight message exactly as it was — still invisible to a receive, and still
// deletable with the handle its receiver is holding.
func TestSQSExpireInFlightIsScopedToOneQueue(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSQS()

	main, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work"})
	if err != nil {
		t.Fatalf("CreateQueue(weir-work) returned error %v, want nil", err)
	}
	dlq, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work-dlq"})
	if err != nil {
		t.Fatalf("CreateQueue(weir-work-dlq) returned error %v, want nil", err)
	}

	for url, body := range map[string]string{main.QueueUrl: "main-msg", dlq.QueueUrl: "dlq-msg"} {
		if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: body}); err != nil {
			t.Fatalf("SendMessage(%q) returned error %v, want nil", body, err)
		}
	}
	mainInFlight := receiveOne(t, f, main.QueueUrl)
	dlqInFlight := receiveOne(t, f, dlq.QueueUrl)

	if got := f.ExpireInFlight(main.QueueUrl); got != 1 {
		t.Fatalf("ExpireInFlight(main) returned %d, want 1", got)
	}

	// The main queue's message came back...
	redelivered := receiveOne(t, f, main.QueueUrl)
	if redelivered.Body != "main-msg" {
		t.Errorf("the main queue redelivered %q, want %q", redelivered.Body, "main-msg")
	}
	if got := redelivered.Attributes["ApproximateReceiveCount"]; got != "2" {
		t.Errorf("the main queue's redelivery reports ApproximateReceiveCount %q, want %q", got, "2")
	}
	if redelivered.MessageId != mainInFlight.MessageId {
		t.Errorf("the main queue redelivered message id %q, want the original %q",
			redelivered.MessageId, mainInFlight.MessageId)
	}

	// ... and the DLQ's did not.
	out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
		QueueUrl: dlq.QueueUrl, MaxNumberOfMessages: 10,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage from the DLQ returned error %v, want nil", err)
	}
	if len(out.Messages) != 0 {
		t.Errorf("the DLQ delivered %v after expiring only the main queue, want nothing: its message is "+
			"still in flight", bodiesOf(out.Messages))
	}
	if got := depthOf(t, f, dlq.QueueUrl); got != "0" {
		t.Errorf("DLQ depth = %q, want %q (its message is still in flight)", got, "0")
	}
	if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
		QueueUrl: dlq.QueueUrl, ReceiptHandle: dlqInFlight.ReceiptHandle,
	}); err != nil {
		t.Errorf("DeleteMessage with the DLQ's untouched handle returned error %v, want nil: expiring "+
			"another queue must not invalidate it", err)
	}
}

// TestSQSDeleteMessageRemovesTheMessage is the acknowledgement half of the
// worker loop: a deleted message is gone for good, and the Deleted record makes
// "did the worker ack it?" directly assertable.
func TestSQSDeleteMessageRemovesTheMessage(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	for _, body := range []string{"first", "second"} {
		if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: body}); err != nil {
			t.Fatalf("SendMessage(%q) returned error %v, want nil", body, err)
		}
	}

	out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 2})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(out.Messages) != 2 {
		t.Fatalf("ReceiveMessage returned %d messages, want 2", len(out.Messages))
	}

	handle := out.Messages[0].ReceiptHandle
	if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
		QueueUrl: url, ReceiptHandle: handle,
	}); err != nil {
		t.Fatalf("DeleteMessage returned error %v, want nil", err)
	}

	if got := f.Deleted[url]; len(got) != 1 || got[0] != handle {
		t.Errorf("Deleted[%q] = %v, want exactly [%q]", url, got, handle)
	}

	t.Run("deleting the same handle twice reports ErrReceiptHandleNotFound", func(t *testing.T) {
		_, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{QueueUrl: url, ReceiptHandle: handle})
		if !errors.Is(err, fake.ErrReceiptHandleNotFound) {
			t.Errorf("second DeleteMessage error = %v, want one matching fake.ErrReceiptHandleNotFound", err)
		}
		if got := len(f.Deleted[url]); got != 1 {
			t.Errorf("Deleted[%q] holds %d handles after a failed delete, want 1", url, got)
		}
	})

	t.Run("an unknown handle reports ErrReceiptHandleNotFound", func(t *testing.T) {
		_, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
			QueueUrl: url, ReceiptHandle: "receipt-does-not-exist",
		})
		if !errors.Is(err, fake.ErrReceiptHandleNotFound) {
			t.Errorf("DeleteMessage error = %v, want one matching fake.ErrReceiptHandleNotFound", err)
		}
	})

	t.Run("the other in-flight message is still deletable", func(t *testing.T) {
		if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
			QueueUrl: url, ReceiptHandle: out.Messages[1].ReceiptHandle,
		}); err != nil {
			t.Errorf("DeleteMessage for the second message returned error %v, want nil", err)
		}
		if got := len(f.Deleted[url]); got != 2 {
			t.Errorf("Deleted[%q] holds %d handles, want 2", url, got)
		}
	})
}

// TestSQSDeleteMessageValidatesTheReceiptHandleNotTheQueue documents a real
// asymmetry in the fake, so nobody writes a test expecting ErrQueueNotFound
// here: DeleteMessage never checks that the queue exists. It looks the handle up
// and requires the handle's own queue to match the URL supplied, so an unknown
// queue URL yields ErrReceiptHandleNotFound.
//
// The behavior that matters is the pairing check, and it is the right one to
// have: it stops a test from "successfully" deleting a message from the wrong
// queue, which is exactly the copy-paste bug a multi-queue setup (main queue +
// DLQ) invites.
func TestSQSDeleteMessageValidatesTheReceiptHandleNotTheQueue(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSQS()

	main, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work"})
	if err != nil {
		t.Fatalf("CreateQueue(weir-work) returned error %v, want nil", err)
	}
	dlq, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work-dlq"})
	if err != nil {
		t.Fatalf("CreateQueue(weir-work-dlq) returned error %v, want nil", err)
	}

	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: main.QueueUrl, Body: "x"}); err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}
	rcv, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: main.QueueUrl})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(rcv.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1", len(rcv.Messages))
	}
	handle := rcv.Messages[0].ReceiptHandle

	t.Run("a handle from another queue is rejected", func(t *testing.T) {
		if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
			QueueUrl: dlq.QueueUrl, ReceiptHandle: handle,
		}); !errors.Is(err, fake.ErrReceiptHandleNotFound) {
			t.Errorf("DeleteMessage with the DLQ's URL error = %v, want one matching "+
				"fake.ErrReceiptHandleNotFound", err)
		}
		if len(f.Deleted[dlq.QueueUrl]) != 0 {
			t.Errorf("Deleted[dlq] = %v, want empty", f.Deleted[dlq.QueueUrl])
		}
	})

	t.Run("an unknown queue URL also reports ErrReceiptHandleNotFound", func(t *testing.T) {
		if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
			QueueUrl: "https://queue.local/000000000000/never-created", ReceiptHandle: handle,
		}); !errors.Is(err, fake.ErrReceiptHandleNotFound) {
			t.Errorf("DeleteMessage against an unknown queue error = %v, want one matching "+
				"fake.ErrReceiptHandleNotFound (DeleteMessage validates the handle, not the queue)", err)
		}
	})

	t.Run("the rejected deletes left the message in flight", func(t *testing.T) {
		if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
			QueueUrl: main.QueueUrl, ReceiptHandle: handle,
		}); err != nil {
			t.Errorf("DeleteMessage with the correct queue URL returned error %v, want nil", err)
		}
	})
}

// TestSQSQueuesAreIndependent: two queues in one fake — the shape every WR-018
// test has, since a pipeline always provisions a main queue and a DLQ — must not
// share messages, counts or records.
func TestSQSQueuesAreIndependent(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSQS()

	main, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work"})
	if err != nil {
		t.Fatalf("CreateQueue(weir-work) returned error %v, want nil", err)
	}
	dlq, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "weir-work-dlq"})
	if err != nil {
		t.Fatalf("CreateQueue(weir-work-dlq) returned error %v, want nil", err)
	}

	for i := range 3 {
		if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{
			QueueUrl: main.QueueUrl, Body: "main-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("SendMessage to the main queue returned error %v, want nil", err)
		}
	}
	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: dlq.QueueUrl, Body: "parked"}); err != nil {
		t.Fatalf("SendMessage to the DLQ returned error %v, want nil", err)
	}

	depth := func(url string) string {
		t.Helper()
		out, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
			QueueUrl: url, AttributeNames: []string{"ApproximateNumberOfMessages"},
		})
		if err != nil {
			t.Fatalf("GetQueueAttributes(%q) returned error %v, want nil", url, err)
		}
		return out.Attributes["ApproximateNumberOfMessages"]
	}

	if got := depth(main.QueueUrl); got != "3" {
		t.Errorf("main queue depth = %q, want %q", got, "3")
	}
	if got := depth(dlq.QueueUrl); got != "1" {
		t.Errorf("DLQ depth = %q, want %q", got, "1")
	}

	out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
		QueueUrl: dlq.QueueUrl, MaxNumberOfMessages: 10,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage from the DLQ returned error %v, want nil", err)
	}
	if got := bodiesOf(out.Messages); !equalStrings(got, []string{"parked"}) {
		t.Errorf("the DLQ delivered %v, want only [parked]: receiving from one queue must not reach "+
			"another's messages", got)
	}
	if got := depth(main.QueueUrl); got != "3" {
		t.Errorf("main queue depth = %q after draining the DLQ, want it unchanged at %q", got, "3")
	}
}

// TestSQSMessageIdsAreUniqueAcrossQueues: ids are the fake's stand-in for SQS
// message ids, which are globally unique. A per-queue counter would hand two
// messages the same id and break any test that tracks a message by id.
func TestSQSMessageIdsAreUniqueAcrossQueues(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSQS()

	urls := make([]string, 0, 2)
	for _, name := range []string{"q1", "q2"} {
		out, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: name})
		if err != nil {
			t.Fatalf("CreateQueue(%q) returned error %v, want nil", name, err)
		}
		urls = append(urls, out.QueueUrl)
	}

	ids := map[string]string{}
	for i := range 5 {
		for _, url := range urls {
			out, err := f.SendMessage(ctx, awsclient.SendMessageInput{
				QueueUrl: url, Body: "body-" + strconv.Itoa(i),
			})
			if err != nil {
				t.Fatalf("SendMessage returned error %v, want nil", err)
			}
			if prev, dup := ids[out.MessageId]; dup {
				t.Fatalf("MessageId %q was already issued (previously on %q, now %q)",
					out.MessageId, prev, url)
			}
			ids[out.MessageId] = url
		}
	}
	if len(ids) != 10 {
		t.Errorf("got %d distinct message ids, want 10", len(ids))
	}
}

// TestSQSSendMessageRecordsNothingWhenItFails: WR-048's load generator will
// need to test its own error handling, and that requires a failed send to leave
// no trace — neither in the backlog count nor in the Sent record.
func TestSQSSendMessageRecordsNothingWhenItFails(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)
	f.InjectError(fake.SQSMethodSendMessage, errInjected, 1)

	out, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "lost"})
	if !errors.Is(err, errInjected) {
		t.Fatalf("SendMessage error = %v, want one matching errInjected", err)
	}
	if out.MessageId != "" {
		t.Errorf("failed SendMessage reported MessageId %q, want the zero value", out.MessageId)
	}
	if len(f.Sent[url]) != 0 {
		t.Errorf("Sent[%q] = %+v after a failed send, want empty", url, f.Sent[url])
	}

	attrs, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
		QueueUrl: url, AttributeNames: []string{"ApproximateNumberOfMessages"},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	if got := attrs.Attributes["ApproximateNumberOfMessages"]; got != "0" {
		t.Errorf("queue depth = %q after a failed send, want %q", got, "0")
	}

	rcv, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 10})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(rcv.Messages) != 0 {
		t.Errorf("ReceiveMessage returned %+v after a failed send, want nothing", rcv.Messages)
	}

	// The retry lands.
	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "delivered"}); err != nil {
		t.Fatalf("SendMessage after the injected failure returned error %v, want nil", err)
	}
	if len(f.Sent[url]) != 1 {
		t.Errorf("Sent[%q] holds %d entries after the retry, want 1", url, len(f.Sent[url]))
	}
}

// TestSQSReceiveMessageFailureLeavesMessagesPending: an injected receive failure
// must not consume the backlog, otherwise a test of the worker's "poll failed,
// back off, poll again" path would silently lose the very messages it is meant
// to process.
func TestSQSReceiveMessageFailureLeavesMessagesPending(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "keep-me"}); err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}

	f.InjectError(fake.SQSMethodReceiveMessage, errInjected, 1)
	if _, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url}); !errors.Is(err, errInjected) {
		t.Fatalf("ReceiveMessage error = %v, want one matching errInjected", err)
	}

	attrs, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
		QueueUrl: url, AttributeNames: []string{"ApproximateNumberOfMessages"},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	if got := attrs.Attributes["ApproximateNumberOfMessages"]; got != "1" {
		t.Errorf("queue depth = %q after a failed receive, want %q: a failed poll must not consume "+
			"the message", got, "1")
	}

	out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("ReceiveMessage after the injected failure returned error %v, want nil", err)
	}
	if got := bodiesOf(out.Messages); !equalStrings(got, []string{"keep-me"}) {
		t.Errorf("the retried receive returned %v, want [keep-me]", got)
	}
}

// TestSQSDeleteMessageFailureKeepsTheMessageInFlight: the ack failed, so the
// message must remain claimed. In real SQS its visibility timeout would
// eventually expire and it would be redelivered; this fake keeps it in flight
// (see TestSQSReceiveMessageDoesNotRedeliverAnInFlightMessage), so what a test
// can assert is that the handle is still valid and a retried delete succeeds.
func TestSQSDeleteMessageFailureKeepsTheMessageInFlight(t *testing.T) {
	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", nil)

	if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "x"}); err != nil {
		t.Fatalf("SendMessage returned error %v, want nil", err)
	}
	rcv, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url})
	if err != nil {
		t.Fatalf("ReceiveMessage returned error %v, want nil", err)
	}
	if len(rcv.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1", len(rcv.Messages))
	}
	handle := rcv.Messages[0].ReceiptHandle

	f.InjectError(fake.SQSMethodDeleteMessage, errInjected, 1)
	if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
		QueueUrl: url, ReceiptHandle: handle,
	}); !errors.Is(err, errInjected) {
		t.Fatalf("DeleteMessage error = %v, want one matching errInjected", err)
	}
	if len(f.Deleted[url]) != 0 {
		t.Errorf("Deleted[%q] = %v after a failed delete, want empty", url, f.Deleted[url])
	}

	if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
		QueueUrl: url, ReceiptHandle: handle,
	}); err != nil {
		t.Fatalf("the retried DeleteMessage returned error %v, want nil: the handle must still be valid "+
			"after a failed delete", err)
	}
	if got := f.Deleted[url]; len(got) != 1 || got[0] != handle {
		t.Errorf("Deleted[%q] = %v, want exactly [%q]", url, got, handle)
	}
}

// --- concurrency ----------------------------------------------------------

// TestSQSConcurrentProducersAndConsumersIsRaceFree is the headline -race test:
// it drives the fake the way the real system will (WR-022 runs several worker
// goroutines against one client while a load generator sends) and asserts an
// exactly-once property that a broken lock would violate — every message is
// delivered to exactly one consumer, none is lost, none is delivered twice.
//
// Determinism: the send phase completes before the consume phase, and consumers
// stop as soon as the known message count has been received, so there is no
// reliance on timing or on scheduler luck. A message can only be counted by the
// goroutine that received it, so the total is a fixed number rather than a
// window.
func TestSQSConcurrentProducersAndConsumersIsRaceFree(t *testing.T) {
	const (
		producers = 32
		consumers = 8
	)

	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", map[string]string{"VisibilityTimeout": "30"})

	// Phase 1: concurrent sends.
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	wg.Add(producers)
	for i := range producers {
		go func(i int) {
			defer wg.Done()
			<-start
			if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{
				QueueUrl: url, Body: "body-" + strconv.Itoa(i),
			}); err != nil {
				t.Errorf("SendMessage returned error %v, want nil", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	attrs, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
		QueueUrl: url, AttributeNames: []string{"ApproximateNumberOfMessages"},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	if got, want := attrs.Attributes["ApproximateNumberOfMessages"], strconv.Itoa(producers); got != want {
		t.Fatalf("queue depth after %d concurrent sends = %q, want %q (no send may be lost to a race)",
			producers, got, want)
	}

	// Phase 2: concurrent receive+delete until the whole batch is consumed.
	var (
		mu       sync.Mutex
		received = map[string]int{}
		total    atomic.Int64
	)
	start = make(chan struct{})
	wg.Add(consumers)
	for range consumers {
		go func() {
			defer wg.Done()
			<-start
			for total.Load() < producers {
				out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
					QueueUrl: url, MaxNumberOfMessages: 3, WaitTimeSeconds: 1,
				})
				if err != nil {
					t.Errorf("ReceiveMessage returned error %v, want nil", err)
					return
				}
				for _, m := range out.Messages {
					mu.Lock()
					received[m.Body]++
					mu.Unlock()
					total.Add(1)

					if _, err := f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
						QueueUrl: url, ReceiptHandle: m.ReceiptHandle,
					}); err != nil {
						t.Errorf("DeleteMessage(%q) returned error %v, want nil", m.ReceiptHandle, err)
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(received) != producers {
		t.Errorf("consumed %d distinct bodies, want %d", len(received), producers)
	}
	for i := range producers {
		body := "body-" + strconv.Itoa(i)
		switch received[body] {
		case 1: // exactly once, as required
		case 0:
			t.Errorf("message %q was never delivered", body)
		default:
			t.Errorf("message %q was delivered %d times, want exactly 1", body, received[body])
		}
	}
	if got := len(f.Deleted[url]); got != producers {
		t.Errorf("Deleted[%q] holds %d handles, want %d", url, got, producers)
	}

	attrs, err = f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
		QueueUrl: url, AttributeNames: []string{"ApproximateNumberOfMessages"},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes returned error %v, want nil", err)
	}
	if got := attrs.Attributes["ApproximateNumberOfMessages"]; got != "0" {
		t.Errorf("queue depth after draining = %q, want %q", got, "0")
	}
}

// TestSQSConcurrentCreateQueueWithTheSameNameYieldsOneQueue is the SQS twin of
// the SNS idempotency race: several reconciles ensuring the same queue at once
// must converge on one URL.
func TestSQSConcurrentCreateQueueWithTheSameNameYieldsOneQueue(t *testing.T) {
	const goroutines = 64

	ctx := context.Background()
	f := fake.NewSQS()

	var (
		mu    sync.Mutex
		urls  = map[string]int{}
		wg    sync.WaitGroup
		start = make(chan struct{})
	)

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			<-start
			out, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{
				Name: "weir-work", Attributes: map[string]string{"VisibilityTimeout": "30"},
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Errorf("CreateQueue returned error %v, want nil", err)
				return
			}
			urls[out.QueueUrl]++
		}()
	}
	close(start)
	wg.Wait()

	if len(urls) != 1 {
		t.Errorf("concurrent CreateQueue calls with one name produced %d distinct URLs (%v), want 1",
			len(urls), urls)
	}
	if got := len(f.Queues); got != 1 {
		t.Errorf("Queues holds %d entries, want 1: %v", got, f.Queues)
	}
}

// TestSQSConcurrentReceiveAndExpireInFlightLosesNoMessage is the -race check for
// ExpireInFlight, and it asserts the invariant that makes the method safe to
// hand to concurrent workers (WR-022): moving a message from in-flight back to
// pending is atomic, so no message is lost and — the failure mode a missing lock
// would actually produce — none is duplicated into two places at once.
//
// Determinism: the sends complete before the burst, the burst is a fixed number
// of rounds, and afterwards the queue is drained to a standstill before
// anything is asserted. The path through the fake varies with scheduling; the
// final accounting does not.
func TestSQSConcurrentReceiveAndExpireInFlightLosesNoMessage(t *testing.T) {
	const (
		messages   = 32
		goroutines = 8
		rounds     = 5
	)

	ctx := context.Background()
	f, url := newSQSWithQueue(t, "weir-work", map[string]string{"VisibilityTimeout": "30"})

	for i := range messages {
		if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{
			QueueUrl: url, Body: "body-" + strconv.Itoa(i),
		}); err != nil {
			t.Fatalf("SendMessage returned error %v, want nil", err)
		}
	}

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			<-start
			for range rounds {
				if _, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
					QueueUrl: url, MaxNumberOfMessages: 3,
				}); err != nil {
					t.Errorf("ReceiveMessage returned error %v, want nil", err)
					return
				}
				// Nothing is deleted: every received message is handed straight
				// back, so the queue must end up with all 32 still in it.
				f.ExpireInFlight(url)
			}
		}()
	}
	close(start)
	wg.Wait()

	// Bring back anything left in flight when the burst ended, then drain.
	// Bounded on purpose: at most `messages` are in flight here and one call
	// requeues all of them, so messages+1 rounds is comfortably sufficient. An
	// ExpireInFlight that copies instead of moves (a lost delete from
	// f.inFlight) would never report zero, and an unbounded loop would hang the
	// whole package until the 10-minute test timeout instead of failing.
	expired := false
	for range messages + 1 {
		if f.ExpireInFlight(url) == 0 {
			expired = true
			break
		}
	}
	if !expired {
		t.Fatalf("ExpireInFlight never reported zero after %d rounds; it must move messages out of "+
			"flight, not copy them", messages+1)
	}

	// Same discipline on the receive-drain: every non-empty batch yields at
	// least one message and there are only `messages` of them, so more than
	// messages+1 batches means ReceiveMessage is not consuming pending.
	drained := map[string]int{}
	emptied := false
	for range messages + 1 {
		out, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
			QueueUrl: url, MaxNumberOfMessages: 10,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage while draining returned error %v, want nil", err)
		}
		if len(out.Messages) == 0 {
			emptied = true
			break
		}
		for _, m := range out.Messages {
			drained[m.Body]++
		}
	}
	if !emptied {
		t.Fatalf("the queue still returned messages after %d receive batches; ReceiveMessage must "+
			"remove what it hands out from pending", messages+1)
	}

	if len(drained) != messages {
		t.Errorf("the queue held %d distinct messages after the concurrent burst, want %d",
			len(drained), messages)
	}
	for i := range messages {
		body := "body-" + strconv.Itoa(i)
		switch drained[body] {
		case 1: // still in the queue exactly once, as required
		case 0:
			t.Errorf("message %q was lost: a concurrent ExpireInFlight must requeue it, not drop it", body)
		default:
			t.Errorf("message %q was in the queue %d times, want exactly 1: ExpireInFlight must move a "+
				"message from in flight to pending, never copy it", body, drained[body])
		}
	}
}

// --- helpers --------------------------------------------------------------

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	slices.Sort(out)
	return out
}

func bodiesOf(msgs []awsclient.Message) []string {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Body)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
