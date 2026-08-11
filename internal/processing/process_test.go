package processing

import (
	"context"
	"errors"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/events"
	"github.com/guycanella/weir/internal/idempotency"
	"github.com/guycanella/weir/internal/worker"
)

// This file pins the behavior of the worker.ProcessFunc returned by New
// (WR-023): parse a message, skip what the idempotency store has already
// seen, run the pluggable stub on the rest, and write each result to the
// output bucket. See helpers_test.go for the API being pinned and the five
// load-bearing design decisions behind it.

// --- construction / validation --------------------------------------------

// TestNewRejectsAnIncompleteConfig pins decision 1: a wiring mistake is an
// error from New, not a ProcessFunc that nil-panics (or silently writes to
// bucket "") on the first delivery. The returned ProcessFunc must be nil in
// that case, so a caller that ignores the error crashes at wiring time
// instead of half-working.
func TestNewRejectsAnIncompleteConfig(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{
			name:    "fully populated config is accepted",
			mutate:  func(*Config) {},
			wantErr: false,
		},
		{
			name:    "nil S3Client is rejected",
			mutate:  func(c *Config) { c.S3Client = nil },
			wantErr: true,
		},
		{
			name:    "empty OutputBucket is rejected",
			mutate:  func(c *Config) { c.OutputBucket = "" },
			wantErr: true,
		},
		{
			name:    "nil Store is rejected: without it there is no duplicate detection at all",
			mutate:  func(c *Config) { c.Store = nil },
			wantErr: true,
		},
		{
			// The stub is the one genuinely optional dependency: the demo has
			// a defensible default (DefaultStub), so requiring it would make
			// every caller restate it.
			name:    "nil Stub is accepted and defaults to DefaultStub",
			mutate:  func(c *Config) { c.Stub = nil },
			wantErr: false,
		},
		{
			name:    "empty ContentType is accepted and defaults to DefaultContentType",
			mutate:  func(c *Config) { c.ContentType = "" },
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := newSpyStub(`{"stub":"result"}`)
			cfg := Config{
				S3Client:     newRecordingS3(),
				OutputBucket: outputBucket,
				Store:        newMemStore(),
				Stub:         stub.fn,
				ContentType:  "application/json",
			}
			tc.mutate(&cfg)

			process, err := New(cfg)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("New returned a nil error for an invalid Config; want a non-nil error so the "+
						"misconfiguration surfaces at wiring time rather than on the first message (got process != nil: %t)",
						process != nil)
				}
				if process != nil {
					t.Error("New returned a non-nil ProcessFunc alongside an error; want nil, so a caller " +
						"that ignores the error cannot run a half-configured processor")
				}
				return
			}

			if err != nil {
				t.Fatalf("New returned error %v, want nil", err)
			}
			if process == nil {
				t.Fatal("New returned a nil ProcessFunc alongside a nil error")
			}
		})
	}
}

// TestNewReturnsAWorkerProcessFunc pins the return type by its NAMED type,
// not merely by signature. Go would happily assign a bare
// func(context.Context, awsclient.Message) error to a worker.ProcessFunc
// variable, so a plain assignment proves nothing; a type assertion on the
// dynamic type does. The point is that New's output drops straight into
// worker.Worker{Process: ...} with no adapter at the call site.
func TestNewReturnsAWorkerProcessFunc(t *testing.T) {
	h := newHarness(t)

	var got any = h.process
	if _, ok := got.(worker.ProcessFunc); !ok {
		t.Fatalf("New returned %T, want worker.ProcessFunc so it can be assigned to worker.Worker.Process directly", got)
	}
}

// --- the fresh event happy path -------------------------------------------

// TestFreshEventRunsTheStubAndWritesTheResult is the base case: an object
// write nobody has seen before is processed and its result lands in the
// OUTPUT bucket (never the input bucket, which would re-trigger the pipeline)
// at the derived key, with the stub's bytes verbatim, and the message is
// reported as done (nil) so worker.Run deletes it.
func TestFreshEventRunsTheStubAndWritesTheResult(t *testing.T) {
	h := newHarness(t)
	obj := objectPut{key: "raw/video1.mp4", size: 123456, etag: "abc123", versionID: "v42"}

	if err := h.process(context.Background(), message(t, obj)); err != nil {
		t.Fatalf("ProcessFunc returned error %v, want nil for a fresh event", err)
	}

	if got := h.stub.callCount(); got != 1 {
		t.Fatalf("stub ran %d times, want exactly 1", got)
	}
	if got, want := h.stub.seenEvents()[0], putEvent(t, obj); got != want {
		t.Errorf("stub received event %+v, want %+v (the parsed event, passed through unmodified)", got, want)
	}

	puts := h.s3.putInputs()
	if len(puts) != 1 {
		t.Fatalf("PutObject was called %d times, want exactly 1", len(puts))
	}
	want := awsclient.PutObjectInput{
		Bucket:      outputBucket,
		Key:         OutputKey(putEvent(t, obj)),
		Body:        []byte(`{"stub":"result"}`),
		ContentType: DefaultContentType,
	}
	if puts[0].Bucket != want.Bucket {
		t.Errorf("PutObject bucket = %q, want %q (cfg.OutputBucket, not the event's source bucket)", puts[0].Bucket, want.Bucket)
	}
	if puts[0].Key != want.Key {
		t.Errorf("PutObject key = %q, want %q", puts[0].Key, want.Key)
	}
	if string(puts[0].Body) != string(want.Body) {
		t.Errorf("PutObject body = %q, want the stub's bytes verbatim %q", puts[0].Body, want.Body)
	}
	if puts[0].ContentType != want.ContentType {
		t.Errorf("PutObject ContentType = %q, want %q when Config.ContentType is unset", puts[0].ContentType, want.ContentType)
	}

	if got := h.s3.storedKeys(outputBucket); len(got) != 1 || got[0] != want.Key {
		t.Errorf("output bucket holds %q, want exactly [%q]", got, want.Key)
	}
	if got := string(h.s3.storedBody(t, outputBucket, want.Key)); got != string(want.Body) {
		t.Errorf("stored body = %q, want %q", got, want.Body)
	}
	if got := h.s3.storedKeys(inputBucket); len(got) != 0 {
		t.Errorf("the input bucket received %q; the result must never be written back to the source bucket, "+
			"which would re-trigger the S3 -> SNS -> SQS pipeline on Weir's own output", got)
	}
}

// TestFreshEventMarksTheKeyInTheStore pins the other half of the happy path:
// the event's key is now recorded, via the identity key WR-008 defines. A
// processor that wrote the result without consulting/marking the store would
// pass the test above and fail every duplicate test below.
func TestFreshEventMarksTheKeyInTheStore(t *testing.T) {
	h := newHarness(t)
	obj := objectPut{key: "raw/video1.mp4", size: 123456, etag: "abc123", versionID: "v42"}

	if err := h.process(context.Background(), message(t, obj)); err != nil {
		t.Fatalf("ProcessFunc returned error %v, want nil", err)
	}

	key := idempotency.Key(inputBucket, obj.key, obj.versionID, obj.etag)
	if got := h.store.callKeys(); len(got) != 1 || got[0] != key {
		t.Fatalf("store was asked about %q, want exactly one call with %q — the key derived from the "+
			"event's four identity fields (bucket, key, versionID, etag)", got, key)
	}
	if !h.store.isMarked(key) {
		t.Error("the event's key is not marked seen after a successful process; the dedup check must go " +
			"through dedup.IsDuplicate, which both checks and marks")
	}
}

// TestConfiguredContentTypeIsUsedVerbatim pins that a caller supplying a
// custom stub can also declare what its bytes are, instead of having them
// mislabeled as the JSON the default stub emits.
func TestConfiguredContentTypeIsUsedVerbatim(t *testing.T) {
	s3 := newRecordingS3()
	stub := newSpyStub("PNG-ish bytes")
	process, err := New(Config{
		S3Client:     s3,
		OutputBucket: outputBucket,
		Store:        newMemStore(),
		Stub:         stub.fn,
		ContentType:  "image/png",
	})
	if err != nil {
		t.Fatalf("New returned error %v, want nil", err)
	}

	if err := process(context.Background(), message(t, objectPut{key: "raw/a.mp4", size: 1, etag: "ta"})); err != nil {
		t.Fatalf("ProcessFunc returned error %v, want nil", err)
	}

	puts := s3.putInputs()
	if len(puts) != 1 {
		t.Fatalf("PutObject was called %d times, want 1", len(puts))
	}
	if puts[0].ContentType != "image/png" {
		t.Errorf("PutObject ContentType = %q, want the configured %q", puts[0].ContentType, "image/png")
	}
	if string(puts[0].Body) != "PNG-ish bytes" {
		t.Errorf("PutObject body = %q, want the stub's bytes verbatim", puts[0].Body)
	}
}

// TestNilStubFallsBackToDefaultStub pins that the documented default is
// actually wired in, not merely documented: with Stub nil the result written
// is exactly what DefaultStub produces for that event.
func TestNilStubFallsBackToDefaultStub(t *testing.T) {
	s3 := newRecordingS3()
	process, err := New(Config{
		S3Client:     s3,
		OutputBucket: outputBucket,
		Store:        newMemStore(),
	})
	if err != nil {
		t.Fatalf("New returned error %v, want nil", err)
	}

	obj := objectPut{key: "raw/video1.mp4", size: 123456, etag: "abc123", versionID: "v42"}
	if err := process(context.Background(), message(t, obj)); err != nil {
		t.Fatalf("ProcessFunc returned error %v, want nil", err)
	}

	evt := putEvent(t, obj)
	want, err := DefaultStub(evt)
	if err != nil {
		t.Fatalf("DefaultStub returned error %v, want nil", err)
	}

	puts := s3.putInputs()
	if len(puts) != 1 {
		t.Fatalf("PutObject was called %d times, want 1", len(puts))
	}
	if string(puts[0].Body) != string(want) {
		t.Errorf("PutObject body = %q, want DefaultStub's output %q", puts[0].Body, want)
	}
}

// --- duplicates ------------------------------------------------------------

// TestDuplicateEventSkipsTheStubAndTheWrite pins decisions 2 and 3: a key the
// store already knows means no stub call and no PutObject — and a nil return,
// because "already done" is a success. Returning an error here would leave the
// message on the queue forever and eventually DLQ a message that was in fact
// fully processed.
func TestDuplicateEventSkipsTheStubAndTheWrite(t *testing.T) {
	h := newHarness(t)
	obj := objectPut{key: "raw/video1.mp4", size: 123456, etag: "abc123", versionID: "v42"}
	h.store.mark(idempotency.Key(inputBucket, obj.key, obj.versionID, obj.etag))

	if err := h.process(context.Background(), message(t, obj)); err != nil {
		t.Fatalf("ProcessFunc returned error %v for an already-processed event, want nil so the message is "+
			"deleted rather than redelivered forever", err)
	}

	if got := h.stub.callCount(); got != 0 {
		t.Errorf("stub ran %d times for a duplicate, want 0: the dedup check must happen before any work", got)
	}
	if got := h.s3.putCount(); got != 0 {
		t.Errorf("PutObject was called %d times for a duplicate, want 0", got)
	}
	if got := h.s3.storedKeys(outputBucket); len(got) != 0 {
		t.Errorf("output bucket holds %q after a duplicate, want nothing", got)
	}
}

// TestRedeliveredMessageDoesNotDoubleWrite is the literal Done-when of
// WR-023: hand the SAME message body to the ProcessFunc twice — exactly what
// SQS does when a visibility timeout expires or a message is delivered more
// than once — and the result is written exactly once, with both calls
// reporting success.
//
// Four deliveries, not two, because a processor that (say) toggled a flag
// would survive two.
func TestRedeliveredMessageDoesNotDoubleWrite(t *testing.T) {
	h := newHarness(t)
	obj := objectPut{key: "raw/video1.mp4", size: 123456, etag: "abc123", versionID: "v42"}
	msg := message(t, obj)

	for i := 1; i <= 4; i++ {
		if err := h.process(context.Background(), msg); err != nil {
			t.Fatalf("delivery #%d: ProcessFunc returned error %v, want nil (every delivery of an "+
				"already-processed message must be reported as done so SQS stops redelivering it)", i, err)
		}
	}

	if got := h.s3.putCount(); got != 1 {
		t.Errorf("PutObject was called %d times across 4 deliveries of the same message, want exactly 1: "+
			"a re-delivered message must not double-write", got)
	}
	if got := h.stub.callCount(); got != 1 {
		t.Errorf("stub ran %d times across 4 deliveries, want exactly 1", got)
	}

	wantKey := OutputKey(putEvent(t, obj))
	if got := h.s3.storedKeys(outputBucket); len(got) != 1 || got[0] != wantKey {
		t.Errorf("output bucket holds %q, want exactly one result at %q", got, wantKey)
	}
	if got := h.store.callCount(); got != 4 {
		t.Errorf("store saw %d calls across 4 deliveries, want 4: every delivery must re-ask the store, "+
			"the processor must not cache verdicts of its own (a second worker process would see none of them)", got)
	}
}

// TestDuplicateRecordWithinASingleMessageIsWrittenOnce covers the same
// property inside one message: S3 batches records, and nothing stops the same
// object write appearing twice in one batch. Deduplicating per message
// instead of per event would write it twice.
func TestDuplicateRecordWithinASingleMessageIsWrittenOnce(t *testing.T) {
	h := newHarness(t)
	obj := objectPut{key: "raw/video1.mp4", size: 123456, etag: "abc123", versionID: "v42"}

	if err := h.process(context.Background(), message(t, obj, obj)); err != nil {
		t.Fatalf("ProcessFunc returned error %v, want nil", err)
	}

	if got := h.s3.putCount(); got != 1 {
		t.Errorf("PutObject was called %d times for a batch carrying the same record twice, want 1", got)
	}
	if got := h.store.callCount(); got != 2 {
		t.Errorf("store saw %d calls, want 2 (one per record — the check is per event, not per message)", got)
	}
}

// --- batches ---------------------------------------------------------------

// TestBatchProcessesFreshEventsAndSkipsDuplicates is the mixed batch: within
// one message, a duplicate and two fresh events must reach different
// outcomes, and the message as a whole still succeeds. A processor that
// aborted on the first duplicate would silently drop b.mp4 and c.mp4; one
// that treated the batch as duplicate-if-any would drop them too.
func TestBatchProcessesFreshEventsAndSkipsDuplicates(t *testing.T) {
	h := newHarness(t)

	seen := objectPut{key: "raw/a.mp4", size: 1, etag: "ta", versionID: "va"}
	fresh1 := objectPut{key: "raw/b.mp4", size: 2, etag: "tb", versionID: "vb"}
	fresh2 := objectPut{key: "raw/c.mp4", size: 3, etag: "tc", versionID: "vc"}
	h.store.mark(idempotency.Key(inputBucket, seen.key, seen.versionID, seen.etag))

	if err := h.process(context.Background(), message(t, seen, fresh1, fresh2)); err != nil {
		t.Fatalf("ProcessFunc returned error %v, want nil: a batch in which every record was either "+
			"processed or skipped as a duplicate is a success", err)
	}

	wantKeys := []string{OutputKey(putEvent(t, fresh1)), OutputKey(putEvent(t, fresh2))}
	got := h.s3.storedKeys(outputBucket)
	if len(got) != len(wantKeys) {
		t.Fatalf("output bucket holds %q, want exactly %q", got, wantKeys)
	}
	for i := range wantKeys {
		if got[i] != wantKeys[i] {
			t.Errorf("output bucket key[%d] = %q, want %q", i, got[i], wantKeys[i])
		}
	}
	if got := h.s3.putCount(); got != 2 {
		t.Errorf("PutObject was called %d times, want 2 (the duplicate must not be written)", got)
	}

	stubbed := h.stub.seenEvents()
	if len(stubbed) != 2 {
		t.Fatalf("stub ran on %d events, want 2", len(stubbed))
	}
	for i, want := range []events.Event{putEvent(t, fresh1), putEvent(t, fresh2)} {
		if stubbed[i] != want {
			t.Errorf("stub event[%d] = %+v, want %+v (order preserved, duplicate excluded)", i, stubbed[i], want)
		}
	}
	if got := h.store.callCount(); got != 3 {
		t.Errorf("store saw %d calls, want 3 (one per record)", got)
	}
}

// --- recognized zero-event outcomes ---------------------------------------

// TestNonNotificationBodyIsANoOpSuccess pins the ErrNotNotification branch of
// decision 4. An SNS SubscriptionConfirmation lands on the queue in normal
// operation (it is how the topic subscription is set up); it carries no S3
// payload, so there is nothing to process and nothing broken. Returning the
// error instead would redeliver a handshake message until it hit the DLQ.
func TestNonNotificationBodyIsANoOpSuccess(t *testing.T) {
	bodies := map[string]string{
		"SubscriptionConfirmation": `{
			"Type": "SubscriptionConfirmation",
			"Token": "2336412f37...",
			"TopicArn": "arn:aws:sns:us-east-2:000000000000:weir-uploads",
			"Message": "You have chosen to subscribe to the topic ...",
			"SubscribeURL": "https://sns.us-east-2.amazonaws.com/?Action=ConfirmSubscription",
			"Timestamp": "2026-07-24T12:00:00.000Z"
		}`,
		"UnsubscribeConfirmation": `{
			"Type": "UnsubscribeConfirmation",
			"Token": "2336412f37...",
			"TopicArn": "arn:aws:sns:us-east-2:000000000000:weir-uploads",
			"Timestamp": "2026-07-24T12:00:00.000Z"
		}`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)

			err := h.process(context.Background(), rawMessage(body))
			if err != nil {
				t.Fatalf("ProcessFunc returned error %v for an SNS %s, want nil: it is a recognized "+
					"skip, not a failure, so the message must be deleted", err, name)
			}
			if errors.Is(err, events.ErrNotNotification) {
				t.Error("ProcessFunc surfaced events.ErrNotNotification to the caller; that sentinel is " +
					"the signal to skip, it must be absorbed here")
			}
			assertNothingHappened(t, h)
		})
	}
}

// TestZeroEventNotificationIsANoOpSuccess pins the (nil, nil) branch: a
// well-formed notification carrying no object records — the s3:TestEvent AWS
// sends when a bucket notification config is first created, or an empty
// Records array — is a success with no work.
func TestZeroEventNotificationIsANoOpSuccess(t *testing.T) {
	cases := map[string]string{
		"s3:TestEvent (no Records key at all)": `{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"weir-uploads","Time":"2026-07-24T12:00:00.000Z"}`,
		"empty Records array":                  `{"Records":[]}`,
	}

	for name, inner := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)

			if err := h.process(context.Background(), rawMessage(snsNotification(t, inner))); err != nil {
				t.Fatalf("ProcessFunc returned error %v for a zero-event notification, want nil", err)
			}
			assertNothingHappened(t, h)
		})
	}
}

// TestMalformedBodyIsAnError pins the other half of decision 4: a body that
// is not a recognizable notification is a genuine fault. It must NOT be
// swallowed as a no-op success, because worker.Run would then delete it and
// the misrouting would be invisible; returning an error leaves it visible and
// lets WR-018's redrive policy park it in the DLQ.
func TestMalformedBodyIsAnError(t *testing.T) {
	cases := map[string]string{
		"empty body":                             ``,
		"truncated outer SNS JSON":               `{"Type":"Notification","Message":"{\"Records\":[`,
		"notification with no Message field":     `{"Type":"Notification","TopicArn":"arn:aws:sns:...","Timestamp":"2026-07-24T12:00:00.000Z"}`,
		"unknown SNS Type":                       `{"Type":"SomethingElse","Message":"{}"}`,
		"not an SNS envelope at all":             `{"foo":"bar"}`,
		"inner payload is arbitrary JSON":        `{"Type":"Notification","Message":"{\"garbage\":true}"}`,
		"record with a non-RFC3339 eventTime":    `{"Type":"Notification","Message":"{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"eventTime\":\"not-a-time\",\"s3\":{\"bucket\":{\"name\":\"weir-uploads\"},\"object\":{\"key\":\"raw/x.mp4\",\"size\":1,\"eTag\":\"t\"}}}]}"}`,
		"record with a missing bucket name":      `{"Type":"Notification","Message":"{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"eventTime\":\"2026-07-24T12:00:00.000Z\",\"s3\":{\"bucket\":{},\"object\":{\"key\":\"raw/x.mp4\",\"size\":1,\"eTag\":\"t\"}}}]}"}`,
		"plain text that is not JSON at all":     `not json at all`,
		"bare S3 payload with no SNS enveloping": `{"Records":[{"eventName":"ObjectCreated:Put","eventTime":"2026-07-24T12:00:00.000Z","s3":{"bucket":{"name":"weir-uploads"},"object":{"key":"raw/x.mp4","size":1,"eTag":"t"}}}]}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)

			err := h.process(context.Background(), rawMessage(body))
			if err == nil {
				t.Fatalf("ProcessFunc returned nil for a malformed body; want a non-nil error so the "+
					"message is not deleted and the fault stays visible (body: %s)", body)
			}
			if errors.Is(err, events.ErrNotNotification) {
				t.Errorf("ProcessFunc returned the ErrNotNotification skip sentinel %v for genuinely "+
					"malformed input; the two outcomes must stay distinguishable", err)
			}
			assertNothingHappened(t, h)
		})
	}
}

// assertNothingHappened is the common assertion for every
// no-event/failed-parse path: the store is never consulted and S3 is never
// touched, since there is no event to key on.
func assertNothingHappened(t *testing.T, h *harness) {
	t.Helper()
	if got := h.store.callCount(); got != 0 {
		t.Errorf("store was consulted %d times, want 0: with no parsed event there is no key to check", got)
	}
	if got := h.stub.callCount(); got != 0 {
		t.Errorf("stub ran %d times, want 0", got)
	}
	if got := h.s3.putCount(); got != 0 {
		t.Errorf("PutObject was called %d times, want 0", got)
	}
}

// --- failure propagation ---------------------------------------------------

// TestStoreErrorIsReturnedAndNothingIsWritten pins the store-error path
// against dedup's documented contract: a store failure means skip processing
// AND skip deleting the message. So no work happens and the error reaches
// worker.Run intact and matchable, because a store outage is exactly the
// condition under which double-processing must not be risked.
func TestStoreErrorIsReturnedAndNothingIsWritten(t *testing.T) {
	h := newHarness(t)
	h.store.failNext(errStore)

	err := h.process(context.Background(), message(t, objectPut{key: "raw/a.mp4", size: 1, etag: "ta"}))
	if err == nil {
		t.Fatal("ProcessFunc returned nil though the idempotency store failed; want a non-nil error so " +
			"the message is left for redelivery instead of being silently consumed")
	}
	if !errors.Is(err, errStore) {
		t.Errorf("ProcessFunc error = %v, want one matching the store's error via errors.Is (wrap with %%w, "+
			"do not replace it)", err)
	}
	if got := h.stub.callCount(); got != 0 {
		t.Errorf("stub ran %d times after a store error, want 0: a failed lookup must never be read as "+
			"'this event is new'", got)
	}
	if got := h.s3.putCount(); got != 0 {
		t.Errorf("PutObject was called %d times after a store error, want 0", got)
	}
}

// TestStoreErrorMidBatchAbortsTheRestOfTheBatch pins that the abort is
// immediate: once the store is unreachable, no later record in the same
// message is processed on faith. The already-written earlier record stays
// written (it genuinely happened), and the returned error redelivers the
// message — where the dedup store, once healthy, reports that first record as
// a duplicate, so it is not written twice.
func TestStoreErrorMidBatchAbortsTheRestOfTheBatch(t *testing.T) {
	h := newHarness(t)

	first := objectPut{key: "raw/a.mp4", size: 1, etag: "ta", versionID: "va"}
	second := objectPut{key: "raw/b.mp4", size: 2, etag: "tb", versionID: "vb"}
	third := objectPut{key: "raw/c.mp4", size: 3, etag: "tc", versionID: "vc"}
	msg := message(t, first, second, third)

	// Let the first record through, then break the store for the rest.
	if err := h.process(context.Background(), message(t, first)); err != nil {
		t.Fatalf("setup: ProcessFunc returned error %v, want nil", err)
	}
	h.store.failNext(errStore)

	err := h.process(context.Background(), msg)
	if err == nil {
		t.Fatal("ProcessFunc returned nil though the store failed mid-batch, want a non-nil error")
	}
	if !errors.Is(err, errStore) {
		t.Errorf("ProcessFunc error = %v, want one matching %v via errors.Is", err, errStore)
	}
	if got := h.s3.putCount(); got != 1 {
		t.Errorf("PutObject was called %d times in total, want 1 (only the pre-outage write); the batch "+
			"must abort at the failing record rather than processing later records without a dedup verdict", got)
	}
	if got := h.store.callCount(); got != 2 {
		t.Errorf("store saw %d calls, want 2 (the successful setup call, then the one that failed and "+
			"stopped the batch)", got)
	}
}

// TestStubErrorIsReturnedAndNothingIsWritten pins that a failing processing
// body is a failure of the message, not a silent skip: no result is written
// and the error propagates so the message is redelivered (and, if it keeps
// failing, DLQ'd) instead of being deleted with no output.
func TestStubErrorIsReturnedAndNothingIsWritten(t *testing.T) {
	h := newHarness(t)
	h.stub.failNext(errStub)

	err := h.process(context.Background(), message(t, objectPut{key: "raw/a.mp4", size: 1, etag: "ta"}))
	if err == nil {
		t.Fatal("ProcessFunc returned nil though the stub failed, want a non-nil error")
	}
	if !errors.Is(err, errStub) {
		t.Errorf("ProcessFunc error = %v, want one matching the stub's error via errors.Is", err)
	}
	if got := h.s3.putCount(); got != 0 {
		t.Errorf("PutObject was called %d times after the stub failed, want 0: there is no result to write", got)
	}
}

// TestPutObjectErrorIsReturned pins the S3 failure path: an unwritten result
// means the message is not done, so the error propagates.
func TestPutObjectErrorIsReturned(t *testing.T) {
	h := newHarness(t)
	h.s3.failOnCall(1, errS3)

	err := h.process(context.Background(), message(t, objectPut{key: "raw/a.mp4", size: 1, etag: "ta"}))
	if err == nil {
		t.Fatal("ProcessFunc returned nil though PutObject failed, want a non-nil error so the message is " +
			"redelivered rather than deleted with no result in the output bucket")
	}
	if !errors.Is(err, errS3) {
		t.Errorf("ProcessFunc error = %v, want one matching the S3 error via errors.Is", err)
	}
	if got := h.s3.storedKeys(outputBucket); len(got) != 0 {
		t.Errorf("output bucket holds %q after a failed PutObject, want nothing", got)
	}
}

// TestPutObjectErrorMidBatchKeepsTheEarlierWriteAndAborts is the partial-
// progress case: the first record's result is genuinely written, the second
// fails, the third is never attempted, and the message fails as a whole.
// Partial progress is expected and safe precisely because the dedup store
// makes the completed part idempotent on redelivery.
func TestPutObjectErrorMidBatchKeepsTheEarlierWriteAndAborts(t *testing.T) {
	h := newHarness(t)

	first := objectPut{key: "raw/a.mp4", size: 1, etag: "ta", versionID: "va"}
	second := objectPut{key: "raw/b.mp4", size: 2, etag: "tb", versionID: "vb"}
	third := objectPut{key: "raw/c.mp4", size: 3, etag: "tc", versionID: "vc"}
	h.s3.failOnCall(2, errS3)

	err := h.process(context.Background(), message(t, first, second, third))
	if err == nil {
		t.Fatal("ProcessFunc returned nil though the second PutObject failed, want a non-nil error")
	}
	if !errors.Is(err, errS3) {
		t.Errorf("ProcessFunc error = %v, want one matching %v via errors.Is", err, errS3)
	}
	if got := h.s3.putCount(); got != 2 {
		t.Errorf("PutObject was called %d times, want 2: the batch must stop at the failing record and "+
			"not attempt the third", got)
	}

	firstKey := OutputKey(putEvent(t, first))
	if got := h.s3.storedKeys(outputBucket); len(got) != 1 || got[0] != firstKey {
		t.Errorf("output bucket holds %q, want exactly [%q]: the record that did succeed stays written", got, firstKey)
	}
}

// TestRedeliveryAfterPutObjectFailureSkipsTheEventEntirely pins design
// decision 5 — dedup's at-most-once-on-crash gap — as ACCEPTED, INHERITED
// behavior, so it is specified rather than accidental.
//
// dedup.Store marks a key OPTIMISTICALLY, at check time, before the caller
// finishes the work (see internal/dedup/dedup.go). So an event whose
// PutObject fails is already marked seen: when SQS redelivers the message,
// that event is reported as a duplicate and skipped, and its result is never
// written. The message is then deleted, since from the processor's point of
// view every record was accounted for.
//
// This is a real, known at-most-once failure mode, explicitly out of scope
// for WR-023 (closing it needs a claim/lease lifecycle in a real Store
// backend, per dedup's own package comment). The test exists so nobody
// "fixes" it halfway — e.g. by un-marking keys on failure, which would
// reintroduce double-writes under concurrency — and so the limitation is
// discoverable from the test suite.
func TestRedeliveryAfterPutObjectFailureSkipsTheEventEntirely(t *testing.T) {
	h := newHarness(t)
	obj := objectPut{key: "raw/a.mp4", size: 1, etag: "ta", versionID: "va"}
	msg := message(t, obj)

	h.s3.failOnCall(1, errS3)
	if err := h.process(context.Background(), msg); err == nil {
		t.Fatal("setup: ProcessFunc returned nil though PutObject failed")
	}

	key := idempotency.Key(inputBucket, obj.key, obj.versionID, obj.etag)
	if !h.store.isMarked(key) {
		t.Fatalf("the key is not marked after a failed write; WR-023 inherits dedup's optimistic marking "+
			"as-is, so it must be marked here (key %q)", key)
	}

	// The redelivery: S3 is healthy again, but the key is already marked.
	if err := h.process(context.Background(), msg); err != nil {
		t.Fatalf("redelivery: ProcessFunc returned error %v, want nil — the event is now reported as a "+
			"duplicate", err)
	}
	if got := h.s3.putCount(); got != 1 {
		t.Errorf("PutObject was called %d times across the failed delivery and its redelivery, want 1 "+
			"(the single failed attempt): the redelivery must not retry, per the accepted "+
			"at-most-once-on-crash gap", got)
	}
	if got := h.s3.storedKeys(outputBucket); len(got) != 0 {
		t.Errorf("output bucket holds %q, want nothing: this is the accepted at-most-once gap — the "+
			"result is lost, not silently written later", got)
	}
}

// --- context plumbing ------------------------------------------------------

type ctxKeyType struct{}

// TestCallerContextReachesTheStoreAndS3 pins that the context worker.Run
// hands to Process is passed through to both dependencies unchanged. Both
// take a ctx precisely so shutdown cancellation and deadlines reach the
// network calls; substituting context.Background() would pass every other
// test here while making the worker's shutdown grace period unenforceable on
// an in-flight PutObject.
func TestCallerContextReachesTheStoreAndS3(t *testing.T) {
	h := newHarness(t)
	ctx := context.WithValue(context.Background(), ctxKeyType{}, "wr-023")

	if err := h.process(ctx, message(t, objectPut{key: "raw/a.mp4", size: 1, etag: "ta"})); err != nil {
		t.Fatalf("ProcessFunc returned error %v, want nil", err)
	}

	if got := h.store.lastContext(); got != ctx {
		t.Errorf("the store received context %v, want the caller's context passed straight through", got)
	}
	if got := h.s3.lastContext(); got != ctx {
		t.Errorf("PutObject received context %v, want the caller's context passed straight through", got)
	}
}

// --- output key collision semantics ---------------------------------------

// TestTwoVersionsOfTheSameObjectShareOneOutputKey documents an accepted
// consequence of the chosen output-key derivation (mirror the source key,
// append ResultSuffix): the derivation deliberately ignores VersionID, while
// the idempotency key does not. So two writes to the same object key on a
// versioned bucket are two distinct events — both fresh, both processed —
// whose results land on the same output object, the later overwriting the
// earlier.
//
// That is latest-wins, chosen for MVP simplicity: the output key stays
// predictable and human-readable, which is what the demo and the WR-026
// end-to-end check need. It does NOT weaken the Done-when — a REDELIVERY of
// the same version is still written exactly once, which is what
// TestRedeliveredMessageDoesNotDoubleWrite proves. Encoding VersionID into
// the output key would be the fix if per-version results ever matter.
func TestTwoVersionsOfTheSameObjectShareOneOutputKey(t *testing.T) {
	h := newHarness(t)

	v1 := objectPut{key: "raw/a.mp4", size: 1, etag: "etag-v1", versionID: "v1"}
	v2 := objectPut{key: "raw/a.mp4", size: 2, etag: "etag-v2", versionID: "v2"}

	if err := h.process(context.Background(), message(t, v1)); err != nil {
		t.Fatalf("first version: ProcessFunc returned error %v, want nil", err)
	}
	if err := h.process(context.Background(), message(t, v2)); err != nil {
		t.Fatalf("second version: ProcessFunc returned error %v, want nil", err)
	}

	if got := h.s3.putCount(); got != 2 {
		t.Errorf("PutObject was called %d times for two distinct object versions, want 2: they are "+
			"different writes (different ETag/VersionID), so neither is a duplicate of the other", got)
	}
	wantKey := OutputKey(putEvent(t, v1))
	if got := h.s3.storedKeys(outputBucket); len(got) != 1 || got[0] != wantKey {
		t.Errorf("output bucket holds %q, want exactly [%q]: the output key mirrors the source key and "+
			"ignores VersionID, so the newer result overwrites the older", got, wantKey)
	}
}
