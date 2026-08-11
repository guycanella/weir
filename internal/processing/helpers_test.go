package processing

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
	"github.com/guycanella/weir/internal/dedup"
	"github.com/guycanella/weir/internal/events"
	"github.com/guycanella/weir/internal/idempotency"
	"github.com/guycanella/weir/internal/worker"
)

// This file holds the fixtures and test doubles shared by the WR-023 suite.
// The behavioral assertions live in process_test.go (the worker.ProcessFunc
// returned by New) and stub_test.go (the pure OutputKey/DefaultStub core).
//
// The API this suite pins down for the implementer (Julia) — package
// internal/processing:
//
//	type StubFunc func(events.Event) ([]byte, error)
//
//	type Config struct {
//	    S3Client     awsclient.S3Client // required
//	    OutputBucket string             // required
//	    Store        dedup.Store        // required
//	    Stub         StubFunc           // optional; nil means DefaultStub
//	    ContentType  string             // optional; "" means DefaultContentType
//	}
//
//	const DefaultContentType = "application/json"
//	const ResultSuffix       = ".result.json"
//
//	func OutputKey(evt events.Event) string
//	func DefaultStub(evt events.Event) ([]byte, error)
//	func New(cfg Config) (worker.ProcessFunc, error)
//
// Five design decisions are load-bearing; each is enforced by a test.
//
//  1. New VALIDATES AND RETURNS AN error, rather than returning a
//     ProcessFunc that nil-panics on the first message. This mirrors
//     worker.Validate's own rationale ("fail cleanly before starting the
//     receive loop instead of panicking or nil-dereferencing on first use"):
//     a missing OutputBucket is a wiring bug, and the cheapest place to
//     surface it is process start, not the first delivery — where it would
//     instead manifest as an endlessly redelivering queue.
//
//  2. THE DEDUP CHECK HAPPENS BEFORE THE STUB AND BEFORE PutObject, per
//     event, not per message. The parser can return a batch, so "already
//     processed" is a per-event verdict; skipping at message granularity
//     would either re-write fresh events or drop them.
//
//  3. A DUPLICATE IS A SUCCESS (nil), NOT A FAILURE. "This was already
//     done" means the message must be deleted — returning an error would
//     make every redelivery redeliver forever until the DLQ.
//
//  4. EVERY OTHER FAILURE (store error, stub error, PutObject error,
//     genuinely malformed body) RETURNS non-nil, so worker.Run leaves the
//     message for redelivery and WR-018's redrive policy can eventually park
//     a poison message in the DLQ. The two RECOGNIZED zero-event outcomes —
//     an SNS handshake (events.ErrNotNotification) and a legitimate
//     s3:TestEvent / empty-Records notification — are the exceptions: they
//     are no-op successes, because there is nothing to do and nothing broken.
//
//  5. WR-023 INHERITS dedup's at-most-once-on-crash GAP AS-IS. dedup marks a
//     key seen optimistically, before the work completes, so an event whose
//     stub or PutObject fails is nonetheless already marked: its redelivery
//     is reported as a duplicate and skipped, and the result is never
//     written. This is explicitly out of scope to fix here (see
//     internal/dedup/dedup.go's package comment) and
//     TestRedeliveryAfterPutObjectFailureSkipsTheEventEntirely pins it as
//     ACCEPTED behavior rather than leaving it unspecified.
//
// Out of scope on purpose: this package does not poll or delete SQS messages
// (internal/worker does), does not parse the wire format itself
// (internal/events does), does not compute keys (internal/idempotency does),
// does not decide duplicate-ness (internal/dedup does), and does not provide
// a real Store backend (a later task). It is the dispatch layer that wires
// those four together into one worker.ProcessFunc.
//
// Also out of scope, structurally: reading the uploaded object's CONTENT.
// awsclient.S3Client has no GetObject, so the pluggable stub derives its
// result from event METADATA only. That is a deliberate scope boundary for
// the demo, not an oversight.

// --- required exports from the pure core packages -------------------------

// This dispatch layer is the first caller of internal/events and
// internal/idempotency from outside their own packages, so the two pure
// functions it composes — currently unexported, since WR-007/WR-008 had no
// external caller yet — must be exported with EXACTLY these signatures.
// Pinning them as typed vars makes that a compile-time requirement rather
// than a convention, and catches a rename that changed the parameter order
// or the return shape while keeping the name.
var (
	_ func(raw []byte) ([]events.Event, error)         = events.ParseS3Events
	_ func(bucket, key, versionID, etag string) string = idempotency.Key
)

// --- the source event fixtures --------------------------------------------

const (
	inputBucket  = "weir-uploads"
	outputBucket = "weir-results"
	eventTimeStr = "2026-07-24T12:00:00.000Z"
)

// errStore, errStub and errS3 stand in for whatever the three failure
// sources fail with in production: a DynamoDB throttle, a broken processing
// body, an S3 5xx.
var (
	errStore = errors.New("processing test: idempotency store unavailable")
	errStub  = errors.New("processing test: stub could not produce a result")
	errS3    = errors.New("processing test: s3 put failed")
)

// snsNotification wraps an inner S3-notification JSON string in a realistic
// SNS "Notification" envelope. Marshaling a struct whose Message field is
// the inner JSON *string* reproduces the real double-encoding exactly as it
// arrives off SQS (ADR-001: S3 -> SNS -> SQS) — no shortcut that skips the
// double-encode, since the ProcessFunc under test must go through
// events.ParseS3Events on that exact wire format.
func snsNotification(t *testing.T, innerS3JSON string) string {
	t.Helper()
	env := struct {
		Type      string `json:"Type"`
		MessageId string `json:"MessageId"`
		TopicArn  string `json:"TopicArn"`
		Message   string `json:"Message"`
		Timestamp string `json:"Timestamp"`
	}{
		Type:      "Notification",
		MessageId: "22b81b1a-0000-0000-0000-000000000000",
		TopicArn:  "arn:aws:sns:us-east-2:000000000000:weir-uploads",
		Message:   innerS3JSON,
		Timestamp: eventTimeStr,
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("snsNotification marshal: %v", err)
	}
	return string(b)
}

// s3Record renders a single S3 event-notification record as JSON.
func s3Record(eventName, bucket, key string, size int64, etag, versionID string) string {
	rec := map[string]any{
		"eventVersion": "2.1",
		"eventSource":  "aws:s3",
		"awsRegion":    "us-east-2",
		"eventTime":    eventTimeStr,
		"eventName":    eventName,
		"s3": map[string]any{
			"bucket": map[string]any{"name": bucket},
			"object": map[string]any{
				"key":       key,
				"size":      size,
				"eTag":      etag,
				"versionId": versionID,
			},
		},
	}
	b, _ := json.Marshal(rec)
	return string(b)
}

// s3Payload renders an inner S3 payload with the given records.
func s3Payload(records ...string) string {
	inner := `{"Records":[`
	for i, r := range records {
		if i > 0 {
			inner += ","
		}
		inner += r
	}
	inner += `]}`
	return inner
}

// objectPut describes one S3 object write, as a test wants to talk about it.
type objectPut struct {
	key       string
	size      int64
	etag      string
	versionID string
}

// putEvent is the events.Event a worker sees for objectPut, so a test can
// assert on the exact key/body the ProcessFunc derives from it.
func putEvent(t *testing.T, obj objectPut) events.Event {
	t.Helper()
	return events.Event{
		Bucket:    inputBucket,
		Key:       obj.key,
		Size:      obj.size,
		ETag:      obj.etag,
		VersionID: obj.versionID,
		EventName: "ObjectCreated:Put",
		EventTime: mustTime(t, eventTimeStr),
	}
}

// message builds the SQS message a worker would receive for the given object
// writes, batched into one SNS notification exactly as S3 batches records.
func message(t *testing.T, objs ...objectPut) awsclient.Message {
	t.Helper()
	records := make([]string, 0, len(objs))
	for _, o := range objs {
		records = append(records, s3Record("ObjectCreated:Put", inputBucket, o.key, o.size, o.etag, o.versionID))
	}
	return awsclient.Message{
		MessageId:     "11111111-2222-3333-4444-555555555555",
		ReceiptHandle: "receipt-handle",
		Body:          snsNotification(t, s3Payload(records...)),
	}
}

// rawMessage builds an SQS message with an arbitrary body, for the parse
// paths (handshake / test event / garbage) that no object write produces.
func rawMessage(body string) awsclient.Message {
	return awsclient.Message{
		MessageId:     "11111111-2222-3333-4444-555555555555",
		ReceiptHandle: "receipt-handle",
		Body:          body,
	}
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("mustTime(%q): %v", s, err)
	}
	return ts
}

// --- the S3 double ---------------------------------------------------------

// recordingS3 wraps the shared in-memory fake (internal/awsclient/fake.S3)
// with an ordered record of every PutObject ATTEMPT, including the ones that
// fail and the context each was called with.
//
// The wrapper exists because fake.S3 stores puts in a map keyed by
// bucket+key, so a second write to the same key is indistinguishable from
// the first — and "was PutObject called exactly once across two deliveries?"
// is precisely the Done-when of this task. Failure injection is likewise
// per-call-ordinal here (failOnCall) rather than fake.S3's
// consume-the-next-N queue, because a batch test needs the first write to
// succeed and the second to fail.
type recordingS3 struct {
	*fake.S3

	mu     sync.Mutex
	puts   []awsclient.PutObjectInput
	ctxs   []context.Context
	failAt map[int]error
}

func newRecordingS3() *recordingS3 {
	return &recordingS3{S3: fake.NewS3(), failAt: make(map[int]error)}
}

// failOnCall makes the n-th PutObject call (1-based) return err without
// storing anything.
func (r *recordingS3) failOnCall(n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failAt[n] = err
}

func (r *recordingS3) PutObject(ctx context.Context, in awsclient.PutObjectInput) (awsclient.PutObjectOutput, error) {
	r.mu.Lock()
	r.puts = append(r.puts, in)
	r.ctxs = append(r.ctxs, ctx)
	ordinal := len(r.puts)
	err := r.failAt[ordinal]
	r.mu.Unlock()

	if err != nil {
		return awsclient.PutObjectOutput{}, err
	}
	return r.S3.PutObject(ctx, in)
}

func (r *recordingS3) putCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.puts)
}

func (r *recordingS3) putInputs() []awsclient.PutObjectInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]awsclient.PutObjectInput(nil), r.puts...)
}

func (r *recordingS3) lastContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ctxs) == 0 {
		return nil
	}
	return r.ctxs[len(r.ctxs)-1]
}

// storedKeys lists the object keys currently stored in bucket, sorted, as a
// test's stand-in for "what is actually in the output bucket".
func (r *recordingS3) storedKeys(bucket string) []string {
	keys := make([]string, 0, len(r.PutObjects[bucket]))
	for k := range r.PutObjects[bucket] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (r *recordingS3) storedBody(t *testing.T, bucket, key string) []byte {
	t.Helper()
	rec, ok := r.PutObjects[bucket][key]
	if !ok {
		t.Fatalf("no object stored at %s/%s; stored keys in %s: %q", bucket, key, bucket, r.storedKeys(bucket))
	}
	return rec.Body
}

var _ awsclient.S3Client = (*recordingS3)(nil)

// --- the idempotency store double -----------------------------------------

// memStore is the in-memory dedup.Store the suite runs against: a map plus a
// mutex, which is all a correct single atomic CheckAndMark needs. It doubles
// as a spy (every key it was asked about, in order, with its context) and
// can be told to fail so the store-error path is reachable.
//
// A failing CheckAndMark marks nothing here. That is a deliberate
// simplification of this double, NOT a guarantee dedup.Store makes — see
// internal/dedup/dedup.go on ambiguous network write outcomes — and it is
// what lets a test distinguish "the store was consulted and failed" from
// "the key got marked anyway".
type memStore struct {
	mu   sync.Mutex
	seen map[string]struct{}

	calls []string
	ctxs  []context.Context

	failWith error
}

func newMemStore() *memStore {
	return &memStore{seen: make(map[string]struct{})}
}

func (s *memStore) CheckAndMark(ctx context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, key)
	s.ctxs = append(s.ctxs, ctx)

	if s.failWith != nil {
		return true, s.failWith
	}

	_, alreadySeen := s.seen[key]
	s.seen[key] = struct{}{}
	return alreadySeen, nil
}

// mark pre-seeds a key as already processed, simulating a delivery that a
// previous worker (or a previous run) already handled.
func (s *memStore) mark(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[key] = struct{}{}
}

func (s *memStore) failNext(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failWith = err
}

func (s *memStore) isMarked(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.seen[key]
	return ok
}

func (s *memStore) callKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *memStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *memStore) lastContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ctxs) == 0 {
		return nil
	}
	return s.ctxs[len(s.ctxs)-1]
}

var _ dedup.Store = (*memStore)(nil)

// --- the stub double -------------------------------------------------------

// spyStub is a StubFunc that records the events it was handed and returns a
// fixed body, so a test can assert both "the stub ran on exactly these
// events" and "its bytes reached S3 verbatim".
type spyStub struct {
	mu sync.Mutex

	body     []byte
	failWith error
	seen     []events.Event
}

func newSpyStub(body string) *spyStub {
	return &spyStub{body: []byte(body)}
}

func (s *spyStub) fn(evt events.Event) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, evt)
	if s.failWith != nil {
		return nil, s.failWith
	}
	return append([]byte(nil), s.body...), nil
}

func (s *spyStub) failNext(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failWith = err
}

func (s *spyStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

func (s *spyStub) seenEvents() []events.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Event(nil), s.seen...)
}

// --- assembling the subject under test ------------------------------------

// harness bundles a configured ProcessFunc with the doubles behind it.
type harness struct {
	process worker.ProcessFunc
	s3      *recordingS3
	store   *memStore
	stub    *spyStub
}

// newHarness builds a valid processing.New configuration wired to fresh
// doubles, failing the test if New rejects it.
func newHarness(t *testing.T) *harness {
	t.Helper()

	h := &harness{
		s3:    newRecordingS3(),
		store: newMemStore(),
		stub:  newSpyStub(`{"stub":"result"}`),
	}

	process, err := New(Config{
		S3Client:     h.s3,
		OutputBucket: outputBucket,
		Store:        h.store,
		Stub:         h.stub.fn,
	})
	if err != nil {
		t.Fatalf("New returned unexpected error for a fully populated Config: %v", err)
	}
	if process == nil {
		t.Fatal("New returned a nil ProcessFunc alongside a nil error")
	}

	h.process = process
	return h
}
