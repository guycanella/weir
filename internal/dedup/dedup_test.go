package dedup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// This file pins the duplicate-event decision for WR-010: given an idempotency
// key already computed by WR-008 (internal/idempotency), answer "have I seen
// this key before?" against a pluggable store, so a re-delivered S3 event is
// not processed twice.
//
// The API this suite pins down for the implementer (Julia):
//
//	type Store interface {
//	    CheckAndMark(ctx context.Context, key string) (alreadySeen bool, err error)
//	}
//
//	var ErrEmptyKey = errors.New(...)   // sentinel, matchable with errors.Is
//
//	func IsDuplicate(ctx context.Context, store Store, key string) (bool, error)
//
// Four design decisions are load-bearing and every one of them is enforced by a
// test below.
//
//  1. ONE ATOMIC METHOD, NOT Seen()+MarkSeen(). Splitting the store into a
//     query and a mutation bakes a check-then-act race into the interface by
//     construction: two workers polling the same SQS queue could both observe
//     "not seen" before either marks the key, and both would then process the
//     event as genuinely new — the exact double-processing bug this task exists
//     to prevent. A single CheckAndMark that decides and records in one
//     operation makes the race unrepresentable at the type level, lets a real
//     backend implement it as one atomic round trip (a DynamoDB PutItem with a
//     attribute_not_exists condition), and makes the in-memory fake trivially
//     correct with one mutex. TestIsDuplicateExactlyOneConcurrentCallerSeesKeyAsNew
//     is what holds this line.
//
//  2. IMPLEMENTATIONS MUST BE SAFE FOR CONCURRENT USE. One Store is shared by
//     every worker goroutine (WR-022 runs bounded concurrency), so "atomic" has
//     to mean atomic across goroutines, not merely single-call.
//
//  3. THE STORE CAN FAIL, SO CheckAndMark RETURNS AN error. The in-memory fake
//     here never fails, but the Done-when frames this interface as being "for
//     later wiring" to a real backend, where a network timeout or a throttle is
//     routine. An error-free interface would force that store to swallow
//     failures and lie about the answer.
//
//  4. false MEANS "NEWLY RECORDED", AND ONLY THAT. IsDuplicate returns false
//     if and only if the key was successfully recorded as seen for the first
//     time. Whenever err != nil the boolean is true. See
//     TestIsDuplicateNeverReportsAFreshKeyAlongsideAnError for the full
//     rationale; briefly: the failure mode of "process it again" is bounded
//     (duplicate work, and the store may reject the second write anyway),
//     whereas the failure mode of a caller that ignores the error and reads
//     false as "brand-new event" is unbounded silent double-processing. Under
//     the intended call pattern (WR-023/WR-024) a non-nil error means the
//     worker neither processes nor deletes the message, so SQS redelivers it
//     and the redrive policy eventually parks it in the DLQ rather than dropping
//     it. Whether that redelivery is then processed depends on the store: it is
//     only guaranteed to be judged new again if the failed CheckAndMark truly
//     marked nothing, which the in-memory fake here guarantees by construction
//     but a real network-backed store cannot (see the fakeStore header). So the
//     claim this suite makes is the narrow one — an error never reports a fresh
//     key, so the message is not silently consumed on the spot — not a general
//     no-loss guarantee.
//
//     It is NOT a claim of no-loss in general. CheckAndMark marks a key
//     optimistically — before the caller has finished processing the event —
//     so a worker that crashes after a nil-error CheckAndMark but before
//     completing the work leaves the key marked "seen": on SQS redelivery
//     IsDuplicate reports a duplicate and the message is skipped and deleted,
//     which is genuine silent work loss. That is a separate,
//     currently-accepted at-most-once-on-crash trade-off, distinct from the
//     store-error path above. Closing it belongs to a real store's
//     crash-recovery semantics (e.g. a conditional write plus a TTL lease that
//     expires if the worker never confirms completion), not to this decision
//     layer, and is out of scope for WR-010.
//
// Out of scope on purpose: this package does not compute keys (WR-008 does),
// does not talk to any real store (WR-016+ does), and does not decide what a
// caller should do about a duplicate (WR-023 does). It is the decision layer,
// and the actual check-and-mark atomicity belongs to the Store implementation,
// not to a reimplementation here.

// --- keys used across the suite -------------------------------------------
//
// Real inputs are 64-char lowercase-hex SHA-256 digests from
// idempotency.Key. keyB differs from keyA only in its final
// character, which is the realistic near-miss: two different object writes
// whose digests share a long prefix must never be conflated.
const (
	keyA = "ce7ebc632f9654cd1da4fd9f9e4cd970a12b655246e4f1dcac431f22a42c8b97"
	keyB = "ce7ebc632f9654cd1da4fd9f9e4cd970a12b655246e4f1dcac431f22a42c8b98"
	keyC = "acce830f88b6f7f6cbf094ab2e12ff2e00411a2959c2029efac27a0596283a5b"
	keyD = "66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925"
)

// --- the fake store -------------------------------------------------------

// fakeStore is the in-memory Store the Done-when calls for: a map of seen keys
// plus a mutex, which is all a correct implementation of a single atomic
// CheckAndMark needs. It doubles as a spy — it records every call, in order,
// with the context it was handed — and can be told to fail so the error path of
// the decision layer is reachable from a test.
//
// Two properties are deliberate and asserted by TestFakeStoreContract, because
// the rest of the suite trusts them:
//   - a failing CheckAndMark marks nothing; and
//   - the check and the mark happen under one lock, so concurrent callers
//     cannot both see a key as new.
//
// The first is a deliberate SIMPLIFICATION the fake makes to keep the error
// path testable, not a guarantee the Store interface makes. A network-backed
// conditional write can commit and then fail to report success — a dropped
// response, an expired context deadline — leaving the key durably marked even
// though the caller saw an error. So "failed call ⇒ nothing marked" is a known
// gap between this fake and a real backend. It is not a correctness risk for
// this package (the decision layer's contract holds either way: err != nil
// means the answer is not "newly recorded"), but whoever implements the real
// store (WR-016+) owns resolving that ambiguous-write outcome.
type fakeStore struct {
	mu   sync.Mutex
	seen map[string]struct{}

	// calls holds every key passed to CheckAndMark in call order, including
	// calls that failed. contexts is positionally aligned with it.
	calls    []string
	contexts []context.Context

	// failWith, when non-nil, makes every call fail with it and leave the key
	// unmarked; failResult is the boolean returned alongside the error, so a
	// test can prove the decision layer does not simply pass that boolean
	// through.
	failWith   error
	failResult bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{seen: make(map[string]struct{})}
}

func (f *fakeStore) CheckAndMark(ctx context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, key)
	f.contexts = append(f.contexts, ctx)

	if f.failWith != nil {
		return f.failResult, f.failWith
	}

	_, alreadySeen := f.seen[key]
	f.seen[key] = struct{}{}
	return alreadySeen, nil
}

// failNext makes every subsequent CheckAndMark return (result, err).
func (f *fakeStore) failNext(err error, result bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failWith, f.failResult = err, result
}

// recover stops failing, simulating a transient store outage clearing.
func (f *fakeStore) recover() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failWith, f.failResult = nil, false
}

func (f *fakeStore) isMarked(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.seen[key]
	return ok
}

func (f *fakeStore) callKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeStore) lastContext() context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.contexts) == 0 {
		return nil
	}
	return f.contexts[len(f.contexts)-1]
}

// compile-time proof that the fake satisfies the interface under test.
var _ Store = (*fakeStore)(nil)

// --- first sight / repeat sight -------------------------------------------

// TestIsDuplicateFirstSightOfKeyIsNotADuplicate is the base case: a key never
// seen before is not a duplicate, the call succeeds, and — the half that is
// easy to forget — the key is now recorded, because checking without marking
// would make every delivery look new forever.
func TestIsDuplicateFirstSightOfKeyIsNotADuplicate(t *testing.T) {
	store := newFakeStore()

	dup, err := IsDuplicate(context.Background(), store, keyA)
	if err != nil {
		t.Fatalf("IsDuplicate(ctx, store, keyA) returned error %v, want nil", err)
	}
	if dup {
		t.Errorf("IsDuplicate(ctx, store, keyA) = true, want false on first sight of a key")
	}

	if !store.isMarked(keyA) {
		t.Error("after the first IsDuplicate call the key is not marked seen in the store; " +
			"the decision layer must go through Store.CheckAndMark, which both checks and marks")
	}
	if got := store.callKeys(); len(got) != 1 || got[0] != keyA {
		t.Errorf("store received calls %q, want exactly one call with the key under test", got)
	}
}

// TestIsDuplicateRepeatSightOfSameKeyIsADuplicate is the whole point of the
// task: the second delivery of the same event is recognized. The third and
// fourth calls check that the mark is stable and not consumed — a store that
// cleared the key on read would report false again and let the event through.
func TestIsDuplicateRepeatSightOfSameKeyIsADuplicate(t *testing.T) {
	store := newFakeStore()

	want := []bool{false, true, true, true}
	for i, wantDup := range want {
		dup, err := IsDuplicate(context.Background(), store, keyA)
		if err != nil {
			t.Fatalf("call #%d: IsDuplicate(ctx, store, keyA) returned error %v, want nil", i+1, err)
		}
		if dup != wantDup {
			t.Errorf("call #%d: IsDuplicate(ctx, store, keyA) = %t, want %t", i+1, dup, wantDup)
		}
	}

	if got := store.callCount(); got != len(want) {
		t.Errorf("store saw %d calls, want %d: every IsDuplicate call must reach the store, "+
			"the decision layer must not cache answers of its own", got, len(want))
	}
}

// TestIsDuplicateDistinctKeysDoNotCollide is the A, B, A sequence from the
// task: B's arrival must not disturb A's state, and A's must not pre-empt B's.
// A single global "have I seen anything?" flag, or a store keyed by something
// coarser than the full key, fails here.
func TestIsDuplicateDistinctKeysDoNotCollide(t *testing.T) {
	store := newFakeStore()

	firstA, err := IsDuplicate(context.Background(), store, keyA)
	if err != nil {
		t.Fatalf("first IsDuplicate for A returned error %v, want nil", err)
	}
	if firstA {
		t.Errorf("first IsDuplicate for A = true, want false")
	}

	firstB, err := IsDuplicate(context.Background(), store, keyB)
	if err != nil {
		t.Fatalf("first IsDuplicate for B returned error %v, want nil", err)
	}
	if firstB {
		t.Errorf("first IsDuplicate for B = true, want false; B is a different key "+
			"(it differs from A only in its last character: %q vs %q) and must be judged on its own",
			keyA, keyB)
	}

	secondA, err := IsDuplicate(context.Background(), store, keyA)
	if err != nil {
		t.Fatalf("second IsDuplicate for A returned error %v, want nil", err)
	}
	if !secondA {
		t.Error("second IsDuplicate for A = false, want true; seeing B in between must not clear A's mark")
	}

	secondB, err := IsDuplicate(context.Background(), store, keyB)
	if err != nil {
		t.Fatalf("second IsDuplicate for B returned error %v, want nil", err)
	}
	if !secondB {
		t.Error("second IsDuplicate for B = false, want true")
	}
}

// TestIsDuplicateTracksKeysIndependently drives longer interleavings than the
// A, B, A case: per-key state must survive arbitrary ordering, repeats and
// near-identical keys. Each scenario is a delivery sequence with the expected
// verdict at every step, so an off-by-one in the bookkeeping shows up as a
// specific step rather than a vague failure.
func TestIsDuplicateTracksKeysIndependently(t *testing.T) {
	type step struct {
		key     string
		wantDup bool
	}

	scenarios := []struct {
		name  string
		steps []step
	}{
		{
			name: "interleaved round robin, each key repeated",
			steps: []step{
				{keyA, false}, {keyB, false}, {keyC, false},
				{keyA, true}, {keyB, true}, {keyC, true},
				{keyA, true}, {keyB, true}, {keyC, true},
			},
		},
		{
			name: "one key hammered while others arrive once",
			steps: []step{
				{keyA, false}, {keyA, true}, {keyA, true},
				{keyB, false},
				{keyA, true},
				{keyC, false},
				{keyA, true}, {keyB, true}, {keyC, true},
			},
		},
		{
			name: "burst of four distinct keys, then the same burst replayed",
			steps: []step{
				{keyA, false}, {keyB, false}, {keyC, false}, {keyD, false},
				{keyA, true}, {keyB, true}, {keyC, true}, {keyD, true},
			},
		},
		{
			name: "keys sharing a long prefix are still distinct",
			steps: []step{
				// A and B share 63 of 64 characters.
				{keyA, false}, {keyB, false}, {keyA, true}, {keyB, true},
			},
		},
		{
			name: "keys differing only in case are distinct",
			steps: []step{
				// Defensive: real keys are lowercase hex, but the store must
				// key on the exact string, not a case-folded form.
				{keyA, false}, {strings.ToUpper(keyA), false}, {keyA, true}, {strings.ToUpper(keyA), true},
			},
		},
		{
			name: "a key that is a strict prefix of another is distinct",
			steps: []step{
				// Guards against a store that matches on prefix, or that
				// truncates keys to a fixed width before storing them.
				{keyA[:32], false}, {keyA, false}, {keyA[:32], true}, {keyA, true},
			},
		},
		{
			name: "reversed arrival order does not change any verdict",
			steps: []step{
				{keyD, false}, {keyC, false}, {keyB, false}, {keyA, false},
				{keyD, true}, {keyC, true}, {keyB, true}, {keyA, true},
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			store := newFakeStore()
			for i, s := range sc.steps {
				dup, err := IsDuplicate(context.Background(), store, s.key)
				if err != nil {
					t.Fatalf("step %d (key %q): IsDuplicate returned error %v, want nil", i+1, s.key, err)
				}
				if dup != s.wantDup {
					t.Errorf("step %d (key %q): IsDuplicate = %t, want %t", i+1, s.key, dup, s.wantDup)
				}
			}
		})
	}
}

// --- error propagation ----------------------------------------------------

// errStore stands in for whatever a real backend fails with: a throttle, a
// timeout, a permissions error.
var errStore = errors.New("dedup test: store unavailable")

// TestIsDuplicateStoreErrorIsPropagated pins that a store failure reaches the
// caller intact and matchable. Wrapping with %w is fine — and preferable, for
// context — but the sentinel must remain discoverable via errors.Is, so a
// caller (or WR-024's retry logic) can tell a store outage apart from other
// failures. Replacing the error with a new one, or logging and returning nil,
// fails here.
func TestIsDuplicateStoreErrorIsPropagated(t *testing.T) {
	store := newFakeStore()
	store.failNext(errStore, false)

	dup, err := IsDuplicate(context.Background(), store, keyA)
	if err == nil {
		t.Fatalf("IsDuplicate returned nil error though the store failed; got dup=%t, want the store error", dup)
	}
	if !errors.Is(err, errStore) {
		t.Errorf("IsDuplicate returned error %v; want an error matching the store's error %v via errors.Is "+
			"(wrap it with %%w rather than replacing it)", err, errStore)
	}
	if store.callCount() != 1 {
		t.Errorf("store saw %d calls, want 1", store.callCount())
	}
}

// TestIsDuplicateNeverReportsAFreshKeyAlongsideAnError pins decision 4 in the
// header: whenever err != nil, the boolean is true.
//
// The reasoning is worth stating precisely, because the intuitive choice —
// return the zero value, false — is the dangerous one. false is the answer that
// tells the caller "brand-new event, go process it". A caller that forgets to
// check err (or a helper that discards it with _) would then double-process
// every event during a store outage: the store is exactly the component that
// was supposed to prevent that, and its failure would silently disable the
// protection. Returning true degrades the other way: the caller treats the
// event as not-known-to-be-new and, per WR-023, does not delete the SQS
// message, so it is redelivered and — if the outage persists — lands in the DLQ
// where it is visible. Deferred work beats invisible duplicate work.
//
// The second subtest is the sharp one: the store reports (false, err), and
// IsDuplicate must NOT pass that boolean through. A bare
// `return store.CheckAndMark(ctx, key)` fails it, which is the point — the
// error contract is the decision layer's own responsibility, not the store's.
func TestIsDuplicateNeverReportsAFreshKeyAlongsideAnError(t *testing.T) {
	cases := []struct {
		name string
		// the boolean the store returns next to its error
		storeResult bool
	}{
		{"store returns (false, err) — must not be surfaced as a fresh key", false},
		{"store returns (true, err)", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			store.failNext(errStore, tc.storeResult)

			dup, err := IsDuplicate(context.Background(), store, keyA)
			if err == nil {
				t.Fatalf("IsDuplicate returned nil error though the store failed")
			}
			if !dup {
				t.Errorf("IsDuplicate = (%t, %v); want true alongside a non-nil error, so that an "+
					"error-ignoring caller cannot mistake a failed lookup for a genuinely new event", dup, err)
			}
		})
	}
}

// TestIsDuplicateAfterStoreRecoversTheKeyIsStillNew closes the loop on the
// retry path against THIS FAKE: a CheckAndMark that failed marked nothing, so
// when the store comes back the redelivered event is still judged new and gets
// processed exactly once.
//
// What is being proven is the decision layer's cooperation with that recovery —
// IsDuplicate keeps no state of its own and re-asks the store — plus the fake's
// own behavior. It is NOT a general property of every Store: see the fakeStore
// header, a real backend may have marked the key before the error surfaced, in
// which case the redelivery is (correctly, per its own contract) reported as a
// duplicate. Handling that ambiguous write outcome belongs to the real store's
// crash-recovery semantics, not to this layer, and no test here can prove it.
func TestIsDuplicateAfterStoreRecoversTheKeyIsStillNew(t *testing.T) {
	store := newFakeStore()

	store.failNext(errStore, false)
	if _, err := IsDuplicate(context.Background(), store, keyA); err == nil {
		t.Fatal("IsDuplicate returned nil error though the store failed")
	}
	if store.isMarked(keyA) {
		t.Fatal("bad test setup: the failing fake store marked the key; a failed conditional write " +
			"must leave the store untouched")
	}

	store.recover()

	dup, err := IsDuplicate(context.Background(), store, keyA)
	if err != nil {
		t.Fatalf("IsDuplicate after recovery returned error %v, want nil", err)
	}
	if dup {
		t.Error("IsDuplicate after recovery = true, want false; the earlier failed attempt must not " +
			"have recorded the key")
	}

	dup, err = IsDuplicate(context.Background(), store, keyA)
	if err != nil {
		t.Fatalf("IsDuplicate returned error %v, want nil", err)
	}
	if !dup {
		t.Error("IsDuplicate = false on the delivery after a successful one, want true")
	}
}

// --- the empty key --------------------------------------------------------

// TestIsDuplicateRejectsAnEmptyKey states and enforces this suite's judgment
// call on empty input.
//
// An empty key is OUT OF CONTRACT and is rejected with ErrEmptyKey, without the
// store being touched. Why reject rather than treat "" as just another string:
//
//   - It cannot arise legitimately. idempotency.Key is total and
//     always returns 64 hex characters, for every input including all-empty
//     fields. So "" can only mean a caller bug — an unset variable, a field
//     read from the wrong struct, a key that was never computed.
//   - Accepting it converts that bug into the worst available failure. Every
//     event carrying the empty key shares one slot in the store, so the first
//     one is processed and every subsequent one is reported as a duplicate,
//     silently discarded with a nil error and its SQS message deleted. That is
//     unbounded, invisible work loss, and it looks exactly like the system
//     working correctly.
//   - Rejecting it is loud and recoverable: the caller gets a non-nil error, so
//     under WR-023/WR-024 the message is neither processed nor deleted, and the
//     redrive policy parks it in the DLQ where the bug is discoverable.
//
// The error is a sentinel so callers and tests can distinguish "you passed me
// garbage" from "the store is down" — the first needs a code fix, the second a
// retry. The boolean is true, consistent with the invariant that false means
// "newly recorded" and nothing else.
func TestIsDuplicateRejectsAnEmptyKey(t *testing.T) {
	store := newFakeStore()

	dup, err := IsDuplicate(context.Background(), store, "")
	if err == nil {
		t.Fatalf("IsDuplicate(ctx, store, \"\") returned (%t, nil); want a non-nil ErrEmptyKey, "+
			"an empty idempotency key is out of contract and must not be silently accepted", dup)
	}
	if !errors.Is(err, ErrEmptyKey) {
		t.Errorf("IsDuplicate(ctx, store, \"\") error = %v; want one matching ErrEmptyKey via errors.Is", err)
	}
	if !dup {
		t.Errorf("IsDuplicate(ctx, store, \"\") = false; want true, since false is reserved for " +
			"\"the key was newly recorded\" and nothing was recorded here")
	}
	if got := store.callCount(); got != 0 {
		t.Errorf("store saw %d calls for an empty key, want 0: validation happens before the store is "+
			"consulted, so no out-of-contract key is ever written", got)
	}
}

// TestIsDuplicateEmptyKeyDoesNotDisturbValidKeys checks the rejection is inert:
// a bad call in the middle of a stream must not poison the state of real keys
// or make the next real delivery look like a duplicate.
func TestIsDuplicateEmptyKeyDoesNotDisturbValidKeys(t *testing.T) {
	store := newFakeStore()

	if dup, err := IsDuplicate(context.Background(), store, keyA); err != nil || dup {
		t.Fatalf("setup: IsDuplicate for A = (%t, %v), want (false, nil)", dup, err)
	}
	if _, err := IsDuplicate(context.Background(), store, ""); err == nil {
		t.Fatal("IsDuplicate with an empty key returned a nil error, want ErrEmptyKey")
	}

	dup, err := IsDuplicate(context.Background(), store, keyB)
	if err != nil {
		t.Fatalf("IsDuplicate for B after an empty-key call returned error %v, want nil", err)
	}
	if dup {
		t.Error("IsDuplicate for B after an empty-key call = true, want false")
	}

	dup, err = IsDuplicate(context.Background(), store, keyA)
	if err != nil {
		t.Fatalf("IsDuplicate for A returned error %v, want nil", err)
	}
	if !dup {
		t.Error("IsDuplicate for A = false, want true; the rejected empty-key call must not have " +
			"cleared A's mark")
	}
}

// --- context -------------------------------------------------------------

type ctxKeyType struct{}

// TestIsDuplicatePassesTheCallerContextToTheStore pins that the caller's
// context reaches the store unchanged. The Store method takes a ctx precisely
// so a real backend can honor deadlines and cancellation on shutdown; a
// decision layer that substituted context.Background() or context.TODO() would
// pass every other test in this file while quietly making those deadlines
// unreachable.
func TestIsDuplicatePassesTheCallerContextToTheStore(t *testing.T) {
	store := newFakeStore()
	ctx := context.WithValue(context.Background(), ctxKeyType{}, "wr-010")

	if _, err := IsDuplicate(ctx, store, keyA); err != nil {
		t.Fatalf("IsDuplicate returned error %v, want nil", err)
	}

	got := store.lastContext()
	if got == nil {
		t.Fatal("the store recorded no context; IsDuplicate must call Store.CheckAndMark with a context")
	}
	if got != ctx {
		t.Errorf("the store received a different context than the caller passed (value = %v); "+
			"pass the caller's ctx straight through, do not substitute context.Background()",
			got.Value(ctxKeyType{}))
	}
}

// --- concurrency ---------------------------------------------------------

// TestIsDuplicateExactlyOneConcurrentCallerSeesKeyAsNew is the test that
// justifies the shape of the Store interface, and the reason CheckAndMark is
// one method instead of Seen + MarkSeen.
//
// Many workers poll the same queue (WR-022), and SQS can hand the same message
// to more than one of them at once. If deduplication is a read followed by a
// write, both callers can read "not seen" before either writes, both get false,
// and the event is processed twice. With a single atomic check-and-mark, one
// caller and exactly one gets false no matter how the goroutines interleave —
// which is what this asserts. Note the assertion is scheduling-independent:
// which goroutine wins is arbitrary, but the count is always exactly one.
//
// Run with -race, this also fails on an in-memory store whose map access is
// unsynchronized.
func TestIsDuplicateExactlyOneConcurrentCallerSeesKeyAsNew(t *testing.T) {
	const goroutines = 64

	store := newFakeStore()

	var (
		mu    sync.Mutex
		fresh int
		errs  []error
		start = make(chan struct{})
		wg    sync.WaitGroup
		ctx   = context.Background()
	)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start // release everyone at once, to maximize contention
			dup, err := IsDuplicate(ctx, store, keyA)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			if !dup {
				fresh++
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("got %d unexpected errors from concurrent IsDuplicate calls, first: %v", len(errs), errs[0])
	}
	if fresh != 1 {
		t.Errorf("%d of %d concurrent callers were told the key was new, want exactly 1; "+
			"the check and the mark must happen as one atomic operation, otherwise every caller that "+
			"observed \"not seen\" before the first mark landed will process the same event",
			fresh, goroutines)
	}
	if !store.isMarked(keyA) {
		t.Error("the key is not marked seen after the concurrent burst, want it marked")
	}
	if got := store.callCount(); got != goroutines {
		t.Errorf("store saw %d calls, want %d (one per caller)", got, goroutines)
	}
}

// TestIsDuplicateConcurrentDistinctKeysEachHaveOneWinner is the previous test
// widened: several keys contended in parallel must each produce exactly one
// "new" verdict, so the atomicity is per key rather than a single global lock
// that serializes but mis-attributes.
func TestIsDuplicateConcurrentDistinctKeysEachHaveOneWinner(t *testing.T) {
	const callersPerKey = 16

	keys := []string{keyA, keyB, keyC, keyD}
	store := newFakeStore()

	var (
		mu    sync.Mutex
		fresh = make(map[string]int, len(keys))
		errs  []error
		start = make(chan struct{})
		wg    sync.WaitGroup
	)

	for _, key := range keys {
		for i := 0; i < callersPerKey; i++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				<-start
				dup, err := IsDuplicate(context.Background(), store, key)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, fmt.Errorf("key %s: %w", key, err))
					return
				}
				if !dup {
					fresh[key]++
				}
			}(key)
		}
	}
	close(start)
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("got %d unexpected errors, first: %v", len(errs), errs[0])
	}
	for _, key := range keys {
		if fresh[key] != 1 {
			t.Errorf("key %s: %d of %d concurrent callers were told it was new, want exactly 1",
				key, fresh[key], callersPerKey)
		}
	}
	if got, want := store.callCount(), len(keys)*callersPerKey; got != want {
		t.Errorf("store saw %d calls, want %d", got, want)
	}
}

// --- the fake itself -----------------------------------------------------

// TestFakeStoreContract guards the guard. Every assertion above is only as
// trustworthy as the fake, so its three behaviors are pinned directly here: it
// reports a key as new exactly once (it is not vacuously always-false, which
// would make the duplicate tests pass for the wrong reason), a failing call
// marks nothing, and it keys on the exact string.
func TestFakeStoreContract(t *testing.T) {
	ctx := context.Background()

	t.Run("reports a key as new exactly once", func(t *testing.T) {
		store := newFakeStore()
		for i, want := range []bool{false, true, true} {
			got, err := store.CheckAndMark(ctx, keyA)
			if err != nil {
				t.Fatalf("call #%d: CheckAndMark returned error %v, want nil", i+1, err)
			}
			if got != want {
				t.Errorf("call #%d: CheckAndMark = %t, want %t", i+1, got, want)
			}
		}
	})

	t.Run("a failing call marks nothing", func(t *testing.T) {
		store := newFakeStore()
		store.failNext(errStore, false)
		if _, err := store.CheckAndMark(ctx, keyA); !errors.Is(err, errStore) {
			t.Fatalf("CheckAndMark error = %v, want %v", err, errStore)
		}
		if store.isMarked(keyA) {
			t.Error("the key is marked after a failed CheckAndMark, want it untouched")
		}
		store.recover()
		if got, err := store.CheckAndMark(ctx, keyA); err != nil || got {
			t.Errorf("CheckAndMark after recovery = (%t, %v), want (false, nil)", got, err)
		}
	})

	t.Run("keys are exact strings", func(t *testing.T) {
		store := newFakeStore()
		if got, _ := store.CheckAndMark(ctx, keyA); got {
			t.Fatalf("CheckAndMark(keyA) = true on a fresh store, want false")
		}
		for _, other := range []string{keyB, keyA[:63], keyA + "0", strings.ToUpper(keyA)} {
			if got, _ := store.CheckAndMark(ctx, other); got {
				t.Errorf("CheckAndMark(%q) = true, want false: it is a different key from %q", other, keyA)
			}
		}
	})
}
