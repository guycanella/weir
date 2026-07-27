// Package dedup decides whether an already-computed idempotency key (WR-008)
// has been seen before, so a re-delivered event is not processed twice. It is
// the decision layer only: it does not compute keys, does not talk to any
// real store, and does not decide what a caller should do about a duplicate.
package dedup

import (
	"context"
	"errors"
	"fmt"
)

// Store records which idempotency keys have already been seen.
// Implementations must be safe for concurrent use.
//
// CheckAndMark records a key as seen optimistically, at check time — not
// after the caller has finished processing the event. On a store-error
// return, the caller skips processing and skips deleting the SQS message,
// so the event redelivers and is visible, eventually reaching the DLQ if it
// keeps failing — PROVIDED the failed call actually left the key unmarked.
// The interface does not, by itself, guarantee that: for a real
// network-backed conditional write (e.g. a DynamoDB PutItem), the write can
// commit successfully server-side and the caller can still observe an error
// (a lost response, or a context deadline that expires before the ack
// arrives). In that case the key is durably marked despite the error, so a
// redelivery of that same event is later reported as a duplicate and
// silently skipped — even though it was never actually processed. This is a
// distinct, deeper failure mode than the crash-after-successful-mark case
// below, because there the error path itself is not reliable.
//
// Separately: a nil-error return that marks the key seen is not itself proof
// the event was processed: if a worker crashes after a successful
// CheckAndMark but before finishing the work, the key is already marked
// seen, so a redelivery of that same event is reported as a duplicate and
// silently skipped and deleted — a genuine at-most-once failure mode.
//
// This decision layer does not attempt to close either gap. Closing the
// ambiguous-write-outcome gap is a real design question for whoever builds
// a real Store backend (a later task): either (a) that implementation must
// itself guarantee "error implies not marked" through its own means (e.g. a
// read-after-write reconciliation check), or (b) this interface may
// eventually need to evolve into an explicit claim lifecycle (e.g.
// acquire-with-lease, then a separate confirm/release step) so ambiguous
// write outcomes are distinguishable at the type level. Both are out of
// scope here.
type Store interface {
	// CheckAndMark atomically reports whether key has been seen before, and
	// marks it seen for all future calls, in one operation.
	CheckAndMark(ctx context.Context, key string) (alreadySeen bool, err error)
}

// ErrEmptyKey is returned when IsDuplicate is called with an empty key — a
// caller bug, since a real idempotency key (WR-008) is never empty.
var ErrEmptyKey = errors.New("dedup: key must not be empty")

// IsDuplicate reports whether key has already been seen, using store to
// check and atomically record it.
//
// false is returned if and only if the key was successfully recorded as seen
// for the first time. Whenever err is non-nil, the returned boolean is true:
// a store failure must never be reported as a fresh key, since a caller that
// ignores the error would otherwise silently double-process events.
func IsDuplicate(ctx context.Context, store Store, key string) (bool, error) {
	if key == "" {
		return true, ErrEmptyKey
	}

	alreadySeen, err := store.CheckAndMark(ctx, key)
	if err != nil {
		return true, fmt.Errorf("dedup: check and mark key: %w", err)
	}

	return alreadySeen, nil
}
