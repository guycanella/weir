package processing

import (
	"context"
	"sync"
	"testing"
)

// TestInMemoryStoreCheckAndMark pins the basic dedup.Store contract for
// InMemoryStore: the first call for a key reports "not seen", every
// subsequent call for the same key reports "seen", and distinct keys don't
// interfere with each other.
func TestInMemoryStoreCheckAndMark(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()

	dup, err := s.CheckAndMark(ctx, "a")
	if err != nil {
		t.Fatalf("CheckAndMark returned error %v, want nil", err)
	}
	if dup {
		t.Fatal("first CheckAndMark for a fresh key reported a duplicate, want false")
	}

	dup, err = s.CheckAndMark(ctx, "a")
	if err != nil {
		t.Fatalf("CheckAndMark returned error %v, want nil", err)
	}
	if !dup {
		t.Fatal("second CheckAndMark for the same key reported not-a-duplicate, want true")
	}

	dup, err = s.CheckAndMark(ctx, "b")
	if err != nil {
		t.Fatalf("CheckAndMark returned error %v, want nil", err)
	}
	if dup {
		t.Fatal("first CheckAndMark for a different key reported a duplicate, want false")
	}
}

// TestInMemoryStoreCheckAndMarkIsConcurrencySafe pins that exactly one
// caller among many concurrent CheckAndMark calls for the same key observes
// "not a duplicate" — the property the dedup decision layer relies on to
// avoid double-processing under redelivery races.
func TestInMemoryStoreCheckAndMarkIsConcurrencySafe(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()

	const n = 100
	results := make([]bool, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			dup, err := s.CheckAndMark(ctx, "race-key")
			if err != nil {
				t.Errorf("CheckAndMark returned error %v, want nil", err)
			}
			results[i] = dup
		}(i)
	}
	wg.Wait()

	freshCount := 0
	for _, dup := range results {
		if !dup {
			freshCount++
		}
	}
	if freshCount != 1 {
		t.Fatalf("got %d calls reporting not-a-duplicate across %d concurrent calls for the same key, want exactly 1", freshCount, n)
	}
}
