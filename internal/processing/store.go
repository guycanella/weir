package processing

import (
	"context"
	"sync"

	"github.com/guycanella/weir/internal/dedup"
)

// InMemoryStore is a dedup.Store backed by a plain map guarded by a mutex.
//
// This is a deliberate MVP scope cut, not an oversight: no task in the
// project's backlog builds a durable, networked backend (e.g. DynamoDB) for
// dedup.Store, so this is the only implementation the demo ships with. Its
// limitations are accepted, not silently papered over:
//
//   - Not durable: every key it has seen is lost when the worker process
//     restarts, so a redelivery after a restart is treated as fresh again.
//   - Not shared: each worker replica gets its own InMemoryStore, so two
//     concurrent replicas can both treat the same event as fresh and
//     double-process it.
//   - Not bounded: keys are never evicted, so memory grows with the number
//     of distinct events the process has seen for as long as it runs. This
//     project's scale-to-zero operating model (ADR-002) bounds a worker
//     pod's typical lifetime in practice; a long-lived deployment outside
//     that model would need eviction or a real backend.
//
// Within a single process, CheckAndMark is safe for concurrent use and
// atomic: two goroutines racing on the same key never both observe
// "not a duplicate".
type InMemoryStore struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewInMemoryStore returns a ready-to-use InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{seen: make(map[string]struct{})}
}

// CheckAndMark implements dedup.Store.
func (s *InMemoryStore) CheckAndMark(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, alreadySeen := s.seen[key]
	s.seen[key] = struct{}{}
	return alreadySeen, nil
}

var _ dedup.Store = (*InMemoryStore)(nil)
