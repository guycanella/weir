// Package fake provides in-memory, concurrency-safe fake implementations of
// the interfaces in internal/awsclient, for use as test doubles by any
// package that needs to exercise S3/SNS/SQS-driven logic without a real
// AWS or LocalStack endpoint (WR-016).
//
// Each fake records what was done to it (objects put, topics created,
// messages sent/received/deleted, ...) as plain exported fields, so a test
// can assert on that state directly. Each fake also supports error
// injection via InjectError, so a test can simulate an AWS failure on a
// specific call.
package fake

import "sync"

// errorQueue lets a test arrange for a fake method to fail on its next N
// calls. It is keyed by method name (see the exported <Fake>Method
// constants in each fake's file) so a single queue can serve every method
// on a fake.
type errorQueue struct {
	mu      sync.Mutex
	pending map[string][]error
}

func newErrorQueue() *errorQueue {
	return &errorQueue{pending: make(map[string][]error)}
}

// push arranges for the next n calls (n < 1 is treated as 1) to method to
// return err.
func (q *errorQueue) push(method string, err error, n int) {
	if n < 1 {
		n = 1
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for i := 0; i < n; i++ {
		q.pending[method] = append(q.pending[method], err)
	}
}

// next reports, and consumes, the next injected error for method, if any.
func (q *errorQueue) next(method string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	pending := q.pending[method]
	if len(pending) == 0 {
		return nil
	}

	err := pending[0]
	q.pending[method] = pending[1:]
	return err
}
