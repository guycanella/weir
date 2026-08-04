// Package worker_test pins a lifecycle guarantee that ROADMAP_WR-021.md
// requires but the other test files do not exercise directly: no watchdog
// goroutine started by Worker.Run may still be alive once Run has returned.
package worker_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/worker"
)

// TestRunDoesNotLeakWatchdogGoroutine drives Run through a normal
// cancellation-triggered shutdown and then asserts, via a bounded
// retry-poll of runtime.NumGoroutine (never a fixed sleep), that the
// goroutine count returns to its pre-Run baseline. A leaked watchdog would
// keep the count elevated for the lifetime of the process.
func TestRunDoesNotLeakWatchdogGoroutine(t *testing.T) {
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

	baseline := runtime.NumGoroutine()

	if err := runWorker(t, w, ctx, cancel); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.Gosched()
		if n := runtime.NumGoroutine(); n <= baseline {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("goroutine count settled at %d, want <= baseline %d — a watchdog goroutine outlived Run", n, baseline)
		}
		time.Sleep(time.Millisecond)
	}
}
