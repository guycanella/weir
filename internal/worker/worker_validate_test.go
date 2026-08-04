// TestRunValidatesRequiredFields proves Worker.Run validates its required
// dependencies and configuration before starting the receive loop
// (ROADMAP_WR-021.md, "The worker validates its required dependencies and
// configuration before starting"), rather than panicking or nil-dereffing
// on the first SQS call. panicIfCalledSQS embeds a nil awsclient.SQSClient
// so any method other than ReceiveMessage still satisfies the interface,
// while ReceiveMessage itself panics if the loop is ever reached.
package worker_test

import (
	"context"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/worker"
)

// panicIfCalledSQS embeds a nil awsclient.SQSClient (so it satisfies the
// interface) and overrides ReceiveMessage to panic, proving that a
// validation failure returns before the receive loop ever calls it.
type panicIfCalledSQS struct {
	awsclient.SQSClient
}

func (panicIfCalledSQS) ReceiveMessage(context.Context, awsclient.ReceiveMessageInput) (awsclient.ReceiveMessageOutput, error) {
	panic("ReceiveMessage must not be called when Worker validation fails")
}

func TestRunValidatesRequiredFields(t *testing.T) {
	validProcess := func(context.Context, awsclient.Message) error { return nil }

	tests := []struct {
		name string
		w    worker.Worker
	}{
		{
			name: "missing SQSClient",
			w:    worker.New(worker.Worker{QueueURL: "http://example/queue", Process: validProcess}),
		},
		{
			name: "missing QueueURL",
			w:    worker.New(worker.Worker{SQSClient: panicIfCalledSQS{}, Process: validProcess}),
		},
		{
			name: "missing Process",
			w:    worker.New(worker.Worker{SQSClient: panicIfCalledSQS{}, QueueURL: "http://example/queue"}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.w.Run(context.Background())
			if err == nil {
				t.Fatal("Run() returned nil error, want a validation error")
			}
		})
	}
}
