// Command worker runs Weir's SQS consume/delete loop (WR-021). It wires
// environment configuration into internal/awsclient/awssdk's real AWS
// clients and internal/worker's Worker, and shuts down cleanly on
// SIGINT/SIGTERM.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/awssdk"
	"github.com/guycanella/weir/internal/worker"
)

const shutdownGrace = 30 * time.Second

// maxConcurrency caps WORKER_CONCURRENCY so a misconfigured or malicious
// value can't drive worker.Worker's O(concurrency) semaphore-priming loop
// into stalling startup or unbounding in-flight goroutines/SQS calls. This
// is a local input-validation clamp only; the CRD's spec.worker.concurrency
// field has no +kubebuilder:validation:Maximum yet — adding one is deferred
// to whichever future task wires that field into this env var.
const maxConcurrency = 100

// parseConcurrency reads WORKER_CONCURRENCY, returning 0 (worker.New's
// default) when it is unset, blank, not a valid integer, or non-positive.
// Unlike QUEUE_URL/AWS_REGION, a bad value here must not fail startup — it
// just falls back to sequential processing. Values above maxConcurrency are
// clamped down to it.
func parseConcurrency(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0
	}
	if n > maxConcurrency {
		return maxConcurrency
	}
	return n
}

func main() {
	logger := slog.Default()

	queueURL := strings.TrimSpace(os.Getenv("QUEUE_URL"))
	region := strings.TrimSpace(os.Getenv("AWS_REGION"))
	endpointURL := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL"))
	concurrency := parseConcurrency(os.Getenv("WORKER_CONCURRENCY"))

	if queueURL == "" || region == "" {
		logger.Error("missing required configuration", "QUEUE_URL_set", queueURL != "", "AWS_REGION_set", region != "")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	clients, err := awssdk.NewClients(ctx, awssdk.Config{Region: region, EndpointURL: endpointURL})
	if err != nil {
		logger.Error("build AWS clients", "error", err)
		os.Exit(1)
	}

	w := worker.New(worker.Worker{
		SQSClient:     clients.SQS,
		QueueURL:      queueURL,
		ShutdownGrace: shutdownGrace,
		Concurrency:   concurrency,
		Process: func(_ context.Context, msg awsclient.Message) error {
			logger.Info("processing message", "message_id", msg.MessageId)
			return nil
		},
	})

	if err := w.Run(ctx); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}
