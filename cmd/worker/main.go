// Command worker runs Weir's SQS consume/delete loop (WR-021). It wires
// environment configuration into internal/awsclient/awssdk's real AWS
// clients and internal/worker's Worker, and shuts down cleanly on
// SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/guycanella/weir/internal/awsclient/awssdk"
	"github.com/guycanella/weir/internal/processing"
	"github.com/guycanella/weir/internal/telemetry"
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
	if errors.Is(err, strconv.ErrRange) && n > 0 {
		// Atoi returns the clamped platform maximum together with ErrRange.
		return maxConcurrency
	}
	if err != nil || n <= 0 {
		return 0
	}
	if n > maxConcurrency {
		return maxConcurrency
	}
	return n
}

func main() {
	logger := newLogger(nil, parseLogLevel(os.Getenv("LOG_LEVEL")))
	if err := run(logger); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

// run wires up configuration, telemetry, AWS clients, and the worker, and
// runs the consume loop to completion. It returns the first error
// encountered instead of calling os.Exit directly, so every deferred
// cleanup (signal-context stop, telemetry shutdown) unwinds normally before
// main decides whether to exit non-zero.
func run(logger *slog.Logger) error {
	queueURL := strings.TrimSpace(os.Getenv("QUEUE_URL"))
	region := strings.TrimSpace(os.Getenv("AWS_REGION"))
	endpointURL := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL"))
	outputBucket := strings.TrimSpace(os.Getenv("OUTPUT_BUCKET"))
	concurrency := parseConcurrency(os.Getenv("WORKER_CONCURRENCY"))

	if queueURL == "" || region == "" || outputBucket == "" {
		return fmt.Errorf("missing required configuration: QUEUE_URL_set=%v AWS_REGION_set=%v OUTPUT_BUCKET_set=%v",
			queueURL != "", region != "", outputBucket != "")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Setup(ctx, os.Stderr)
	if err != nil {
		return fmt.Errorf("set up telemetry: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			logger.Warn("shut down telemetry", "error", err)
		}
	}()

	clients, err := awssdk.NewClients(ctx, awssdk.Config{Region: region, EndpointURL: endpointURL})
	if err != nil {
		return fmt.Errorf("build AWS clients: %w", err)
	}

	process, err := processing.New(processing.Config{
		S3Client:     clients.S3,
		OutputBucket: outputBucket,
		Store:        processing.NewInMemoryStore(),
	})
	if err != nil {
		return fmt.Errorf("build processing pipeline: %w", err)
	}

	logger.Warn("deduplication is in-memory and per-process",
		"durable", false,
		"shared_across_replicas", false,
	)

	instrumentedProcess, err := telemetry.InstrumentProcess(process, telemetry.Config{})
	if err != nil {
		return fmt.Errorf("instrument processing pipeline: %w", err)
	}

	w := worker.New(worker.Worker{
		SQSClient:     clients.SQS,
		QueueURL:      queueURL,
		ShutdownGrace: shutdownGrace,
		Concurrency:   concurrency,
		Process:       instrumentedProcess,
	})

	if err := w.Run(ctx); err != nil {
		return fmt.Errorf("worker run: %w", err)
	}

	return nil
}
