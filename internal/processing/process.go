// Package processing is the dispatch layer that wires the pure S3 event
// parser (internal/events), the pure idempotency key derivation
// (internal/idempotency) and the duplicate-detection decision layer
// (internal/dedup) into one worker.ProcessFunc (WR-023): parse a message,
// skip what has already been seen, run a pluggable "processing body" (the
// Stub) on the rest, and write each result to an output S3 bucket.
//
// See helpers_test.go for the full API this package pins down and the five
// load-bearing design decisions behind it.
package processing

import (
	"context"
	"errors"
	"fmt"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/dedup"
	"github.com/guycanella/weir/internal/events"
	"github.com/guycanella/weir/internal/idempotency"
	"github.com/guycanella/weir/internal/worker"
)

// StubFunc computes the processing result for a single S3 event. It derives
// its output purely from the event's metadata (see DefaultStub's doc
// comment for why: awsclient.S3Client has no GetObject).
type StubFunc func(events.Event) ([]byte, error)

// Config configures New. S3Client, OutputBucket and Store are required;
// Stub and ContentType are optional and fall back to DefaultStub and
// DefaultContentType respectively.
type Config struct {
	S3Client     awsclient.S3Client
	OutputBucket string
	Store        dedup.Store
	Stub         StubFunc
	ContentType  string
}

// New validates cfg and returns a worker.ProcessFunc that parses each
// message, skips duplicate events, runs the stub on fresh ones, and writes
// each result to cfg.OutputBucket. A missing required dependency is a
// wiring bug and is reported here, at construction time, rather than
// surfacing later as an endlessly redelivered message.
func New(cfg Config) (worker.ProcessFunc, error) {
	if cfg.S3Client == nil {
		return nil, fmt.Errorf("processing: S3Client is required")
	}
	if cfg.OutputBucket == "" {
		return nil, fmt.Errorf("processing: OutputBucket is required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("processing: Store is required")
	}

	stub := cfg.Stub
	if stub == nil {
		stub = DefaultStub
	}
	contentType := cfg.ContentType
	if contentType == "" {
		contentType = DefaultContentType
	}

	return func(ctx context.Context, msg awsclient.Message) error {
		evts, err := events.ParseS3Events([]byte(msg.Body))
		if err != nil {
			if errors.Is(err, events.ErrNotNotification) {
				return nil
			}
			return fmt.Errorf("processing: parse message: %w", err)
		}
		if len(evts) == 0 {
			return nil
		}

		for _, evt := range evts {
			key := idempotency.Key(evt.Bucket, evt.Key, evt.VersionID, evt.ETag)

			dup, err := dedup.IsDuplicate(ctx, cfg.Store, key)
			if err != nil {
				return fmt.Errorf("processing: %s/%s: check duplicate: %w", evt.Bucket, evt.Key, err)
			}
			if dup {
				continue
			}

			body, err := stub(evt)
			if err != nil {
				return fmt.Errorf("processing: %s/%s: run stub: %w", evt.Bucket, evt.Key, err)
			}

			if _, err := cfg.S3Client.PutObject(ctx, awsclient.PutObjectInput{
				Bucket:      cfg.OutputBucket,
				Key:         OutputKey(evt),
				Body:        body,
				ContentType: contentType,
			}); err != nil {
				return fmt.Errorf("processing: %s/%s: put object: %w", evt.Bucket, evt.Key, err)
			}
		}

		return nil
	}, nil
}
