package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

// WR-025's first Done-when is "logs are structured". slog's API is already
// structured, but slog.Default() writes key=value TEXT, which is not
// machine-parseable in a log pipeline — so the thing worth pinning is the
// HANDLER, not the call sites.
//
// This assumes one small seam in main.go:
//
//	func newLogger(out io.Writer) *slog.Logger
//
// where a nil out means os.Stdout. That is the minimum needed to test the
// handler choice at all; main() itself stays untestable by nature (it exits
// the process), so nothing further in it is test-driven here.

func TestNewLoggerEmitsOneJSONObjectPerRecord(t *testing.T) {
	var buf bytes.Buffer

	logger := newLogger(&buf)
	if logger == nil {
		t.Fatal("newLogger returned nil")
	}

	logger.Error("worker exited with error", "error", "boom", "attempts", 3)

	line := bytes.TrimSpace(buf.Bytes())
	if len(line) == 0 {
		t.Fatal("newLogger wrote nothing to the provided writer")
	}
	if bytes.Contains(line, []byte("msg=")) {
		t.Fatalf("output looks like slog's TextHandler, not JSON: %s", line)
	}

	var rec map[string]any
	if err := json.Unmarshal(line, &rec); err != nil {
		t.Fatalf("log record is not valid JSON (%v): %s", err, line)
	}

	// Structural keys a log pipeline filters on, plus the caller's attributes
	// as first-class fields rather than interpolated into the message.
	for _, key := range []string{"time", "level", "msg", "error", "attempts"} {
		if _, ok := rec[key]; !ok {
			t.Errorf("log record has no %q key: %s", key, line)
		}
	}
	if got, want := rec["level"], "ERROR"; got != want {
		t.Errorf("level = %v, want %v", got, want)
	}
	if got, want := rec["msg"], "worker exited with error"; got != want {
		t.Errorf("msg = %v, want %v", got, want)
	}
	if got, want := rec["attempts"], float64(3); got != want {
		t.Errorf("attempts = %v (%T), want the number %v, not a string", got, got, want)
	}
}

// TestNewLoggerDefaultsToStdout pins that main() can call newLogger(nil), so
// the default sink lives in one place instead of being restated at the call
// site.
func TestNewLoggerDefaultsToStdout(t *testing.T) {
	if newLogger(nil) == nil {
		t.Fatal("newLogger(nil) returned nil; a nil writer must mean os.Stdout")
	}
}
