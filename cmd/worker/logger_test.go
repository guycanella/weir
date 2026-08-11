package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// WR-025's first Done-when is "logs are structured". slog's API is already
// structured, but slog.Default() writes key=value TEXT, which is not
// machine-parseable in a log pipeline — so the thing worth pinning is the
// HANDLER, not the call sites.
//
// This assumes two small seams in cmd/worker:
//
//	func parseLogLevel(raw string) slog.Level
//	func newLogger(out io.Writer, level slog.Level) *slog.Logger
//
// where a nil out means os.Stdout. The split mirrors the one main.go already
// uses for WORKER_CONCURRENCY (`parseConcurrency(os.Getenv(...))`): the
// env-var STRING is parsed by a pure function that is exhaustively
// table-tested, and the imperative shell (main) does the os.Getenv. Reading
// the environment inside newLogger would instead force every level test
// through t.Setenv and hide the parsing rules inside an I/O-shaped seam, so
// it is deliberately not done that way.
//
// main() itself stays untestable by nature (it exits the process), so nothing
// further in it is test-driven here.

func TestNewLoggerEmitsOneJSONObjectPerRecord(t *testing.T) {
	var buf bytes.Buffer

	logger := newLogger(&buf, slog.LevelInfo)
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

// TestNewLoggerDefaultsToStdout pins that main() can call newLogger(nil, ...),
// so the default sink lives in one place instead of being restated at the call
// site.
func TestNewLoggerDefaultsToStdout(t *testing.T) {
	if newLogger(nil, slog.LevelInfo) == nil {
		t.Fatal("newLogger(nil, ...) returned nil; a nil writer must mean os.Stdout")
	}
}

// --- log level ------------------------------------------------------------

// TestParseLogLevel pins the LOG_LEVEL contract. Verbosity has to be
// changeable on a running deployment (edit the env var, restart the pod), not
// at build time — but a bad value must never be able to stop the worker from
// starting or, worse, silence it. So parsing is total: every input maps to a
// level, and anything unrecognised falls back to Info.
//
// Inputs are trimmed before parsing, matching every other env var main.go
// reads (QUEUE_URL, AWS_REGION, WORKER_CONCURRENCY, ...): a stray newline out
// of a ConfigMap must not silently downgrade a deliberate DEBUG request back
// to Info.
//
// Only the four canonical names are pinned. slog's own
// (*slog.Level).UnmarshalText additionally understands offset forms such as
// "DEBUG+2"; that is an implementation detail of whichever parser is used and
// is deliberately left unpinned rather than frozen either way.
func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want slog.Level
	}{
		{name: "unset", raw: "", want: slog.LevelInfo},
		{name: "blank", raw: "   ", want: slog.LevelInfo},
		{name: "debug", raw: "DEBUG", want: slog.LevelDebug},
		{name: "info", raw: "INFO", want: slog.LevelInfo},
		{name: "warn", raw: "WARN", want: slog.LevelWarn},
		{name: "error", raw: "ERROR", want: slog.LevelError},
		{name: "lowercase is accepted", raw: "debug", want: slog.LevelDebug},
		{name: "mixed case is accepted", raw: "Warn", want: slog.LevelWarn},
		{name: "surrounding whitespace is trimmed", raw: " error \n", want: slog.LevelError},
		{name: "an unknown name falls back to info", raw: "verbose", want: slog.LevelInfo},
		{name: "a bare number falls back to info", raw: "42", want: slog.LevelInfo},
		{name: "garbage falls back to info", raw: "!!", want: slog.LevelInfo},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseLogLevel(tc.raw); got != tc.want {
				t.Errorf("parseLogLevel(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// TestNewLoggerRespectsTheConfiguredLevel is the half that actually matters:
// parsing the level is useless if the handler ignores it. A logger built at
// Debug must EMIT debug records (that is the whole point of raising
// verbosity), and a logger built at the Info default must DROP them — pinning
// the drop too, because a handler built with a nil HandlerOptions silently
// takes Info and would make the Debug case the only detectable failure.
func TestNewLoggerRespectsTheConfiguredLevel(t *testing.T) {
	tests := []struct {
		name        string
		level       slog.Level
		wantEmitted map[string]bool // record level name -> should appear
	}{
		{
			name:        "debug emits every level",
			level:       slog.LevelDebug,
			wantEmitted: map[string]bool{"DEBUG": true, "INFO": true, "WARN": true, "ERROR": true},
		},
		{
			name:        "info drops debug",
			level:       slog.LevelInfo,
			wantEmitted: map[string]bool{"DEBUG": false, "INFO": true, "WARN": true, "ERROR": true},
		},
		{
			name:        "error keeps only errors",
			level:       slog.LevelError,
			wantEmitted: map[string]bool{"DEBUG": false, "INFO": false, "WARN": false, "ERROR": true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := newLogger(&buf, tc.level)
			if logger == nil {
				t.Fatal("newLogger returned nil")
			}

			logger.Debug("debug record")
			logger.Info("info record")
			logger.Warn("warn record")
			logger.Error("error record")

			out := buf.String()
			for _, levelName := range []string{"DEBUG", "INFO", "WARN", "ERROR"} {
				got := strings.Contains(out, `"level":"`+levelName+`"`)
				if want := tc.wantEmitted[levelName]; got != want {
					t.Errorf("with newLogger(..., %v): %s record emitted = %v, want %v\n--- output ---\n%s",
						tc.level, levelName, got, want, out)
				}
			}
		})
	}
}

// TestNewLoggerAtDebugEmitsParseableDebugRecords guards the case an
// enabled-level check alone would miss: raising verbosity must not degrade the
// output format, since the pipeline parsing these lines does not care which
// level produced them.
func TestNewLoggerAtDebugEmitsParseableDebugRecords(t *testing.T) {
	var buf bytes.Buffer

	newLogger(&buf, parseLogLevel("debug")).Debug("received message", "message_id", "msg-0001")

	line := bytes.TrimSpace(buf.Bytes())
	if len(line) == 0 {
		t.Fatal("a Debug record was dropped by a logger built from LOG_LEVEL=debug")
	}

	var rec map[string]any
	if err := json.Unmarshal(line, &rec); err != nil {
		t.Fatalf("debug record is not valid JSON (%v): %s", err, line)
	}
	if got, want := rec["level"], "DEBUG"; got != want {
		t.Errorf("level = %v, want %v", got, want)
	}
	if got, want := rec["message_id"], "msg-0001"; got != want {
		t.Errorf("message_id = %v, want %v", got, want)
	}
}
