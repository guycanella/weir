//go:build integration

// WR-021's integration proof, gated by the `integration` build tag (the
// Makefile's test-integration target is the only thing that sets it) and by
// a defensive AWS_ENDPOINT_URL skip, so `go test -tags=integration ./...`
// without LocalStack skips cleanly instead of failing.
//
// What this test adds over the in-process unit tests (worker_test.go) is
// exactly the part they cannot cover: real environment/client wiring, a
// real long poll against LocalStack, and BINARY-level SIGTERM handling —
// the compiled ./cmd/worker process, signal.NotifyContext, and the exit
// code. The deterministic proofs of cancel-mid-batch and grace expiry stay
// in the unit tests; this one deliberately does not race a signal against
// processing.
//
// It builds the binary rather than using `go run`, because `go run` is a
// wrapper process: SIGTERM would be delivered to the wrapper, and the exit
// code observed would be the wrapper's, not the worker's.
package worker_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/awssdk"
)

const (
	itEnvEndpointURL = "AWS_ENDPOINT_URL"
	itEnvRegion      = "AWS_REGION"
	itFallbackRegion = "us-east-1"

	// itMessageCount is the batch seeded into the queue. Ten is one full
	// ReceiveMessage batch, so a single receive can (but need not) drain it.
	itMessageCount = 10

	// itVisibilityTimeout is deliberately short: if the worker receives a
	// message and fails to delete it, it becomes visible again quickly and
	// the drain assertion below fails fast instead of stalling for the
	// default 30s.
	itVisibilityTimeout = 5

	// itDrainDeadline bounds "the queue reached zero", covering process
	// startup, the AWS client build and the long poll.
	itDrainDeadline = 90 * time.Second

	// itPollInterval is short so the drain is observed promptly — a fixed
	// startup sleep would be both slower and less reliable.
	itPollInterval = 250 * time.Millisecond

	// itExitDeadline bounds the graceful shutdown after SIGTERM. It must
	// exceed the worker's own shutdown grace only in the pathological case;
	// with an empty queue the worker's long poll is interrupted at once.
	itExitDeadline = 45 * time.Second

	// itBuildTimeout bounds `go build ./cmd/worker`.
	itBuildTimeout = 5 * time.Minute

	// itCleanupTimeout bounds teardown. Cleanups cannot use t.Context():
	// it is canceled before t.Cleanup runs.
	itCleanupTimeout = 30 * time.Second
)

// itLocalStackEnv returns the endpoint and region to test against, skipping
// rather than failing when no endpoint is configured.
func itLocalStackEnv(t *testing.T) (endpoint, region string) {
	t.Helper()

	endpoint = strings.TrimSpace(os.Getenv(itEnvEndpointURL))
	if endpoint == "" {
		t.Skipf("%s is not set — run this suite via 'make test-integration' with LocalStack up", itEnvEndpointURL)
	}

	region = strings.TrimSpace(os.Getenv(itEnvRegion))
	if region == "" {
		region = itFallbackRegion
		t.Logf("%s is not set; defaulting to %q", itEnvRegion, itFallbackRegion)
	}

	return endpoint, region
}

// itClients builds the adapters the same way cmd/worker does.
func itClients(t *testing.T, ctx context.Context) *awssdk.Clients {
	t.Helper()

	endpoint, region := itLocalStackEnv(t)

	clients, err := awssdk.NewClients(ctx, awssdk.Config{Region: region, EndpointURL: endpoint})
	if err != nil {
		t.Fatalf("awssdk.NewClients(region=%q, endpoint=%q): %v", region, endpoint, err)
	}
	if clients == nil || clients.SQS == nil {
		t.Fatalf("awssdk.NewClients returned incomplete Clients: %+v", clients)
	}
	return clients
}

// itRawSQSClient builds an SQS client directly (not through awssdk.Config)
// for fixture work the awsclient interface does not expose — DeleteQueue.
// Keeping it independent of the code under test also makes it an oracle.
func itRawSQSClient(t *testing.T, ctx context.Context) *sqs.Client {
	t.Helper()

	endpoint, region := itLocalStackEnv(t)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("load AWS config for raw SQS fixture client: %v", err)
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

// itUniqueName builds a collision-resistant queue name, so repeat and
// concurrent runs never contend and a leaked queue from an earlier run
// cannot make a later one pass.
func itUniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%06d", prefix, time.Now().UnixNano(), rand.IntN(1_000_000))
}

// TestWorkerBinaryDrainsQueueAndExitsOnSIGTERM is WR-021's Done-when,
// literally: the worker drains a test queue and shuts down cleanly on
// signal.
func TestWorkerBinaryDrainsQueueAndExitsOnSIGTERM(t *testing.T) {
	ctx := t.Context()
	endpoint, region := itLocalStackEnv(t)
	clients := itClients(t, ctx)
	rawSQS := itRawSQSClient(t, ctx)

	// ── fixture queue ───────────────────────────────────────────────────
	name := itUniqueName("weir-wr021")
	created, err := clients.SQS.CreateQueue(ctx, awsclient.CreateQueueInput{
		Name: name,
		Attributes: map[string]string{
			"VisibilityTimeout": strconv.Itoa(itVisibilityTimeout),
		},
	})
	if err != nil {
		t.Fatalf("fixture CreateQueue(%q): %v", name, err)
	}
	queueURL := created.QueueUrl
	if queueURL == "" {
		t.Fatal("fixture CreateQueue returned an empty QueueUrl")
	}

	// Registered immediately, so the queue is removed even if the test
	// fatals partway through.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), itCleanupTimeout)
		defer cancel()
		if _, err := rawSQS.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)}); err != nil {
			t.Errorf("teardown DeleteQueue(%q): %v — LocalStack may be left with a leftover test queue", queueURL, err)
		}
	})

	for i := 1; i <= itMessageCount; i++ {
		if _, err := clients.SQS.SendMessage(ctx, awsclient.SendMessageInput{
			QueueUrl: queueURL,
			Body:     fmt.Sprintf("wr-021 integration message %d", i),
		}); err != nil {
			t.Fatalf("fixture SendMessage(%d): %v", i, err)
		}
	}

	// ── build and start the worker binary ───────────────────────────────
	bin := buildWorkerBinary(t)

	// Captured through a mutex-protected writer, not a bare bytes.Buffer:
	// because Stdout/Stderr are not *os.File, os/exec spawns copy goroutines
	// that keep writing until Wait returns, while the assertions below (and
	// t.Cleanup) read the captured text. Per os/exec's docs that read is only
	// unsynchronized-safe after Wait; the mutex makes it safe at any time.
	var stdout, stderr itSafeBuffer
	cmd := exec.Command(bin)
	cmd.Env = []string{
		"QUEUE_URL=" + queueURL,
		itEnvRegion + "=" + region,
		itEnvEndpointURL + "=" + endpoint,
		// LocalStack's dummy credentials, supplied explicitly so the
		// subprocess never picks up a real profile from the environment.
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_EC2_METADATA_DISABLED=true",
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker binary %q: %v", bin, err)
	}

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	// Kill the subprocess unconditionally on the way out, so a failing
	// assertion never leaves an orphan polling LocalStack.
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		t.Logf("worker stdout:\n%s", stdout.String())
		t.Logf("worker stderr:\n%s", stderr.String())
	})

	// ── the worker drains the queue ─────────────────────────────────────
	// Both counts must reach zero: checking only ApproximateNumberOfMessages
	// would let a received-but-undeleted message masquerade as a drained
	// queue (it is merely invisible, and comes back after the visibility
	// timeout).
	deadline := time.Now().Add(itDrainDeadline)
	var lastVisible, lastInFlight string
	drained := false
	for time.Now().Before(deadline) {
		select {
		case err := <-waitErr:
			t.Fatalf("worker exited before draining the queue (%v)\nstdout:\n%s\nstderr:\n%s",
				err, stdout.String(), stderr.String())
		default:
		}

		out, err := clients.SQS.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
			QueueUrl: queueURL,
			AttributeNames: []string{
				"ApproximateNumberOfMessages",
				"ApproximateNumberOfMessagesNotVisible",
			},
		})
		if err != nil {
			t.Fatalf("GetQueueAttributes while polling for drain: %v", err)
		}
		lastVisible = out.Attributes["ApproximateNumberOfMessages"]
		lastInFlight = out.Attributes["ApproximateNumberOfMessagesNotVisible"]

		if lastVisible == "0" && lastInFlight == "0" {
			drained = true
			break
		}
		time.Sleep(itPollInterval)
	}
	if !drained {
		t.Fatalf("queue %q did not drain within %s: ApproximateNumberOfMessages=%q, ApproximateNumberOfMessagesNotVisible=%q\nstdout:\n%s\nstderr:\n%s",
			queueURL, itDrainDeadline, lastVisible, lastInFlight, stdout.String(), stderr.String())
	}

	// ── SIGTERM ends the process cleanly ────────────────────────────────
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM to worker (pid %d): %v", cmd.Process.Pid, err)
	}

	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("worker exited with %v after SIGTERM, want a clean exit (status 0)\nstdout:\n%s\nstderr:\n%s",
				err, stdout.String(), stderr.String())
		}
		if code := cmd.ProcessState.ExitCode(); code != 0 {
			t.Errorf("worker exit code = %d after SIGTERM, want 0", code)
		}
	case <-time.After(itExitDeadline):
		t.Fatalf("worker did not exit within %s of SIGTERM — graceful shutdown is stuck\nstdout:\n%s\nstderr:\n%s",
			itExitDeadline, stdout.String(), stderr.String())
	}
}

// buildWorkerBinary compiles ./cmd/worker into the test's temp dir and
// returns the binary's path. Compiling (instead of `go run`) is what makes
// the SIGTERM and exit-code assertions meaningful: there is no wrapper
// process between the test and the worker.
func buildWorkerBinary(t *testing.T) string {
	t.Helper()

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	bin := filepath.Join(t.TempDir(), "weir-worker")

	buildCtx, cancel := context.WithTimeout(context.Background(), itBuildTimeout)
	defer cancel()

	build := exec.CommandContext(buildCtx, "go", "build", "-o", bin, "./cmd/worker")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build ./cmd/worker: %v\n%s", err, out)
	}

	return bin
}

// itSafeBuffer is a bytes.Buffer guarded by a mutex, so os/exec's stdout and
// stderr copy goroutines can write to it while the test reads the captured
// text — including from t.Cleanup, which kills the process without awaiting
// Wait. Every access takes the lock, so no read can race a write.
type itSafeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *itSafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *itSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
