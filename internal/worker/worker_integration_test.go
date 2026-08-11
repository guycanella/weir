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
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/awssdk"
	"github.com/guycanella/weir/internal/events"
	"github.com/guycanella/weir/internal/idempotency"
	"github.com/guycanella/weir/internal/processing"
)

const (
	itEnvEndpointURL = "AWS_ENDPOINT_URL"
	itEnvRegion      = "AWS_REGION"
	itFallbackRegion = "us-east-1"

	// itMessageCount is the number of distinct-object-key messages seeded into
	// the queue. Ten is one full ReceiveMessage batch, so a single receive can
	// (but need not) drain it.
	itMessageCount = 10

	// itSameKeyVersionCount is how many EXTRA messages reuse an existing
	// message's bucket and object key with a different ETag, standing in for
	// the same object being overwritten. Two, not one, so the assertion covers
	// "several versions each keep their own result", not just a single pair.
	itSameKeyVersionCount = 2

	// itResultPrefix is the fixed prefix processing.OutputKey puts every result
	// under. It is restated here as a literal on purpose: reconstructing the
	// expected key from its parts is what lets this test catch a regression in
	// OutputKey's own logic, which building the expectation by calling
	// OutputKey could not.
	itResultPrefix = "results/"

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

	// itInputBucket is the bucket name embedded in the fake S3 event
	// notifications the fixture seeds. It is never created: the worker only
	// ever reads the name out of the event body (WR-023's stub derives its
	// result from event METADATA, never from the object's content), so an
	// existing bucket would add nothing but teardown litter.
	itInputBucket = "weir-wr021-uploads"

	// itEventTime is the fixed eventTime stamped into every seeded record,
	// keeping the fixture free of wall-clock dependence.
	itEventTime = "2026-07-24T12:00:00.000Z"
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

// itRawS3Client builds an S3 client directly, for the same reason
// itRawSQSClient exists: the fixture needs bucket lifecycle operations
// (CreateBucket / ListObjectsV2 / DeleteObject / DeleteBucket) that
// awsclient.S3Client deliberately does not expose. Path-style addressing is
// required against LocalStack, whose single endpoint cannot serve
// virtual-host bucket names.
func itRawS3Client(t *testing.T, ctx context.Context) *s3.Client {
	t.Helper()

	endpoint, region := itLocalStackEnv(t)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("load AWS config for raw S3 fixture client: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

// itSNSBody builds the exact wire format a message arrives in off SQS under
// ADR-001 (S3 -> SNS -> SQS): an S3 event notification JSON, encoded as a
// *string* inside an SNS "Notification" envelope. The double encoding is
// reproduced faithfully — via json.Marshal of the inner payload, then of the
// envelope carrying it as a field — because the worker now runs the real
// events.ParseS3Events over this body, and a shortcut that skipped the
// double-encode would be rejected as malformed.
func itSNSBody(t *testing.T, region, bucket, key string, size int64, etag string) string {
	t.Helper()

	inner, err := json.Marshal(map[string]any{
		"Records": []any{
			map[string]any{
				"eventVersion": "2.1",
				"eventSource":  "aws:s3",
				"awsRegion":    region,
				"eventTime":    itEventTime,
				"eventName":    "ObjectCreated:Put",
				"s3": map[string]any{
					"bucket": map[string]any{"name": bucket},
					"object": map[string]any{
						"key":  key,
						"size": size,
						"eTag": etag,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal inner S3 notification for %s/%s: %v", bucket, key, err)
	}

	envelope, err := json.Marshal(map[string]any{
		"Type":      "Notification",
		"MessageId": fmt.Sprintf("22b81b1a-0000-0000-0000-%012d", rand.IntN(1_000_000_000)),
		"TopicArn":  "arn:aws:sns:" + region + ":000000000000:" + bucket,
		"Message":   string(inner),
		"Timestamp": itEventTime,
	})
	if err != nil {
		t.Fatalf("marshal SNS envelope for %s/%s: %v", bucket, key, err)
	}

	// Guard the fixture against itself: if this body did not parse to exactly
	// one event, the drain assertion below would still pass (the worker treats
	// an SNS handshake or an empty-Records notification as a no-op success and
	// deletes the message), and the test would be silently vacuous.
	got, err := events.ParseS3Events([]byte(envelope))
	if err != nil {
		t.Fatalf("fixture body for %s/%s does not parse as an S3 event notification: %v\nbody: %s", bucket, key, err, envelope)
	}
	if len(got) != 1 || got[0].Key != key {
		t.Fatalf("fixture body for %s/%s parsed to %+v, want exactly one event with that key", bucket, key, got)
	}

	return string(envelope)
}

// itWantResultKey reconstructs the output-bucket key the worker must write for
// one source write, INDEPENDENTLY of processing.OutputKey.
//
// That independence is the point. Deriving the expectation by calling
// processing.OutputKey — the very function the worker uses — makes the
// assertion tautological: if OutputKey regressed, both the expectation and the
// actual object keys would change identically and the test would stay green.
// Composing the key from its three parts instead (the literal prefix, the
// exported ResultSuffix constant, and idempotency.Key) means a changed prefix,
// a missing suffix, or the wrong fields hashed all show up as a mismatch.
//
// idempotency.Key is called rather than reimplemented because it is a
// different, separately unit-tested pure function, and hand-rolling SHA-256
// here would test the standard library rather than Weir. A bug inside
// idempotency.Key remains out of this test's reach by design.
func itWantResultKey(key, versionID, etag string) string {
	return itResultPrefix + idempotency.Key(itInputBucket, key, versionID, etag) + processing.ResultSuffix
}

// itUnique returns the distinct values of in, for fixture self-checks that
// need to know two expectations really did differ.
func itUnique(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
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
	rawS3 := itRawS3Client(t, ctx)

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

	// ── fixture output bucket ───────────────────────────────────────────
	// WR-023 made OUTPUT_BUCKET required configuration and the worker now
	// writes one result object per event, so the bucket has to really exist:
	// a PutObject into a missing bucket fails, the message is never deleted,
	// and the drain below would time out.
	outBucket := itUniqueName("weir-wr021-out")
	createBucket := &s3.CreateBucketInput{Bucket: aws.String(outBucket)}
	// Outside us-east-1, S3 rejects a CreateBucket with no LocationConstraint
	// ("the unspecified location constraint is incompatible for the region
	// specific endpoint this request was sent to"), and LocalStack faithfully
	// reproduces that — the Makefile runs this suite against us-east-2.
	// us-east-1 is the opposite: it rejects the constraint being stated.
	if region != "us-east-1" {
		createBucket.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}
	if _, err := rawS3.CreateBucket(ctx, createBucket); err != nil {
		t.Fatalf("fixture CreateBucket(%q) in region %q: %v", outBucket, region, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), itCleanupTimeout)
		defer cancel()
		itDeleteBucket(t, cleanupCtx, rawS3, outBucket)
	})

	// ── seed the queue ──────────────────────────────────────────────────
	// Bodies are real SNS-wrapped S3 event notifications: since WR-023 the
	// worker parses the body for real, so plain text would be a genuine
	// unmarshal error, leave every message undeleted, and fail the drain.
	//
	// The expected output keys are reconstructed here from the SAME FORMAT the
	// production derivation uses, but WITHOUT calling processing.OutputKey.
	// Calling it would make the assertion tautological: the worker derives its
	// keys with that function too, so a regression inside it — a changed
	// prefix, a dropped suffix, the wrong fields fed into the hash — would move
	// both sides of the comparison together and the test would still pass. By
	// composing the literal prefix, the exported ResultSuffix constant and
	// idempotency.Key (a different, independently unit-tested pure function),
	// this reproduces the contract from its parts instead of from the code
	// under test. A bug inside idempotency.Key itself is still invisible here;
	// that is internal/idempotency's own suite's job, and this test is not the
	// place to re-derive SHA-256 by hand.
	wantResultKeys := make([]string, 0, itMessageCount+itSameKeyVersionCount)
	seed := func(key, etag string, size int64) {
		t.Helper()
		body := itSNSBody(t, region, itInputBucket, key, size, etag)
		if _, err := clients.SQS.SendMessage(ctx, awsclient.SendMessageInput{
			QueueUrl: queueURL,
			Body:     body,
		}); err != nil {
			t.Fatalf("fixture SendMessage(%s, etag=%s): %v", key, etag, err)
		}
		// VersionID is empty on both sides: this fixture emits no versionId, so
		// the ETag is the only thing distinguishing two writes to one key —
		// exactly the unversioned-bucket case.
		wantResultKeys = append(wantResultKeys, itWantResultKey(key, "", etag))
	}

	// itMessageCount messages, one distinct object key each.
	for i := 1; i <= itMessageCount; i++ {
		seed(fmt.Sprintf("wr-021-integration/message-%d.txt", i), fmt.Sprintf("%032x", i), int64(i*100))
	}

	// Then itSameKeyVersionCount extra writes that REUSE the first message's
	// bucket and object key, differing only in ETag (and size) — two later
	// versions of one object, which is what actually happens when a source
	// object is overwritten. Without these, every fixture message had a unique
	// key, so the end-to-end flow never demonstrated that two writes to the
	// SAME key produce two DISTINCT result objects; that property was only unit
	// tested in internal/processing. It matters here because the queue is
	// standard, not FIFO: if the derivation collapsed these onto one output
	// key, whichever event happened to be processed last would win, and an
	// older write could silently overwrite a newer result. The full-list
	// comparison below is what catches it — a collapsing derivation yields
	// fewer objects than wantResultKeys has entries.
	sharedKey := "wr-021-integration/message-1.txt"
	for i := 1; i <= itSameKeyVersionCount; i++ {
		seed(sharedKey, fmt.Sprintf("overwrite-%032x", i), int64(1000+i))
	}

	// Guard the fixture against itself: the extra writes must genuinely have
	// produced extra expected keys, or the assertion below is unchanged from
	// before and proves nothing new.
	if got := len(itUnique(wantResultKeys)); got != itMessageCount+itSameKeyVersionCount {
		t.Fatalf("fixture produced %d distinct expected result keys, want %d: the same-key/different-ETag "+
			"writes must each get their own result object, or this test cannot detect a derivation that "+
			"collapses two versions of one object onto a single output key", got, itMessageCount+itSameKeyVersionCount)
	}
	sort.Strings(wantResultKeys)

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
		"OUTPUT_BUCKET=" + outBucket,
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

	// ── the messages were really processed, not just deleted ────────────
	// A deleted message alone does not prove work happened: the worker also
	// deletes bodies it recognizes as no-ops. One result object per seeded
	// WRITE is what distinguishes "drained because processed" from "drained
	// because ignored". The puts precede the deletes, so by the time the
	// queue reads zero these objects must already be listable.
	//
	// The comparison is against the FULL sorted list, so it fails in both
	// directions: a missing object means a message was deleted without being
	// processed, and a collapsed pair (fewer objects than writes) means the
	// same-key/different-ETag writes overwrote each other instead of each
	// keeping its own result.
	if gotKeys := itListKeys(t, ctx, rawS3, outBucket); !slices.Equal(gotKeys, wantResultKeys) {
		t.Errorf("objects in output bucket %q = %q, want %q\nstdout:\n%s\nstderr:\n%s",
			outBucket, gotKeys, wantResultKeys, stdout.String(), stderr.String())
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

// itListKeys returns every object key in bucket, sorted, paging through the
// listing so the assertion cannot be fooled by a truncated first page.
func itListKeys(t *testing.T, ctx context.Context, client *s3.Client, bucket string) []string {
	t.Helper()

	var keys []string
	var token *string
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("ListObjectsV2(%q): %v", bucket, err)
		}
		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) || aws.ToString(out.NextContinuationToken) == "" {
			break
		}
		token = out.NextContinuationToken
	}
	sort.Strings(keys)
	return keys
}

// itDeleteBucket empties then removes a bucket. S3 (LocalStack included)
// refuses to delete a non-empty bucket, so the objects the worker wrote must
// go first — otherwise every run would leave litter behind.
//
// It deliberately does NOT reuse itListKeys: that helper reports a listing
// failure with t.Fatalf, whose runtime.Goexit would unwind this function
// before DeleteBucket ever ran — leaking the very bucket this teardown exists
// to remove. Here a listing failure is reported with t.Errorf and the loop
// breaks, so the deletes below always get their chance. In teardown, a
// best-effort continue beats an abort.
func itDeleteBucket(t *testing.T, ctx context.Context, client *s3.Client, bucket string) {
	t.Helper()

	var keys []string
	var token *string
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: token,
		})
		if err != nil {
			t.Errorf("teardown ListObjectsV2(%q): %v", bucket, err)
			break
		}
		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) || aws.ToString(out.NextContinuationToken) == "" {
			break
		}
		token = out.NextContinuationToken
	}

	for _, key := range keys {
		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}); err != nil {
			t.Errorf("teardown DeleteObject(%s/%s): %v", bucket, key, err)
		}
	}
	if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Errorf("teardown DeleteBucket(%q): %v — LocalStack may be left with a leftover test bucket", bucket, err)
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
