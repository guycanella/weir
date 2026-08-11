//go:build integration

// WR-024's integration proof, gated by the `integration` build tag and by the
// same defensive AWS_ENDPOINT_URL skip as worker_integration_test.go, whose
// fixtures/helpers this file reuses wholesale (itLocalStackEnv, itClients,
// itRawSQSClient, itRawS3Client, itUniqueName, buildWorkerBinary, itListKeys,
// itDeleteBucket, itSafeBuffer, and the it* timing constants).
//
// What it adds is the one thing nothing else in the repo can show. The
// deterministic tests in worker_redelivery_test.go prove the worker-visible
// preconditions for redrive (a failed message is not deleted, comes back, and
// its ApproximateReceiveCount climbs), and WR-018's
// internal/awsclient/awssdk/queue_integration_test.go proves the provisioned
// RedrivePolicy ATTRIBUTE is correct and points at the DLQ's ARN. Neither
// proves that a REAL queue, with that policy, actually MOVES a genuinely
// unprocessable message to the DLQ while a REAL worker keeps failing on it.
// The attribute could be well-formed and the mechanism still never fire —
// wrong queue, wrong worker behavior (a worker that deleted on failure would
// make the DLQ permanently empty), or a redrive policy applied to the wrong
// resource. This test closes exactly that gap: WR-024's Done-when, "an
// always-failing message ends up in the DLQ".
//
// It deliberately provisions through provisioner.EnsureQueue — the production
// code path — rather than hand-rolling CreateQueue + RedrivePolicy. A
// hand-rolled fixture would prove SQS redrives messages (which is AWS's job to
// get right, not Weir's); going through EnsureQueue proves WEIR'S OWN
// provisioning produces a queue on which redrive really works.
package worker_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/awssdk"
	"github.com/guycanella/weir/internal/events"
	"github.com/guycanella/weir/internal/provisioner"
)

const (
	// itDLQMaxReceiveCount and itDLQVisibilityTimeout are deliberately at the
	// small end of anything a real pipeline would use, and the reason is test
	// duration: SQS only redrives a message once it has been DELIVERED more
	// than maxReceiveCount times, and each redelivery has to wait out a real
	// wall-clock visibility timeout. Their product is therefore the floor on
	// how long this test must wait — roughly 2 x 3s = 6s here, against
	// 3 x 30s = 90s for WR-018's more production-like values.
	//
	// Nothing about the MECHANISM depends on the values being small: the
	// worker fails on the very first delivery and every one after it, so a
	// higher threshold would only mean more identical failures before the same
	// redrive. What matters is that they are both > 1 (a maxReceiveCount of 1
	// would redrive on the first failure and could not distinguish redrive from
	// "the queue immediately dumps everything") and that the visibility timeout
	// is long enough for LocalStack to be seen handing the message back rather
	// than racing itself.
	itDLQMaxReceiveCount   = 2
	itDLQVisibilityTimeout = 3

	// itDLQDeadline bounds "the poison message reached the DLQ". It is far
	// larger than the ~6s floor above because it also has to cover the go
	// build, process startup, the AWS client build, and LocalStack's own
	// scheduling slack for evaluating the redrive policy.
	itDLQDeadline = 90 * time.Second

	// itMainQueueEmptyDeadline bounds "and it is no longer on the main queue".
	// Shorter than itDLQDeadline: by the time the message is observed on the
	// DLQ the move has already happened, so this only absorbs the lag of SQS's
	// approximate queue-depth attributes.
	itMainQueueEmptyDeadline = 30 * time.Second
)

// itPoisonBody is a message that cannot be processed on ANY delivery, no
// matter how many times it is retried. It is plain text, so the very first
// step of the production pipeline — events.ParseS3Events unmarshalling the SNS
// envelope — fails, and it fails deterministically on identical input.
//
// It is intrinsically unprocessable rather than made to fail by a test hook,
// which is what makes this test exercise the real ./cmd/worker binary with its
// real internal/processing wiring: no build-tag-only failure injection, no
// special production support, nothing that could diverge from what a genuinely
// malformed message on a real queue would do.
//
// The one shape it must NOT have is an SNS control message
// (SubscriptionConfirmation) or an empty/test-event notification: internal/
// processing treats those as recognized no-ops, returns nil, and the worker
// DELETES them. Such a message would drain instead of redriving, and this test
// would fail for a reason that has nothing to do with the DLQ. itAssertPoison
// below enforces that.
const itPoisonBody = "wr-024 poison message: this is not JSON, let alone an SNS-wrapped S3 event notification"

// itAssertPoison guards the fixture against itself: it requires the poison
// body to be rejected by the real parser with a real error, and specifically
// NOT with events.ErrNotNotification, which internal/processing swallows as a
// successful no-op. Without this check, a future edit to itPoisonBody that
// made it merely "unrecognized SNS traffic" would turn the whole test vacuous
// in the worst way — the message would be deleted on first delivery, the DLQ
// would stay empty, and the failure would look like a redrive bug.
func itAssertPoison(t *testing.T, body string) {
	t.Helper()

	evts, err := events.ParseS3Events([]byte(body))
	if err == nil {
		t.Fatalf("fixture poison body parsed successfully into %+v — it must be unprocessable on every delivery, or it will be processed and deleted instead of redriven", evts)
	}
	if errors.Is(err, events.ErrNotNotification) {
		t.Fatalf("fixture poison body fails with events.ErrNotNotification (%v), which internal/processing treats as a successful no-op: the worker would DELETE this message on first delivery and the DLQ would never see it", err)
	}
}

// itRawSNSClient builds an SNS client directly, for the same reason
// itRawSQSClient and itRawS3Client exist: teardown needs DeleteTopic, which
// awsclient.SNSClient deliberately does not expose.
func itRawSNSClient(t *testing.T, ctx context.Context) *sns.Client {
	t.Helper()

	endpoint, region := itLocalStackEnv(t)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("load AWS config for raw SNS fixture client: %v", err)
	}
	return sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

// TestPoisonMessageIsRedrivenToDLQ is WR-024's Done-when, literally: an
// always-failing message ends up in the DLQ.
func TestPoisonMessageIsRedrivenToDLQ(t *testing.T) {
	ctx := t.Context()
	endpoint, region := itLocalStackEnv(t)
	clients := itClients(t, ctx)
	rawSQS := itRawSQSClient(t, ctx)
	rawS3 := itRawS3Client(t, ctx)
	rawSNS := itRawSNSClient(t, ctx)

	itAssertPoison(t, itPoisonBody)

	// ── provision through the production path ───────────────────────────
	name := itUniqueName("weir-wr024")
	cfg := provisioner.QueueConfig{
		MainQueueName:     name,
		DLQueueName:       name + "-dlq",
		TopicName:         name + "-topic",
		VisibilityTimeout: itDLQVisibilityTimeout,
		MaxReceiveCount:   itDLQMaxReceiveCount,
	}

	// Registered BEFORE EnsureQueue, and resolving each resource by NAME rather
	// than closing over the returned QueueSet, so a call that fails partway
	// through (say, after creating the DLQ but before the main queue) still has
	// everything it did create torn down.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), itCleanupTimeout)
		defer cancel()
		itDeleteQueueByName(t, cleanupCtx, rawSQS, cfg.MainQueueName)
		itDeleteQueueByName(t, cleanupCtx, rawSQS, cfg.DLQueueName)
		itDeleteTopicByName(t, cleanupCtx, rawSNS, cfg.TopicName)
	})

	qs, err := provisioner.EnsureQueue(ctx, clients.SQS, clients.SNS, cfg)
	if err != nil {
		t.Fatalf("EnsureQueue(%+v): %v", cfg, err)
	}
	if qs.MainQueueURL == "" || qs.DLQueueURL == "" {
		t.Fatalf("EnsureQueue returned an incomplete QueueSet: %+v", qs)
	}
	if qs.MainQueueURL == qs.DLQueueURL {
		t.Fatalf("EnsureQueue returned the same URL for the main queue and the DLQ (%q) — this test cannot distinguish the two", qs.MainQueueURL)
	}

	// ── fixture output bucket ───────────────────────────────────────────
	// The poison message never reaches the point of writing a result, so this
	// bucket stays empty — but OUTPUT_BUCKET is required configuration since
	// WR-023 and internal/processing validates it at construction time, so the
	// worker will not start without it. Versioning is deliberately left off:
	// with no result ever written there is no write count to observe, and an
	// unversioned empty bucket needs no object cleanup.
	outBucket := itUniqueName("weir-wr024-out")
	createBucket := &s3.CreateBucketInput{Bucket: aws.String(outBucket)}
	// Same regional quirk worker_integration_test.go documents: outside
	// us-east-1 the LocationConstraint is required, inside it it is rejected.
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

	// ── one poison message, sent straight to the main queue ─────────────
	// Sent directly rather than published through the SNS topic: the topic is
	// provisioned (EnsureQueue creates it) but the fan-out path is WR-019/
	// WR-020's subject and already covered there. What this test needs is
	// simply that ONE unprocessable message is on the queue, with nothing else
	// that could confuse the DLQ assertion.
	if _, err := clients.SQS.SendMessage(ctx, awsclient.SendMessageInput{
		QueueUrl: qs.MainQueueURL,
		Body:     itPoisonBody,
	}); err != nil {
		t.Fatalf("fixture SendMessage(poison): %v", err)
	}

	// ── build and start the real worker binary ──────────────────────────
	bin := buildWorkerBinary(t)

	var stdout, stderr itSafeBuffer
	cmd := exec.Command(bin)
	cmd.Env = []string{
		"QUEUE_URL=" + qs.MainQueueURL,
		"OUTPUT_BUCKET=" + outBucket,
		itEnvRegion + "=" + region,
		itEnvEndpointURL + "=" + endpoint,
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

	// Kill unconditionally on the way out, so a failing assertion never leaves
	// an orphan polling LocalStack.
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		t.Logf("worker stdout:\n%s", stdout.String())
		t.Logf("worker stderr:\n%s", stderr.String())
	})

	// ── the poison message reaches the DLQ ──────────────────────────────
	// Polled on an interval with a generous bound, not slept for a fixed
	// duration: the lower bound (maxReceiveCount x visibilityTimeout) is known
	// but LocalStack's own scheduling slack is not, so a fixed sleep would be
	// either flaky or needlessly slow.
	//
	// The worker is doing nothing clever here, and that is the point — it keeps
	// receiving the message, keeps failing to parse it, and keeps NOT deleting
	// it (delete-on-success only). The redrive is SQS acting on the policy
	// EnsureQueue wrote.
	deadline := time.Now().Add(itDLQDeadline)
	var dlqMessages []sqstypes.Message
	for time.Now().Before(deadline) {
		select {
		case err := <-waitErr:
			t.Fatalf("worker exited before the poison message reached the DLQ (%v)\nstdout:\n%s\nstderr:\n%s",
				err, stdout.String(), stderr.String())
		default:
		}

		out, err := rawSQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qs.DLQueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
			// 1, not 0: this loop only OBSERVES, it must not consume, so a
			// message seen on one poll has to come back on the next. It must be
			// non-zero — the AWS SDK v2 serializer omits VisibilityTimeout from
			// the request when it is 0, which would silently fall back to the
			// DLQ's default (~30s) invisibility, not "no timeout".
			VisibilityTimeout: 1,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage while polling the DLQ %q: %v", qs.DLQueueURL, err)
		}
		if len(out.Messages) > 0 {
			dlqMessages = out.Messages
			break
		}
	}
	if len(dlqMessages) == 0 {
		t.Fatalf("no message reached DLQ %q within %s — the poison message failed on every delivery and was never deleted, so the redrive policy EnsureQueue wrote (maxReceiveCount=%d, visibilityTimeout=%ds) should have moved it\nmain queue: %s\nstdout:\n%s\nstderr:\n%s",
			qs.DLQueueURL, itDLQDeadline, cfg.MaxReceiveCount, cfg.VisibilityTimeout,
			itQueueDepth(t, ctx, clients, qs.MainQueueURL), stdout.String(), stderr.String())
	}

	if len(dlqMessages) != 1 {
		t.Errorf("DLQ holds %d messages, want exactly 1 — one poison message must be redriven once, not duplicated", len(dlqMessages))
	}
	if got := aws.ToString(dlqMessages[0].Body); got != itPoisonBody {
		t.Errorf("DLQ message body = %q, want the poison body %q — a redrive must move the message verbatim", got, itPoisonBody)
	}

	// ── and it is GONE from the main queue ──────────────────────────────
	// Without this, the assertion above would also pass for a queue that
	// COPIED the message to the DLQ while still redelivering it forever. Both
	// depth attributes must reach zero: ApproximateNumberOfMessages alone would
	// be satisfied by a message that is merely invisible mid-flight.
	emptyDeadline := time.Now().Add(itMainQueueEmptyDeadline)
	var lastDepth string
	empty := false
	for time.Now().Before(emptyDeadline) {
		visible, inFlight := itQueueCounts(t, ctx, clients, qs.MainQueueURL)
		lastDepth = itFormatDepth(visible, inFlight)
		if visible == "0" && inFlight == "0" {
			empty = true
			break
		}
		time.Sleep(itPollInterval)
	}
	if !empty {
		t.Errorf("main queue %q still reports messages %s after the redrive within %s — the poison message must have MOVED to the DLQ, not been copied there while still circulating\nstdout:\n%s\nstderr:\n%s",
			qs.MainQueueURL, lastDepth, itMainQueueEmptyDeadline, stdout.String(), stderr.String())
	}

	// ── nothing was written for it ───────────────────────────────────────
	// A message that never parsed can have produced no result. An object here
	// would mean the worker wrote something for input it could not read.
	if got := itListKeys(t, ctx, rawS3, outBucket); len(got) != 0 {
		t.Errorf("output bucket %q contains %q, want it empty — an unprocessable message must produce no result object", outBucket, got)
	}

	// ── the worker is still healthy and shuts down cleanly ──────────────
	// Repeated processing failures must not have killed it: a worker that
	// exited on a poison message would be a crash loop in production, and the
	// redrive above would then have been LocalStack outliving a dead consumer
	// rather than a working one. Asserting the clean SIGTERM exit here is what
	// pins "it kept running the whole time".
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM to worker (pid %d): %v", cmd.Process.Pid, err)
	}
	select {
	case err := <-waitErr:
		if err != nil {
			t.Errorf("worker exited with %v after SIGTERM, want a clean exit (status 0) — repeatedly failing to process a poison message must not degrade the worker\nstdout:\n%s\nstderr:\n%s",
				err, stdout.String(), stderr.String())
		}
	case <-time.After(itExitDeadline):
		t.Errorf("worker did not exit within %s of SIGTERM — graceful shutdown is stuck\nstdout:\n%s\nstderr:\n%s",
			itExitDeadline, stdout.String(), stderr.String())
	}
}

// itFormatDepth renders visible and in-flight depths for a failure message.
func itFormatDepth(visible, inFlight string) string {
	return fmt.Sprintf("ApproximateNumberOfMessages=%q, ApproximateNumberOfMessagesNotVisible=%q", visible, inFlight)
}

// itFetchQueueCounts issues the depth query and returns any error to the caller.
func itFetchQueueCounts(ctx context.Context, clients *awssdk.Clients, queueURL string) (visible, inFlight string, err error) {
	out, err := clients.SQS.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
		QueueUrl: queueURL,
		AttributeNames: []string{
			"ApproximateNumberOfMessages",
			"ApproximateNumberOfMessagesNotVisible",
		},
	})
	if err != nil {
		return "", "", err
	}
	return out.Attributes["ApproximateNumberOfMessages"], out.Attributes["ApproximateNumberOfMessagesNotVisible"], nil
}

// itQueueCounts returns a queue's visible and in-flight approximate depths.
func itQueueCounts(t *testing.T, ctx context.Context, clients *awssdk.Clients, queueURL string) (visible, inFlight string) {
	t.Helper()

	visible, inFlight, err := itFetchQueueCounts(ctx, clients, queueURL)
	if err != nil {
		t.Fatalf("GetQueueAttributes(%q): %v", queueURL, err)
	}
	return visible, inFlight
}

// itQueueDepth renders a queue's depths for a failure message. It reports a
// lookup failure inline instead of failing the test: it is only ever called
// while building the diagnosis for a failure that has already happened, and
// replacing that diagnosis with "GetQueueAttributes failed" would be strictly
// less useful.
func itQueueDepth(t *testing.T, ctx context.Context, clients *awssdk.Clients, queueURL string) string {
	t.Helper()

	visible, inFlight, err := itFetchQueueCounts(ctx, clients, queueURL)
	if err != nil {
		return fmt.Sprintf("<GetQueueAttributes(%q) failed: %v>", queueURL, err)
	}
	return itFormatDepth(visible, inFlight)
}

// itDeleteQueueByName resolves a queue by NAME and deletes it, tolerating a
// queue that was never created (an EnsureQueue that failed before reaching it).
// Teardown reports problems with t.Errorf, never t.Fatalf: a Fatalf's
// runtime.Goexit would skip the remaining cleanups and leak the resources they
// own.
func itDeleteQueueByName(t *testing.T, ctx context.Context, client *sqs.Client, name string) {
	t.Helper()

	out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		var notExist *sqstypes.QueueDoesNotExist
		if errors.As(err, &notExist) || strings.Contains(err.Error(), "NonExistentQueue") {
			return
		}
		t.Errorf("teardown GetQueueUrl(%q): %v — LocalStack may be left with a leftover test queue", name, err)
		return
	}
	if _, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: out.QueueUrl}); err != nil {
		t.Errorf("teardown DeleteQueue(%q): %v — LocalStack may be left with a leftover test queue", name, err)
	}
}

// itDeleteTopicByName finds a topic by the NAME segment of its ARN and deletes
// it. Looking it up rather than closing over the QueueSet's TopicARN is what
// makes teardown correct when EnsureQueue created the topic and then failed on
// a later step, which is precisely when the ARN never reaches the caller.
func itDeleteTopicByName(t *testing.T, ctx context.Context, client *sns.Client, name string) {
	t.Helper()

	suffix := ":" + name
	var token *string
	for {
		out, err := client.ListTopics(ctx, &sns.ListTopicsInput{NextToken: token})
		if err != nil {
			t.Errorf("teardown ListTopics: %v — LocalStack may be left with a leftover test topic %q", err, name)
			return
		}
		for _, topic := range out.Topics {
			arn := aws.ToString(topic.TopicArn)
			if !strings.HasSuffix(arn, suffix) {
				continue
			}
			if _, err := client.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(arn)}); err != nil {
				t.Errorf("teardown DeleteTopic(%q): %v — LocalStack may be left with a leftover test topic", arn, err)
			}
			return
		}
		if aws.ToString(out.NextToken) == "" {
			return
		}
		token = out.NextToken
	}
}
