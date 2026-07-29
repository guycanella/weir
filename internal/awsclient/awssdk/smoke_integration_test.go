//go:build integration

// Package awssdk_test smoke-tests the real aws-sdk-go-v2 adapters (WR-017)
// against a running LocalStack, from OUTSIDE the package: every call goes
// through the exported surface production code uses (awssdk.NewClients plus
// the awsclient.S3Client/SNSClient/SQSClient interface methods), so nothing
// passes only because a test could reach unexported state.
//
// WR-017's Done-when says "a smoke test lists buckets against LocalStack",
// and TestS3ListBucketsSmoke does exactly that — but ListBuckets alone is a
// deliberately weak guard: it is a *service-level* S3 operation whose request
// carries no bucket name in the hostname, so it succeeds even against a
// client that never set UsePathStyle. Every *bucket-scoped* operation Weir
// actually depends on later (PutObject in WR-023, bucket notification
// configuration in WR-019) is addressed virtual-host style by default, i.e.
// "<bucket>.<endpoint-host>", which does not resolve against LocalStack. So
// TestS3BucketScopedOperationsAgainstLocalStack exists to catch exactly the
// regression ListBuckets cannot see: path-style addressing silently dropped
// from Config.applyS3Options.
//
// Note that the bucket-scoped assertion is only a *path-style* guard on hosts
// whose resolver does not wildcard "*.localhost" (Go's resolver does not, as
// of writing). config_test.go therefore also pins path-style addressing
// hermetically, with an httptest server and no DNS involved at all.
//
// Scope: this is a smoke test, not the AWS-layer integration suite — that is
// WR-020, which owns the full S3 -> SNS -> SQS -> worker flow. Here each
// adapter is exercised directly against its own service to prove the wiring
// (credentials, request/response field mapping) works against a real server;
// cross-service delivery is left to WR-020. The *endpoint override* is
// deliberately not among the claims made here: this suite runs with
// AWS_ENDPOINT_URL already set (the Makefile exports it), and the SDK resolves
// that env var into aws.Config.BaseEndpoint on its own, so a request reaching
// LocalStack proves nothing about whether Config.EndpointURL was applied —
// config_test.go pins that hermetically instead, with every ambient
// AWS_ENDPOINT_URL* cleared.
//
// This file is gated twice. The `integration` build tag is the primary gate
// (the Makefile's test-integration target is the only thing that sets it);
// the AWS_ENDPOINT_URL check below is a defensive second layer, so running
// `go test -tags=integration ./...` by hand without LocalStack skips cleanly
// instead of failing.
package awssdk_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/awssdk"
)

const (
	// envEndpointURL and envRegion are the variables the Makefile's
	// test-integration target exports; production code (cmd/worker,
	// cmd/manager) reads the same two to build an awssdk.Config.
	envEndpointURL = "AWS_ENDPOINT_URL"
	envRegion      = "AWS_REGION"

	// fallbackRegion is used only if AWS_REGION is unset: awssdk.Config
	// requires a region, and us-east-1 is the one region for which S3's
	// CreateBucket must NOT carry a LocationConstraint.
	fallbackRegion = "us-east-1"

	// cleanupTimeout bounds every teardown call. Cleanups cannot use
	// t.Context(): that context is canceled before t.Cleanup functions run,
	// so teardown would fail with "context canceled" instead of running.
	cleanupTimeout = 30 * time.Second
)

// localStackEnv returns the endpoint and region to test against, skipping the
// test (rather than failing it) when no endpoint is configured.
func localStackEnv(t *testing.T) (endpoint, region string) {
	t.Helper()

	endpoint = strings.TrimSpace(os.Getenv(envEndpointURL))
	if endpoint == "" {
		t.Skipf("%s is not set — run this suite via 'make test-integration' with LocalStack up", envEndpointURL)
	}

	region = strings.TrimSpace(os.Getenv(envRegion))
	if region == "" {
		region = fallbackRegion
		t.Logf("%s is not set; defaulting to %q", envRegion, fallbackRegion)
	}

	return endpoint, region
}

// newClients builds the adapters under test exactly the way production code
// will: an explicit awssdk.Config carrying the region and the endpoint
// override, and nothing else.
func newClients(t *testing.T, ctx context.Context) *awssdk.Clients {
	t.Helper()

	endpoint, region := localStackEnv(t)

	clients, err := awssdk.NewClients(ctx, awssdk.Config{Region: region, EndpointURL: endpoint})
	if err != nil {
		t.Fatalf("awssdk.NewClients(region=%q, endpoint=%q): unexpected error: %v", region, endpoint, err)
	}
	if clients == nil || clients.S3 == nil || clients.SNS == nil || clients.SQS == nil {
		t.Fatalf("awssdk.NewClients returned incomplete Clients: %+v", clients)
	}

	return clients
}

// uniqueName builds a collision-resistant resource name so repeat runs (and
// concurrent ones) never contend for the same bucket/queue/topic, and so a
// leaked resource from an earlier run cannot make a later run pass.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%06d", prefix, time.Now().UnixNano(), rand.IntN(1_000_000))
}

// rawS3Client builds an aws-sdk-go-v2 S3 client directly, deliberately NOT
// through awssdk.Config. Bucket creation is Terraform's job in production, so
// awsclient.S3Client has no CreateBucket — fixtures need a raw client. Keeping
// it independent of the code under test also makes it an oracle: it verifies
// what the adapter wrote, without sharing the adapter's option wiring.
func rawS3Client(t *testing.T, ctx context.Context) *s3.Client {
	t.Helper()

	endpoint, region := localStackEnv(t)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("load AWS config for raw S3 fixture client: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func rawSNSClient(t *testing.T, ctx context.Context) *sns.Client {
	t.Helper()

	endpoint, region := localStackEnv(t)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("load AWS config for raw SNS teardown client: %v", err)
	}

	return sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func rawSQSClient(t *testing.T, ctx context.Context) *sqs.Client {
	t.Helper()

	endpoint, region := localStackEnv(t)

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("load AWS config for raw SQS teardown client: %v", err)
	}

	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

// createTestBucket creates a uniquely named bucket via the raw fixture client
// and registers teardown that empties and deletes it. Teardown failures are
// reported as test errors: a leaked bucket means a later run is no longer
// starting from a known-clean state.
func createTestBucket(t *testing.T, ctx context.Context, raw *s3.Client) string {
	t.Helper()

	_, region := localStackEnv(t)
	bucket := uniqueName("weir-smoke")

	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if region != "us-east-1" {
		// Outside us-east-1, S3 requires an explicit LocationConstraint;
		// LocalStack enforces this too.
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}

	if _, err := raw.CreateBucket(ctx, in); err != nil {
		t.Fatalf("fixture CreateBucket(%q) in region %q: %v", bucket, region, err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		emptyAndDeleteBucket(t, cleanupCtx, raw, bucket)
	})

	return bucket
}

// emptyAndDeleteBucket removes every object in the bucket (S3 refuses to
// delete a non-empty bucket) and then the bucket itself.
func emptyAndDeleteBucket(t *testing.T, ctx context.Context, raw *s3.Client, bucket string) {
	t.Helper()

	pages := s3.NewListObjectsV2Paginator(raw, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Errorf("teardown ListObjectsV2(%q): %v", bucket, err)
			return
		}
		for _, obj := range page.Contents {
			if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			}); err != nil {
				t.Errorf("teardown DeleteObject(%q/%q): %v", bucket, aws.ToString(obj.Key), err)
			}
		}
	}

	if _, err := raw.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Errorf("teardown DeleteBucket(%q): %v — LocalStack may be left with a leftover test bucket", bucket, err)
	}
}

// TestS3ListBucketsSmoke is WR-017's Done-when, literally: list buckets
// against LocalStack through the real adapter. It asserts only that the call
// round-trips without error — see this file's package comment for why that
// alone is not a sufficient guard, and TestS3BucketScopedOperationsAgainstLocalStack
// for the assertion that is.
func TestS3ListBucketsSmoke(t *testing.T) {
	ctx := t.Context()
	clients := newClients(t, ctx)

	out, err := clients.S3.ListBuckets(ctx, awsclient.ListBucketsInput{})
	if err != nil {
		t.Fatalf("S3.ListBuckets against LocalStack: unexpected error: %v", err)
	}
	if out.Buckets == nil {
		t.Error("S3.ListBuckets returned a nil Buckets slice; the adapter should always return an allocated slice")
	}

	t.Logf("S3.ListBuckets returned %d bucket(s)", len(out.Buckets))
}

// TestS3BucketScopedOperationsAgainstLocalStack exercises the bucket-scoped S3
// operations — the ones that are addressed "<bucket>.<endpoint-host>" unless
// the client forces path-style addressing, and therefore the ones that FAIL
// against LocalStack (DNS: no such host) if Config.applyS3Options ever stops
// setting UsePathStyle alongside BaseEndpoint. ListBuckets cannot catch that
// regression; these subtests can.
func TestS3BucketScopedOperationsAgainstLocalStack(t *testing.T) {
	ctx := t.Context()
	clients := newClients(t, ctx)
	raw := rawS3Client(t, ctx)
	bucket := createTestBucket(t, ctx, raw)

	t.Run("PutObject writes an object that reads back byte-identical", func(t *testing.T) {
		ctx := t.Context()

		const (
			key         = "smoke/wr-017.txt"
			contentType = "text/plain"
		)
		body := []byte("weir WR-017 smoke test payload\n")

		out, err := clients.S3.PutObject(ctx, awsclient.PutObjectInput{
			Bucket:      bucket,
			Key:         key,
			Body:        body,
			ContentType: contentType,
		})
		if err != nil {
			// A DNS/no-such-host error here almost certainly means the S3
			// client lost UsePathStyle: the request was addressed
			// "<bucket>.<endpoint-host>" instead of "<endpoint>/<bucket>".
			t.Fatalf("S3.PutObject(%q/%q): unexpected error: %v", bucket, key, err)
		}
		if out.ETag == "" {
			t.Error("S3.PutObject returned an empty ETag; the adapter should surface S3's ETag")
		}

		got, err := raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("oracle GetObject(%q/%q): %v", bucket, key, err)
		}
		defer func() { _ = got.Body.Close() }()

		gotBody, err := io.ReadAll(got.Body)
		if err != nil {
			t.Fatalf("read oracle GetObject body: %v", err)
		}
		if !bytes.Equal(gotBody, body) {
			t.Errorf("object body = %q, want %q", gotBody, body)
		}
		if ct := aws.ToString(got.ContentType); ct != contentType {
			t.Errorf("object ContentType = %q, want %q — the adapter should pass PutObjectInput.ContentType through", ct, contentType)
		}
		if gotETag, wantETag := unquote(aws.ToString(got.ETag)), unquote(out.ETag); gotETag != wantETag {
			t.Errorf("stored object ETag = %q, but PutObject reported %q", gotETag, wantETag)
		}
	})

	t.Run("ListBuckets reports the fixture bucket", func(t *testing.T) {
		ctx := t.Context()

		out, err := clients.S3.ListBuckets(ctx, awsclient.ListBucketsInput{})
		if err != nil {
			t.Fatalf("S3.ListBuckets: unexpected error: %v", err)
		}

		for _, b := range out.Buckets {
			if b.Name == bucket {
				return
			}
		}
		t.Errorf("S3.ListBuckets did not report the fixture bucket %q; got %d bucket(s)", bucket, len(out.Buckets))
	})

	t.Run("GetBucketNotificationConfiguration on a fresh bucket is empty", func(t *testing.T) {
		// Bucket-scoped (so path-style-sensitive), and it pins the interface's
		// documented semantics: a bucket with no notifications configured
		// reports a zero-value Configuration rather than an error. WR-019 owns
		// actually writing a topic configuration; this only proves the read
		// path reaches LocalStack and maps an empty response correctly.
		ctx := t.Context()

		out, err := clients.S3.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
			Bucket: bucket,
		})
		if err != nil {
			t.Fatalf("S3.GetBucketNotificationConfiguration(%q): unexpected error: %v", bucket, err)
		}

		cfg := out.Configuration
		if n := len(cfg.TopicConfigurations); n != 0 {
			t.Errorf("fresh bucket reported %d topic configuration(s), want 0", n)
		}
		if n := len(cfg.QueueConfigurations); n != 0 {
			t.Errorf("fresh bucket reported %d queue configuration(s), want 0", n)
		}
		if n := len(cfg.LambdaFunctionConfigurations); n != 0 {
			t.Errorf("fresh bucket reported %d lambda configuration(s), want 0", n)
		}
		if cfg.EventBridgeConfiguration != nil {
			t.Error("fresh bucket reported EventBridge delivery as enabled, want nil")
		}
	})
}

// TestSQSAdapterAgainstLocalStack exercises every awsclient.SQSClient method
// against a real SQS implementation: create, look up by name, read and mutate
// attributes, and a send -> receive -> delete message round trip. This is the
// wiring proof for the worker's consume loop (WR-021/WR-024) and the scaling
// poll (WR-031) — not the end-to-end flow, which is WR-020's.
func TestSQSAdapterAgainstLocalStack(t *testing.T) {
	ctx := t.Context()
	clients := newClients(t, ctx)
	rawSQS := rawSQSClient(t, ctx)

	name := uniqueName("weir-smoke-queue")

	created, err := clients.SQS.CreateQueue(ctx, awsclient.CreateQueueInput{
		Name:       name,
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	if err != nil {
		t.Fatalf("SQS.CreateQueue(%q): unexpected error: %v", name, err)
	}
	if created.QueueUrl == "" {
		t.Fatal("SQS.CreateQueue returned an empty QueueUrl")
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := rawSQS.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(created.QueueUrl)}); err != nil {
			t.Errorf("teardown DeleteQueue(%q): %v — LocalStack may be left with a leftover test queue", created.QueueUrl, err)
		}
	})

	t.Run("GetQueueUrl resolves the queue by name", func(t *testing.T) {
		ctx := t.Context()

		out, err := clients.SQS.GetQueueUrl(ctx, awsclient.GetQueueUrlInput{Name: name})
		if err != nil {
			t.Fatalf("SQS.GetQueueUrl(%q): unexpected error: %v", name, err)
		}
		if out.QueueUrl != created.QueueUrl {
			t.Errorf("SQS.GetQueueUrl(%q) = %q, want the created queue's URL %q", name, out.QueueUrl, created.QueueUrl)
		}
	})

	t.Run("GetQueueAttributes reports the queue ARN", func(t *testing.T) {
		ctx := t.Context()

		out, err := clients.SQS.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
			QueueUrl:       created.QueueUrl,
			AttributeNames: []string{"QueueArn"},
		})
		if err != nil {
			t.Fatalf("SQS.GetQueueAttributes(QueueArn): unexpected error: %v", err)
		}
		if arn := out.Attributes["QueueArn"]; arn == "" {
			t.Errorf("SQS.GetQueueAttributes returned no QueueArn; got attributes %v", out.Attributes)
		}
	})

	t.Run("SetQueueAttributes mutation is visible to GetQueueAttributes", func(t *testing.T) {
		ctx := t.Context()

		const want = "45"
		if _, err := clients.SQS.SetQueueAttributes(ctx, awsclient.SetQueueAttributesInput{
			QueueUrl:   created.QueueUrl,
			Attributes: map[string]string{"VisibilityTimeout": want},
		}); err != nil {
			t.Fatalf("SQS.SetQueueAttributes(VisibilityTimeout=%s): unexpected error: %v", want, err)
		}

		out, err := clients.SQS.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
			QueueUrl:       created.QueueUrl,
			AttributeNames: []string{"VisibilityTimeout"},
		})
		if err != nil {
			t.Fatalf("SQS.GetQueueAttributes(VisibilityTimeout): unexpected error: %v", err)
		}
		if got := out.Attributes["VisibilityTimeout"]; got != want {
			t.Errorf("VisibilityTimeout = %q after SetQueueAttributes, want %q", got, want)
		}
	})

	t.Run("send, receive and delete a message", func(t *testing.T) {
		ctx := t.Context()

		body := fmt.Sprintf("wr-017 smoke message %d", time.Now().UnixNano())

		sent, err := clients.SQS.SendMessage(ctx, awsclient.SendMessageInput{
			QueueUrl: created.QueueUrl,
			Body:     body,
		})
		if err != nil {
			t.Fatalf("SQS.SendMessage: unexpected error: %v", err)
		}
		if sent.MessageId == "" {
			t.Error("SQS.SendMessage returned an empty MessageId")
		}

		// Long-poll rather than sleeping, so the test is deterministic
		// without depending on wall-clock timing.
		received, err := clients.SQS.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
			QueueUrl:            created.QueueUrl,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     10,
			VisibilityTimeout:   30,
		})
		if err != nil {
			t.Fatalf("SQS.ReceiveMessage: unexpected error: %v", err)
		}
		if len(received.Messages) != 1 {
			t.Fatalf("SQS.ReceiveMessage returned %d message(s), want exactly 1: %+v", len(received.Messages), received.Messages)
		}

		msg := received.Messages[0]
		if msg.Body != body {
			t.Errorf("received Body = %q, want %q", msg.Body, body)
		}
		if msg.MessageId != sent.MessageId {
			t.Errorf("received MessageId = %q, want the sent %q", msg.MessageId, sent.MessageId)
		}
		if msg.ReceiptHandle == "" {
			t.Error("received message has an empty ReceiptHandle; DeleteMessage would be impossible")
		}
		// The adapter always requests every system attribute so the worker can
		// read ApproximateReceiveCount (its retry/DLQ signal). Asserting it is
		// present is what keeps that wiring from being silently dropped.
		if got := msg.Attributes["ApproximateReceiveCount"]; got != "1" {
			t.Errorf("ApproximateReceiveCount = %q on first receive, want %q (attributes: %v)", got, "1", msg.Attributes)
		}

		if _, err := clients.SQS.DeleteMessage(ctx, awsclient.DeleteMessageInput{
			QueueUrl:      created.QueueUrl,
			ReceiptHandle: msg.ReceiptHandle,
		}); err != nil {
			t.Fatalf("SQS.DeleteMessage: unexpected error: %v", err)
		}

		// Sanity check that the delete took effect and the queue drained.
		after, err := clients.SQS.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
			QueueUrl:            created.QueueUrl,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			t.Fatalf("SQS.ReceiveMessage after delete: unexpected error: %v", err)
		}
		if len(after.Messages) != 0 {
			t.Errorf("queue still returned %d message(s) after DeleteMessage: %+v", len(after.Messages), after.Messages)
		}
	})
}

// TestSNSAdapterAgainstLocalStack exercises every awsclient.SNSClient method
// against a real SNS implementation: create a topic, subscribe an SQS queue to
// it, and read the subscription back. ADR-001's S3 -> SNS -> SQS delivery
// itself is WR-019/WR-020's concern; this only proves the SNS adapter's
// endpoint override and field mapping work end to end.
func TestSNSAdapterAgainstLocalStack(t *testing.T) {
	ctx := t.Context()
	clients := newClients(t, ctx)
	rawSNS := rawSNSClient(t, ctx)
	rawSQS := rawSQSClient(t, ctx)

	topicName := uniqueName("weir-smoke-topic")

	topic, err := clients.SNS.CreateTopic(ctx, awsclient.CreateTopicInput{Name: topicName})
	if err != nil {
		t.Fatalf("SNS.CreateTopic(%q): unexpected error: %v", topicName, err)
	}
	if !strings.HasSuffix(topic.TopicArn, ":"+topicName) {
		t.Errorf("SNS.CreateTopic returned TopicArn %q, want an ARN ending in %q", topic.TopicArn, ":"+topicName)
	}
	t.Cleanup(func() {
		// DeleteTopic also removes the topic's subscriptions.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := rawSNS.DeleteTopic(cleanupCtx, &sns.DeleteTopicInput{TopicArn: aws.String(topic.TopicArn)}); err != nil {
			t.Errorf("teardown DeleteTopic(%q): %v — LocalStack may be left with a leftover test topic", topic.TopicArn, err)
		}
	})

	// CreateTopic is idempotent on name in SNS; Weir relies on that (WR-018's
	// create-or-adopt), so pin it here rather than discovering it in the
	// reconciler.
	again, err := clients.SNS.CreateTopic(ctx, awsclient.CreateTopicInput{Name: topicName})
	if err != nil {
		t.Fatalf("second SNS.CreateTopic(%q): unexpected error: %v", topicName, err)
	}
	if again.TopicArn != topic.TopicArn {
		t.Errorf("second SNS.CreateTopic(%q) = %q, want the same ARN %q", topicName, again.TopicArn, topic.TopicArn)
	}

	// A subscription needs a real endpoint; an SQS queue is the one Weir
	// actually uses (ADR-001).
	queueName := uniqueName("weir-smoke-sub-queue")
	queue, err := rawSQS.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	if err != nil {
		t.Fatalf("fixture CreateQueue(%q): %v", queueName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := rawSQS.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: queue.QueueUrl}); err != nil {
			t.Errorf("teardown DeleteQueue(%q): %v — LocalStack may be left with a leftover test queue", aws.ToString(queue.QueueUrl), err)
		}
	})

	queueAttrs, err := clients.SQS.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
		QueueUrl:       aws.ToString(queue.QueueUrl),
		AttributeNames: []string{"QueueArn"},
	})
	if err != nil {
		t.Fatalf("fixture GetQueueAttributes(QueueArn): %v", err)
	}
	queueArn := queueAttrs.Attributes["QueueArn"]
	if queueArn == "" {
		t.Fatalf("fixture queue %q reported no QueueArn: %v", queueName, queueAttrs.Attributes)
	}

	sub, err := clients.SNS.Subscribe(ctx, awsclient.SubscribeInput{
		TopicArn: topic.TopicArn,
		Protocol: "sqs",
		Endpoint: queueArn,
	})
	if err != nil {
		t.Fatalf("SNS.Subscribe(topic=%q, sqs=%q): unexpected error: %v", topic.TopicArn, queueArn, err)
	}
	// The adapter sets ReturnSubscriptionArn, so the ARN must be a real ARN
	// and never the "pending confirmation" placeholder.
	if !strings.HasPrefix(sub.SubscriptionArn, "arn:") {
		t.Errorf("SNS.Subscribe returned SubscriptionArn %q, want a real ARN (the adapter sets ReturnSubscriptionArn)", sub.SubscriptionArn)
	}

	listed, err := clients.SNS.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
		TopicArn: topic.TopicArn,
	})
	if err != nil {
		t.Fatalf("SNS.ListSubscriptionsByTopic(%q): unexpected error: %v", topic.TopicArn, err)
	}

	var found bool
	for _, s := range listed.Subscriptions {
		if s.SubscriptionArn != sub.SubscriptionArn {
			continue
		}
		found = true
		if s.Protocol != "sqs" {
			t.Errorf("subscription Protocol = %q, want %q", s.Protocol, "sqs")
		}
		if s.Endpoint != queueArn {
			t.Errorf("subscription Endpoint = %q, want the queue ARN %q", s.Endpoint, queueArn)
		}
		if s.TopicArn != topic.TopicArn {
			t.Errorf("subscription TopicArn = %q, want %q", s.TopicArn, topic.TopicArn)
		}
	}
	if !found {
		t.Errorf("SNS.ListSubscriptionsByTopic did not report subscription %q; got %+v", sub.SubscriptionArn, listed.Subscriptions)
	}
}

// unquote strips S3's surrounding quotes from an ETag so two ETags can be
// compared regardless of quoting.
func unquote(s string) string {
	return strings.Trim(s, `"`)
}
