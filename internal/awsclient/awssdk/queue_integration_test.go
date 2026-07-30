//go:build integration

// Package awssdk_test — WR-018 integration test.
//
// TestEnsureQueueAgainstLocalStack exercises the EnsureQueue provisioner
// (internal/provisioner) end-to-end against a running LocalStack:
//
//   - First call provisions DLQ, main queue, SNS topic and SNS→SQS subscription.
//   - Second call (identical config) is a no-op: no new queues, topics or
//     subscriptions are created, and the returned QueueSet is identical.
//   - The DLQ and main queue are distinct resources.
//   - The redrive policy on the main queue references the DLQ's ARN.
//   - An SNS Publish to the topic delivers a message to the SQS queue (proving
//     the fan-out wiring works end to end, not just the API calls).
//
// This file is gated by the `integration` build tag (Makefile's
// test-integration target) AND by the localStackEnv skip check (defensive
// skip when AWS_ENDPOINT_URL is absent). It must be placed in the awssdk_test
// package alongside smoke_integration_test.go so it can reuse the helpers
// defined there (localStackEnv, newClients, uniqueName, rawSQSClient, ...).
package awssdk_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/provisioner"
)

// TestEnsureQueueAgainstLocalStack is WR-018's integration Done-when:
// running EnsureQueue twice against LocalStack is a no-op, and the DLQ
// + redrive are wired correctly.
func TestEnsureQueueAgainstLocalStack(t *testing.T) {
	ctx := t.Context()
	clients := newClients(t, ctx)
	rawSQS := rawSQSClient(t, ctx)
	rawSNS := rawSNSClient(t, ctx)

	suffix := uniqueName("wr018")
	cfg := provisioner.QueueConfig{
		MainQueueName:     "weir-" + suffix,
		DLQueueName:       "weir-" + suffix + "-dlq",
		TopicName:         "weir-" + suffix + "-topic",
		VisibilityTimeout: 30,
		MaxReceiveCount:   3,
	}

	// Register teardown before provisioning so cleanup runs even if the test
	// panics or fatals partway through.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		deleteProvisionedResources(t, cleanupCtx, rawSQS, rawSNS, cfg)
	})

	// ── First call ──────────────────────────────────────────────────────────
	first, err := provisioner.EnsureQueue(ctx, clients.SQS, clients.SNS, cfg)
	if err != nil {
		t.Fatalf("EnsureQueue (first call): %v", err)
	}
	assertQueueSetNonEmpty(t, "first call", first)

	t.Run("DLQ is distinct from main queue", func(t *testing.T) {
		if first.MainQueueURL == first.DLQueueURL {
			t.Error("MainQueueURL and DLQueueURL are the same — must be distinct queues")
		}
		if first.MainQueueARN == first.DLQueueARN {
			t.Error("MainQueueARN and DLQueueARN are the same — must be distinct queues")
		}
	})

	t.Run("main queue redrive policy points at DLQ", func(t *testing.T) {
		ctx := t.Context()
		out, err := clients.SQS.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
			QueueUrl:       first.MainQueueURL,
			AttributeNames: []string{"RedrivePolicy"},
		})
		if err != nil {
			t.Fatalf("GetQueueAttributes(RedrivePolicy): %v", err)
		}
		raw := out.Attributes["RedrivePolicy"]
		if raw == "" {
			t.Fatal("main queue has no RedrivePolicy attribute")
		}
		var policy struct {
			DeadLetterTargetArn string `json:"deadLetterTargetArn"`
			MaxReceiveCount     int    `json:"maxReceiveCount"`
		}
		if err := json.Unmarshal([]byte(raw), &policy); err != nil {
			t.Fatalf("parse RedrivePolicy JSON %q: %v", raw, err)
		}
		if policy.DeadLetterTargetArn != first.DLQueueARN {
			t.Errorf("RedrivePolicy.deadLetterTargetArn = %q, want DLQ ARN %q", policy.DeadLetterTargetArn, first.DLQueueARN)
		}
		if policy.MaxReceiveCount != cfg.MaxReceiveCount {
			t.Errorf("RedrivePolicy.maxReceiveCount = %d, want %d", policy.MaxReceiveCount, cfg.MaxReceiveCount)
		}
	})

	// ── Second call (idempotency) ────────────────────────────────────────────
	t.Run("second call is a no-op", func(t *testing.T) {
		ctx := t.Context()
		second, err := provisioner.EnsureQueue(ctx, clients.SQS, clients.SNS, cfg)
		if err != nil {
			t.Fatalf("EnsureQueue (second call): %v", err)
		}
		assertQueueSetNonEmpty(t, "second call", second)

		if first != second {
			t.Errorf("second EnsureQueue returned a different QueueSet:\n  first:  %+v\n  second: %+v", first, second)
		}

		// Verify exactly one subscription exists (not two).
		listed, err := clients.SNS.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
			TopicArn: first.TopicARN,
		})
		if err != nil {
			t.Fatalf("ListSubscriptionsByTopic after second call: %v", err)
		}
		sqsSubs := 0
		for _, s := range listed.Subscriptions {
			if s.Protocol == "sqs" && s.Endpoint == first.MainQueueARN {
				sqsSubs++
			}
		}
		if sqsSubs != 1 {
			t.Errorf("found %d sqs subscription(s) to main queue ARN after second call, want exactly 1", sqsSubs)
		}
	})

	// ── End-to-end fan-out: SNS Publish → SQS Receive ───────────────────────
	t.Run("SNS publish reaches SQS queue", func(t *testing.T) {
		ctx := t.Context()

		msg := "wr-018 integration test " + time.Now().Format(time.RFC3339Nano)
		if _, err := rawSNS.Publish(ctx, &sns.PublishInput{
			TopicArn: aws.String(first.TopicARN),
			Message:  aws.String(msg),
		}); err != nil {
			t.Fatalf("SNS.Publish to %q: %v", first.TopicARN, err)
		}

		// Long-poll for the delivered message. LocalStack SNS→SQS delivery is
		// synchronous, so this should arrive on the first receive call.
		recv, err := rawSQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(first.MainQueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     10,
		})
		if err != nil {
			t.Fatalf("SQS.ReceiveMessage from %q: %v", first.MainQueueURL, err)
		}
		if len(recv.Messages) == 0 {
			t.Fatal("SQS queue is empty after SNS publish — SNS→SQS fan-out did not deliver the message")
		}

		// The SNS message body is JSON-wrapped by LocalStack ({"Type":"Notification","Message":"..."}).
		// We only assert the original message text appears somewhere in the body.
		delivered := aws.ToString(recv.Messages[0].Body)
		if !strings.Contains(delivered, msg) {
			t.Errorf("SQS message body %q does not contain the published message %q", delivered, msg)
		}

		// Clean up: delete the received message.
		if _, err := rawSQS.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(first.MainQueueURL),
			ReceiptHandle: recv.Messages[0].ReceiptHandle,
		}); err != nil {
			t.Errorf("teardown DeleteMessage: %v", err)
		}
	})
}

// assertQueueSetNonEmpty fatals if any QueueSet field is empty, since an
// empty URL/ARN means the provisioner silently failed to populate a field.
func assertQueueSetNonEmpty(t *testing.T, label string, qs provisioner.QueueSet) {
	t.Helper()
	if qs.MainQueueURL == "" {
		t.Fatalf("%s: QueueSet.MainQueueURL is empty", label)
	}
	if qs.MainQueueARN == "" {
		t.Fatalf("%s: QueueSet.MainQueueARN is empty", label)
	}
	if qs.DLQueueURL == "" {
		t.Fatalf("%s: QueueSet.DLQueueURL is empty", label)
	}
	if qs.DLQueueARN == "" {
		t.Fatalf("%s: QueueSet.DLQueueARN is empty", label)
	}
	if qs.TopicARN == "" {
		t.Fatalf("%s: QueueSet.TopicARN is empty", label)
	}
}

// deleteProvisionedResources tears down the resources EnsureQueue created.
// Teardown failures are reported as errors (not fatals) so other cleanup
// steps still run.
func deleteProvisionedResources(t *testing.T, ctx context.Context, rawSQS *sqs.Client, rawSNS *sns.Client, cfg provisioner.QueueConfig) {
	t.Helper()

	// Resolve queue URLs by name (they may differ from what EnsureQueue
	// returned if we're re-running cleanup after a partial failure).
	for _, name := range []string{cfg.MainQueueName, cfg.DLQueueName} {
		urlOut, err := rawSQS.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			t.Logf("teardown GetQueueUrl(%q): %v — may already be deleted", name, err)
			continue
		}
		if _, err := rawSQS.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: urlOut.QueueUrl}); err != nil {
			t.Errorf("teardown DeleteQueue(%q): %v", name, err)
		}
	}

	// Find the topic ARN by listing topics (SNS has no get-by-name API).
	listed, err := rawSNS.ListTopics(ctx, &sns.ListTopicsInput{})
	if err != nil {
		t.Errorf("teardown ListTopics: %v", err)
		return
	}
	suffix := ":" + cfg.TopicName
	for _, topic := range listed.Topics {
		if strings.HasSuffix(aws.ToString(topic.TopicArn), suffix) {
			if _, err := rawSNS.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: topic.TopicArn}); err != nil {
				t.Errorf("teardown DeleteTopic(%q): %v", aws.ToString(topic.TopicArn), err)
			}
			return
		}
	}
	t.Logf("teardown: topic %q not found in ListTopics — may already be deleted", cfg.TopicName)
}
