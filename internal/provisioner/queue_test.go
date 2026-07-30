package provisioner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
	"github.com/guycanella/weir/internal/provisioner"
)

// defaultCfg returns a QueueConfig with sensible defaults for tests that
// don't need to vary the parameters.
func defaultCfg() provisioner.QueueConfig {
	return provisioner.QueueConfig{
		MainQueueName:     "test-main",
		DLQueueName:       "test-dlq",
		TopicName:         "test-topic",
		VisibilityTimeout: 30,
		MaxReceiveCount:   3,
	}
}

// callEnsure is a test helper that calls EnsureQueue and fatals on error.
func callEnsure(t *testing.T, ctx context.Context, sqsFake *fake.SQS, snsFake *fake.SNS, cfg provisioner.QueueConfig) provisioner.QueueSet {
	t.Helper()
	qs, err := provisioner.EnsureQueue(ctx, sqsFake, snsFake, cfg)
	if err != nil {
		t.Fatalf("EnsureQueue: unexpected error: %v", err)
	}
	return qs
}

// TestEnsureQueueCreatesAllResources verifies that a first call to
// EnsureQueue provisions the DLQ, main queue, SNS topic and SNS→SQS
// subscription.
func TestEnsureQueueCreatesAllResources(t *testing.T) {
	ctx := t.Context()
	sqsFake := fake.NewSQS()
	snsFake := fake.NewSNS()
	cfg := defaultCfg()

	qs := callEnsure(t, ctx, sqsFake, snsFake, cfg)

	// ── SQS queues ────────────────────────────────────────────────────────────
	if qs.MainQueueURL == "" {
		t.Error("QueueSet.MainQueueURL is empty")
	}
	if qs.MainQueueARN == "" {
		t.Error("QueueSet.MainQueueARN is empty")
	}
	if qs.DLQueueURL == "" {
		t.Error("QueueSet.DLQueueURL is empty")
	}
	if qs.DLQueueARN == "" {
		t.Error("QueueSet.DLQueueARN is empty")
	}
	if qs.TopicARN == "" {
		t.Error("QueueSet.TopicARN is empty")
	}

	// Both queues must have been registered in the fake.
	if _, ok := sqsFake.Queues[cfg.MainQueueName]; !ok {
		t.Errorf("main queue %q not found in fake after EnsureQueue", cfg.MainQueueName)
	}
	if _, ok := sqsFake.Queues[cfg.DLQueueName]; !ok {
		t.Errorf("DLQ %q not found in fake after EnsureQueue", cfg.DLQueueName)
	}

	// Topic must exist in the fake.
	if _, ok := snsFake.Topics[cfg.TopicName]; !ok {
		t.Errorf("SNS topic %q not found in fake after EnsureQueue", cfg.TopicName)
	}

	// Exactly one subscription must exist for the topic.
	subs := snsFake.Subscriptions[qs.TopicARN]
	if len(subs) != 1 {
		t.Fatalf("expected 1 subscription on topic %q, got %d", qs.TopicARN, len(subs))
	}
	sub := subs[0]
	if sub.Protocol != "sqs" {
		t.Errorf("subscription Protocol = %q, want %q", sub.Protocol, "sqs")
	}
	if sub.Endpoint != qs.MainQueueARN {
		t.Errorf("subscription Endpoint = %q, want main queue ARN %q", sub.Endpoint, qs.MainQueueARN)
	}
}

// TestEnsureQueueIsIdempotent is WR-018's core Done-when: calling EnsureQueue
// twice with the same config must be a no-op on the second call.
//
// "No-op" means:
//   - No new queue or topic is created (the fake's Queues/Topics maps don't
//     grow).
//   - Exactly one subscription exists after both calls (no duplicate subscribe).
//   - The returned QueueSet is identical both times.
func TestEnsureQueueIsIdempotent(t *testing.T) {
	ctx := t.Context()
	sqsFake := fake.NewSQS()
	snsFake := fake.NewSNS()
	cfg := defaultCfg()

	first := callEnsure(t, ctx, sqsFake, snsFake, cfg)
	second := callEnsure(t, ctx, sqsFake, snsFake, cfg)

	// QueueSet must be identical on both calls.
	if first != second {
		t.Errorf("second EnsureQueue returned a different QueueSet:\n  first:  %+v\n  second: %+v", first, second)
	}

	// Exactly one queue for each name — no extras.
	if got := len(sqsFake.Queues); got != 2 {
		t.Errorf("fake has %d queue(s) after two EnsureQueue calls, want exactly 2 (main + DLQ)", got)
	}
	if got := len(snsFake.Topics); got != 1 {
		t.Errorf("fake has %d topic(s) after two EnsureQueue calls, want exactly 1", got)
	}

	// The critical invariant: exactly ONE subscription, not two.
	subs := snsFake.Subscriptions[first.TopicARN]
	if got := len(subs); got != 1 {
		t.Errorf("expected exactly 1 subscription after two EnsureQueue calls, got %d — duplicate subscribe detected", got)
	}
}

// TestEnsureQueueSetsVisibilityTimeout verifies that the main queue's
// VisibilityTimeout attribute is set to the configured value.
func TestEnsureQueueSetsVisibilityTimeout(t *testing.T) {
	ctx := t.Context()
	sqsFake := fake.NewSQS()
	snsFake := fake.NewSNS()
	cfg := defaultCfg()
	cfg.VisibilityTimeout = 45

	qs := callEnsure(t, ctx, sqsFake, snsFake, cfg)

	attrs := sqsFake.QueueAttributes[qs.MainQueueURL]
	if got := attrs["VisibilityTimeout"]; got != "45" {
		t.Errorf("main queue VisibilityTimeout = %q, want %q", got, "45")
	}
}

// TestEnsureQueueSetsRedrivePolicy verifies that the main queue's
// RedrivePolicy attribute points at the DLQ and uses the configured
// MaxReceiveCount.
func TestEnsureQueueSetsRedrivePolicy(t *testing.T) {
	ctx := t.Context()
	sqsFake := fake.NewSQS()
	snsFake := fake.NewSNS()
	cfg := defaultCfg()
	cfg.MaxReceiveCount = 5

	qs := callEnsure(t, ctx, sqsFake, snsFake, cfg)

	attrs := sqsFake.QueueAttributes[qs.MainQueueURL]
	raw := attrs["RedrivePolicy"]
	if raw == "" {
		t.Fatal("main queue has no RedrivePolicy attribute after EnsureQueue")
	}

	var policy struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
		MaxReceiveCount     int    `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		t.Fatalf("failed to parse RedrivePolicy JSON %q: %v", raw, err)
	}
	if policy.DeadLetterTargetArn != qs.DLQueueARN {
		t.Errorf("RedrivePolicy.deadLetterTargetArn = %q, want DLQ ARN %q", policy.DeadLetterTargetArn, qs.DLQueueARN)
	}
	if policy.MaxReceiveCount != 5 {
		t.Errorf("RedrivePolicy.maxReceiveCount = %d, want 5", policy.MaxReceiveCount)
	}
}

// TestEnsureQueueSetsQueuePolicy verifies that EnsureQueue writes a
// resource-based IAM policy on the main queue that allows
// sns.amazonaws.com to call sqs:SendMessage, restricted by
// aws:SourceArn to the SNS topic ARN (required for delivery on real AWS).
//
// Removing the "Policy" SetQueueAttributes call would leave this test red,
// providing a mutation-detection guarantee that the other tests do not.
func TestEnsureQueueSetsQueuePolicy(t *testing.T) {
	ctx := t.Context()
	sqsFake := fake.NewSQS()
	snsFake := fake.NewSNS()

	qs := callEnsure(t, ctx, sqsFake, snsFake, defaultCfg())

	raw := sqsFake.QueueAttributes[qs.MainQueueURL]["Policy"]
	if raw == "" {
		t.Fatal("main queue has no Policy attribute after EnsureQueue — SNS cannot deliver on real AWS without it")
	}

	// Parse into a minimal IAM policy shape sufficient to check the fields
	// that govern whether SNS delivery will work.
	var doc struct {
		Statement []struct {
			Principal map[string]string            `json:"Principal"`
			Action    string                       `json:"Action"`
			Resource  string                       `json:"Resource"`
			Condition map[string]map[string]string `json:"Condition"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("failed to parse Policy JSON %q: %v", raw, err)
	}
	if len(doc.Statement) == 0 {
		t.Fatal("Policy has no statements")
	}
	stmt := doc.Statement[0]

	if got := stmt.Principal["Service"]; got != "sns.amazonaws.com" {
		t.Errorf("Policy statement Principal.Service = %q, want \"sns.amazonaws.com\"", got)
	}
	if stmt.Action != "sqs:SendMessage" {
		t.Errorf("Policy statement Action = %q, want \"sqs:SendMessage\"", stmt.Action)
	}
	if stmt.Resource != qs.MainQueueARN {
		t.Errorf("Policy statement Resource = %q, want main queue ARN %q", stmt.Resource, qs.MainQueueARN)
	}
	if got := stmt.Condition["ArnEquals"]["aws:SourceArn"]; got != qs.TopicARN {
		t.Errorf("Policy statement Condition.ArnEquals[\"aws:SourceArn\"] = %q, want topic ARN %q", got, qs.TopicARN)
	}
}

// TestEnsureQueueDLQIsDistinctFromMainQueue verifies that the DLQ and the
// main queue are two different queues (distinct URLs and ARNs).
func TestEnsureQueueDLQIsDistinctFromMainQueue(t *testing.T) {
	ctx := t.Context()
	sqsFake := fake.NewSQS()
	snsFake := fake.NewSNS()
	cfg := defaultCfg()

	qs := callEnsure(t, ctx, sqsFake, snsFake, cfg)

	if qs.MainQueueURL == qs.DLQueueURL {
		t.Error("MainQueueURL and DLQueueURL are identical — main queue and DLQ must be distinct")
	}
	if qs.MainQueueARN == qs.DLQueueARN {
		t.Error("MainQueueARN and DLQueueARN are identical — main queue and DLQ must be distinct")
	}
}

// TestEnsureQueueRedrivePolicyUpdatedOnSecondCall verifies the level-triggered
// convergence property: if the main queue was previously created (possibly
// by a different EnsureQueue call, or by some other means) with a different
// VisibilityTimeout, a subsequent EnsureQueue call with updated config
// converges the attribute to the new desired value.
func TestEnsureQueueRedrivePolicyUpdatedOnSecondCall(t *testing.T) {
	ctx := t.Context()
	sqsFake := fake.NewSQS()
	snsFake := fake.NewSNS()

	// First call: MaxReceiveCount=3.
	cfg := defaultCfg()
	cfg.MaxReceiveCount = 3
	callEnsure(t, ctx, sqsFake, snsFake, cfg)

	// Second call: MaxReceiveCount updated to 7 — the reconciler's "desired
	// state has changed" scenario.
	cfg.MaxReceiveCount = 7
	qs := callEnsure(t, ctx, sqsFake, snsFake, cfg)

	attrs := sqsFake.QueueAttributes[qs.MainQueueURL]
	raw := attrs["RedrivePolicy"]
	if raw == "" {
		t.Fatal("main queue has no RedrivePolicy attribute after second EnsureQueue")
	}

	var policy struct {
		MaxReceiveCount int `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		t.Fatalf("parse RedrivePolicy JSON %q: %v", raw, err)
	}
	if policy.MaxReceiveCount != 7 {
		t.Errorf("MaxReceiveCount = %d after config update, want 7", policy.MaxReceiveCount)
	}
}

// TestEnsureQueueSubscriptionExistsAfterPaginatedList exercises both the
// pagination path in ensureSubscription:
//
//   - "endpoint is on the last page": the real queue ARN is seeded on the
//     last page only, so removing the pagination loop would cause the code to
//     miss it and issue a duplicate Subscribe.
//   - "endpoint absent across all pages": confirms that when no page contains
//     the endpoint, exactly one Subscribe call is made.
func TestEnsureQueueSubscriptionExistsAfterPaginatedList(t *testing.T) {
	t.Run("endpoint on last page", func(t *testing.T) {
		ctx := t.Context()
		sqsFake := fake.NewSQS()
		snsFake := fake.NewSNS()

		// Use a page size of 2 to force pagination with a small number of records.
		snsFake.ListPageSize = 2
		cfg := defaultCfg()

		// EnsureQueue once to create the topic + queue so we have real ARNs.
		first := callEnsure(t, ctx, sqsFake, snsFake, cfg)

		// Remove the subscription EnsureQueue just created so we can control
		// exactly what is in the list when we call it a second time.
		delete(snsFake.Subscriptions, first.TopicARN)

		// Seed 4 noise subscriptions first (occupies pages 1 and 2 at size 2).
		// The real endpoint will be seeded last, on page 3.
		for i := range 4 {
			snsFake.Subscriptions[first.TopicARN] = append(
				snsFake.Subscriptions[first.TopicARN],
				awsclient.Subscription{
					TopicArn:        first.TopicARN,
					Protocol:        "sqs",
					Endpoint:        fmt.Sprintf("arn:aws:sqs:local:000000000000:noise-%c", rune('a'+i)),
					SubscriptionArn: fmt.Sprintf("%s:noise-%d", first.TopicARN, i),
				},
			)
		}
		// Real endpoint goes last — only on page 3.
		snsFake.Subscriptions[first.TopicARN] = append(
			snsFake.Subscriptions[first.TopicARN],
			awsclient.Subscription{
				TopicArn:        first.TopicARN,
				Protocol:        "sqs",
				Endpoint:        first.MainQueueARN,
				SubscriptionArn: first.TopicARN + ":real",
			},
		)

		// Second call: must traverse all three pages, find the endpoint on page 3,
		// and NOT subscribe again.
		callEnsure(t, ctx, sqsFake, snsFake, cfg)

		realSubs := 0
		for _, s := range snsFake.Subscriptions[first.TopicARN] {
			if s.Endpoint == first.MainQueueARN {
				realSubs++
			}
		}
		if realSubs != 1 {
			t.Errorf("found %d subscription(s) to the main queue ARN after paginated idempotency check, want exactly 1", realSubs)
		}
	})

	t.Run("endpoint absent across all pages triggers single subscribe", func(t *testing.T) {
		ctx := t.Context()
		sqsFake := fake.NewSQS()
		snsFake := fake.NewSNS()
		snsFake.ListPageSize = 2
		cfg := defaultCfg()

		// EnsureQueue once to create the topic + queue.
		first := callEnsure(t, ctx, sqsFake, snsFake, cfg)

		// Clear all subscriptions so the next call finds no endpoint on any page.
		delete(snsFake.Subscriptions, first.TopicARN)

		// Seed noise-only subscriptions (3 pages worth at size 2).
		for i := range 4 {
			snsFake.Subscriptions[first.TopicARN] = append(
				snsFake.Subscriptions[first.TopicARN],
				awsclient.Subscription{
					TopicArn:        first.TopicARN,
					Protocol:        "sqs",
					Endpoint:        fmt.Sprintf("arn:aws:sqs:local:000000000000:other-%c", rune('a'+i)),
					SubscriptionArn: fmt.Sprintf("%s:other-%d", first.TopicARN, i),
				},
			)
		}

		// EnsureQueue must page through all entries, find no match, and subscribe once.
		callEnsure(t, ctx, sqsFake, snsFake, cfg)

		realSubs := 0
		for _, s := range snsFake.Subscriptions[first.TopicARN] {
			if s.Endpoint == first.MainQueueARN {
				realSubs++
			}
		}
		if realSubs != 1 {
			t.Errorf("after absent-endpoint path: got %d subscription(s) to main queue ARN, want exactly 1", realSubs)
		}
	})
}

// TestEnsureQueueTopicARNsSameAcrossCalls verifies the SNS topic's ARN is
// stable across calls (CreateTopic is idempotent on name in SNS).
func TestEnsureQueueTopicARNsSameAcrossCalls(t *testing.T) {
	ctx := t.Context()
	sqsFake := fake.NewSQS()
	snsFake := fake.NewSNS()
	cfg := defaultCfg()

	first := callEnsure(t, ctx, sqsFake, snsFake, cfg)
	second := callEnsure(t, ctx, sqsFake, snsFake, cfg)

	if first.TopicARN != second.TopicARN {
		t.Errorf("TopicARN changed between calls: first=%q second=%q", first.TopicARN, second.TopicARN)
	}
}

// TestEnsureQueueErrorPropagation verifies that errors from the underlying
// SQS and SNS clients are surfaced with enough context to identify the
// failing step, and that EnsureQueue returns the error rather than
// silently continuing.
func TestEnsureQueueErrorPropagation(t *testing.T) {
	t.Run("CreateQueue DLQ fails", func(t *testing.T) {
		ctx := t.Context()
		sqsFake := fake.NewSQS()
		snsFake := fake.NewSNS()
		sqsFake.InjectError(fake.SQSMethodCreateQueue, errSentinel("dlq-create"), 1)

		_, err := provisioner.EnsureQueue(ctx, sqsFake, snsFake, defaultCfg())
		if err == nil {
			t.Fatal("expected an error from DLQ CreateQueue failure, got nil")
		}
		if !strings.Contains(err.Error(), "DLQ") {
			t.Errorf("error %q should mention DLQ to identify the failing step", err.Error())
		}
	})

	t.Run("CreateQueue main queue fails", func(t *testing.T) {
		ctx := t.Context()
		snsFake := fake.NewSNS()
		cfg := defaultCfg()

		// sqsFailOnMain wraps the SQS fake and injects an error only when
		// CreateQueue is called with the main queue name. This is necessary
		// because the fake's InjectError fires on the very next call
		// regardless of which queue is being created, so we can't use it to
		// target the main queue specifically without also failing the DLQ step.
		sqsFake := fake.NewSQS()
		sqs := &sqsFailOnName{SQS: sqsFake, failName: cfg.MainQueueName, failErr: errSentinel("main-create")}

		_, err := provisioner.EnsureQueue(ctx, sqs, snsFake, cfg)
		if err == nil {
			t.Fatal("expected an error from main queue CreateQueue failure, got nil")
		}
		if !strings.Contains(err.Error(), "main queue") {
			t.Errorf("error %q should mention \"main queue\" to identify the failing step", err.Error())
		}
	})

	t.Run("CreateTopic fails", func(t *testing.T) {
		ctx := t.Context()
		sqsFake := fake.NewSQS()
		snsFake := fake.NewSNS()
		snsFake.InjectError(fake.SNSMethodCreateTopic, errSentinel("topic-create"), 1)

		_, err := provisioner.EnsureQueue(ctx, sqsFake, snsFake, defaultCfg())
		if err == nil {
			t.Fatal("expected an error from SNS CreateTopic failure, got nil")
		}
		if !strings.Contains(err.Error(), "topic") {
			t.Errorf("error %q should mention topic to identify the failing step", err.Error())
		}
	})

	t.Run("ListSubscriptionsByTopic fails", func(t *testing.T) {
		ctx := t.Context()
		sqsFake := fake.NewSQS()
		snsFake := fake.NewSNS()
		snsFake.InjectError(fake.SNSMethodListSubscriptionsByTopic, errSentinel("list-subs"), 1)

		_, err := provisioner.EnsureQueue(ctx, sqsFake, snsFake, defaultCfg())
		if err == nil {
			t.Fatal("expected an error from ListSubscriptionsByTopic failure, got nil")
		}
	})

	t.Run("Subscribe fails", func(t *testing.T) {
		ctx := t.Context()
		sqsFake := fake.NewSQS()
		snsFake := fake.NewSNS()
		snsFake.InjectError(fake.SNSMethodSubscribe, errSentinel("subscribe"), 1)

		_, err := provisioner.EnsureQueue(ctx, sqsFake, snsFake, defaultCfg())
		if err == nil {
			t.Fatal("expected an error from SNS Subscribe failure, got nil")
		}
	})
}

// errSentinel is a tiny named-error type used to make test error injections
// traceable without depending on errors.New (which gives opaque values).
type errSentinel string

func (e errSentinel) Error() string { return string(e) }

// sqsFailOnName is a thin awsclient.SQSClient wrapper that intercepts
// CreateQueue and returns failErr when the queue name matches failName.
// All other calls are forwarded to the embedded SQS fake unchanged.
// Used by the "CreateQueue main queue fails" error-propagation test to
// target the main queue step precisely without also failing the DLQ step.
type sqsFailOnName struct {
	*fake.SQS
	failName string
	failErr  error
}

func (s *sqsFailOnName) CreateQueue(ctx context.Context, in awsclient.CreateQueueInput) (awsclient.CreateQueueOutput, error) {
	if in.Name == s.failName {
		return awsclient.CreateQueueOutput{}, s.failErr
	}
	return s.SQS.CreateQueue(ctx, in)
}

// Ensure the awsclient import is used (the Subscription type is used in the
// paginated-list test above). If the import is unused the compiler will
// flag it; this comment is here for clarity only.
