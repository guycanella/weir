// Package provisioner contains idempotent ensure-functions that reconcile
// AWS resources to a desired state (WR-018, WR-019, ...). Each function is
// level-triggered: it inspects the current state of the world, creates or
// updates only what is missing or wrong, and is safe to call repeatedly with
// the same arguments — calling it twice must be a no-op.
//
// No business logic lives here; the functions operate purely through the
// awsclient interfaces (WR-016) so they can be tested with in-memory fakes
// (WR-016's fake package) and exercised against real AWS via the awssdk
// adapters (WR-017).
package provisioner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/guycanella/weir/internal/awsclient"
)

// QueueSet is the fully-resolved set of AWS resource identifiers that back
// one ProcessingPipeline's event-delivery path. Every field is populated by
// EnsureQueue; callers (the reconciler, WR-020's integration tests) read
// these to wire the next step (S3 → SNS notification, WR-019).
type QueueSet struct {
	// MainQueueURL is the URL of the primary SQS queue that workers consume.
	MainQueueURL string

	// MainQueueARN is the ARN of the primary queue. Used when subscribing the
	// queue to the SNS topic (the topic delivers to an ARN, not a URL).
	MainQueueARN string

	// DLQueueURL is the URL of the dead-letter queue.
	DLQueueURL string

	// DLQueueARN is the ARN of the dead-letter queue. Written into the main
	// queue's RedrivePolicy so SQS can route exhausted messages there.
	DLQueueARN string

	// TopicARN is the ARN of the SNS topic that fans out S3 events to the
	// main queue (ADR-001). WR-019 uses this to set the S3 bucket
	// notification destination.
	TopicARN string
}

// QueueConfig parameterises an EnsureQueue call. Every field corresponds
// to a named resource that must exist (or be created) for the pipeline.
type QueueConfig struct {
	// MainQueueName is the SQS queue name for the pipeline's primary queue.
	// Must be unique within the AWS account/region.
	MainQueueName string

	// DLQueueName is the SQS queue name for the dead-letter queue. Exhausted
	// messages from MainQueue are routed here.
	DLQueueName string

	// TopicName is the SNS topic name that fans out S3 events (ADR-001).
	TopicName string

	// VisibilityTimeout is applied to the main queue. Expressed in seconds;
	// the worker must process and delete a message within this window or SQS
	// will redeliver it (and eventually send it to the DLQ).
	VisibilityTimeout int

	// MaxReceiveCount is the number of times a message may be received from
	// the main queue before SQS routes it to the DLQ. Must be >= 1.
	MaxReceiveCount int
}

// EnsureQueue idempotently provisions the full AWS resource set for one
// processing pipeline:
//
//  1. DLQ (SQS) — created if absent; idempotent on name.
//  2. Main queue (SQS) — created if absent; visibility timeout and redrive
//     policy set (or updated) on every call so the queue always converges
//     to cfg's values, regardless of whether it was just created or already
//     existed.
//  3. SNS topic — created if absent; idempotent on name.
//  4. SQS queue policy — sets (or updates) the resource-based policy on the
//     main queue that allows sns.amazonaws.com to call sqs:SendMessage,
//     restricted by aws:SourceArn to the topic ARN. Without this policy the
//     SNS subscription is created but SNS cannot actually deliver messages
//     to the queue on real AWS (LocalStack skips this check).
//  5. SNS → SQS subscription — only created if no existing subscription on
//     the topic already has the main queue ARN as its endpoint. This is the
//     key idempotency invariant: calling EnsureQueue twice must not create a
//     duplicate subscription.
//
// On success it returns the QueueSet of resource identifiers that callers
// need for subsequent provisioning steps (WR-019) and for wiring the
// reconciler's status.
func EnsureQueue(ctx context.Context, sqs awsclient.SQSClient, sns awsclient.SNSClient, cfg QueueConfig) (QueueSet, error) {
	// ── 1. DLQ ──────────────────────────────────────────────────────────────
	dlqOut, err := sqs.CreateQueue(ctx, awsclient.CreateQueueInput{
		Name: cfg.DLQueueName,
	})
	if err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: create DLQ %q: %w", cfg.DLQueueName, err)
	}
	dlqURL := dlqOut.QueueUrl

	dlqAttrs, err := sqs.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
		QueueUrl:       dlqURL,
		AttributeNames: []string{"QueueArn"},
	})
	if err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: get DLQ ARN for %q: %w", cfg.DLQueueName, err)
	}
	dlqARN := dlqAttrs.Attributes["QueueArn"]
	if dlqARN == "" {
		return QueueSet{}, fmt.Errorf("provisioner: DLQ %q reported no QueueArn", cfg.DLQueueName)
	}

	// ── 2. Main queue ────────────────────────────────────────────────────────
	// CreateQueue is idempotent on name (returns the existing URL), so we
	// always call it and then unconditionally apply the visibility timeout and
	// redrive policy via SetQueueAttributes — this is the level-triggered
	// "converge to desired" pattern: even if the queue already existed with
	// different settings, the next call to EnsureQueue will correct them.
	redrivePolicy, err := marshalRedrivePolicy(dlqARN, cfg.MaxReceiveCount)
	if err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: marshal redrive policy: %w", err)
	}

	mainOut, err := sqs.CreateQueue(ctx, awsclient.CreateQueueInput{
		Name: cfg.MainQueueName,
	})
	if err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: create main queue %q: %w", cfg.MainQueueName, err)
	}
	mainURL := mainOut.QueueUrl

	mainAttrs, err := sqs.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{
		QueueUrl:       mainURL,
		AttributeNames: []string{"QueueArn"},
	})
	if err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: get main queue ARN for %q: %w", cfg.MainQueueName, err)
	}
	mainARN := mainAttrs.Attributes["QueueArn"]
	if mainARN == "" {
		return QueueSet{}, fmt.Errorf("provisioner: main queue %q reported no QueueArn", cfg.MainQueueName)
	}

	if _, err := sqs.SetQueueAttributes(ctx, awsclient.SetQueueAttributesInput{
		QueueUrl: mainURL,
		Attributes: map[string]string{
			"VisibilityTimeout": fmt.Sprintf("%d", cfg.VisibilityTimeout),
			"RedrivePolicy":     redrivePolicy,
		},
	}); err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: set main queue attributes for %q: %w", cfg.MainQueueName, err)
	}

	// ── 3. SNS topic ─────────────────────────────────────────────────────────
	topicOut, err := sns.CreateTopic(ctx, awsclient.CreateTopicInput{
		Name: cfg.TopicName,
	})
	if err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: create SNS topic %q: %w", cfg.TopicName, err)
	}
	topicARN := topicOut.TopicArn

	// ── 4. SQS queue policy (allow SNS to deliver messages) ──────────────────
	// On real AWS, SNS cannot deliver to SQS unless the queue has a
	// resource-based policy granting sns.amazonaws.com sqs:SendMessage,
	// conditioned on aws:SourceArn = topicARN. We set this unconditionally on
	// every call (level-triggered): if the policy already matches the desired
	// state, AWS accepts it as a no-op. The fake stores it as the "Policy"
	// attribute and does not enforce it, matching LocalStack's behaviour.
	queuePolicy, err := marshalQueuePolicy(mainARN, topicARN)
	if err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: marshal queue policy: %w", err)
	}
	if _, err := sqs.SetQueueAttributes(ctx, awsclient.SetQueueAttributesInput{
		QueueUrl: mainURL,
		Attributes: map[string]string{
			"Policy": queuePolicy,
		},
	}); err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: set queue policy for %q: %w", cfg.MainQueueName, err)
	}

	// ── 5. SNS → SQS subscription (idempotent ensure) ───────────────────────
	// Real SNS does NOT de-duplicate subscriptions the way CreateTopic/
	// CreateQueue do: calling Subscribe twice with the same endpoint creates
	// two subscriptions. We must list first and only subscribe if the
	// endpoint is absent. The fake mirrors this behaviour (see fake/sns.go's
	// Subscribe doc comment) so unit tests catch a naive double-subscribe.
	if err := ensureSubscription(ctx, sns, topicARN, mainARN); err != nil {
		return QueueSet{}, fmt.Errorf("provisioner: ensure SNS subscription (topic=%q, queue=%q): %w", topicARN, mainARN, err)
	}

	return QueueSet{
		MainQueueURL: mainURL,
		MainQueueARN: mainARN,
		DLQueueURL:   dlqURL,
		DLQueueARN:   dlqARN,
		TopicARN:     topicARN,
	}, nil
}

// ensureSubscription subscribes queueARN to topicARN with protocol "sqs"
// only if no existing subscription on the topic already has queueARN as its
// endpoint. It paginates through all subscription pages before deciding,
// so it works correctly even when a topic has more than one page's worth of
// subscriptions (WR-016 review finding #2).
func ensureSubscription(ctx context.Context, sns awsclient.SNSClient, topicARN, queueARN string) error {
	var nextToken string
	for {
		out, err := sns.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
			TopicArn:  topicARN,
			NextToken: nextToken,
		})
		if err != nil {
			return fmt.Errorf("list subscriptions: %w", err)
		}

		for _, s := range out.Subscriptions {
			if s.Protocol == "sqs" && s.Endpoint == queueARN {
				// Subscription already exists — no-op.
				return nil
			}
		}

		if out.NextToken == "" {
			break
		}
		nextToken = out.NextToken
	}

	// No existing subscription found; create one.
	if _, err := sns.Subscribe(ctx, awsclient.SubscribeInput{
		TopicArn: topicARN,
		Protocol: "sqs",
		Endpoint: queueARN,
	}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	return nil
}

// redrivePolicy is the JSON shape SQS expects for a queue's RedrivePolicy
// attribute. The field names must match exactly.
type redrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

// marshalRedrivePolicy serialises a redrive policy to the JSON string SQS
// expects as the value of the "RedrivePolicy" queue attribute.
func marshalRedrivePolicy(dlqARN string, maxReceiveCount int) (string, error) {
	p := redrivePolicy{
		DeadLetterTargetArn: dlqARN,
		MaxReceiveCount:     maxReceiveCount,
	}
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// sqsQueuePolicy is the IAM policy document structure set on the main SQS
// queue to allow the SNS topic to deliver messages. The outer wrapper matches
// what SQS expects in the "Policy" attribute.
type sqsQueuePolicy struct {
	Version   string               `json:"Version"`
	Statement []sqsPolicyStatement `json:"Statement"`
}

type sqsPolicyStatement struct {
	Sid       string                       `json:"Sid"`
	Effect    string                       `json:"Effect"`
	Principal map[string]string            `json:"Principal"`
	Action    string                       `json:"Action"`
	Resource  string                       `json:"Resource"`
	Condition map[string]map[string]string `json:"Condition"`
}

// marshalQueuePolicy builds the IAM policy JSON that grants sns.amazonaws.com
// permission to call sqs:SendMessage on queueARN, conditioned on
// aws:SourceArn matching topicARN. This is required on real AWS for SNS to
// deliver messages; without it delivery silently fails.
func marshalQueuePolicy(queueARN, topicARN string) (string, error) {
	p := sqsQueuePolicy{
		Version: "2012-10-17",
		Statement: []sqsPolicyStatement{
			{
				Sid:    "AllowSNSDelivery",
				Effect: "Allow",
				Principal: map[string]string{
					"Service": "sns.amazonaws.com",
				},
				Action:   "sqs:SendMessage",
				Resource: queueARN,
				Condition: map[string]map[string]string{
					"ArnEquals": {
						"aws:SourceArn": topicARN,
					},
				},
			},
		},
	}
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
