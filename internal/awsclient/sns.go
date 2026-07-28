package awsclient

import "context"

// SNSClient is the subset of SNS operations Weir needs to provision and
// idempotently maintain the topic that fans out S3 events to SQS
// (ADR-001, WR-018).
type SNSClient interface {
	// CreateTopic creates a topic, or reports the existing one if a topic
	// with that name already exists (SNS's own CreateTopic is idempotent
	// on name).
	CreateTopic(ctx context.Context, in CreateTopicInput) (CreateTopicOutput, error)

	// Subscribe subscribes an endpoint (an SQS queue ARN, in Weir's case)
	// to a topic.
	Subscribe(ctx context.Context, in SubscribeInput) (SubscribeOutput, error)

	// ListSubscriptionsByTopic lists a topic's current subscriptions, so a
	// caller can check whether one already exists before subscribing again
	// (WR-018's idempotent "ensure" semantics).
	ListSubscriptionsByTopic(ctx context.Context, in ListSubscriptionsByTopicInput) (ListSubscriptionsByTopicOutput, error)
}

// CreateTopicInput names the topic to create.
type CreateTopicInput struct {
	Name string
}

// CreateTopicOutput reports the created (or already-existing) topic's ARN.
type CreateTopicOutput struct {
	TopicArn string
}

// SubscribeInput describes a subscription to create against a topic.
type SubscribeInput struct {
	TopicArn string

	// Protocol is the subscription protocol, "sqs" for every use Weir
	// makes of it.
	Protocol string

	// Endpoint is the subscriber's address: an SQS queue ARN when
	// Protocol is "sqs".
	Endpoint string
}

// SubscribeOutput reports the created subscription's ARN.
type SubscribeOutput struct {
	SubscriptionArn string
}

// ListSubscriptionsByTopicInput identifies the topic to list. Real SNS
// returns at most 100 subscriptions per call; NextToken requests the page
// following a previous ListSubscriptionsByTopicOutput.NextToken. An empty
// NextToken requests the first page.
type ListSubscriptionsByTopicInput struct {
	TopicArn  string
	NextToken string
}

// ListSubscriptionsByTopicOutput lists a page of a topic's current
// subscriptions. A non-empty NextToken means further pages remain; an empty
// NextToken means the caller now has every subscription.
type ListSubscriptionsByTopicOutput struct {
	Subscriptions []Subscription
	NextToken     string
}

// Subscription describes one subscription to a topic.
type Subscription struct {
	SubscriptionArn string
	Owner           string
	Protocol        string
	Endpoint        string
	TopicArn        string
}
