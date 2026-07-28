package awsclient

import "context"

// S3Client is the subset of S3 operations Weir needs: writing processed
// output objects, listing buckets (used by WR-017's LocalStack smoke test),
// and reading/writing a bucket's event notification configuration so S3
// events can be idempotently wired to SNS (WR-019).
type S3Client interface {
	// PutObject writes an object to a bucket. Used by the worker (WR-023)
	// to persist a processed result.
	PutObject(ctx context.Context, in PutObjectInput) (PutObjectOutput, error)

	// ListBuckets lists every bucket visible to the caller's credentials.
	ListBuckets(ctx context.Context, in ListBucketsInput) (ListBucketsOutput, error)

	// GetBucketNotificationConfiguration reads a bucket's current event
	// notification configuration, so a caller can compare it against the
	// desired state before writing (WR-019's get-then-compare-then-put).
	GetBucketNotificationConfiguration(ctx context.Context, in GetBucketNotificationConfigurationInput) (GetBucketNotificationConfigurationOutput, error)

	// PutBucketNotificationConfiguration replaces a bucket's event
	// notification configuration wholesale, as S3 itself does.
	PutBucketNotificationConfiguration(ctx context.Context, in PutBucketNotificationConfigurationInput) (PutBucketNotificationConfigurationOutput, error)
}

// PutObjectInput describes an object to write.
type PutObjectInput struct {
	Bucket      string
	Key         string
	Body        []byte
	ContentType string
}

// PutObjectOutput reports the result of a successful PutObject call.
type PutObjectOutput struct {
	ETag string
}

// ListBucketsInput takes no parameters today; it exists so ListBuckets
// matches the (ctx, Input) shape of every other method in this package.
type ListBucketsInput struct{}

// ListBucketsOutput lists the buckets visible to the caller.
type ListBucketsOutput struct {
	Buckets []Bucket
}

// Bucket identifies a single S3 bucket.
type Bucket struct {
	Name string
}

// GetBucketNotificationConfigurationInput identifies the bucket to read.
type GetBucketNotificationConfigurationInput struct {
	Bucket string
}

// GetBucketNotificationConfigurationOutput carries a bucket's current
// notification configuration. A bucket with no notifications configured
// yet reports a zero-value Configuration, matching real S3's behavior.
type GetBucketNotificationConfigurationOutput struct {
	Configuration NotificationConfiguration
}

// PutBucketNotificationConfigurationInput identifies the bucket to write
// and the full desired notification configuration. Like real S3, writing
// replaces the configuration wholesale rather than merging.
type PutBucketNotificationConfigurationInput struct {
	Bucket        string
	Configuration NotificationConfiguration
}

// PutBucketNotificationConfigurationOutput carries no data on success.
type PutBucketNotificationConfigurationOutput struct{}

// NotificationConfiguration is a bucket's event notification configuration.
//
// Weir only ever creates or modifies TopicConfigurations (ADR-001: S3 ->
// SNS). QueueConfigurations, LambdaFunctionConfigurations, and
// EventBridgeConfiguration are modeled purely so a Get -> modify -> Put
// round-trip preserves any pre-existing destinations (or, for EventBridge,
// the enabled/disabled marker) Weir doesn't touch — S3's Put replaces the
// whole configuration, so omitting these fields would silently delete or
// disable them.
type NotificationConfiguration struct {
	TopicConfigurations          []TopicConfiguration
	QueueConfigurations          []QueueConfiguration
	LambdaFunctionConfigurations []LambdaFunctionConfiguration

	// EventBridgeConfiguration's mere presence (non-nil) means the bucket
	// has EventBridge delivery enabled — it carries no fields, matching
	// AWS's own shape for this. Weir never creates it; it exists only so a
	// Get -> Put round-trip preserves a pre-existing EventBridge setting.
	EventBridgeConfiguration *EventBridgeConfiguration
}

// TopicConfiguration routes matching bucket events to an SNS topic.
type TopicConfiguration struct {
	// ID identifies this configuration entry within the bucket's
	// notification configuration; optional, but useful for idempotent
	// updates that need to recognize an entry Weir previously wrote.
	ID string

	TopicArn string

	// Events lists the S3 event types to route, e.g. "s3:ObjectCreated:*".
	Events []string

	// Filter optionally restricts matching events by key prefix/suffix.
	Filter *NotificationFilter
}

// QueueConfiguration routes matching bucket events to an SQS queue. Weir
// never creates these; they exist only so a Get -> Put round-trip preserves
// any pre-existing SQS notification destinations someone else configured
// on the bucket.
type QueueConfiguration struct {
	ID       string
	QueueArn string
	Events   []string
	Filter   *NotificationFilter
}

// LambdaFunctionConfiguration routes matching bucket events to a Lambda
// function. Weir never creates these; they exist only so a Get -> Put
// round-trip preserves any pre-existing Lambda notification destinations
// someone else configured on the bucket.
type LambdaFunctionConfiguration struct {
	ID                string
	LambdaFunctionArn string
	Events            []string
	Filter            *NotificationFilter
}

// NotificationFilter restricts a notification configuration entry to keys
// matching the given prefix and/or suffix. A zero-value field is not
// applied.
type NotificationFilter struct {
	Prefix string
	Suffix string
}

// EventBridgeConfiguration is a marker type: its presence (a non-nil
// *EventBridgeConfiguration) on a NotificationConfiguration means the
// bucket has EventBridge delivery enabled. It intentionally carries no
// fields, matching real S3's shape for this configuration.
type EventBridgeConfiguration struct{}
