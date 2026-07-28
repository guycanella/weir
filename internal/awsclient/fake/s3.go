package fake

import (
	"context"
	"crypto/md5" //nolint:gosec // fake ETag only, not used for anything security-sensitive
	"encoding/hex"
	"sync"

	"github.com/guycanella/weir/internal/awsclient"
)

// Method name constants for S3's InjectError, so callers get typo-safety
// and IDE completion instead of a bare string.
const (
	S3MethodPutObject                          = "PutObject"
	S3MethodListBuckets                        = "ListBuckets"
	S3MethodGetBucketNotificationConfiguration = "GetBucketNotificationConfiguration"
	S3MethodPutBucketNotificationConfiguration = "PutBucketNotificationConfiguration"
)

// PutObjectRecord is what S3 remembers about a single successful
// PutObject call.
type PutObjectRecord struct {
	Body        []byte
	ContentType string
}

// S3 is an in-memory fake of awsclient.S3Client, safe for concurrent use.
//
// Buckets seeds the list ListBuckets returns; tests set it directly, and it
// is never mutated by PutObject — Weir's Terraform creates buckets, not the
// operator, so a bucket having a PutObject record here does not make it
// appear in Buckets, and PutObject does not check Buckets before writing.
// PutObjects records every successful PutObject call, keyed first by
// bucket then by key.
type S3 struct {
	mu sync.Mutex

	Buckets    []awsclient.Bucket
	PutObjects map[string]map[string]PutObjectRecord

	notifications map[string]awsclient.NotificationConfiguration

	errs *errorQueue
}

// NewS3 returns an empty, ready-to-use S3 fake.
func NewS3() *S3 {
	return &S3{
		PutObjects:    make(map[string]map[string]PutObjectRecord),
		notifications: make(map[string]awsclient.NotificationConfiguration),
		errs:          newErrorQueue(),
	}
}

// InjectError arranges for the next n calls (n < 1 is treated as 1) to the
// named method (one of the S3Method* constants) to return err instead of
// performing their normal work.
func (f *S3) InjectError(method string, err error, n int) {
	f.errs.push(method, err, n)
}

var _ awsclient.S3Client = (*S3)(nil)

// PutObject implements awsclient.S3Client.
func (f *S3) PutObject(_ context.Context, in awsclient.PutObjectInput) (awsclient.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(S3MethodPutObject); err != nil {
		return awsclient.PutObjectOutput{}, err
	}

	if _, ok := f.PutObjects[in.Bucket]; !ok {
		f.PutObjects[in.Bucket] = make(map[string]PutObjectRecord)
	}

	body := append([]byte(nil), in.Body...)
	f.PutObjects[in.Bucket][in.Key] = PutObjectRecord{Body: body, ContentType: in.ContentType}

	sum := md5.Sum(body) //nolint:gosec // fake ETag only
	return awsclient.PutObjectOutput{ETag: hex.EncodeToString(sum[:])}, nil
}

// cloneNotificationConfiguration deep-copies a NotificationConfiguration so
// the copy shares no backing slice or pointer with the original: not its
// TopicConfigurations/QueueConfigurations/LambdaFunctionConfigurations
// slices, not any entry's Events slice, not any *NotificationFilter it
// points to, and not its *EventBridgeConfiguration marker. Real S3
// serializes this config over the wire on every call, so a caller mutating
// what it got back (or what it passed in) can never reach into S3's stored
// state; the fake must not allow that aliasing either.
func cloneNotificationConfiguration(cfg awsclient.NotificationConfiguration) awsclient.NotificationConfiguration {
	topics := append([]awsclient.TopicConfiguration(nil), cfg.TopicConfigurations...)
	for i, t := range topics {
		t.Events = append([]string(nil), t.Events...)
		if t.Filter != nil {
			filter := *t.Filter
			t.Filter = &filter
		}
		topics[i] = t
	}

	queues := append([]awsclient.QueueConfiguration(nil), cfg.QueueConfigurations...)
	for i, q := range queues {
		q.Events = append([]string(nil), q.Events...)
		if q.Filter != nil {
			filter := *q.Filter
			q.Filter = &filter
		}
		queues[i] = q
	}

	lambdas := append([]awsclient.LambdaFunctionConfiguration(nil), cfg.LambdaFunctionConfigurations...)
	for i, l := range lambdas {
		l.Events = append([]string(nil), l.Events...)
		if l.Filter != nil {
			filter := *l.Filter
			l.Filter = &filter
		}
		lambdas[i] = l
	}

	var eventBridge *awsclient.EventBridgeConfiguration
	if cfg.EventBridgeConfiguration != nil {
		eventBridge = &awsclient.EventBridgeConfiguration{}
	}

	return awsclient.NotificationConfiguration{
		TopicConfigurations:          topics,
		QueueConfigurations:          queues,
		LambdaFunctionConfigurations: lambdas,
		EventBridgeConfiguration:     eventBridge,
	}
}

// ListBuckets implements awsclient.S3Client.
func (f *S3) ListBuckets(_ context.Context, _ awsclient.ListBucketsInput) (awsclient.ListBucketsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(S3MethodListBuckets); err != nil {
		return awsclient.ListBucketsOutput{}, err
	}

	buckets := append([]awsclient.Bucket(nil), f.Buckets...)
	return awsclient.ListBucketsOutput{Buckets: buckets}, nil
}

// GetBucketNotificationConfiguration implements awsclient.S3Client.
func (f *S3) GetBucketNotificationConfiguration(_ context.Context, in awsclient.GetBucketNotificationConfigurationInput) (awsclient.GetBucketNotificationConfigurationOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(S3MethodGetBucketNotificationConfiguration); err != nil {
		return awsclient.GetBucketNotificationConfigurationOutput{}, err
	}

	// A bucket with no notifications configured yet reports a zero-value
	// configuration, matching real S3's behavior.
	cfg := cloneNotificationConfiguration(f.notifications[in.Bucket])
	return awsclient.GetBucketNotificationConfigurationOutput{Configuration: cfg}, nil
}

// PutBucketNotificationConfiguration implements awsclient.S3Client.
func (f *S3) PutBucketNotificationConfiguration(_ context.Context, in awsclient.PutBucketNotificationConfigurationInput) (awsclient.PutBucketNotificationConfigurationOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(S3MethodPutBucketNotificationConfiguration); err != nil {
		return awsclient.PutBucketNotificationConfigurationOutput{}, err
	}

	f.notifications[in.Bucket] = cloneNotificationConfiguration(in.Configuration)
	return awsclient.PutBucketNotificationConfigurationOutput{}, nil
}
