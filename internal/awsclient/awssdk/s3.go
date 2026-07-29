package awssdk

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/guycanella/weir/internal/awsclient"
)

// S3 adapts awsclient.S3Client to a real aws-sdk-go-v2 S3 client.
type S3 struct {
	client *s3.Client
}

var _ awsclient.S3Client = (*S3)(nil)

// PutObject implements awsclient.S3Client.
func (a *S3) PutObject(ctx context.Context, in awsclient.PutObjectInput) (awsclient.PutObjectOutput, error) {
	out, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(in.Bucket),
		Key:         aws.String(in.Key),
		Body:        bytes.NewReader(in.Body),
		ContentType: nonEmptyStringPtr(in.ContentType),
	})
	if err != nil {
		return awsclient.PutObjectOutput{}, fmt.Errorf("awssdk: PutObject: %w", err)
	}

	return awsclient.PutObjectOutput{ETag: aws.ToString(out.ETag)}, nil
}

// ListBuckets implements awsclient.S3Client.
func (a *S3) ListBuckets(ctx context.Context, _ awsclient.ListBucketsInput) (awsclient.ListBucketsOutput, error) {
	out, err := a.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return awsclient.ListBucketsOutput{}, fmt.Errorf("awssdk: ListBuckets: %w", err)
	}

	buckets := make([]awsclient.Bucket, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		buckets = append(buckets, awsclient.Bucket{Name: aws.ToString(b.Name)})
	}

	return awsclient.ListBucketsOutput{Buckets: buckets}, nil
}

// GetBucketNotificationConfiguration implements awsclient.S3Client.
func (a *S3) GetBucketNotificationConfiguration(ctx context.Context, in awsclient.GetBucketNotificationConfigurationInput) (awsclient.GetBucketNotificationConfigurationOutput, error) {
	out, err := a.client.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{
		Bucket: aws.String(in.Bucket),
	})
	if err != nil {
		return awsclient.GetBucketNotificationConfigurationOutput{}, fmt.Errorf("awssdk: GetBucketNotificationConfiguration: %w", err)
	}

	cfg := awsclient.NotificationConfiguration{
		TopicConfigurations:          toWeirTopicConfigurations(out.TopicConfigurations),
		QueueConfigurations:          toWeirQueueConfigurations(out.QueueConfigurations),
		LambdaFunctionConfigurations: toWeirLambdaFunctionConfigurations(out.LambdaFunctionConfigurations),
		EventBridgeConfiguration:     toWeirEventBridgeConfiguration(out.EventBridgeConfiguration),
	}

	return awsclient.GetBucketNotificationConfigurationOutput{Configuration: cfg}, nil
}

// PutBucketNotificationConfiguration implements awsclient.S3Client.
func (a *S3) PutBucketNotificationConfiguration(ctx context.Context, in awsclient.PutBucketNotificationConfigurationInput) (awsclient.PutBucketNotificationConfigurationOutput, error) {
	_, err := a.client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(in.Bucket),
		NotificationConfiguration: &types.NotificationConfiguration{
			TopicConfigurations:          toSDKTopicConfigurations(in.Configuration.TopicConfigurations),
			QueueConfigurations:          toSDKQueueConfigurations(in.Configuration.QueueConfigurations),
			LambdaFunctionConfigurations: toSDKLambdaFunctionConfigurations(in.Configuration.LambdaFunctionConfigurations),
			EventBridgeConfiguration:     toSDKEventBridgeConfiguration(in.Configuration.EventBridgeConfiguration),
		},
	})
	if err != nil {
		return awsclient.PutBucketNotificationConfigurationOutput{}, fmt.Errorf("awssdk: PutBucketNotificationConfiguration: %w", err)
	}

	return awsclient.PutBucketNotificationConfigurationOutput{}, nil
}

func nonEmptyStringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}

func toSDKFilter(f *awsclient.NotificationFilter) *types.NotificationConfigurationFilter {
	if f == nil || (f.Prefix == "" && f.Suffix == "") {
		return nil
	}

	var rules []types.FilterRule
	if f.Prefix != "" {
		rules = append(rules, types.FilterRule{Name: types.FilterRuleNamePrefix, Value: aws.String(f.Prefix)})
	}
	if f.Suffix != "" {
		rules = append(rules, types.FilterRule{Name: types.FilterRuleNameSuffix, Value: aws.String(f.Suffix)})
	}

	return &types.NotificationConfigurationFilter{Key: &types.S3KeyFilter{FilterRules: rules}}
}

func toWeirFilter(f *types.NotificationConfigurationFilter) *awsclient.NotificationFilter {
	if f == nil || f.Key == nil {
		return nil
	}

	filter := &awsclient.NotificationFilter{}
	for _, r := range f.Key.FilterRules {
		switch r.Name {
		case types.FilterRuleNamePrefix:
			filter.Prefix = aws.ToString(r.Value)
		case types.FilterRuleNameSuffix:
			filter.Suffix = aws.ToString(r.Value)
		}
	}

	return filter
}

func toSDKTopicConfigurations(cfgs []awsclient.TopicConfiguration) []types.TopicConfiguration {
	out := make([]types.TopicConfiguration, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, types.TopicConfiguration{
			Id:       nonEmptyStringPtr(c.ID),
			TopicArn: aws.String(c.TopicArn),
			Events:   toSDKEvents(c.Events),
			Filter:   toSDKFilter(c.Filter),
		})
	}
	return out
}

func toWeirTopicConfigurations(cfgs []types.TopicConfiguration) []awsclient.TopicConfiguration {
	out := make([]awsclient.TopicConfiguration, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, awsclient.TopicConfiguration{
			ID:       aws.ToString(c.Id),
			TopicArn: aws.ToString(c.TopicArn),
			Events:   toWeirEvents(c.Events),
			Filter:   toWeirFilter(c.Filter),
		})
	}
	return out
}

func toSDKQueueConfigurations(cfgs []awsclient.QueueConfiguration) []types.QueueConfiguration {
	out := make([]types.QueueConfiguration, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, types.QueueConfiguration{
			Id:       nonEmptyStringPtr(c.ID),
			QueueArn: aws.String(c.QueueArn),
			Events:   toSDKEvents(c.Events),
			Filter:   toSDKFilter(c.Filter),
		})
	}
	return out
}

func toWeirQueueConfigurations(cfgs []types.QueueConfiguration) []awsclient.QueueConfiguration {
	out := make([]awsclient.QueueConfiguration, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, awsclient.QueueConfiguration{
			ID:       aws.ToString(c.Id),
			QueueArn: aws.ToString(c.QueueArn),
			Events:   toWeirEvents(c.Events),
			Filter:   toWeirFilter(c.Filter),
		})
	}
	return out
}

func toSDKLambdaFunctionConfigurations(cfgs []awsclient.LambdaFunctionConfiguration) []types.LambdaFunctionConfiguration {
	out := make([]types.LambdaFunctionConfiguration, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, types.LambdaFunctionConfiguration{
			Id:                nonEmptyStringPtr(c.ID),
			LambdaFunctionArn: aws.String(c.LambdaFunctionArn),
			Events:            toSDKEvents(c.Events),
			Filter:            toSDKFilter(c.Filter),
		})
	}
	return out
}

func toWeirLambdaFunctionConfigurations(cfgs []types.LambdaFunctionConfiguration) []awsclient.LambdaFunctionConfiguration {
	out := make([]awsclient.LambdaFunctionConfiguration, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, awsclient.LambdaFunctionConfiguration{
			ID:                aws.ToString(c.Id),
			LambdaFunctionArn: aws.ToString(c.LambdaFunctionArn),
			Events:            toWeirEvents(c.Events),
			Filter:            toWeirFilter(c.Filter),
		})
	}
	return out
}

func toSDKEvents(events []string) []types.Event {
	out := make([]types.Event, 0, len(events))
	for _, e := range events {
		out = append(out, types.Event(e))
	}
	return out
}

func toWeirEvents(events []types.Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, string(e))
	}
	return out
}

func toSDKEventBridgeConfiguration(eb *awsclient.EventBridgeConfiguration) *types.EventBridgeConfiguration {
	if eb == nil {
		return nil
	}
	return &types.EventBridgeConfiguration{}
}

func toWeirEventBridgeConfiguration(eb *types.EventBridgeConfiguration) *awsclient.EventBridgeConfiguration {
	if eb == nil {
		return nil
	}
	return &awsclient.EventBridgeConfiguration{}
}
