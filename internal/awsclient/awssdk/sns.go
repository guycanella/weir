package awssdk

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/guycanella/weir/internal/awsclient"
)

// SNS adapts awsclient.SNSClient to a real aws-sdk-go-v2 SNS client.
type SNS struct {
	client *sns.Client
}

var _ awsclient.SNSClient = (*SNS)(nil)

// CreateTopic implements awsclient.SNSClient.
func (a *SNS) CreateTopic(ctx context.Context, in awsclient.CreateTopicInput) (awsclient.CreateTopicOutput, error) {
	out, err := a.client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(in.Name),
	})
	if err != nil {
		return awsclient.CreateTopicOutput{}, fmt.Errorf("awssdk: CreateTopic: %w", err)
	}

	return awsclient.CreateTopicOutput{TopicArn: aws.ToString(out.TopicArn)}, nil
}

// Subscribe implements awsclient.SNSClient.
func (a *SNS) Subscribe(ctx context.Context, in awsclient.SubscribeInput) (awsclient.SubscribeOutput, error) {
	out, err := a.client.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn:              aws.String(in.TopicArn),
		Protocol:              aws.String(in.Protocol),
		Endpoint:              aws.String(in.Endpoint),
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		return awsclient.SubscribeOutput{}, fmt.Errorf("awssdk: Subscribe: %w", err)
	}

	return awsclient.SubscribeOutput{SubscriptionArn: aws.ToString(out.SubscriptionArn)}, nil
}

// ListSubscriptionsByTopic implements awsclient.SNSClient.
func (a *SNS) ListSubscriptionsByTopic(ctx context.Context, in awsclient.ListSubscriptionsByTopicInput) (awsclient.ListSubscriptionsByTopicOutput, error) {
	out, err := a.client.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
		TopicArn:  aws.String(in.TopicArn),
		NextToken: nonEmptyStringPtr(in.NextToken),
	})
	if err != nil {
		return awsclient.ListSubscriptionsByTopicOutput{}, fmt.Errorf("awssdk: ListSubscriptionsByTopic: %w", err)
	}

	subs := make([]awsclient.Subscription, 0, len(out.Subscriptions))
	for _, s := range out.Subscriptions {
		subs = append(subs, awsclient.Subscription{
			SubscriptionArn: aws.ToString(s.SubscriptionArn),
			Owner:           aws.ToString(s.Owner),
			Protocol:        aws.ToString(s.Protocol),
			Endpoint:        aws.ToString(s.Endpoint),
			TopicArn:        aws.ToString(s.TopicArn),
		})
	}

	return awsclient.ListSubscriptionsByTopicOutput{
		Subscriptions: subs,
		NextToken:     aws.ToString(out.NextToken),
	}, nil
}

// GetTopicAttributes implements awsclient.SNSClient.
func (a *SNS) GetTopicAttributes(ctx context.Context, in awsclient.GetTopicAttributesInput) (awsclient.GetTopicAttributesOutput, error) {
	out, err := a.client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: aws.String(in.TopicArn),
	})
	if err != nil {
		return awsclient.GetTopicAttributesOutput{}, fmt.Errorf("awssdk: GetTopicAttributes: %w", err)
	}

	attributes := make(map[string]string, len(out.Attributes))
	for name, value := range out.Attributes {
		attributes[name] = value
	}
	return awsclient.GetTopicAttributesOutput{Attributes: attributes}, nil
}

// SetTopicAttributes implements awsclient.SNSClient.
func (a *SNS) SetTopicAttributes(ctx context.Context, in awsclient.SetTopicAttributesInput) (awsclient.SetTopicAttributesOutput, error) {
	_, err := a.client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
		TopicArn:       aws.String(in.TopicArn),
		AttributeName:  aws.String(in.AttributeName),
		AttributeValue: aws.String(in.AttributeValue),
	})
	if err != nil {
		return awsclient.SetTopicAttributesOutput{}, fmt.Errorf("awssdk: SetTopicAttributes: %w", err)
	}

	return awsclient.SetTopicAttributesOutput{}, nil
}
