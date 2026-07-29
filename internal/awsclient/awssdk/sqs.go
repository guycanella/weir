package awssdk

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/guycanella/weir/internal/awsclient"
)

// SQS adapts awsclient.SQSClient to a real aws-sdk-go-v2 SQS client.
type SQS struct {
	client *sqs.Client
}

var _ awsclient.SQSClient = (*SQS)(nil)

// CreateQueue implements awsclient.SQSClient.
func (a *SQS) CreateQueue(ctx context.Context, in awsclient.CreateQueueInput) (awsclient.CreateQueueOutput, error) {
	out, err := a.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(in.Name),
		Attributes: in.Attributes,
	})
	if err != nil {
		return awsclient.CreateQueueOutput{}, fmt.Errorf("awssdk: CreateQueue: %w", err)
	}

	return awsclient.CreateQueueOutput{QueueUrl: aws.ToString(out.QueueUrl)}, nil
}

// GetQueueUrl implements awsclient.SQSClient.
func (a *SQS) GetQueueUrl(ctx context.Context, in awsclient.GetQueueUrlInput) (awsclient.GetQueueUrlOutput, error) {
	out, err := a.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(in.Name),
	})
	if err != nil {
		return awsclient.GetQueueUrlOutput{}, fmt.Errorf("awssdk: GetQueueUrl: %w", err)
	}

	return awsclient.GetQueueUrlOutput{QueueUrl: aws.ToString(out.QueueUrl)}, nil
}

// GetQueueAttributes implements awsclient.SQSClient. An empty
// in.AttributeNames requests every attribute, matching the interface's
// documented semantics.
func (a *SQS) GetQueueAttributes(ctx context.Context, in awsclient.GetQueueAttributesInput) (awsclient.GetQueueAttributesOutput, error) {
	names := []types.QueueAttributeName{types.QueueAttributeNameAll}
	if len(in.AttributeNames) > 0 {
		names = make([]types.QueueAttributeName, 0, len(in.AttributeNames))
		for _, n := range in.AttributeNames {
			names = append(names, types.QueueAttributeName(n))
		}
	}

	out, err := a.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(in.QueueUrl),
		AttributeNames: names,
	})
	if err != nil {
		return awsclient.GetQueueAttributesOutput{}, fmt.Errorf("awssdk: GetQueueAttributes: %w", err)
	}

	return awsclient.GetQueueAttributesOutput{Attributes: out.Attributes}, nil
}

// SetQueueAttributes implements awsclient.SQSClient.
func (a *SQS) SetQueueAttributes(ctx context.Context, in awsclient.SetQueueAttributesInput) (awsclient.SetQueueAttributesOutput, error) {
	_, err := a.client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(in.QueueUrl),
		Attributes: in.Attributes,
	})
	if err != nil {
		return awsclient.SetQueueAttributesOutput{}, fmt.Errorf("awssdk: SetQueueAttributes: %w", err)
	}

	return awsclient.SetQueueAttributesOutput{}, nil
}

// SendMessage implements awsclient.SQSClient.
func (a *SQS) SendMessage(ctx context.Context, in awsclient.SendMessageInput) (awsclient.SendMessageOutput, error) {
	out, err := a.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(in.QueueUrl),
		MessageBody: aws.String(in.Body),
	})
	if err != nil {
		return awsclient.SendMessageOutput{}, fmt.Errorf("awssdk: SendMessage: %w", err)
	}

	return awsclient.SendMessageOutput{MessageId: aws.ToString(out.MessageId)}, nil
}

// ReceiveMessage implements awsclient.SQSClient. It always requests every
// system attribute (e.g. ApproximateReceiveCount) so a received Message's
// Attributes map is populated, matching the interface's documented
// semantics.
func (a *SQS) ReceiveMessage(ctx context.Context, in awsclient.ReceiveMessageInput) (awsclient.ReceiveMessageOutput, error) {
	out, err := a.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(in.QueueUrl),
		MaxNumberOfMessages:         in.MaxNumberOfMessages,
		WaitTimeSeconds:             in.WaitTimeSeconds,
		VisibilityTimeout:           in.VisibilityTimeout,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil {
		return awsclient.ReceiveMessageOutput{}, fmt.Errorf("awssdk: ReceiveMessage: %w", err)
	}

	messages := make([]awsclient.Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		messages = append(messages, awsclient.Message{
			MessageId:     aws.ToString(m.MessageId),
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
			Body:          aws.ToString(m.Body),
			Attributes:    m.Attributes,
		})
	}

	return awsclient.ReceiveMessageOutput{Messages: messages}, nil
}

// DeleteMessage implements awsclient.SQSClient.
func (a *SQS) DeleteMessage(ctx context.Context, in awsclient.DeleteMessageInput) (awsclient.DeleteMessageOutput, error) {
	_, err := a.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(in.QueueUrl),
		ReceiptHandle: aws.String(in.ReceiptHandle),
	})
	if err != nil {
		return awsclient.DeleteMessageOutput{}, fmt.Errorf("awssdk: DeleteMessage: %w", err)
	}

	return awsclient.DeleteMessageOutput{}, nil
}
