//go:build integration

package awssdk_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/provisioner"
)

// TestEnsureBucketNotificationAgainstLocalStack verifies the end-to-end flow of:
//  1. Setting up an S3 bucket and provisioning the queue/topic infrastructure via EnsureQueue.
//  2. Wiring S3 bucket event notifications to the SNS topic via EnsureBucketNotification.
//  3. Putting an object into S3 under the watched prefix.
//  4. Receiving the resulting event notification from the SQS queue via S3 -> SNS -> SQS fan-out delivery.
//  5. Calling EnsureBucketNotification a second time to verify idempotency against LocalStack.
//  6. Cleaning up all created S3 objects, bucket, queues, and topic.
func TestEnsureBucketNotificationAgainstLocalStack(t *testing.T) {
	ctx := t.Context()
	clients := newClients(t, ctx)
	rawS3 := rawS3Client(t, ctx)
	rawSQS := rawSQSClient(t, ctx)
	rawSNS := rawSNSClient(t, ctx)

	suffix := uniqueName("s3notif")
	bucketName := "weir-bucket-" + suffix
	watchedPrefix := "incoming/"
	objectKey := watchedPrefix + "test-file.txt"

	// 1. Create S3 Bucket
	_, err := rawS3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket(%q): %v", bucketName, err)
	}

	queueCfg := provisioner.QueueConfig{
		MainQueueName:     "weir-queue-" + suffix,
		DLQueueName:       "weir-dlq-" + suffix,
		TopicName:         "weir-topic-" + suffix,
		VisibilityTimeout: 30,
		MaxReceiveCount:   3,
	}

	// Register teardown before provisioning so cleanup runs even if test fails
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()

		// Delete objects from S3 bucket
		listOut, err := rawS3.ListObjectsV2(cleanupCtx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
		if err == nil {
			for _, obj := range listOut.Contents {
				_, _ = rawS3.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    obj.Key,
				})
			}
		}
		// Delete bucket
		_, _ = rawS3.DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

		// Delete SQS queues and SNS topic
		deleteProvisionedResources(t, cleanupCtx, rawSQS, rawSNS, queueCfg)
	})

	// 2. Run EnsureQueue to set up SQS, DLQ, SNS topic, policy, and subscription
	qSet, err := provisioner.EnsureQueue(ctx, clients.SQS, clients.SNS, queueCfg)
	if err != nil {
		t.Fatalf("EnsureQueue: %v", err)
	}

	// 3. Run EnsureBucketNotification (S3 -> SNS topic for watched prefix)
	notifCfg := provisioner.BucketNotificationConfig{
		Bucket:   bucketName,
		TopicARN: qSet.TopicARN,
		Prefix:   watchedPrefix,
		Events:   []string{"s3:ObjectCreated:*"},
	}

	err = provisioner.EnsureBucketNotification(ctx, clients.S3, clients.SNS, notifCfg)
	if err != nil {
		t.Fatalf("EnsureBucketNotification (first call): %v", err)
	}

	// 4. Put an object into S3 under watched prefix
	objectContent := []byte("hello weir integration test")
	_, err = clients.S3.PutObject(ctx, awsclient.PutObjectInput{
		Bucket:      bucketName,
		Key:         objectKey,
		Body:        objectContent,
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// 5. Receive message from SQS queue to verify S3 -> SNS -> SQS fan-out delivery
	var receivedMessage string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		recOut, err := clients.SQS.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{
			QueueUrl:            qSet.MainQueueURL,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     2,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		if len(recOut.Messages) > 0 {
			receivedMessage = recOut.Messages[0].Body
			// Delete received message
			_, _ = clients.SQS.DeleteMessage(ctx, awsclient.DeleteMessageInput{
				QueueUrl:      qSet.MainQueueURL,
				ReceiptHandle: recOut.Messages[0].ReceiptHandle,
			})
			break
		}
	}

	if receivedMessage == "" {
		t.Fatalf("Timed out waiting for S3 notification message in SQS queue %q", qSet.MainQueueURL)
	}

	// Verify message body contains bucket name and object key
	if !strings.Contains(receivedMessage, bucketName) || !strings.Contains(receivedMessage, objectKey) {
		t.Errorf("Received message body = %q, expected it to reference bucket %q and key %q", receivedMessage, bucketName, objectKey)
	}

	// 6. Call EnsureBucketNotification a second time to verify idempotency against LocalStack
	err = provisioner.EnsureBucketNotification(ctx, clients.S3, clients.SNS, notifCfg)
	if err != nil {
		t.Fatalf("EnsureBucketNotification (second call - idempotency check): %v", err)
	}

	// Read notification config back from S3 to verify it remains valid and intact
	readBack, err := clients.S3.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: bucketName,
	})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration after 2nd call: %v", err)
	}

	if len(readBack.Configuration.TopicConfigurations) != 1 {
		t.Fatalf("expected exactly 1 TopicConfiguration after 2nd call, got %d", len(readBack.Configuration.TopicConfigurations))
	}
	tc := readBack.Configuration.TopicConfigurations[0]
	if tc.TopicArn != qSet.TopicARN {
		t.Errorf("TopicArn = %q, want %q", tc.TopicArn, qSet.TopicARN)
	}
}
