package provisioner_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
	"github.com/guycanella/weir/internal/provisioner"
)

func TestEnsureBucketNotification_Validation(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	t.Run("missing bucket", func(t *testing.T) {
		err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, provisioner.BucketNotificationConfig{
			TopicARN: "arn:aws:sns:us-east-1:123456789012:my-topic",
		})
		if err == nil {
			t.Fatal("expected error when Bucket is missing, got nil")
		}
	})

	t.Run("missing topic ARN", func(t *testing.T) {
		err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, provisioner.BucketNotificationConfig{
			Bucket: "my-bucket",
		})
		if err == nil {
			t.Fatal("expected error when TopicARN is missing, got nil")
		}
	})
}

func TestEnsureBucketNotification_SetsSNSTopicPolicy(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	cfg := provisioner.BucketNotificationConfig{
		Bucket:   "my-bucket",
		TopicARN: "arn:aws:sns:us-east-1:123456789012:my-topic",
		Prefix:   "incoming/",
	}

	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfg); err != nil {
		t.Fatalf("EnsureBucketNotification failed: %v", err)
	}

	rawPolicy := snsFake.TopicAttributes[cfg.TopicARN]["Policy"]
	if rawPolicy == "" {
		t.Fatal("SNS topic Policy attribute was not set by EnsureBucketNotification")
	}

	var doc struct {
		Statement []struct {
			Principal map[string]string            `json:"Principal"`
			Action    string                       `json:"Action"`
			Resource  string                       `json:"Resource"`
			Condition map[string]map[string]string `json:"Condition"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(rawPolicy), &doc); err != nil {
		t.Fatalf("failed to parse Policy JSON %q: %v", rawPolicy, err)
	}
	if len(doc.Statement) == 0 {
		t.Fatal("Policy statement is empty")
	}

	stmt := doc.Statement[0]
	if got := stmt.Principal["Service"]; got != "s3.amazonaws.com" {
		t.Errorf("Principal.Service = %q, want \"s3.amazonaws.com\"", got)
	}
	if stmt.Action != "sns:Publish" {
		t.Errorf("Action = %q, want \"sns:Publish\"", stmt.Action)
	}
	if stmt.Resource != cfg.TopicARN {
		t.Errorf("Resource = %q, want topic ARN %q", stmt.Resource, cfg.TopicARN)
	}
	expectedSourceArn := "arn:aws:s3:::my-bucket"
	if got := stmt.Condition["ArnEquals"]["aws:SourceArn"]; got != expectedSourceArn {
		t.Errorf("Condition aws:SourceArn = %q, want %q", got, expectedSourceArn)
	}
}

func TestEnsureBucketNotification_MergesSNSTopicPolicy(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()
	topicARN := "arn:aws:sns:us-east-1:123456789012:shared-topic"
	snsFake.TopicAttributes[topicARN] = map[string]string{
		"Policy": `{
			"Version":"2012-10-17",
			"Id":"existing-policy",
			"Statement":[{
				"Sid":"ExternalPermission",
				"Effect":"Allow",
				"Principal":{"AWS":"arn:aws:iam::123456789012:root"},
				"Action":"sns:Subscribe",
				"Resource":"arn:aws:sns:us-east-1:123456789012:shared-topic"
			}]
		}`,
	}

	for _, bucket := range []string{"bucket-one", "bucket-two", "bucket-one"} {
		err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, provisioner.BucketNotificationConfig{
			Bucket:   bucket,
			TopicARN: topicARN,
		})
		if err != nil {
			t.Fatalf("EnsureBucketNotification(%q): %v", bucket, err)
		}
	}

	var doc struct {
		ID        string `json:"Id"`
		Statement []struct {
			Sid       string                       `json:"Sid"`
			Principal map[string]string            `json:"Principal"`
			Condition map[string]map[string]string `json:"Condition"`
		} `json:"Statement"`
	}
	rawPolicy := snsFake.TopicAttributes[topicARN]["Policy"]
	if err := json.Unmarshal([]byte(rawPolicy), &doc); err != nil {
		t.Fatalf("parse merged policy %q: %v", rawPolicy, err)
	}
	if doc.ID != "existing-policy" {
		t.Errorf("existing top-level policy Id = %q, want existing-policy", doc.ID)
	}
	if len(doc.Statement) != 3 {
		t.Fatalf("merged policy has %d statements, want external + two bucket permissions", len(doc.Statement))
	}

	sourceARNs := map[string]bool{}
	externalPreserved := false
	for _, statement := range doc.Statement {
		if statement.Sid == "ExternalPermission" &&
			statement.Principal["AWS"] == "arn:aws:iam::123456789012:root" {
			externalPreserved = true
		}
		if sourceARN := statement.Condition["ArnEquals"]["aws:SourceArn"]; sourceARN != "" {
			sourceARNs[sourceARN] = true
		}
	}
	if !externalPreserved {
		t.Error("pre-existing external policy statement was not preserved")
	}
	for _, want := range []string{"arn:aws:s3:::bucket-one", "arn:aws:s3:::bucket-two"} {
		if !sourceARNs[want] {
			t.Errorf("merged policy lacks S3 permission for %q", want)
		}
	}
}

func TestEnsureBucketNotification_CreatesNewNotification(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	cfg := provisioner.BucketNotificationConfig{
		Bucket:   "test-bucket",
		TopicARN: "arn:aws:sns:us-east-1:123456789012:test-topic",
		Prefix:   "uploads/",
		Suffix:   ".json",
	}

	err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("unexpected error getting notification config: %v", err)
	}

	if len(out.Configuration.TopicConfigurations) != 1 {
		t.Fatalf("expected 1 TopicConfiguration, got %d", len(out.Configuration.TopicConfigurations))
	}

	tc := out.Configuration.TopicConfigurations[0]
	if !strings.HasPrefix(tc.ID, "weir-sns-") || len(tc.ID) != len("weir-sns-")+32 {
		t.Errorf("ID = %q, want deterministic weir-sns- prefix plus 128-bit hex digest", tc.ID)
	}
	if tc.TopicArn != cfg.TopicARN {
		t.Errorf("TopicArn = %q, want %q", tc.TopicArn, cfg.TopicARN)
	}
	if len(tc.Events) != 1 || tc.Events[0] != "s3:ObjectCreated:*" {
		t.Errorf("Events = %v, want [s3:ObjectCreated:*]", tc.Events)
	}
	if tc.Filter == nil || tc.Filter.Prefix != "uploads/" || tc.Filter.Suffix != ".json" {
		t.Errorf("Filter = %+v, want Prefix=uploads/ Suffix=.json", tc.Filter)
	}
}

func TestEnsureBucketNotification_Idempotent(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	cfg := provisioner.BucketNotificationConfig{
		Bucket:   "test-bucket",
		TopicARN: "arn:aws:sns:us-east-1:123456789012:test-topic",
		Events:   []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
		ID:       "custom-id",
	}

	// First call provisions notification
	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfg); err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	firstOut, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration after first call failed: %v", err)
	}

	// Second call should be a no-op
	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfg); err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	secondOut, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("error reading notification config: %v", err)
	}

	if len(firstOut.Configuration.TopicConfigurations) != len(secondOut.Configuration.TopicConfigurations) {
		t.Errorf("TopicConfigurations count changed on 2nd call: first %d, second %d",
			len(firstOut.Configuration.TopicConfigurations), len(secondOut.Configuration.TopicConfigurations))
	}
	if firstOut.Configuration.TopicConfigurations[0].ID != secondOut.Configuration.TopicConfigurations[0].ID {
		t.Errorf("ID changed on 2nd call: first %q, second %q",
			firstOut.Configuration.TopicConfigurations[0].ID, secondOut.Configuration.TopicConfigurations[0].ID)
	}
}

func TestEnsureBucketNotification_UpdatesExistingDiffers(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	cfgInitial := provisioner.BucketNotificationConfig{
		Bucket:   "test-bucket",
		TopicARN: "arn:aws:sns:us-east-1:123456789012:test-topic",
		ID:       "weir-notification-1",
		Events:   []string{"s3:ObjectCreated:*"},
		Prefix:   "old/",
	}

	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfgInitial); err != nil {
		t.Fatalf("initial ensure failed: %v", err)
	}

	cfgUpdated := provisioner.BucketNotificationConfig{
		Bucket:   "test-bucket",
		TopicARN: "arn:aws:sns:us-east-1:123456789012:test-topic",
		ID:       "weir-notification-1",
		Events:   []string{"s3:ObjectCreated:*"},
		Prefix:   "new/",
	}

	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfgUpdated); err != nil {
		t.Fatalf("updated ensure failed: %v", err)
	}

	out, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	if len(out.Configuration.TopicConfigurations) != 1 {
		t.Fatalf("expected 1 TopicConfiguration, got %d", len(out.Configuration.TopicConfigurations))
	}
	tc := out.Configuration.TopicConfigurations[0]
	if tc.Filter == nil || tc.Filter.Prefix != "new/" {
		t.Errorf("Filter = %+v, want Prefix=new/", tc.Filter)
	}
}

func TestEnsureBucketNotification_PreservesExistingDestinations(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	// Seed pre-existing queue, lambda, eventbridge, and other topic configuration
	_, err := s3Fake.PutBucketNotificationConfiguration(ctx, awsclient.PutBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
		Configuration: awsclient.NotificationConfiguration{
			TopicConfigurations: []awsclient.TopicConfiguration{
				{
					ID:       "existing-topic-id",
					TopicArn: "arn:aws:sns:us-east-1:123456789012:other-topic",
					Events:   []string{"s3:ObjectCreated:*"},
				},
			},
			QueueConfigurations: []awsclient.QueueConfiguration{
				{
					ID:       "existing-queue-id",
					QueueArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
					Events:   []string{"s3:ObjectRemoved:*"},
				},
			},
			LambdaFunctionConfigurations: []awsclient.LambdaFunctionConfiguration{
				{
					ID:                "existing-lambda-id",
					LambdaFunctionArn: "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
					Events:            []string{"s3:ObjectCreated:*"},
				},
			},
			EventBridgeConfiguration: &awsclient.EventBridgeConfiguration{},
		},
	})
	if err != nil {
		t.Fatalf("failed to seed bucket configuration: %v", err)
	}

	cfg := provisioner.BucketNotificationConfig{
		Bucket:   "test-bucket",
		TopicARN: "arn:aws:sns:us-east-1:123456789012:new-weir-topic",
	}

	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfg); err != nil {
		t.Fatalf("EnsureBucketNotification failed: %v", err)
	}

	out, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("error reading notification config: %v", err)
	}

	c := out.Configuration
	if len(c.TopicConfigurations) != 2 {
		t.Fatalf("expected 2 TopicConfigurations, got %d", len(c.TopicConfigurations))
	}
	if c.TopicConfigurations[0].ID != "existing-topic-id" {
		t.Errorf("TopicConfigurations[0].ID = %q, want existing-topic-id", c.TopicConfigurations[0].ID)
	}
	if !strings.HasPrefix(c.TopicConfigurations[1].ID, "weir-sns-") {
		t.Errorf("TopicConfigurations[1].ID = %q, want a Weir-managed ID", c.TopicConfigurations[1].ID)
	}

	if len(c.QueueConfigurations) != 1 || c.QueueConfigurations[0].ID != "existing-queue-id" {
		t.Errorf("QueueConfigurations lost or modified: %v", c.QueueConfigurations)
	}
	if len(c.LambdaFunctionConfigurations) != 1 || c.LambdaFunctionConfigurations[0].ID != "existing-lambda-id" {
		t.Errorf("LambdaFunctionConfigurations lost or modified: %v", c.LambdaFunctionConfigurations)
	}
	if c.EventBridgeConfiguration == nil {
		t.Error("EventBridgeConfiguration was lost")
	}
}

func TestEnsureBucketNotification_MatchesByIDPrecedenceOverTopicARN(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	// Seed pre-existing topic configurations:
	// tc[0] has ID "weir-target" and TopicArn "arn:aws:sns:...:shared-topic"
	// tc[1] has ID "third-party" and TopicArn "arn:aws:sns:...:shared-topic"
	_, err := s3Fake.PutBucketNotificationConfiguration(ctx, awsclient.PutBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
		Configuration: awsclient.NotificationConfiguration{
			TopicConfigurations: []awsclient.TopicConfiguration{
				{
					ID:       "weir-target",
					TopicArn: "arn:aws:sns:us-east-1:123456789012:shared-topic",
					Events:   []string{"s3:ObjectCreated:*"},
					Filter:   &awsclient.NotificationFilter{Prefix: "old-prefix/"},
				},
				{
					ID:       "third-party",
					TopicArn: "arn:aws:sns:us-east-1:123456789012:shared-topic",
					Events:   []string{"s3:ObjectRemoved:*"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to seed bucket configuration: %v", err)
	}

	// Calling EnsureBucketNotification with ID "weir-target" must update tc[0] by ID,
	// NOT overwrite tc[1] or create a duplicate.
	cfg := provisioner.BucketNotificationConfig{
		Bucket:   "test-bucket",
		TopicARN: "arn:aws:sns:us-east-1:123456789012:shared-topic",
		ID:       "weir-target",
		Prefix:   "new-prefix/",
	}

	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfg); err != nil {
		t.Fatalf("EnsureBucketNotification failed: %v", err)
	}

	out, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("error reading notification config: %v", err)
	}

	configs := out.Configuration.TopicConfigurations
	if len(configs) != 2 {
		t.Fatalf("expected 2 TopicConfigurations, got %d", len(configs))
	}
	if configs[0].ID != "weir-target" || configs[0].Filter.Prefix != "new-prefix/" {
		t.Errorf("configs[0] = %+v, want ID weir-target with Prefix new-prefix/", configs[0])
	}
	if configs[1].ID != "third-party" {
		t.Errorf("configs[1] = %+v, third-party entry was overwritten or modified!", configs[1])
	}
}

func TestEnsureBucketNotification_FallbackMatchesUnIDedEntry(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	topicARN := "arn:aws:sns:us-east-1:123456789012:legacy-topic"
	_, err := s3Fake.PutBucketNotificationConfiguration(ctx, awsclient.PutBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
		Configuration: awsclient.NotificationConfiguration{
			TopicConfigurations: []awsclient.TopicConfiguration{
				{
					// Empty ID — simulates a notification entry created outside Weir (CLI, console)
					TopicArn: topicARN,
					Events:   []string{"s3:ObjectCreated:*"},
					Filter:   &awsclient.NotificationFilter{Prefix: "legacy/"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	cfg := provisioner.BucketNotificationConfig{
		Bucket:   "test-bucket",
		TopicARN: topicARN,
		Prefix:   "legacy/",
	}

	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, cfg); err != nil {
		t.Fatalf("EnsureBucketNotification failed: %v", err)
	}

	out, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("get config failed: %v", err)
	}

	if len(out.Configuration.TopicConfigurations) != 1 {
		t.Fatalf("expected fallback match to update in place, got %d configs", len(out.Configuration.TopicConfigurations))
	}
	tc := out.Configuration.TopicConfigurations[0]
	if tc.ID == "" {
		t.Error("expected fallback match to replace the legacy entry with a managed ID")
	}
	if tc.TopicArn != topicARN || tc.Filter == nil || tc.Filter.Prefix != "legacy/" {
		t.Errorf("unexpected configuration state after fallback update: %+v", tc)
	}
}

func TestEnsureBucketNotification_MultiplePipelinesSameBucket(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()

	bucket := "shared-bucket"

	pipe1 := provisioner.BucketNotificationConfig{
		Bucket:   bucket,
		TopicARN: "arn:aws:sns:us-east-1:123456789012:topic-1",
		Prefix:   "orders/",
	}
	pipe2 := provisioner.BucketNotificationConfig{
		Bucket:   bucket,
		TopicARN: "arn:aws:sns:us-east-1:123456789012:topic-2",
		Prefix:   "reports/",
	}

	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, pipe1); err != nil {
		t.Fatalf("pipe1 EnsureBucketNotification failed: %v", err)
	}
	if err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, pipe2); err != nil {
		t.Fatalf("pipe2 EnsureBucketNotification failed: %v", err)
	}

	out, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	configs := out.Configuration.TopicConfigurations
	if len(configs) != 2 {
		t.Fatalf("expected 2 TopicConfigurations for multiple pipelines on same bucket, got %d", len(configs))
	}
	if configs[0].Filter.Prefix != "orders/" {
		t.Errorf("configs[0].Filter.Prefix = %q, want orders/", configs[0].Filter.Prefix)
	}
	if configs[1].Filter.Prefix != "reports/" {
		t.Errorf("configs[1].Filter.Prefix = %q, want reports/", configs[1].Filter.Prefix)
	}
}

func TestEnsureBucketNotification_DerivedIDsDoNotCollideAfterSlugNormalization(t *testing.T) {
	ctx := t.Context()
	s3Fake := fake.NewS3()
	snsFake := fake.NewSNS()
	topicARN := "arn:aws:sns:us-east-1:123456789012:shared-topic"

	for _, prefix := range []string{"a/b", "a-b"} {
		err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, provisioner.BucketNotificationConfig{
			Bucket:   "shared-bucket",
			TopicARN: topicARN,
			Prefix:   prefix,
		})
		if err != nil {
			t.Fatalf("EnsureBucketNotification(prefix=%q): %v", prefix, err)
		}
	}

	out, err := s3Fake.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: "shared-bucket",
	})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration: %v", err)
	}
	configs := out.Configuration.TopicConfigurations
	if len(configs) != 2 {
		t.Fatalf("got %d topic configurations, want both distinct filters", len(configs))
	}
	if configs[0].ID == configs[1].ID {
		t.Errorf("derived IDs collided for distinct filters: %q", configs[0].ID)
	}
}

func TestEnsureBucketNotification_ErrorPropagation(t *testing.T) {
	ctx := t.Context()

	t.Run("SNS policy error", func(t *testing.T) {
		s3Fake := fake.NewS3()
		snsFake := fake.NewSNS()
		snsFake.InjectError(fake.SNSMethodSetTopicAttributes, fmt.Errorf("sns policy error"), 1)

		err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, provisioner.BucketNotificationConfig{
			Bucket:   "b",
			TopicARN: "arn:aws:sns:us-east-1:123456789012:t",
		})
		if err == nil {
			t.Fatal("expected SNS policy error, got nil")
		}
	})

	t.Run("SNS get attributes error", func(t *testing.T) {
		s3Fake := fake.NewS3()
		snsFake := fake.NewSNS()
		snsFake.InjectError(fake.SNSMethodGetTopicAttributes, fmt.Errorf("sns attributes error"), 1)

		err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, provisioner.BucketNotificationConfig{
			Bucket:   "b",
			TopicARN: "arn:aws:sns:us-east-1:123456789012:t",
		})
		if err == nil {
			t.Fatal("expected SNS GetTopicAttributes error, got nil")
		}
	})

	t.Run("Get error", func(t *testing.T) {
		s3Fake := fake.NewS3()
		snsFake := fake.NewSNS()
		s3Fake.InjectError(fake.S3MethodGetBucketNotificationConfiguration, fmt.Errorf("get error"), 1)

		err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, provisioner.BucketNotificationConfig{
			Bucket:   "b",
			TopicARN: "arn:aws:sns:us-east-1:123456789012:t",
		})
		if err == nil {
			t.Fatal("expected get error, got nil")
		}
	})

	t.Run("Put error", func(t *testing.T) {
		s3Fake := fake.NewS3()
		snsFake := fake.NewSNS()
		s3Fake.InjectError(fake.S3MethodPutBucketNotificationConfiguration, fmt.Errorf("put error"), 1)

		err := provisioner.EnsureBucketNotification(ctx, s3Fake, snsFake, provisioner.BucketNotificationConfig{
			Bucket:   "b",
			TopicARN: "arn:aws:sns:us-east-1:123456789012:t",
		})
		if err == nil {
			t.Fatal("expected put error, got nil")
		}
	})
}
