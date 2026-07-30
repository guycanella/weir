package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/guycanella/weir/internal/awsclient"
)

// BucketNotificationConfig parameterises an EnsureBucketNotification call.
type BucketNotificationConfig struct {
	// Bucket is the target S3 bucket name (required).
	Bucket string

	// TopicARN is the target SNS topic ARN (required).
	TopicARN string

	// Events is the list of S3 events to subscribe to.
	// If empty, defaults to []string{"s3:ObjectCreated:*"}.
	Events []string

	// Prefix is an optional object key prefix filter.
	Prefix string

	// Suffix is an optional object key suffix filter.
	Suffix string

	// ID is an optional configuration ID within S3 bucket notification config.
	// If empty, derived from TopicARN, Prefix, and Suffix to ensure uniqueness
	// across pipelines watching different prefixes or topics on the same bucket.
	ID string
}

// EnsureBucketNotification idempotently configures an S3 bucket to send event
// notifications to an SNS topic.
//
// Reconciles the SNS topic policy and S3 bucket notification configuration:
//  1. Applies an IAM resource policy to the SNS topic permitting s3.amazonaws.com
//     to call sns:Publish for the bucket (required by real AWS S3 validation).
//  2. Reads the bucket's notification configuration via GetBucketNotificationConfiguration.
//  3. Reconciles TopicConfigurations by ID (or un-ID'd TopicArn fallback).
//  4. Preserves all other existing notification configurations (queue, lambda, eventbridge,
//     and non-matching topic configs).
//  5. Writes updated configuration via PutBucketNotificationConfiguration if changes are needed.
func EnsureBucketNotification(ctx context.Context, s3 awsclient.S3Client, sns awsclient.SNSClient, cfg BucketNotificationConfig) error {
	if cfg.Bucket == "" {
		return fmt.Errorf("provisioner: Bucket is required")
	}
	if cfg.TopicARN == "" {
		return fmt.Errorf("provisioner: TopicARN is required")
	}

	// ── 1. Ensure SNS Topic Policy ───────────────────────────────────────────
	// Real AWS S3 validates destination permissions during PutBucketNotificationConfiguration
	// by sending a test notification. The SNS topic must allow s3.amazonaws.com sns:Publish
	// restricted by aws:SourceArn = arn:aws:s3:::bucket.
	topicAttrs, err := sns.GetTopicAttributes(ctx, awsclient.GetTopicAttributesInput{
		TopicArn: cfg.TopicARN,
	})
	if err != nil {
		return fmt.Errorf("provisioner: get SNS topic attributes for %q: %w", cfg.TopicARN, err)
	}
	policy, err := mergeS3PublishPermission(topicAttrs.Attributes["Policy"], cfg.TopicARN, cfg.Bucket)
	if err != nil {
		return fmt.Errorf("provisioner: merge SNS topic policy: %w", err)
	}
	if policy != topicAttrs.Attributes["Policy"] {
		if _, err := sns.SetTopicAttributes(ctx, awsclient.SetTopicAttributesInput{
			TopicArn:       cfg.TopicARN,
			AttributeName:  "Policy",
			AttributeValue: policy,
		}); err != nil {
			return fmt.Errorf("provisioner: set SNS topic policy for %q: %w", cfg.TopicARN, err)
		}
	}

	// ── 2. Derive stable ID and desired configuration ─────────────────────────
	id := cfg.ID
	if id == "" {
		id = deriveNotificationID(cfg.TopicARN, cfg.Prefix, cfg.Suffix)
	}

	events := cfg.Events
	if len(events) == 0 {
		events = []string{"s3:ObjectCreated:*"}
	}

	var filter *awsclient.NotificationFilter
	if cfg.Prefix != "" || cfg.Suffix != "" {
		filter = &awsclient.NotificationFilter{
			Prefix: cfg.Prefix,
			Suffix: cfg.Suffix,
		}
	}

	desired := awsclient.TopicConfiguration{
		ID:       id,
		TopicArn: cfg.TopicARN,
		Events:   events,
		Filter:   filter,
	}

	// ── 3. Reconcile Bucket Notification Configuration ───────────────────────
	getOut, err := s3.GetBucketNotificationConfiguration(ctx, awsclient.GetBucketNotificationConfigurationInput{
		Bucket: cfg.Bucket,
	})
	if err != nil {
		return fmt.Errorf("provisioner: get bucket notification configuration for %q: %w", cfg.Bucket, err)
	}

	currentConfig := getOut.Configuration
	matchIdx := -1

	// Pass 1: Match by exact ID precedence
	for i, tc := range currentConfig.TopicConfigurations {
		if tc.ID == id {
			matchIdx = i
			break
		}
	}

	// Pass 2: Fallback match ONLY for un-ID'd entries matching TopicArn and Filter.
	// We MUST NOT overwrite an entry with a non-empty, different ID, as that belongs
	// to another pipeline or external system.
	if matchIdx == -1 {
		for i, tc := range currentConfig.TopicConfigurations {
			if tc.ID == "" && tc.TopicArn == cfg.TopicARN && filterEqual(tc.Filter, desired.Filter) {
				matchIdx = i
				break
			}
		}
	}

	if matchIdx != -1 {
		if topicConfigEqual(currentConfig.TopicConfigurations[matchIdx], desired) {
			return nil
		}
		currentConfig.TopicConfigurations[matchIdx] = desired
	} else {
		currentConfig.TopicConfigurations = append(currentConfig.TopicConfigurations, desired)
	}

	_, err = s3.PutBucketNotificationConfiguration(ctx, awsclient.PutBucketNotificationConfigurationInput{
		Bucket:        cfg.Bucket,
		Configuration: currentConfig,
	})
	if err != nil {
		return fmt.Errorf("provisioner: put bucket notification configuration for %q: %w", cfg.Bucket, err)
	}

	return nil
}

func deriveNotificationID(topicARN, prefix, suffix string) string {
	sum := sha256.Sum256([]byte(topicARN + "\x00" + prefix + "\x00" + suffix))
	return fmt.Sprintf("weir-sns-%x", sum[:16])
}

type snsPolicyStatement struct {
	Sid       string                       `json:"Sid"`
	Effect    string                       `json:"Effect"`
	Principal map[string]string            `json:"Principal"`
	Action    string                       `json:"Action"`
	Resource  string                       `json:"Resource"`
	Condition map[string]map[string]string `json:"Condition"`
}

func mergeS3PublishPermission(existingPolicy, topicARN, bucketName string) (string, error) {
	bucketARN := fmt.Sprintf("arn:aws:s3:::%s", bucketName)
	sidHash := sha256.Sum256([]byte(bucketARN))
	managedSID := fmt.Sprintf("AllowS3BucketNotification-%x", sidHash[:8])
	managed := snsPolicyStatement{
		Sid:    managedSID,
		Effect: "Allow",
		Principal: map[string]string{
			"Service": "s3.amazonaws.com",
		},
		Action:   "sns:Publish",
		Resource: topicARN,
		Condition: map[string]map[string]string{
			"ArnEquals": {
				"aws:SourceArn": bucketARN,
			},
		},
	}
	managedJSON, err := json.Marshal(managed)
	if err != nil {
		return "", err
	}

	document := make(map[string]json.RawMessage)
	if existingPolicy != "" {
		if err := json.Unmarshal([]byte(existingPolicy), &document); err != nil {
			return "", fmt.Errorf("parse existing policy: %w", err)
		}
	}
	if _, ok := document["Version"]; !ok {
		document["Version"] = json.RawMessage(`"2012-10-17"`)
	}

	var statements []json.RawMessage
	if raw := document["Statement"]; len(raw) != 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &statements); err != nil {
			return "", fmt.Errorf("parse existing policy statements: %w", err)
		}
	}

	merged := make([]json.RawMessage, 0, len(statements)+1)
	for _, statement := range statements {
		var identity struct {
			Sid string `json:"Sid"`
		}
		if err := json.Unmarshal(statement, &identity); err != nil {
			return "", fmt.Errorf("parse existing policy statement: %w", err)
		}
		if identity.Sid != managedSID {
			merged = append(merged, statement)
		}
	}
	merged = append(merged, managedJSON)
	statementJSON, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	document["Statement"] = statementJSON

	policyJSON, err := json.Marshal(document)
	if err != nil {
		return "", err
	}
	return string(policyJSON), nil
}

func topicConfigEqual(a, b awsclient.TopicConfiguration) bool {
	if a.ID != b.ID {
		return false
	}
	if a.TopicArn != b.TopicArn {
		return false
	}
	if !eventsEqual(a.Events, b.Events) {
		return false
	}
	if !filterEqual(a.Filter, b.Filter) {
		return false
	}
	return true
}

func eventsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aSorted := append([]string(nil), a...)
	bSorted := append([]string(nil), b...)
	sort.Strings(aSorted)
	sort.Strings(bSorted)
	for i := range aSorted {
		if aSorted[i] != bSorted[i] {
			return false
		}
	}
	return true
}

func filterEqual(f1, f2 *awsclient.NotificationFilter) bool {
	var p1, s1 string
	if f1 != nil {
		p1 = f1.Prefix
		s1 = f1.Suffix
	}
	var p2, s2 string
	if f2 != nil {
		p2 = f2.Prefix
		s2 = f2.Suffix
	}
	return p1 == p2 && s1 == s2
}
