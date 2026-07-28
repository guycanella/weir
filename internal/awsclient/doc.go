// Package awsclient defines the small, mockable interfaces Weir uses to
// talk to S3, SNS and SQS (WR-016).
//
// The interfaces are deliberately decoupled from aws-sdk-go-v2: every
// input and output type here is Weir-owned, not an SDK request/response
// struct. Field names loosely mirror AWS's own naming (Bucket, Key,
// QueueUrl, TopicArn, ...) for familiarity, but nothing in this package
// imports the SDK. A later task (WR-017) adds aws-sdk-go-v2 as a
// dependency and writes a thin adapter that converts between these types
// and the real SDK's, so the same interfaces can be satisfied by a real
// client against LocalStack or AWS, or by the in-memory fakes in
// internal/awsclient/fake for unit tests.
//
// Every method takes a context.Context first and returns (Output, error),
// mirroring the SDK v2 call shape loosely so a future adapter's job is a
// mechanical field-by-field conversion.
package awsclient
