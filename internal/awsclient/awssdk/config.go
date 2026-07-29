// Package awssdk adapts internal/awsclient's Weir-owned S3Client, SNSClient
// and SQSClient interfaces to the real aws-sdk-go-v2 clients (WR-017).
//
// Every adapter method here is mechanical field-mapping between Weir's own
// input/output types and the corresponding aws-sdk-go-v2 request/response
// types — no business logic lives in this package.
//
// The same binary runs unmodified against LocalStack or real AWS: Config's
// EndpointURL, when set, overrides the SDK's default endpoint resolution;
// when empty, the SDK resolves the real AWS endpoint (and, on EKS, picks up
// IRSA credentials transparently). This is the only place in the codebase
// that branches on "is an endpoint override configured" — nothing else
// branches on local vs. real AWS.
package awssdk

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Config carries the explicit settings needed to build real AWS clients.
// It intentionally does not read environment variables itself: callers
// (cmd/worker, cmd/manager, ...) read AWS_REGION/AWS_ENDPOINT_URL and
// construct a Config from them, keeping this package pure and testable.
type Config struct {
	// Region is required; NewClients fails fast if it is empty.
	Region string

	// EndpointURL overrides the SDK's default endpoint resolution when
	// non-empty (LocalStack locally). Left empty, endpoint resolution
	// defers entirely to the SDK's own chain — the real AWS endpoint for
	// Region, unless AWS_ENDPOINT_URL or a per-service
	// AWS_ENDPOINT_URL_<SERVICE> variable is already set in the
	// environment.
	EndpointURL string
}

// validate reports an error if cfg is missing required fields.
func (c Config) validate() error {
	if c.Region == "" {
		return fmt.Errorf("awssdk: Config.Region is required")
	}
	return nil
}

// applyS3Options overrides the S3 client's base endpoint and forces
// path-style addressing when EndpointURL is set. Path-style is required
// together with the base endpoint override: bucket-scoped operations
// resolve to "<bucket>.<endpoint-host>" by default, which does not resolve
// against LocalStack.
func (c Config) applyS3Options(o *s3.Options) {
	if c.EndpointURL != "" {
		o.BaseEndpoint = aws.String(c.EndpointURL)
		o.UsePathStyle = true
	}
}

// applySNSOptions overrides the SNS client's base endpoint when
// EndpointURL is set. SNS has no path-style concept; only BaseEndpoint
// applies.
func (c Config) applySNSOptions(o *sns.Options) {
	if c.EndpointURL != "" {
		o.BaseEndpoint = aws.String(c.EndpointURL)
	}
}

// applySQSOptions overrides the SQS client's base endpoint when
// EndpointURL is set. SQS has no path-style concept; only BaseEndpoint
// applies.
func (c Config) applySQSOptions(o *sqs.Options) {
	if c.EndpointURL != "" {
		o.BaseEndpoint = aws.String(c.EndpointURL)
	}
}

// Clients bundles the three real aws-sdk-go-v2-backed adapters Weir needs.
type Clients struct {
	S3  *S3
	SNS *SNS
	SQS *SQS
}

// NewClients loads the AWS SDK default configuration for cfg.Region and
// builds the S3, SNS and SQS adapters from it, applying cfg's endpoint
// override (if any) identically to all three.
func NewClients(ctx context.Context, cfg Config) (*Clients, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("awssdk: load default AWS config: %w", err)
	}

	return &Clients{
		S3:  &S3{client: s3.NewFromConfig(awsCfg, cfg.applyS3Options)},
		SNS: &SNS{client: sns.NewFromConfig(awsCfg, cfg.applySNSOptions)},
		SQS: &SQS{client: sqs.NewFromConfig(awsCfg, cfg.applySQSOptions)},
	}, nil
}
