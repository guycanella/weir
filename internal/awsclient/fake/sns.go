package fake

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/guycanella/weir/internal/awsclient"
)

// Method name constants for SNS's InjectError, so callers get typo-safety
// and IDE completion instead of a bare string.
const (
	SNSMethodCreateTopic              = "CreateTopic"
	SNSMethodSubscribe                = "Subscribe"
	SNSMethodListSubscriptionsByTopic = "ListSubscriptionsByTopic"
	SNSMethodGetTopicAttributes       = "GetTopicAttributes"
	SNSMethodSetTopicAttributes       = "SetTopicAttributes"
)

// ErrInvalidNextToken is returned by ListSubscriptionsByTopic when the
// given NextToken doesn't decode to a valid offset into the topic's
// subscriptions (bad input, or out of range), mirroring the
// InvalidParameter/InvalidNextToken error real SNS returns for a
// malformed or stale pagination token.
var ErrInvalidNextToken = errors.New("fake sns: invalid next token")

// defaultListPageSize mirrors real SNS's ListSubscriptionsByTopic page
// size when a SNS fake's ListPageSize is left unset.
const defaultListPageSize = 100

// SNS is an in-memory fake of awsclient.SNSClient, safe for concurrent use.
//
// Topics records every created topic's ARN, keyed by name.
// Subscriptions records every subscription created, keyed by topic ARN.
// TopicAttributes records attributes set via SetTopicAttributes, keyed by topic ARN.
//
// ListPageSize caps how many subscriptions ListSubscriptionsByTopic
// returns per call, mirroring real SNS's pagination; zero/unset defaults
// to defaultListPageSize (100), matching real SNS. Tests can set it lower
// to exercise multi-page traversal without creating 100 real
// subscription records.
type SNS struct {
	mu sync.Mutex

	Topics          map[string]string
	Subscriptions   map[string][]awsclient.Subscription
	TopicAttributes map[string]map[string]string

	ListPageSize int

	nextSubID int
	errs      *errorQueue
}

// NewSNS returns an empty, ready-to-use SNS fake.
func NewSNS() *SNS {
	return &SNS{
		Topics:          make(map[string]string),
		Subscriptions:   make(map[string][]awsclient.Subscription),
		TopicAttributes: make(map[string]map[string]string),
		errs:            newErrorQueue(),
	}
}

// InjectError arranges for the next n calls (n < 1 is treated as 1) to the
// named method (one of the SNSMethod* constants) to return err instead of
// performing their normal work.
func (f *SNS) InjectError(method string, err error, n int) {
	f.errs.push(method, err, n)
}

var _ awsclient.SNSClient = (*SNS)(nil)

// CreateTopic implements awsclient.SNSClient. Like real SNS, creating a
// topic with a name that already exists is a no-op that returns the
// existing topic's ARN.
func (f *SNS) CreateTopic(_ context.Context, in awsclient.CreateTopicInput) (awsclient.CreateTopicOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SNSMethodCreateTopic); err != nil {
		return awsclient.CreateTopicOutput{}, err
	}

	if arn, ok := f.Topics[in.Name]; ok {
		return awsclient.CreateTopicOutput{TopicArn: arn}, nil
	}

	arn := fmt.Sprintf("arn:aws:sns:local:000000000000:%s", in.Name)
	f.Topics[in.Name] = arn
	return awsclient.CreateTopicOutput{TopicArn: arn}, nil
}

// Subscribe implements awsclient.SNSClient. Unlike real SNS, this fake does
// NOT de-duplicate an identical (TopicArn, Protocol, Endpoint) triple: each
// call records a new subscription. This is deliberate — it forces callers
// to list existing subscriptions first (via ListSubscriptionsByTopic)
// rather than relying on provider-side de-dup.
func (f *SNS) Subscribe(_ context.Context, in awsclient.SubscribeInput) (awsclient.SubscribeOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SNSMethodSubscribe); err != nil {
		return awsclient.SubscribeOutput{}, err
	}

	f.nextSubID++
	sub := awsclient.Subscription{
		SubscriptionArn: fmt.Sprintf("%s:%d", in.TopicArn, f.nextSubID),
		Owner:           "000000000000",
		Protocol:        in.Protocol,
		Endpoint:        in.Endpoint,
		TopicArn:        in.TopicArn,
	}
	f.Subscriptions[in.TopicArn] = append(f.Subscriptions[in.TopicArn], sub)

	return awsclient.SubscribeOutput{SubscriptionArn: sub.SubscriptionArn}, nil
}

// ListSubscriptionsByTopic implements awsclient.SNSClient, returning at
// most ListPageSize subscriptions per call (defaulting to
// defaultListPageSize when unset), mirroring real SNS's pagination.
func (f *SNS) ListSubscriptionsByTopic(_ context.Context, in awsclient.ListSubscriptionsByTopicInput) (awsclient.ListSubscriptionsByTopicOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SNSMethodListSubscriptionsByTopic); err != nil {
		return awsclient.ListSubscriptionsByTopicOutput{}, err
	}

	all := f.Subscriptions[in.TopicArn]

	offset := 0
	if in.NextToken != "" {
		parsed, err := strconv.Atoi(in.NextToken)
		if err != nil || parsed < 0 || parsed > len(all) {
			return awsclient.ListSubscriptionsByTopicOutput{}, ErrInvalidNextToken
		}
		offset = parsed
	}

	pageSize := f.ListPageSize
	if pageSize <= 0 {
		pageSize = defaultListPageSize
	}

	end := offset + pageSize
	if end > len(all) {
		end = len(all)
	}

	page := append([]awsclient.Subscription(nil), all[offset:end]...)

	var nextToken string
	if end < len(all) {
		nextToken = strconv.Itoa(end)
	}

	return awsclient.ListSubscriptionsByTopicOutput{Subscriptions: page, NextToken: nextToken}, nil
}

// GetTopicAttributes implements awsclient.SNSClient.
func (f *SNS) GetTopicAttributes(_ context.Context, in awsclient.GetTopicAttributesInput) (awsclient.GetTopicAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SNSMethodGetTopicAttributes); err != nil {
		return awsclient.GetTopicAttributesOutput{}, err
	}

	stored := f.TopicAttributes[in.TopicArn]
	attributes := make(map[string]string, len(stored))
	for name, value := range stored {
		attributes[name] = value
	}
	return awsclient.GetTopicAttributesOutput{Attributes: attributes}, nil
}

// SetTopicAttributes implements awsclient.SNSClient.
func (f *SNS) SetTopicAttributes(_ context.Context, in awsclient.SetTopicAttributesInput) (awsclient.SetTopicAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.errs.next(SNSMethodSetTopicAttributes); err != nil {
		return awsclient.SetTopicAttributesOutput{}, err
	}

	if _, ok := f.TopicAttributes[in.TopicArn]; !ok {
		f.TopicAttributes[in.TopicArn] = make(map[string]string)
	}
	f.TopicAttributes[in.TopicArn][in.AttributeName] = in.AttributeValue
	return awsclient.SetTopicAttributesOutput{}, nil
}
