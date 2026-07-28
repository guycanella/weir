package fake_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
)

// --- CreateTopic ----------------------------------------------------------

// TestSNSCreateTopicIsIdempotentOnName is the property WR-018 leans on: its
// "ensure the topic exists" step runs on every reconcile, so calling
// CreateTopic repeatedly must converge on one topic with a stable ARN. Real SNS
// behaves this way, and a fake that appended a new topic per call would let a
// reconciler that creates duplicates look correct.
func TestSNSCreateTopicIsIdempotentOnName(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()

	first, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
	if err != nil {
		t.Fatalf("first CreateTopic returned error %v, want nil", err)
	}
	if first.TopicArn == "" {
		t.Fatal("CreateTopic returned an empty TopicArn, want a non-empty ARN")
	}

	// Several more reconciles.
	for i := 2; i <= 4; i++ {
		again, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
		if err != nil {
			t.Fatalf("CreateTopic call #%d returned error %v, want nil", i, err)
		}
		if again.TopicArn != first.TopicArn {
			t.Errorf("CreateTopic call #%d returned ARN %q, want the existing %q: creating a topic "+
				"whose name already exists must report the existing one", i, again.TopicArn, first.TopicArn)
		}
	}

	if got := len(f.Topics); got != 1 {
		t.Errorf("Topics holds %d entries after four calls with the same name, want 1: %v", got, f.Topics)
	}
	if got := f.Topics["weir-events"]; got != first.TopicArn {
		t.Errorf("Topics[%q] = %q, want %q", "weir-events", got, first.TopicArn)
	}
}

// TestSNSCreateTopicGivesDistinctNamesDistinctARNs: WR-018 provisions per
// pipeline, so two pipelines must not collide on one topic. The ARN is also
// asserted to end in the topic name, which is true of real SNS ARNs and is what
// makes a fake ARN readable in a failure message.
func TestSNSCreateTopicGivesDistinctNamesDistinctARNs(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()

	names := []string{"weir-events", "weir-events-2", "other"}
	arns := make(map[string]string, len(names))

	for _, name := range names {
		out, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: name})
		if err != nil {
			t.Fatalf("CreateTopic(%q) returned error %v, want nil", name, err)
		}
		if !strings.HasPrefix(out.TopicArn, "arn:aws:sns:") {
			t.Errorf("CreateTopic(%q) ARN = %q, want an arn:aws:sns: prefix", name, out.TopicArn)
		}
		if !strings.HasSuffix(out.TopicArn, ":"+name) {
			t.Errorf("CreateTopic(%q) ARN = %q, want it to end in %q", name, out.TopicArn, ":"+name)
		}
		if prev, dup := arns[out.TopicArn]; dup {
			t.Errorf("CreateTopic(%q) reused the ARN %q already issued to %q", name, out.TopicArn, prev)
		}
		arns[out.TopicArn] = name
	}

	if got, want := len(f.Topics), len(names); got != want {
		t.Errorf("Topics holds %d entries, want %d", got, want)
	}
}

// TestSNSCreateTopicRecordsNothingWhenItFails keeps the error path honest: a
// failed create must not leave a half-registered topic that a retry would then
// treat as already existing.
func TestSNSCreateTopicRecordsNothingWhenItFails(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()
	f.InjectError(fake.SNSMethodCreateTopic, errInjected, 1)

	out, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
	if !errors.Is(err, errInjected) {
		t.Fatalf("CreateTopic error = %v, want one matching errInjected", err)
	}
	if out.TopicArn != "" {
		t.Errorf("failed CreateTopic reported ARN %q, want the zero value", out.TopicArn)
	}
	if len(f.Topics) != 0 {
		t.Errorf("Topics = %v after a failed create, want it empty", f.Topics)
	}

	retried, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
	if err != nil {
		t.Fatalf("CreateTopic after the injected failure returned error %v, want nil", err)
	}
	if retried.TopicArn == "" {
		t.Error("the retried CreateTopic returned an empty ARN")
	}
}

// --- Subscribe / ListSubscriptionsByTopic ---------------------------------

// TestSNSSubscribeThenListShowsTheSubscription is the pair WR-018 uses to wire
// SNS -> SQS (ADR-001): every field the caller supplied has to come back out of
// the list, because the ensure logic compares protocol and endpoint to decide
// whether the subscription it wants already exists.
func TestSNSSubscribeThenListShowsTheSubscription(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()

	topic, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
	if err != nil {
		t.Fatalf("CreateTopic returned error %v, want nil", err)
	}
	topicArn := topic.TopicArn
	const queueArn = "arn:aws:sqs:local:000000000000:weir-work"

	sub, err := f.Subscribe(ctx, awsclient.SubscribeInput{
		TopicArn: topicArn, Protocol: "sqs", Endpoint: queueArn,
	})
	if err != nil {
		t.Fatalf("Subscribe returned error %v, want nil", err)
	}
	if sub.SubscriptionArn == "" {
		t.Fatal("Subscribe returned an empty SubscriptionArn, want a non-empty ARN")
	}

	list, err := f.ListSubscriptionsByTopic(ctx,
		awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic returned error %v, want nil", err)
	}
	if len(list.Subscriptions) != 1 {
		t.Fatalf("ListSubscriptionsByTopic returned %d subscriptions, want 1: %+v",
			len(list.Subscriptions), list.Subscriptions)
	}

	got := list.Subscriptions[0]
	if got.SubscriptionArn != sub.SubscriptionArn {
		t.Errorf("listed SubscriptionArn = %q, want the %q returned by Subscribe",
			got.SubscriptionArn, sub.SubscriptionArn)
	}
	if got.TopicArn != topicArn {
		t.Errorf("listed TopicArn = %q, want %q", got.TopicArn, topicArn)
	}
	if got.Protocol != "sqs" {
		t.Errorf("listed Protocol = %q, want %q", got.Protocol, "sqs")
	}
	if got.Endpoint != queueArn {
		t.Errorf("listed Endpoint = %q, want %q", got.Endpoint, queueArn)
	}
	if got.Owner == "" {
		t.Error("listed Owner is empty; real SNS always reports an account id, and code that logs or " +
			"compares it should not see a zero value from the fake")
	}
}

// TestSNSListSubscriptionsByTopicOnAnUnknownTopic pins the empty-and-nil-error
// result. WR-018's first reconcile lists before it subscribes, so this is the
// path taken on a brand-new topic; an error here would make "not subscribed
// yet" indistinguishable from "SNS is broken".
func TestSNSListSubscriptionsByTopicOnAnUnknownTopic(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()

	list, err := f.ListSubscriptionsByTopic(ctx,
		awsclient.ListSubscriptionsByTopicInput{TopicArn: "arn:aws:sns:local:000000000000:nope"})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic on an unknown topic returned error %v, want nil", err)
	}
	if len(list.Subscriptions) != 0 {
		t.Errorf("ListSubscriptionsByTopic on an unknown topic = %+v, want an empty list", list.Subscriptions)
	}
}

// TestSNSSubscribeIsNotDeduplicatedByEndpoint documents a DELIBERATE divergence
// from real SNS, and it matters to WR-018.
//
// Real SNS's Subscribe is idempotent for an identical (topic, protocol,
// endpoint) triple: it returns the existing subscription's ARN and creates
// nothing new. This fake instead records a second, distinct subscription. That
// is the pessimistic choice, and the useful one for a test double: an "ensure"
// implementation that subscribes unconditionally on every reconcile is a bug
// (it depends on a provider-side de-dup that is easy to lose, e.g. if the
// endpoint or protocol string ever varies), and against this fake that bug is
// visible as a growing subscription list rather than invisible.
//
// The contract this pins for WR-018: list first, subscribe only if absent.
func TestSNSSubscribeIsNotDeduplicatedByEndpoint(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()

	topic, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
	if err != nil {
		t.Fatalf("CreateTopic returned error %v, want nil", err)
	}
	topicArn := topic.TopicArn
	in := awsclient.SubscribeInput{
		TopicArn: topicArn, Protocol: "sqs", Endpoint: "arn:aws:sqs:local:000000000000:weir-work",
	}

	first, err := f.Subscribe(ctx, in)
	if err != nil {
		t.Fatalf("first Subscribe returned error %v, want nil", err)
	}
	second, err := f.Subscribe(ctx, in)
	if err != nil {
		t.Fatalf("second Subscribe returned error %v, want nil", err)
	}

	if first.SubscriptionArn == second.SubscriptionArn {
		t.Errorf("both Subscribe calls returned %q, want distinct ARNs: the fake does not de-duplicate "+
			"by endpoint, so a caller that subscribes without listing first is visibly wrong",
			first.SubscriptionArn)
	}

	list, err := f.ListSubscriptionsByTopic(ctx,
		awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic returned error %v, want nil", err)
	}
	if len(list.Subscriptions) != 2 {
		t.Errorf("the topic has %d subscriptions after two identical Subscribe calls, want 2",
			len(list.Subscriptions))
	}
}

// TestSNSSubscriptionsArePerTopicAndOrdered: subscriptions must be attributed
// to the topic they were made against (a single flat list keyed on nothing
// would pass a one-topic test and fail in production), and the order they were
// created in is preserved so a test can reason about "the first subscription".
// Subscription ARNs are globally unique, not per topic.
func TestSNSSubscriptionsArePerTopicAndOrdered(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()

	topicA, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "topic-a"})
	if err != nil {
		t.Fatalf("CreateTopic(topic-a) returned error %v, want nil", err)
	}
	topicB, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "topic-b"})
	if err != nil {
		t.Fatalf("CreateTopic(topic-b) returned error %v, want nil", err)
	}

	seen := map[string]bool{}
	subscribe := func(topicArn, endpoint string) {
		t.Helper()
		out, err := f.Subscribe(ctx, awsclient.SubscribeInput{
			TopicArn: topicArn, Protocol: "sqs", Endpoint: endpoint,
		})
		if err != nil {
			t.Fatalf("Subscribe(%q, %q) returned error %v, want nil", topicArn, endpoint, err)
		}
		if seen[out.SubscriptionArn] {
			t.Errorf("Subscribe(%q, %q) reused subscription ARN %q; subscription ARNs must be unique "+
				"across topics", topicArn, endpoint, out.SubscriptionArn)
		}
		seen[out.SubscriptionArn] = true
	}

	subscribe(topicA.TopicArn, "arn:sqs:a1")
	subscribe(topicB.TopicArn, "arn:sqs:b1")
	subscribe(topicA.TopicArn, "arn:sqs:a2")

	wantEndpoints := map[string][]string{
		topicA.TopicArn: {"arn:sqs:a1", "arn:sqs:a2"},
		topicB.TopicArn: {"arn:sqs:b1"},
	}
	for topicArn, want := range wantEndpoints {
		list, err := f.ListSubscriptionsByTopic(ctx,
			awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn})
		if err != nil {
			t.Fatalf("ListSubscriptionsByTopic(%q) returned error %v, want nil", topicArn, err)
		}
		got := make([]string, 0, len(list.Subscriptions))
		for _, s := range list.Subscriptions {
			got = append(got, s.Endpoint)
			if s.TopicArn != topicArn {
				t.Errorf("subscription %q listed under topic %q reports TopicArn %q",
					s.SubscriptionArn, topicArn, s.TopicArn)
			}
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("topic %q endpoints = %v, want %v in creation order", topicArn, got, want)
		}
	}
}

// TestSNSListSubscriptionsByTopicReturnsACopy: the returned slice must be the
// caller's to sort or filter. Handing out the internal slice would let one
// assertion corrupt the next one in the same test.
func TestSNSListSubscriptionsByTopicReturnsACopy(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()

	topic, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
	if err != nil {
		t.Fatalf("CreateTopic returned error %v, want nil", err)
	}
	topicArn := topic.TopicArn
	if _, err := f.Subscribe(ctx, awsclient.SubscribeInput{
		TopicArn: topicArn, Protocol: "sqs", Endpoint: "arn:sqs:original",
	}); err != nil {
		t.Fatalf("Subscribe returned error %v, want nil", err)
	}

	first, err := f.ListSubscriptionsByTopic(ctx,
		awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic returned error %v, want nil", err)
	}
	first.Subscriptions[0].Endpoint = "arn:sqs:clobbered"

	second, err := f.ListSubscriptionsByTopic(ctx,
		awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn})
	if err != nil {
		t.Fatalf("second ListSubscriptionsByTopic returned error %v, want nil", err)
	}
	if got := second.Subscriptions[0].Endpoint; got != "arn:sqs:original" {
		t.Errorf("second list reports endpoint %q after the caller mutated the first result, want %q: "+
			"ListSubscriptionsByTopic must return a copy", got, "arn:sqs:original")
	}
}

// TestSNSSubscribeRecordsNothingWhenItFails: a failed Subscribe must leave the
// topic with no subscription, so a test can assert that a retry is still
// required (and that WR-018 does not report success prematurely).
func TestSNSSubscribeRecordsNothingWhenItFails(t *testing.T) {
	ctx := context.Background()
	f := fake.NewSNS()

	topic, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
	if err != nil {
		t.Fatalf("CreateTopic returned error %v, want nil", err)
	}
	topicArn := topic.TopicArn
	in := awsclient.SubscribeInput{
		TopicArn: topicArn, Protocol: "sqs", Endpoint: "arn:sqs:weir-work",
	}

	f.InjectError(fake.SNSMethodSubscribe, errInjected, 1)

	out, err := f.Subscribe(ctx, in)
	if !errors.Is(err, errInjected) {
		t.Fatalf("Subscribe error = %v, want one matching errInjected", err)
	}
	if out.SubscriptionArn != "" {
		t.Errorf("failed Subscribe reported ARN %q, want the zero value", out.SubscriptionArn)
	}

	list, err := f.ListSubscriptionsByTopic(ctx,
		awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic returned error %v, want nil", err)
	}
	if len(list.Subscriptions) != 0 {
		t.Errorf("the topic has %d subscriptions after a failed Subscribe, want 0: %+v",
			len(list.Subscriptions), list.Subscriptions)
	}

	// The retry succeeds and is the only subscription.
	if _, err := f.Subscribe(ctx, in); err != nil {
		t.Fatalf("Subscribe after the injected failure returned error %v, want nil", err)
	}
	list, err = f.ListSubscriptionsByTopic(ctx,
		awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic returned error %v, want nil", err)
	}
	if len(list.Subscriptions) != 1 {
		t.Errorf("the topic has %d subscriptions after the retry, want 1", len(list.Subscriptions))
	}
}

// --- pagination -----------------------------------------------------------

// seedSNSTopic creates a topic on f and subscribes n endpoints to it, returning
// the topic ARN and the subscription ARNs in creation order.
func seedSNSTopic(t *testing.T, f *fake.SNS, name string, n int) (string, []string) {
	t.Helper()

	ctx := context.Background()
	topic, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: name})
	if err != nil {
		t.Fatalf("setup: CreateTopic(%q) returned error %v, want nil", name, err)
	}

	arns := make([]string, 0, n)
	for i := range n {
		out, err := f.Subscribe(ctx, awsclient.SubscribeInput{
			TopicArn: topic.TopicArn, Protocol: "sqs", Endpoint: "arn:sqs:worker-" + strconv.Itoa(i),
		})
		if err != nil {
			t.Fatalf("setup: Subscribe #%d returned error %v, want nil", i, err)
		}
		arns = append(arns, out.SubscriptionArn)
	}
	return topic.TopicArn, arns
}

// listAllPages drives ListSubscriptionsByTopic the way a correct caller must:
// follow NextToken until it comes back empty. It returns the size of each page
// in order and the subscription ARNs seen across all of them, and fails the test
// rather than looping forever if the fake keeps handing out tokens.
func listAllPages(t *testing.T, f *fake.SNS, topicArn string, maxPages int) ([]int, []string) {
	t.Helper()

	ctx := context.Background()
	var (
		pageSizes []int
		arns      []string
		token     string
	)
	for {
		out, err := f.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
			TopicArn: topicArn, NextToken: token,
		})
		if err != nil {
			t.Fatalf("ListSubscriptionsByTopic(page %d, token %q) returned error %v, want nil",
				len(pageSizes)+1, token, err)
		}

		pageSizes = append(pageSizes, len(out.Subscriptions))
		for _, s := range out.Subscriptions {
			arns = append(arns, s.SubscriptionArn)
		}
		if len(pageSizes) > maxPages {
			t.Fatalf("ListSubscriptionsByTopic produced more than %d pages (%v so far); pagination must "+
				"terminate with an empty NextToken", maxPages, pageSizes)
		}

		token = out.NextToken
		if token == "" {
			return pageSizes, arns
		}
	}
}

// TestSNSListSubscriptionsByTopicPaginatesWithNextToken is what makes WR-018's
// "is my queue already subscribed?" check trustworthy against real SNS, which
// returns at most 100 subscriptions per call. A caller that reads only the first
// page of a heavily subscribed topic concludes "not subscribed yet" and
// subscribes again on every single reconcile — and because this fake does not
// de-duplicate by endpoint (see TestSNSSubscribeIsNotDeduplicatedByEndpoint),
// that bug shows up as an ever-growing subscription list instead of hiding.
//
// The table walks the boundaries that break naive cursor arithmetic: a partial
// final page, a total that is an exact multiple of the page size (which must NOT
// yield a phantom empty extra page), a page size larger than the total, and an
// empty topic. Every case asserts three things — the page sizes, that the union
// of the pages is every subscription in creation order, and that nothing was
// returned twice.
func TestSNSListSubscriptionsByTopicPaginatesWithNextToken(t *testing.T) {
	cases := []struct {
		name          string
		total         int
		pageSize      int
		wantPageSizes []int
	}{
		{name: "5 subscriptions in pages of 2", total: 5, pageSize: 2, wantPageSizes: []int{2, 2, 1}},
		{name: "an exact multiple leaves no trailing empty page", total: 4, pageSize: 2,
			wantPageSizes: []int{2, 2}},
		{name: "a page size of 1 walks one at a time", total: 3, pageSize: 1, wantPageSizes: []int{1, 1, 1}},
		{name: "a page size equal to the total is a single page", total: 5, pageSize: 5,
			wantPageSizes: []int{5}},
		{name: "a page size above the total is a single page", total: 5, pageSize: 10,
			wantPageSizes: []int{5}},
		{name: "a single subscription", total: 1, pageSize: 2, wantPageSizes: []int{1}},
		{name: "no subscriptions is one empty page", total: 0, pageSize: 2, wantPageSizes: []int{0}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fake.NewSNS()
			f.ListPageSize = tc.pageSize
			topicArn, wantARNs := seedSNSTopic(t, f, "weir-events", tc.total)

			gotPageSizes, gotARNs := listAllPages(t, f, topicArn, len(tc.wantPageSizes))

			if !reflect.DeepEqual(gotPageSizes, tc.wantPageSizes) {
				t.Errorf("page sizes = %v, want %v (at most ListPageSize=%d per call)",
					gotPageSizes, tc.wantPageSizes, tc.pageSize)
			}
			if !equalStrings(gotARNs, wantARNs) {
				t.Errorf("the pages together listed %v, want every subscription in creation order %v: "+
					"no subscription may be skipped between pages", gotARNs, wantARNs)
			}

			seen := make(map[string]int, len(gotARNs))
			for _, arn := range gotARNs {
				seen[arn]++
			}
			for arn, n := range seen {
				if n != 1 {
					t.Errorf("subscription %q appeared on %d pages, want exactly 1: the cursor must not "+
						"overlap pages", arn, n)
				}
			}
		})
	}
}

// TestSNSListSubscriptionsByTopicRejectsAnInvalidNextToken: a token the fake did
// not issue must be an error, not silently coerced to the first page. Silently
// restarting is the dangerous failure — a caller with an off-by-one cursor would
// loop over the first page forever, or terminate early believing it had seen
// everything, and its test would pass.
func TestSNSListSubscriptionsByTopicRejectsAnInvalidNextToken(t *testing.T) {
	ctx := context.Background()

	const total = 3

	t.Run("malformed and out-of-range tokens", func(t *testing.T) {
		f := fake.NewSNS()
		f.ListPageSize = 2
		topicArn, _ := seedSNSTopic(t, f, "weir-events", total)

		tokens := []struct {
			name  string
			token string
		}{
			{"not a number", "not-a-number"},
			{"a float", "1.5"},
			{"leading space", " 1"},
			{"negative", "-1"},
			{"one past the end", strconv.Itoa(total + 1)},
			{"far out of range", "999"},
		}
		for _, tc := range tokens {
			t.Run(tc.name, func(t *testing.T) {
				out, err := f.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
					TopicArn: topicArn, NextToken: tc.token,
				})
				if !errors.Is(err, fake.ErrInvalidNextToken) {
					t.Fatalf("ListSubscriptionsByTopic(NextToken=%q) error = %v, want one matching "+
						"fake.ErrInvalidNextToken", tc.token, err)
				}
				if len(out.Subscriptions) != 0 || out.NextToken != "" {
					t.Errorf("a rejected call returned %+v, want the zero-value output", out)
				}
			})
		}
	})

	t.Run("an empty token means the first page, not an error", func(t *testing.T) {
		f := fake.NewSNS()
		f.ListPageSize = 2
		topicArn, wantARNs := seedSNSTopic(t, f, "weir-events", total)

		out, err := f.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
			TopicArn: topicArn, NextToken: "",
		})
		if err != nil {
			t.Fatalf("ListSubscriptionsByTopic with an empty NextToken returned error %v, want nil", err)
		}
		if len(out.Subscriptions) != 2 || out.Subscriptions[0].SubscriptionArn != wantARNs[0] {
			t.Errorf("the first page = %+v, want the first two subscriptions", out.Subscriptions)
		}
	})

	t.Run("a token pointing exactly at the end is a valid empty page", func(t *testing.T) {
		// The offset one PAST the last element is in range: it is where a cursor
		// legitimately lands. The fake never hands this token out (it stops
		// emitting one when the page is the last), but accepting it keeps a
		// caller that computed the cursor itself from getting a spurious error.
		f := fake.NewSNS()
		f.ListPageSize = 2
		topicArn, _ := seedSNSTopic(t, f, "weir-events", total)

		out, err := f.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
			TopicArn: topicArn, NextToken: strconv.Itoa(total),
		})
		if err != nil {
			t.Fatalf("ListSubscriptionsByTopic(NextToken=%q) returned error %v, want nil (an offset equal "+
				"to the subscription count is in range)", strconv.Itoa(total), err)
		}
		if len(out.Subscriptions) != 0 {
			t.Errorf("the page at the end = %+v, want it empty", out.Subscriptions)
		}
		if out.NextToken != "" {
			t.Errorf("NextToken = %q at the end of the list, want it empty", out.NextToken)
		}
	})

	t.Run("on a topic with no subscriptions only the zero offset is valid", func(t *testing.T) {
		f := fake.NewSNS()
		topicArn := "arn:aws:sns:local:000000000000:nope"

		if _, err := f.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
			TopicArn: topicArn, NextToken: "0",
		}); err != nil {
			t.Errorf("ListSubscriptionsByTopic(NextToken=%q) on an empty topic returned error %v, want nil",
				"0", err)
		}
		if _, err := f.ListSubscriptionsByTopic(ctx, awsclient.ListSubscriptionsByTopicInput{
			TopicArn: topicArn, NextToken: "1",
		}); !errors.Is(err, fake.ErrInvalidNextToken) {
			t.Errorf("ListSubscriptionsByTopic(NextToken=%q) on an empty topic error = %v, want one "+
				"matching fake.ErrInvalidNextToken", "1", err)
		}
	})
}

// TestSNSListSubscriptionsByTopicDefaultsToTheRealSNSPageSize pins the default
// at real SNS's 100, not "unbounded". The distinction is the whole point: a fake
// that returned everything in one page by default would let a single-page caller
// pass every test here and then misbehave against a topic with 101
// subscriptions in production. ListPageSize exists only so a test can shrink the
// page and exercise traversal cheaply — leaving it unset must still behave like
// SNS.
func TestSNSListSubscriptionsByTopicDefaultsToTheRealSNSPageSize(t *testing.T) {
	const defaultPageSize = 100

	t.Run("a handful of subscriptions fit on one page", func(t *testing.T) {
		f := fake.NewSNS()
		topicArn, wantARNs := seedSNSTopic(t, f, "weir-events", 5)

		gotPageSizes, gotARNs := listAllPages(t, f, topicArn, 1)
		if !reflect.DeepEqual(gotPageSizes, []int{5}) {
			t.Errorf("page sizes = %v, want [5]: five subscriptions are well under the default page size, "+
				"so one call must return them all with an empty NextToken", gotPageSizes)
		}
		if !equalStrings(gotARNs, wantARNs) {
			t.Errorf("listed %v, want %v", gotARNs, wantARNs)
		}
	})

	// The boundary that distinguishes "defaults to 100" from "returns
	// everything": one more subscription than a page holds.
	for _, tc := range []struct {
		name     string
		pageSize int
	}{
		{"unset ListPageSize", 0},
		{"negative ListPageSize", -1},
	} {
		t.Run(tc.name+" caps a page at 100", func(t *testing.T) {
			f := fake.NewSNS()
			f.ListPageSize = tc.pageSize
			topicArn, wantARNs := seedSNSTopic(t, f, "weir-events", defaultPageSize+1)

			gotPageSizes, gotARNs := listAllPages(t, f, topicArn, 2)
			if want := []int{defaultPageSize, 1}; !reflect.DeepEqual(gotPageSizes, want) {
				t.Errorf("page sizes with ListPageSize=%d = %v, want %v: a non-positive page size must "+
					"fall back to real SNS's %d, not to unbounded",
					tc.pageSize, gotPageSizes, want, defaultPageSize)
			}
			if !equalStrings(gotARNs, wantARNs) {
				t.Errorf("the pages together listed %d subscriptions, want all %d in creation order",
					len(gotARNs), len(wantARNs))
			}
		})
	}
}

// --- concurrency ----------------------------------------------------------

// TestSNSConcurrentCreateTopicWithTheSameNameYieldsOneTopic is the SNS -race
// check, and it asserts something stronger than "no data race": the
// check-then-create inside CreateTopic happens under one lock, so a burst of
// concurrent reconciles converges on a single topic with one ARN. Without the
// mutex this is both a concurrent map write and a lost-update race that could
// leave two ARNs in circulation.
func TestSNSConcurrentCreateTopicWithTheSameNameYieldsOneTopic(t *testing.T) {
	const goroutines = 64

	ctx := context.Background()
	f := fake.NewSNS()

	var (
		mu    sync.Mutex
		arns  = map[string]int{}
		wg    sync.WaitGroup
		start = make(chan struct{})
	)

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			<-start
			out, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Errorf("CreateTopic returned error %v, want nil", err)
				return
			}
			arns[out.TopicArn]++
		}()
	}
	close(start)
	wg.Wait()

	if len(arns) != 1 {
		t.Errorf("concurrent CreateTopic calls with one name produced %d distinct ARNs (%v), want 1",
			len(arns), arns)
	}
	if got := len(f.Topics); got != 1 {
		t.Errorf("Topics holds %d entries, want 1: %v", got, f.Topics)
	}
}

// TestSNSConcurrentSubscribeIssuesUniqueARNs contends the subscription-ARN
// counter, which is the piece of SNS state most likely to be corrupted by a
// missing lock: an unsynchronized nextSubID++ loses increments and hands two
// subscriptions the same ARN.
func TestSNSConcurrentSubscribeIssuesUniqueARNs(t *testing.T) {
	const goroutines = 64

	ctx := context.Background()
	f := fake.NewSNS()

	topic, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "weir-events"})
	if err != nil {
		t.Fatalf("CreateTopic returned error %v, want nil", err)
	}
	topicArn := topic.TopicArn

	var (
		mu    sync.Mutex
		arns  = map[string]struct{}{}
		wg    sync.WaitGroup
		start = make(chan struct{})
	)

	wg.Add(goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			<-start
			out, err := f.Subscribe(ctx, awsclient.SubscribeInput{
				TopicArn: topicArn, Protocol: "sqs", Endpoint: "arn:sqs:worker-" + strconv.Itoa(i),
			})
			if err != nil {
				t.Errorf("Subscribe returned error %v, want nil", err)
				return
			}
			// Concurrent reads must be safe alongside the writes.
			if _, err := f.ListSubscriptionsByTopic(ctx,
				awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn}); err != nil {
				t.Errorf("ListSubscriptionsByTopic returned error %v, want nil", err)
			}
			mu.Lock()
			defer mu.Unlock()
			arns[out.SubscriptionArn] = struct{}{}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(arns) != goroutines {
		t.Errorf("%d concurrent Subscribe calls produced %d distinct subscription ARNs, want %d",
			goroutines, len(arns), goroutines)
	}

	list, err := f.ListSubscriptionsByTopic(ctx,
		awsclient.ListSubscriptionsByTopicInput{TopicArn: topicArn})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic returned error %v, want nil", err)
	}
	if len(list.Subscriptions) != goroutines {
		t.Errorf("the topic has %d subscriptions, want %d (none may be lost to a racing append)",
			len(list.Subscriptions), goroutines)
	}
}
