// Package fake_test exercises the in-memory S3/SNS/SQS fakes from OUTSIDE the
// package (WR-016's "fakes exist for unit tests"). Using an external test
// package is deliberate: every assertion here goes through the exact exported
// surface a consumer gets — NewX, the interface methods, InjectError and the
// exported record fields — so nothing passes only because the test could reach
// unexported state.
//
// What this file covers:
//   - the compile-time and assignability proof that each fake satisfies its
//     awsclient interface (a signature drift stops the build);
//   - the shared errorQueue contract, applied uniformly to all 14 methods of
//     the three fakes, since that helper is the one piece of logic all three
//     share and a bug in it would silently disable error injection everywhere;
//   - that each Method* constant names a method that actually exists, which is
//     what makes those constants worth having.
//
// Per-fake behavior lives in s3_test.go, sns_test.go and sqs_test.go.
package fake_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
)

// Compile-time proof that each fake implements its interface. These lines are
// the cheapest test in the suite: if WR-017 changes a method signature on
// awsclient.S3Client and forgets the fake, the package stops compiling instead
// of failing subtly at a call site much later.
var (
	_ awsclient.S3Client  = (*fake.S3)(nil)
	_ awsclient.SNSClient = (*fake.SNS)(nil)
	_ awsclient.SQSClient = (*fake.SQS)(nil)
)

// errInjected is the stand-in for whatever AWS fails with: a throttle, a
// timeout, an AccessDenied. What matters is that it comes back out of the fake
// intact and matchable with errors.Is.
var errInjected = errors.New("fake test: injected AWS failure")

// errOther is a second, distinct sentinel, used to prove the queue is FIFO and
// not "last injection wins".
var errOther = errors.New("fake test: a different injected failure")

// TestConstructorsReturnValuesUsableAsTheInterfaces complements the var block
// above. The var block proves the pointer TYPE satisfies the interface; this
// proves the CONSTRUCTOR's return value is directly assignable to it, which is
// how every caller will actually use these (`var c awsclient.SQSClient =
// fake.NewSQS()`), and that a freshly constructed fake is immediately usable
// rather than needing extra setup — a nil inner map would panic here.
func TestConstructorsReturnValuesUsableAsTheInterfaces(t *testing.T) {
	ctx := context.Background()

	t.Run("S3", func(t *testing.T) {
		var c awsclient.S3Client = fake.NewS3()
		if _, err := c.PutObject(ctx, awsclient.PutObjectInput{Bucket: "b", Key: "k"}); err != nil {
			t.Errorf("PutObject on a freshly constructed fake returned error %v, want nil", err)
		}
		if _, err := c.ListBuckets(ctx, awsclient.ListBucketsInput{}); err != nil {
			t.Errorf("ListBuckets on a freshly constructed fake returned error %v, want nil", err)
		}
		if _, err := c.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: "b"}); err != nil {
			t.Errorf("GetBucketNotificationConfiguration on a freshly constructed fake returned error %v, want nil", err)
		}
	})

	t.Run("SNS", func(t *testing.T) {
		var c awsclient.SNSClient = fake.NewSNS()
		out, err := c.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "t"})
		if err != nil {
			t.Fatalf("CreateTopic on a freshly constructed fake returned error %v, want nil", err)
		}
		if _, err := c.Subscribe(ctx, awsclient.SubscribeInput{
			TopicArn: out.TopicArn, Protocol: "sqs", Endpoint: "arn:aws:sqs:local:000000000000:q",
		}); err != nil {
			t.Errorf("Subscribe on a freshly constructed fake returned error %v, want nil", err)
		}
	})

	t.Run("SQS", func(t *testing.T) {
		var c awsclient.SQSClient = fake.NewSQS()
		out, err := c.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "q"})
		if err != nil {
			t.Fatalf("CreateQueue on a freshly constructed fake returned error %v, want nil", err)
		}
		if _, err := c.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: out.QueueUrl, Body: "hi"}); err != nil {
			t.Errorf("SendMessage on a freshly constructed fake returned error %v, want nil", err)
		}
	})
}

// --- the shared error-injection contract ----------------------------------

// injectable is the error-injection surface every fake exposes. The three
// fakes do not share a Go type, but they do share this method and the
// errorQueue behind it, so the contract below is tested once and applied to
// every method of every fake.
type injectable interface {
	InjectError(method string, err error, n int)
}

// operation is one (fake, method) pair: a freshly built fake plus a closure
// that performs ONE successful call of the method under test against it,
// returning whatever error that call produced. Any setup a method needs (a
// queue to exist, a message to be in flight) happens inside, wrapped so a
// setup failure is distinguishable from the injected error.
type operation struct {
	name   string // "SQS.DeleteMessage", for subtest names
	method string // the Method* constant the fake expects in InjectError
	build  func(t *testing.T) (injectable, func() error)

	// setupMethods lists other methods the closure has to call to reach the
	// method under test (only SQS.DeleteMessage needs any: a message must be
	// sent and received before it can be deleted). The scoping test below
	// leaves these alone, since failing them would break the setup rather
	// than test the scoping.
	setupMethods []string
}

// operations enumerates all 14 interface methods across the three fakes.
// Keeping them in one table means a new method added to an interface in a
// later task only has to be listed here to inherit the full error-injection
// contract.
func operations() []operation {
	ctx := context.Background()

	newQueue := func(t *testing.T) (*fake.SQS, string) {
		t.Helper()
		f := fake.NewSQS()
		out, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "q"})
		if err != nil {
			t.Fatalf("setup: CreateQueue returned error %v, want nil", err)
		}
		return f, out.QueueUrl
	}

	return []operation{
		{
			name:   "S3.PutObject",
			method: fake.S3MethodPutObject,
			build: func(*testing.T) (injectable, func() error) {
				f := fake.NewS3()
				return f, func() error {
					_, err := f.PutObject(ctx, awsclient.PutObjectInput{Bucket: "b", Key: "k", Body: []byte("x")})
					return err
				}
			},
		},
		{
			name:   "S3.ListBuckets",
			method: fake.S3MethodListBuckets,
			build: func(*testing.T) (injectable, func() error) {
				f := fake.NewS3()
				return f, func() error {
					_, err := f.ListBuckets(ctx, awsclient.ListBucketsInput{})
					return err
				}
			},
		},
		{
			name:   "S3.GetBucketNotificationConfiguration",
			method: fake.S3MethodGetBucketNotificationConfiguration,
			build: func(*testing.T) (injectable, func() error) {
				f := fake.NewS3()
				return f, func() error {
					_, err := f.GetBucketNotificationConfiguration(ctx,
						awsclient.GetBucketNotificationConfigurationInput{Bucket: "b"})
					return err
				}
			},
		},
		{
			name:   "S3.PutBucketNotificationConfiguration",
			method: fake.S3MethodPutBucketNotificationConfiguration,
			build: func(*testing.T) (injectable, func() error) {
				f := fake.NewS3()
				return f, func() error {
					_, err := f.PutBucketNotificationConfiguration(ctx,
						awsclient.PutBucketNotificationConfigurationInput{Bucket: "b"})
					return err
				}
			},
		},
		{
			name:   "SNS.CreateTopic",
			method: fake.SNSMethodCreateTopic,
			build: func(*testing.T) (injectable, func() error) {
				f := fake.NewSNS()
				return f, func() error {
					_, err := f.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "t"})
					return err
				}
			},
		},
		{
			name:   "SNS.Subscribe",
			method: fake.SNSMethodSubscribe,
			build: func(*testing.T) (injectable, func() error) {
				f := fake.NewSNS()
				return f, func() error {
					_, err := f.Subscribe(ctx, awsclient.SubscribeInput{
						TopicArn: "arn:aws:sns:local:000000000000:t", Protocol: "sqs", Endpoint: "e",
					})
					return err
				}
			},
		},
		{
			name:   "SNS.ListSubscriptionsByTopic",
			method: fake.SNSMethodListSubscriptionsByTopic,
			build: func(*testing.T) (injectable, func() error) {
				f := fake.NewSNS()
				return f, func() error {
					_, err := f.ListSubscriptionsByTopic(ctx,
						awsclient.ListSubscriptionsByTopicInput{TopicArn: "arn:aws:sns:local:000000000000:t"})
					return err
				}
			},
		},
		{
			name:   "SQS.CreateQueue",
			method: fake.SQSMethodCreateQueue,
			build: func(*testing.T) (injectable, func() error) {
				f := fake.NewSQS()
				return f, func() error {
					_, err := f.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "q"})
					return err
				}
			},
		},
		{
			name:   "SQS.GetQueueUrl",
			method: fake.SQSMethodGetQueueUrl,
			build: func(t *testing.T) (injectable, func() error) {
				f, _ := newQueue(t)
				return f, func() error {
					_, err := f.GetQueueUrl(ctx, awsclient.GetQueueUrlInput{Name: "q"})
					return err
				}
			},
		},
		{
			name:   "SQS.GetQueueAttributes",
			method: fake.SQSMethodGetQueueAttributes,
			build: func(t *testing.T) (injectable, func() error) {
				f, url := newQueue(t)
				return f, func() error {
					_, err := f.GetQueueAttributes(ctx, awsclient.GetQueueAttributesInput{QueueUrl: url})
					return err
				}
			},
		},
		{
			name:   "SQS.SetQueueAttributes",
			method: fake.SQSMethodSetQueueAttributes,
			build: func(t *testing.T) (injectable, func() error) {
				f, url := newQueue(t)
				return f, func() error {
					_, err := f.SetQueueAttributes(ctx, awsclient.SetQueueAttributesInput{
						QueueUrl: url, Attributes: map[string]string{"VisibilityTimeout": "30"},
					})
					return err
				}
			},
		},
		{
			name:   "SQS.SendMessage",
			method: fake.SQSMethodSendMessage,
			build: func(t *testing.T) (injectable, func() error) {
				f, url := newQueue(t)
				return f, func() error {
					_, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "hi"})
					return err
				}
			},
		},
		{
			name:   "SQS.ReceiveMessage",
			method: fake.SQSMethodReceiveMessage,
			build: func(t *testing.T) (injectable, func() error) {
				f, url := newQueue(t)
				return f, func() error {
					_, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url})
					return err
				}
			},
		},
		{
			name:         "SQS.DeleteMessage",
			method:       fake.SQSMethodDeleteMessage,
			setupMethods: []string{fake.SQSMethodSendMessage, fake.SQSMethodReceiveMessage},
			build: func(t *testing.T) (injectable, func() error) {
				f, url := newQueue(t)
				// Each successful DeleteMessage needs its own in-flight
				// message, so the closure sends and receives one first. Those
				// two calls are setup, not the call under test: their errors
				// are wrapped so a failure here cannot be mistaken for the
				// injected one.
				return f, func() error {
					if _, err := f.SendMessage(ctx, awsclient.SendMessageInput{QueueUrl: url, Body: "hi"}); err != nil {
						return fmt.Errorf("setup send: %w", err)
					}
					rcv, err := f.ReceiveMessage(ctx, awsclient.ReceiveMessageInput{QueueUrl: url})
					if err != nil {
						return fmt.Errorf("setup receive: %w", err)
					}
					if len(rcv.Messages) != 1 {
						return fmt.Errorf("setup receive: got %d messages, want 1", len(rcv.Messages))
					}
					_, err = f.DeleteMessage(ctx, awsclient.DeleteMessageInput{
						QueueUrl: url, ReceiptHandle: rcv.Messages[0].ReceiptHandle,
					})
					return err
				}
			},
		},
	}
}

// TestInjectErrorFailsExactlyTheNextNCalls is the core of the injection
// contract: n is a COUNT of calls to fail, not a latch. A fake that kept
// failing forever (or failed only once regardless of n) would make it
// impossible to test the realistic "AWS failed, we retried, it worked" path
// that WR-018's ensure-loops and WR-024's retry logic depend on.
func TestInjectErrorFailsExactlyTheNextNCalls(t *testing.T) {
	for _, op := range operations() {
		for _, n := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("%s/n=%d", op.name, n), func(t *testing.T) {
				f, call := op.build(t)
				f.InjectError(op.method, errInjected, n)

				for i := 1; i <= n; i++ {
					if err := call(); !errors.Is(err, errInjected) {
						t.Fatalf("call #%d of %d injected: error = %v, want one matching errInjected", i, n, err)
					}
				}
				if err := call(); err != nil {
					t.Errorf("call #%d (one past the %d injected) returned error %v, want nil: "+
						"InjectError must fail exactly n calls and then let the fake work normally", n+1, n, err)
				}
			})
		}
	}
}

// TestInjectErrorTreatsNonPositiveNAsOne pins the documented n < 1 behavior.
// It matters because the obvious alternative — treating 0 as "fail zero times"
// — would make `InjectError(m, err, 0)` a silent no-op, and a test written
// against it would pass while never exercising its error path at all.
func TestInjectErrorTreatsNonPositiveNAsOne(t *testing.T) {
	for _, op := range operations() {
		for _, n := range []int{0, -1, -100} {
			t.Run(fmt.Sprintf("%s/n=%d", op.name, n), func(t *testing.T) {
				f, call := op.build(t)
				f.InjectError(op.method, errInjected, n)

				if err := call(); !errors.Is(err, errInjected) {
					t.Fatalf("first call after InjectError(n=%d): error = %v, want one matching errInjected "+
						"(a non-positive n is documented to mean 1, not zero)", n, err)
				}
				if err := call(); err != nil {
					t.Errorf("second call returned error %v, want nil", err)
				}
			})
		}
	}
}

// TestInjectErrorIsFIFOAcrossSuccessiveInjections proves queued errors are
// consumed in the order they were pushed, so a test can script a sequence of
// distinct failures (throttle, then AccessDenied) and know which one it is
// observing at each step.
func TestInjectErrorIsFIFOAcrossSuccessiveInjections(t *testing.T) {
	for _, op := range operations() {
		t.Run(op.name, func(t *testing.T) {
			f, call := op.build(t)
			f.InjectError(op.method, errInjected, 2)
			f.InjectError(op.method, errOther, 1)

			want := []error{errInjected, errInjected, errOther, nil}
			for i, wantErr := range want {
				err := call()
				switch {
				case wantErr == nil && err != nil:
					t.Fatalf("call #%d returned error %v, want nil", i+1, err)
				case wantErr != nil && !errors.Is(err, wantErr):
					t.Fatalf("call #%d error = %v, want one matching %v (errors are consumed FIFO)", i+1, err, wantErr)
				}
			}
		})
	}
}

// TestInjectErrorIsScopedToTheNamedMethod is what keeps injection usable in a
// test that drives several methods of one fake (WR-018 creates a queue, reads
// its attributes, then sets them): failing one step must not fail the others.
// It also covers the typo case — an unrecognized method name must not fail
// anything by accident.
func TestInjectErrorIsScopedToTheNamedMethod(t *testing.T) {
	for _, op := range operations() {
		t.Run(op.name+"/other methods unaffected", func(t *testing.T) {
			f, call := op.build(t)
			// Inject on every OTHER method of every fake, plus a nonsense
			// name; the method under test must still succeed.
			skip := map[string]bool{op.method: true}
			for _, m := range op.setupMethods {
				skip[m] = true
			}
			for _, other := range operations() {
				if skip[other.method] {
					continue
				}
				f.InjectError(other.method, errInjected, 5)
			}
			f.InjectError("NoSuchMethod", errInjected, 5)

			if err := call(); err != nil {
				t.Errorf("%s returned error %v, want nil: errors injected for other method names "+
					"must not affect this one", op.name, err)
			}
		})
	}
}

// TestInjectErrorDoesNotLeakBetweenFakeInstances guards against a package-level
// or otherwise shared queue. Tests routinely build one fake per subtest and run
// them in parallel; a shared queue would make injection in one leak into
// another, producing failures that look flaky rather than wrong.
func TestInjectErrorDoesNotLeakBetweenFakeInstances(t *testing.T) {
	ctx := context.Background()

	t.Run("S3", func(t *testing.T) {
		a, b := fake.NewS3(), fake.NewS3()
		a.InjectError(fake.S3MethodPutObject, errInjected, 1)

		if _, err := b.PutObject(ctx, awsclient.PutObjectInput{Bucket: "b", Key: "k"}); err != nil {
			t.Errorf("PutObject on the second fake returned error %v, want nil", err)
		}
		if _, err := a.PutObject(ctx, awsclient.PutObjectInput{Bucket: "b", Key: "k"}); !errors.Is(err, errInjected) {
			t.Errorf("PutObject on the injected fake error = %v, want one matching errInjected "+
				"(the other fake's call must not have consumed it)", err)
		}
	})

	t.Run("SNS", func(t *testing.T) {
		a, b := fake.NewSNS(), fake.NewSNS()
		a.InjectError(fake.SNSMethodCreateTopic, errInjected, 1)

		if _, err := b.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "t"}); err != nil {
			t.Errorf("CreateTopic on the second fake returned error %v, want nil", err)
		}
		if _, err := a.CreateTopic(ctx, awsclient.CreateTopicInput{Name: "t"}); !errors.Is(err, errInjected) {
			t.Errorf("CreateTopic on the injected fake error = %v, want one matching errInjected", err)
		}
	})

	t.Run("SQS", func(t *testing.T) {
		a, b := fake.NewSQS(), fake.NewSQS()
		a.InjectError(fake.SQSMethodCreateQueue, errInjected, 1)

		if _, err := b.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "q"}); err != nil {
			t.Errorf("CreateQueue on the second fake returned error %v, want nil", err)
		}
		if _, err := a.CreateQueue(ctx, awsclient.CreateQueueInput{Name: "q"}); !errors.Is(err, errInjected) {
			t.Errorf("CreateQueue on the injected fake error = %v, want one matching errInjected", err)
		}
	})
}

// injectErrorMethod is the one exported method every fake has that is not an
// interface method and therefore has no Method* constant of its own.
const injectErrorMethod = "InjectError"

// TestMethodNameConstantsMatchRealMethodNames is the reason the Method*
// constants exist at all: typo-safety. The compiler checks that the CONSTANT
// exists, but nothing checks that its VALUE names a real method — a constant
// holding "PutObjekt" would compile, and every InjectError using it would
// become a silent no-op, so an error-path test would pass while never
// exercising the error path. Reflection closes that gap.
//
// The expected set of constants is derived from the awsclient INTERFACE each
// fake implements, not from a count of the fake's exported methods. That
// distinction is load-bearing: the fake legitimately carries methods the
// interface does not have — InjectError, and test-control knobs like
// SQS.ExpireInFlight that real AWS has no equivalent of and so cannot have an
// error injected into. Counting methods conflated the two and had to be
// hand-adjusted every time one was added; comparing against the interface's
// own method set makes the check exact in both directions and self-maintaining.
func TestMethodNameConstantsMatchRealMethodNames(t *testing.T) {
	cases := []struct {
		fakeType reflect.Type
		// iface is the awsclient interface whose method set the Method*
		// constants must cover exactly.
		iface  reflect.Type
		consts []string
		// fakeOnly names the fake's exported methods that deliberately have no
		// counterpart on iface, and therefore deliberately have no Method*
		// constant. Listing one here is a claim that it is a test-control knob
		// rather than an interface method someone forgot a constant for; the
		// test below verifies the claim against iface.
		fakeOnly []string
	}{
		{
			fakeType: reflect.TypeOf(fake.NewS3()),
			iface:    reflect.TypeOf((*awsclient.S3Client)(nil)).Elem(),
			consts: []string{
				fake.S3MethodPutObject,
				fake.S3MethodListBuckets,
				fake.S3MethodGetBucketNotificationConfiguration,
				fake.S3MethodPutBucketNotificationConfiguration,
			},
		},
		{
			fakeType: reflect.TypeOf(fake.NewSNS()),
			iface:    reflect.TypeOf((*awsclient.SNSClient)(nil)).Elem(),
			consts: []string{
				fake.SNSMethodCreateTopic,
				fake.SNSMethodSubscribe,
				fake.SNSMethodListSubscriptionsByTopic,
				fake.SNSMethodGetTopicAttributes,
				fake.SNSMethodSetTopicAttributes,
			},
		},
		{
			fakeType: reflect.TypeOf(fake.NewSQS()),
			iface:    reflect.TypeOf((*awsclient.SQSClient)(nil)).Elem(),
			consts: []string{
				fake.SQSMethodCreateQueue,
				fake.SQSMethodGetQueueUrl,
				fake.SQSMethodGetQueueAttributes,
				fake.SQSMethodSetQueueAttributes,
				fake.SQSMethodSendMessage,
				fake.SQSMethodReceiveMessage,
				fake.SQSMethodDeleteMessage,
			},
			// ExpireInFlight simulates a visibility timeout elapsing. Real SQS
			// does that on a wall-clock timer with no API to trigger it, so
			// there is no AWS call for a test to inject an error into.
			fakeOnly: []string{"ExpireInFlight"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.fakeType.String(), func(t *testing.T) {
			ifaceMethods := map[string]bool{}
			for i := range tc.iface.NumMethod() {
				ifaceMethods[tc.iface.Method(i).Name] = true
			}

			declaredConsts := map[string]bool{}
			for _, name := range tc.consts {
				declaredConsts[name] = true

				// The typo check: the constant's value must name a real method.
				if _, ok := tc.fakeType.MethodByName(name); !ok {
					t.Errorf("method-name constant %q does not name a method on %s; "+
						"InjectError with it would be a silent no-op", name, tc.fakeType)
				}
				// ... and that method must be one the interface declares, so a
				// constant cannot quietly point at a fake-only helper.
				if !ifaceMethods[name] {
					t.Errorf("method-name constant %q is not a method of %s; a Method* constant must name "+
						"an interface method", name, tc.iface)
				}
			}

			// The reverse direction: every interface method needs a constant,
			// otherwise a caller cannot inject an error for it at all.
			for name := range ifaceMethods {
				if !declaredConsts[name] {
					t.Errorf("interface method %s.%s has no Method* constant; every interface method needs "+
						"one, or its error path cannot be exercised", tc.iface, name)
				}
			}

			fakeOnly := map[string]bool{}
			for _, name := range tc.fakeOnly {
				fakeOnly[name] = true
				if _, ok := tc.fakeType.MethodByName(name); !ok {
					t.Errorf("%s is declared fake-only but %s has no such method", name, tc.fakeType)
				}
				if ifaceMethods[name] {
					t.Errorf("%s is declared fake-only but %s does declare it; it needs a Method* constant "+
						"rather than a carve-out", name, tc.iface)
				}
			}

			// Nothing exported on the fake may fall outside those three
			// buckets, so a newly added method is either covered by a constant
			// or a deliberate, documented fake-only knob — never an oversight.
			for i := range tc.fakeType.NumMethod() {
				name := tc.fakeType.Method(i).Name
				switch {
				case name == injectErrorMethod, ifaceMethods[name], fakeOnly[name]:
				default:
					t.Errorf("%s.%s is neither an interface method, %s, nor a declared fake-only method; "+
						"give it a Method* constant if it wraps an AWS call, or list it in fakeOnly if it "+
						"is a test-control knob", tc.fakeType, name, injectErrorMethod)
				}
			}
		})
	}
}
