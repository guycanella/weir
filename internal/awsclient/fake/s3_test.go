package fake_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/fake"
)

// hexETag matches the fake's ETag format: 32 lowercase hex characters, i.e. a
// hex-encoded MD5 digest. Downstream code (WR-023 writing a processed result)
// may log or compare ETags, so the shape is worth pinning even though the
// concrete digest is only pinned by the golden vectors below.
var hexETag = regexp.MustCompile(`^[0-9a-f]{32}$`)

// --- PutObject ------------------------------------------------------------

// TestS3PutObjectRecordsWhatWasWritten covers the fake's primary job: a test
// that drives WR-023's "process, then write the result" path needs to assert
// afterwards that the right bytes landed under the right bucket and key. The
// cases walk the input shapes a worker can realistically produce, including the
// ones easy to get wrong: an empty body, binary (non-UTF-8) content, and a
// nested key with slashes that must be stored verbatim rather than split.
func TestS3PutObjectRecordsWhatWasWritten(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		in   awsclient.PutObjectInput
	}{
		{
			name: "plain text result",
			in: awsclient.PutObjectInput{
				Bucket: "weir-results", Key: "out/report.json",
				Body: []byte(`{"ok":true}`), ContentType: "application/json",
			},
		},
		{
			name: "empty body",
			in:   awsclient.PutObjectInput{Bucket: "weir-results", Key: "out/empty", Body: nil},
		},
		{
			name: "binary body",
			in: awsclient.PutObjectInput{
				Bucket: "weir-results", Key: "out/blob.bin",
				Body: []byte{0x00, 0xff, 0x10, 0x00, 0x7f}, ContentType: "application/octet-stream",
			},
		},
		{
			name: "deeply nested key with slashes is stored verbatim",
			in: awsclient.PutObjectInput{
				Bucket: "weir-results", Key: "a/b/c/d/e/result-1.txt", Body: []byte("x"),
			},
		},
		{
			name: "key with spaces and unicode",
			in: awsclient.PutObjectInput{
				Bucket: "weir-results", Key: "out/relatório final.txt", Body: []byte("olá"),
			},
		},
		{
			name: "no content type",
			in:   awsclient.PutObjectInput{Bucket: "weir-results", Key: "out/untyped", Body: []byte("x")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fake.NewS3()

			out, err := f.PutObject(ctx, tc.in)
			if err != nil {
				t.Fatalf("PutObject returned error %v, want nil", err)
			}
			if !hexETag.MatchString(out.ETag) {
				t.Errorf("PutObject ETag = %q, want 32 lowercase hex characters", out.ETag)
			}

			objects, ok := f.PutObjects[tc.in.Bucket]
			if !ok {
				t.Fatalf("PutObjects has no entry for bucket %q; want the call recorded under it "+
					"(buckets are the outer key)", tc.in.Bucket)
			}
			rec, ok := objects[tc.in.Key]
			if !ok {
				t.Fatalf("PutObjects[%q] has no entry for key %q, only %v",
					tc.in.Bucket, tc.in.Key, keysOf(objects))
			}
			if !bytes.Equal(rec.Body, tc.in.Body) {
				t.Errorf("recorded body = %q, want %q", rec.Body, tc.in.Body)
			}
			if rec.ContentType != tc.in.ContentType {
				t.Errorf("recorded content type = %q, want %q", rec.ContentType, tc.in.ContentType)
			}
		})
	}
}

// TestS3PutObjectETagIsTheMD5OfTheBody pins the two golden digests that prove
// the ETag is a real MD5 of the exact body rather than a counter or a random
// string. Real S3 returns the MD5 of a single-part upload's content, and code
// that verifies an upload by comparing digests (or a test that asserts two
// writes produced identical content) only works if the fake preserves that
// relationship.
func TestS3PutObjectETagIsTheMD5OfTheBody(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		body []byte
		want string
	}{
		{"empty body", nil, "d41d8cd98f00b204e9800998ecf8427e"},
		{"empty non-nil body", []byte{}, "d41d8cd98f00b204e9800998ecf8427e"},
		{"hello", []byte("hello"), "5d41402abc4b2a76b9719d911017c592"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fake.NewS3()
			out, err := f.PutObject(ctx, awsclient.PutObjectInput{Bucket: "b", Key: "k", Body: tc.body})
			if err != nil {
				t.Fatalf("PutObject returned error %v, want nil", err)
			}
			if out.ETag != tc.want {
				t.Errorf("ETag for body %q = %q, want %q (the hex MD5 of the body)", tc.body, out.ETag, tc.want)
			}
		})
	}
}

// TestS3PutObjectETagDependsOnlyOnTheBody states the property behind the
// golden vectors without depending on the digest algorithm: same bytes, same
// ETag, regardless of bucket, key or content type; different bytes, different
// ETag. This is what lets a test use the ETag as a cheap content fingerprint.
func TestS3PutObjectETagDependsOnlyOnTheBody(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	put := func(bucket, key, contentType string, body []byte) string {
		t.Helper()
		out, err := f.PutObject(ctx, awsclient.PutObjectInput{
			Bucket: bucket, Key: key, Body: body, ContentType: contentType,
		})
		if err != nil {
			t.Fatalf("PutObject(%q, %q) returned error %v, want nil", bucket, key, err)
		}
		return out.ETag
	}

	same := put("b1", "k1", "text/plain", []byte("payload"))
	other := put("b2", "k2", "application/json", []byte("payload"))
	if same != other {
		t.Errorf("identical bodies produced different ETags (%q vs %q); the ETag must depend on the "+
			"body alone, not on the bucket, key or content type", same, other)
	}

	if differing := put("b1", "k1", "text/plain", []byte("payload!")); differing == same {
		t.Errorf("a different body produced the same ETag %q, want a different one", differing)
	}
}

// TestS3PutObjectOverwriteReplacesTheObject: S3 has no partial update, and a
// worker reprocessing an event legitimately rewrites the same key. The last
// write must win completely — a fake that appended, or that kept the first
// content type, would let a "did the retry actually rewrite the result?" test
// pass for the wrong reason.
func TestS3PutObjectOverwriteReplacesTheObject(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	first, err := f.PutObject(ctx, awsclient.PutObjectInput{
		Bucket: "b", Key: "k", Body: []byte("first"), ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("first PutObject returned error %v, want nil", err)
	}

	second, err := f.PutObject(ctx, awsclient.PutObjectInput{
		Bucket: "b", Key: "k", Body: []byte("second"), ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("second PutObject returned error %v, want nil", err)
	}

	if first.ETag == second.ETag {
		t.Errorf("both writes reported ETag %q, want different ETags for different bodies", first.ETag)
	}
	if got := len(f.PutObjects["b"]); got != 1 {
		t.Errorf("bucket %q holds %d keys after two writes to the same key, want 1", "b", got)
	}

	rec := f.PutObjects["b"]["k"]
	if string(rec.Body) != "second" {
		t.Errorf("recorded body = %q, want %q: the second write must replace the first", rec.Body, "second")
	}
	if rec.ContentType != "application/json" {
		t.Errorf("recorded content type = %q, want %q", rec.ContentType, "application/json")
	}
}

// TestS3PutObjectKeepsBucketsAndKeysDistinct guards the two-level map: the same
// key in two buckets, and two keys in one bucket, are independent objects. A
// fake that flattened the key to bucket+key without a separator, or that keyed
// only on the object key, would conflate them.
func TestS3PutObjectKeepsBucketsAndKeysDistinct(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	writes := []struct {
		bucket, key, body string
	}{
		{"in", "same/key", "in-body"},
		{"out", "same/key", "out-body"},
		{"out", "other/key", "other-body"},
		// A pathological pair: bucket+key concatenated without a delimiter
		// collides ("ab"+"c" == "a"+"bc"), so these two must stay distinct.
		{"ab", "c", "ab-c"},
		{"a", "bc", "a-bc"},
	}
	for _, w := range writes {
		if _, err := f.PutObject(ctx, awsclient.PutObjectInput{
			Bucket: w.bucket, Key: w.key, Body: []byte(w.body),
		}); err != nil {
			t.Fatalf("PutObject(%q, %q) returned error %v, want nil", w.bucket, w.key, err)
		}
	}

	for _, w := range writes {
		rec, ok := f.PutObjects[w.bucket][w.key]
		if !ok {
			t.Errorf("PutObjects[%q][%q] missing", w.bucket, w.key)
			continue
		}
		if string(rec.Body) != w.body {
			t.Errorf("PutObjects[%q][%q] body = %q, want %q", w.bucket, w.key, rec.Body, w.body)
		}
	}
	if got, want := len(f.PutObjects), 4; got != want {
		t.Errorf("recorded %d buckets, want %d", got, want)
	}
}

// TestS3PutObjectCopiesTheBody is a real-world hazard for a fake: a worker that
// writes from a reusable buffer (or slices one payload into several writes)
// would, with a fake that stored the caller's slice, see its recorded history
// silently rewritten by later mutations — and the resulting test failure would
// look like a bug in the worker. The fake defends by copying, and this pins it.
func TestS3PutObjectCopiesTheBody(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	buf := []byte("original")
	if _, err := f.PutObject(ctx, awsclient.PutObjectInput{Bucket: "b", Key: "k", Body: buf}); err != nil {
		t.Fatalf("PutObject returned error %v, want nil", err)
	}

	copy(buf, "MUTATED!")

	if got := string(f.PutObjects["b"]["k"].Body); got != "original" {
		t.Errorf("recorded body = %q after the caller mutated its buffer, want %q: PutObject must copy "+
			"the body so recorded history cannot be rewritten from outside", got, "original")
	}
}

// TestS3PutObjectRecordsNothingWhenItFails: an injected failure must leave the
// fake untouched, otherwise a test asserting "nothing was written when the
// upload failed" would find a phantom object.
func TestS3PutObjectRecordsNothingWhenItFails(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()
	f.InjectError(fake.S3MethodPutObject, errInjected, 1)

	out, err := f.PutObject(ctx, awsclient.PutObjectInput{Bucket: "b", Key: "k", Body: []byte("x")})
	if !errors.Is(err, errInjected) {
		t.Fatalf("PutObject error = %v, want one matching errInjected", err)
	}
	if out.ETag != "" {
		t.Errorf("failed PutObject reported ETag %q, want the zero value", out.ETag)
	}
	if len(f.PutObjects) != 0 {
		t.Errorf("PutObjects = %v after a failed call, want it untouched", f.PutObjects)
	}

	// And the failure is not sticky: the retry lands normally.
	if _, err := f.PutObject(ctx, awsclient.PutObjectInput{Bucket: "b", Key: "k", Body: []byte("x")}); err != nil {
		t.Fatalf("PutObject after the injected failure returned error %v, want nil", err)
	}
	if _, ok := f.PutObjects["b"]["k"]; !ok {
		t.Error("the retried PutObject was not recorded")
	}
}

// --- ListBuckets ----------------------------------------------------------

// TestS3ListBucketsReturnsOnlyTheSeededBuckets pins a deliberate design choice
// worth stating out loud, because the intuitive expectation is the opposite:
// PutObject does NOT create a bucket. ListBuckets reports exactly what a test
// put in the exported Buckets field and nothing else.
//
// That is the right call for this fake. ListBuckets exists for WR-017's
// LocalStack smoke test ("can I reach the endpoint at all"), not for modelling
// bucket lifecycle, and Weir never creates buckets — Terraform does (WR-013).
// Auto-vivifying a bucket on PutObject would also hide the real S3 error a
// caller gets when writing to a bucket that does not exist. Seeding the field
// keeps the fake honest about what it does model.
func TestS3ListBucketsReturnsOnlyTheSeededBuckets(t *testing.T) {
	ctx := context.Background()

	t.Run("a fresh fake lists no buckets", func(t *testing.T) {
		f := fake.NewS3()
		out, err := f.ListBuckets(ctx, awsclient.ListBucketsInput{})
		if err != nil {
			t.Fatalf("ListBuckets returned error %v, want nil", err)
		}
		if len(out.Buckets) != 0 {
			t.Errorf("ListBuckets on a fresh fake = %v, want an empty list", out.Buckets)
		}
	})

	t.Run("seeded buckets are returned in order", func(t *testing.T) {
		f := fake.NewS3()
		f.Buckets = []awsclient.Bucket{{Name: "weir-input"}, {Name: "weir-results"}}

		out, err := f.ListBuckets(ctx, awsclient.ListBucketsInput{})
		if err != nil {
			t.Fatalf("ListBuckets returned error %v, want nil", err)
		}
		if !reflect.DeepEqual(out.Buckets, f.Buckets) {
			t.Errorf("ListBuckets = %v, want %v", out.Buckets, f.Buckets)
		}
	})

	t.Run("PutObject does not register a bucket", func(t *testing.T) {
		f := fake.NewS3()
		if _, err := f.PutObject(ctx, awsclient.PutObjectInput{Bucket: "never-listed", Key: "k"}); err != nil {
			t.Fatalf("PutObject returned error %v, want nil", err)
		}

		out, err := f.ListBuckets(ctx, awsclient.ListBucketsInput{})
		if err != nil {
			t.Fatalf("ListBuckets returned error %v, want nil", err)
		}
		if len(out.Buckets) != 0 {
			t.Errorf("ListBuckets = %v after a PutObject, want an empty list: the fake models bucket "+
				"existence only through the seeded Buckets field, and writing an object must not "+
				"invent one", out.Buckets)
		}
	})
}

// TestS3ListBucketsReturnsACopy: a caller that sorts or truncates the returned
// slice must not corrupt the fake's state for the next call. Returning the
// internal slice directly is the classic Go aliasing bug, and it produces
// order-dependent test failures that are painful to diagnose.
func TestS3ListBucketsReturnsACopy(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()
	f.Buckets = []awsclient.Bucket{{Name: "a"}, {Name: "b"}}

	first, err := f.ListBuckets(ctx, awsclient.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets returned error %v, want nil", err)
	}
	first.Buckets[0].Name = "clobbered"

	second, err := f.ListBuckets(ctx, awsclient.ListBucketsInput{})
	if err != nil {
		t.Fatalf("second ListBuckets returned error %v, want nil", err)
	}
	if second.Buckets[0].Name != "a" {
		t.Errorf("second ListBuckets[0].Name = %q after the caller mutated the first result, want %q: "+
			"ListBuckets must return a copy", second.Buckets[0].Name, "a")
	}
}

// --- bucket notification configuration ------------------------------------

// TestS3NotificationConfigurationRoundTrips is the behavior WR-019 is built on:
// its get-then-compare-then-put loop can only be tested if what Put stores is
// exactly what Get returns, down to the optional filter pointer.
func TestS3NotificationConfigurationRoundTrips(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		cfg  awsclient.NotificationConfiguration
	}{
		{
			name: "empty configuration",
			cfg:  awsclient.NotificationConfiguration{},
		},
		{
			name: "one topic configuration, no filter",
			cfg: awsclient.NotificationConfiguration{
				TopicConfigurations: []awsclient.TopicConfiguration{{
					ID:       "weir-uploads",
					TopicArn: "arn:aws:sns:local:000000000000:weir-events",
					Events:   []string{"s3:ObjectCreated:*"},
				}},
			},
		},
		{
			name: "prefix and suffix filter",
			cfg: awsclient.NotificationConfiguration{
				TopicConfigurations: []awsclient.TopicConfiguration{{
					ID:       "weir-uploads",
					TopicArn: "arn:aws:sns:local:000000000000:weir-events",
					Events:   []string{"s3:ObjectCreated:Put", "s3:ObjectCreated:CompleteMultipartUpload"},
					Filter:   &awsclient.NotificationFilter{Prefix: "incoming/", Suffix: ".json"},
				}},
			},
		},
		{
			name: "several topic configurations",
			cfg: awsclient.NotificationConfiguration{
				TopicConfigurations: []awsclient.TopicConfiguration{
					{ID: "a", TopicArn: "arn:a", Events: []string{"s3:ObjectCreated:*"}},
					{ID: "b", TopicArn: "arn:b", Events: []string{"s3:ObjectRemoved:*"},
						Filter: &awsclient.NotificationFilter{Prefix: "tmp/"}},
				},
			},
		},
		// The three destination kinds below are the ones Weir never creates
		// (ADR-001 routes S3 -> SNS only). They still have to survive a
		// round-trip untouched: S3's put replaces the whole configuration, so
		// WR-019 has to carry back whatever it read, and it can only do that if
		// the fake reports these entries at all. A fake that dropped them would
		// make a reconciler that destroys somebody else's SQS or Lambda
		// notification look correct.
		{
			name: "one queue configuration, no filter",
			cfg: awsclient.NotificationConfiguration{
				QueueConfigurations: []awsclient.QueueConfiguration{{
					ID:       "someone-elses-queue",
					QueueArn: "arn:aws:sqs:local:000000000000:not-weirs",
					Events:   []string{"s3:ObjectCreated:*"},
				}},
			},
		},
		{
			name: "queue configurations with filters",
			cfg: awsclient.NotificationConfiguration{
				QueueConfigurations: []awsclient.QueueConfiguration{
					{ID: "q1", QueueArn: "arn:q1", Events: []string{"s3:ObjectCreated:Post"},
						Filter: &awsclient.NotificationFilter{Prefix: "raw/", Suffix: ".csv"}},
					{ID: "q2", QueueArn: "arn:q2", Events: []string{"s3:ObjectRemoved:Delete"}},
				},
			},
		},
		{
			name: "one lambda function configuration",
			cfg: awsclient.NotificationConfiguration{
				LambdaFunctionConfigurations: []awsclient.LambdaFunctionConfiguration{{
					ID:                "someone-elses-lambda",
					LambdaFunctionArn: "arn:aws:lambda:local:000000000000:function:thumbnailer",
					Events:            []string{"s3:ObjectCreated:*"},
					Filter:            &awsclient.NotificationFilter{Suffix: ".png"},
				}},
			},
		},
		{
			name: "all three destination kinds at once",
			cfg: awsclient.NotificationConfiguration{
				TopicConfigurations: []awsclient.TopicConfiguration{{
					ID:       "weir-uploads",
					TopicArn: "arn:aws:sns:local:000000000000:weir-events",
					Events:   []string{"s3:ObjectCreated:*"},
					Filter:   &awsclient.NotificationFilter{Prefix: "incoming/"},
				}},
				QueueConfigurations: []awsclient.QueueConfiguration{{
					ID:       "someone-elses-queue",
					QueueArn: "arn:aws:sqs:local:000000000000:not-weirs",
					Events:   []string{"s3:ObjectRemoved:*"},
					Filter:   &awsclient.NotificationFilter{Prefix: "archive/", Suffix: ".tar"},
				}},
				LambdaFunctionConfigurations: []awsclient.LambdaFunctionConfiguration{{
					ID:                "someone-elses-lambda",
					LambdaFunctionArn: "arn:aws:lambda:local:000000000000:function:thumbnailer",
					Events:            []string{"s3:ObjectCreated:Put", "s3:ObjectCreated:Copy"},
				}},
			},
		},
		// EventBridge is not a destination list but a presence marker: a
		// non-nil *EventBridgeConfiguration means the bucket has EventBridge
		// delivery enabled. Weir never enables it, so like the queue and
		// lambda entries above it only has to survive a round-trip — a fake
		// that dropped the marker would make a reconciler that silently
		// DISABLES somebody's EventBridge delivery look correct.
		{
			name: "eventbridge enabled, nothing else",
			cfg: awsclient.NotificationConfiguration{
				EventBridgeConfiguration: &awsclient.EventBridgeConfiguration{},
			},
		},
		{
			name: "eventbridge enabled alongside Weir's topic",
			cfg: awsclient.NotificationConfiguration{
				TopicConfigurations: []awsclient.TopicConfiguration{{
					ID:       "weir-uploads",
					TopicArn: "arn:aws:sns:local:000000000000:weir-events",
					Events:   []string{"s3:ObjectCreated:*"},
					Filter:   &awsclient.NotificationFilter{Prefix: "incoming/"},
				}},
				EventBridgeConfiguration: &awsclient.EventBridgeConfiguration{},
			},
		},
		{
			name: "every destination kind and eventbridge at once",
			cfg: awsclient.NotificationConfiguration{
				TopicConfigurations: []awsclient.TopicConfiguration{{
					ID:       "weir-uploads",
					TopicArn: "arn:aws:sns:local:000000000000:weir-events",
					Events:   []string{"s3:ObjectCreated:*"},
				}},
				QueueConfigurations: []awsclient.QueueConfiguration{{
					ID:       "someone-elses-queue",
					QueueArn: "arn:aws:sqs:local:000000000000:not-weirs",
					Events:   []string{"s3:ObjectRemoved:*"},
				}},
				LambdaFunctionConfigurations: []awsclient.LambdaFunctionConfiguration{{
					ID:                "someone-elses-lambda",
					LambdaFunctionArn: "arn:aws:lambda:local:000000000000:function:thumbnailer",
					Events:            []string{"s3:ObjectCreated:Put"},
				}},
				EventBridgeConfiguration: &awsclient.EventBridgeConfiguration{},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fake.NewS3()

			if _, err := f.PutBucketNotificationConfiguration(ctx,
				awsclient.PutBucketNotificationConfigurationInput{
					Bucket: "weir-input", Configuration: tc.cfg,
				}); err != nil {
				t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
			}

			got, err := f.GetBucketNotificationConfiguration(ctx,
				awsclient.GetBucketNotificationConfigurationInput{Bucket: "weir-input"})
			if err != nil {
				t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
			}
			if !reflect.DeepEqual(got.Configuration, tc.cfg) {
				t.Errorf("round-tripped configuration = %+v, want %+v", got.Configuration, tc.cfg)
			}
		})
	}
}

// TestS3NotificationConfigurationEventBridgeMarkerRoundTripsAsPresence pins the
// one thing that makes *EventBridgeConfiguration different from every other
// field on the configuration: it carries no data, so the ONLY information it
// holds is nil vs non-nil. reflect.DeepEqual in the round-trip table above does
// compare that, but silently — this test states both directions outright so a
// regression names itself.
//
// Both directions matter and fail differently. Losing a non-nil marker means
// Weir's put would disable somebody's EventBridge delivery; inventing a non-nil
// marker where the caller sent nil means Weir would ENABLE it on a bucket
// nobody asked for (and, more insidiously here, a fake that always allocated
// the marker would make a WR-019 diff/compare step look stable when it is not).
func TestS3NotificationConfigurationEventBridgeMarkerRoundTripsAsPresence(t *testing.T) {
	ctx := context.Background()

	const bucket = "weir-input"

	// A realistic shape either way: Weir's own topic entry is always present,
	// only the marker varies.
	configWith := func(eb *awsclient.EventBridgeConfiguration) awsclient.NotificationConfiguration {
		return awsclient.NotificationConfiguration{
			TopicConfigurations: []awsclient.TopicConfiguration{{
				ID:       "weir-uploads",
				TopicArn: "arn:aws:sns:local:000000000000:weir-events",
				Events:   []string{"s3:ObjectCreated:*"},
			}},
			EventBridgeConfiguration: eb,
		}
	}

	cases := []struct {
		name        string
		cfg         awsclient.NotificationConfiguration
		wantEnabled bool
	}{
		{
			name:        "enabled survives as non-nil",
			cfg:         configWith(&awsclient.EventBridgeConfiguration{}),
			wantEnabled: true,
		},
		{
			name:        "absent stays nil",
			cfg:         configWith(nil),
			wantEnabled: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fake.NewS3()

			if _, err := f.PutBucketNotificationConfiguration(ctx,
				awsclient.PutBucketNotificationConfigurationInput{
					Bucket: bucket, Configuration: tc.cfg,
				}); err != nil {
				t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
			}

			got, err := f.GetBucketNotificationConfiguration(ctx,
				awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
			if err != nil {
				t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
			}

			if gotEnabled := got.Configuration.EventBridgeConfiguration != nil; gotEnabled != tc.wantEnabled {
				t.Errorf("round-tripped EventBridgeConfiguration != nil is %t, want %t: the marker's "+
					"presence is the whole payload, so nil and non-nil must both survive verbatim",
					gotEnabled, tc.wantEnabled)
			}
			// The rest of the configuration must be unaffected by carrying (or
			// not carrying) the marker.
			if !reflect.DeepEqual(got.Configuration, tc.cfg) {
				t.Errorf("round-tripped configuration = %+v, want %+v", got.Configuration, tc.cfg)
			}
		})
	}
}

// TestS3GetBucketNotificationConfigurationOnAnUnconfiguredBucket pins the
// "no notifications yet" case as a zero value with a NIL error, matching real
// S3. WR-019's first reconcile hits exactly this path, and if the fake returned
// an error instead, the ensure logic would look broken on a fresh bucket.
func TestS3GetBucketNotificationConfigurationOnAnUnconfiguredBucket(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	out, err := f.GetBucketNotificationConfiguration(ctx,
		awsclient.GetBucketNotificationConfigurationInput{Bucket: "never-configured"})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration on an unconfigured bucket returned error %v, "+
			"want nil (real S3 reports an empty configuration, not an error)", err)
	}
	if len(out.Configuration.TopicConfigurations) != 0 {
		t.Errorf("configuration = %+v, want the zero value", out.Configuration)
	}
}

// TestS3PutBucketNotificationConfigurationReplacesWholesale: real S3 has no
// merge semantics for this API — a PUT is the complete new configuration. A
// fake that merged would let an incorrect WR-019 implementation (one that
// forgets to carry existing entries forward) pass its tests and then drop
// somebody else's notification config against real S3.
func TestS3PutBucketNotificationConfigurationReplacesWholesale(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	initial := awsclient.NotificationConfiguration{
		TopicConfigurations: []awsclient.TopicConfiguration{
			{ID: "a", TopicArn: "arn:a", Events: []string{"s3:ObjectCreated:*"}},
			{ID: "b", TopicArn: "arn:b", Events: []string{"s3:ObjectRemoved:*"}},
		},
	}
	replacement := awsclient.NotificationConfiguration{
		TopicConfigurations: []awsclient.TopicConfiguration{
			{ID: "c", TopicArn: "arn:c", Events: []string{"s3:ObjectCreated:Put"}},
		},
	}

	for _, cfg := range []awsclient.NotificationConfiguration{initial, replacement} {
		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{Bucket: "b", Configuration: cfg}); err != nil {
			t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
		}
	}

	got, err := f.GetBucketNotificationConfiguration(ctx,
		awsclient.GetBucketNotificationConfigurationInput{Bucket: "b"})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
	}
	if !reflect.DeepEqual(got.Configuration, replacement) {
		t.Errorf("configuration after the second put = %+v, want exactly %+v (a put replaces, it does "+
			"not merge)", got.Configuration, replacement)
	}

	// An empty configuration is a legitimate "remove all notifications".
	if _, err := f.PutBucketNotificationConfiguration(ctx,
		awsclient.PutBucketNotificationConfigurationInput{Bucket: "b"}); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration (empty) returned error %v, want nil", err)
	}
	got, err = f.GetBucketNotificationConfiguration(ctx,
		awsclient.GetBucketNotificationConfigurationInput{Bucket: "b"})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
	}
	if len(got.Configuration.TopicConfigurations) != 0 {
		t.Errorf("configuration after putting an empty one = %+v, want it cleared", got.Configuration)
	}
}

// TestS3NotificationConfigurationGetModifyPutPreservesForeignDestinations is
// the scenario the wholesale-replace semantics make dangerous, and the reason
// NotificationConfiguration models SQS and Lambda destinations, and the
// EventBridge enabled marker, at all even though Weir never creates one
// (ADR-001: S3 -> SNS).
//
// A bucket Weir is asked to wire may already carry notifications somebody else
// configured. Because S3's put replaces the entire configuration, WR-019's
// get-then-modify-then-put is the only safe shape: read the live configuration,
// add Weir's topic entry to WHAT CAME BACK, and write that. That only works if
// the fake reports the foreign entries on the way out and stores them on the
// way back in — a fake that modeled only TopicConfigurations would report an
// empty configuration for a bucket that has an SQS destination, and the
// reconciler would silently delete it on the very first reconcile while every
// test stayed green.
//
// Both halves are asserted: carrying the entries forward preserves them, and
// (the second subtest) dropping them really does destroy them, which is what
// makes carrying them forward the caller's responsibility rather than the
// fake's.
func TestS3NotificationConfigurationGetModifyPutPreservesForeignDestinations(t *testing.T) {
	ctx := context.Background()

	const bucket = "weir-input"

	// What something other than Weir left on the bucket: an SQS destination, a
	// Lambda destination, EventBridge delivery already switched on, and no SNS
	// topic at all. EventBridge belongs in this fixture rather than a test of
	// its own because it fails in exactly the same way as the other two — Weir
	// reads it, must carry it forward untouched, and S3's wholesale put turns
	// "forgot to carry it" into "silently disabled it".
	preExisting := awsclient.NotificationConfiguration{
		QueueConfigurations: []awsclient.QueueConfiguration{{
			ID:       "someone-elses-queue",
			QueueArn: "arn:aws:sqs:local:000000000000:not-weirs",
			Events:   []string{"s3:ObjectCreated:Post"},
			Filter:   &awsclient.NotificationFilter{Prefix: "raw/", Suffix: ".csv"},
		}},
		LambdaFunctionConfigurations: []awsclient.LambdaFunctionConfiguration{{
			ID:                "someone-elses-lambda",
			LambdaFunctionArn: "arn:aws:lambda:local:000000000000:function:thumbnailer",
			Events:            []string{"s3:ObjectCreated:*"},
		}},
		EventBridgeConfiguration: &awsclient.EventBridgeConfiguration{},
	}

	weirTopic := awsclient.TopicConfiguration{
		ID:       "weir-uploads",
		TopicArn: "arn:aws:sns:local:000000000000:weir-events",
		Events:   []string{"s3:ObjectCreated:*"},
		Filter:   &awsclient.NotificationFilter{Prefix: "incoming/"},
	}

	seed := func(t *testing.T) *fake.S3 {
		t.Helper()
		f := fake.NewS3()
		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{
				Bucket: bucket, Configuration: preExisting,
			}); err != nil {
			t.Fatalf("setup: PutBucketNotificationConfiguration returned error %v, want nil", err)
		}
		return f
	}

	t.Run("carrying the existing entries forward keeps them", func(t *testing.T) {
		f := seed(t)

		live, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
		}
		// The precondition for the whole pattern: the Get has to surface the
		// foreign entries, or the caller has nothing to carry forward.
		if !reflect.DeepEqual(live.Configuration, preExisting) {
			t.Fatalf("the live configuration read back = %+v, want the pre-existing %+v: a Get that "+
				"omitted the SQS or Lambda destinations, or the EventBridge marker, would leave a "+
				"correct caller unable to preserve them", live.Configuration, preExisting)
		}

		// WR-019's shape: add Weir's topic to exactly what was read.
		desired := live.Configuration
		desired.TopicConfigurations = append(desired.TopicConfigurations, weirTopic)

		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{Bucket: bucket, Configuration: desired}); err != nil {
			t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
		}

		got, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("second GetBucketNotificationConfiguration returned error %v, want nil", err)
		}

		if !reflect.DeepEqual(got.Configuration.QueueConfigurations, preExisting.QueueConfigurations) {
			t.Errorf("QueueConfigurations = %+v after Weir added its topic, want them untouched at %+v",
				got.Configuration.QueueConfigurations, preExisting.QueueConfigurations)
		}
		if !reflect.DeepEqual(got.Configuration.LambdaFunctionConfigurations,
			preExisting.LambdaFunctionConfigurations) {
			t.Errorf("LambdaFunctionConfigurations = %+v after Weir added its topic, want them untouched "+
				"at %+v", got.Configuration.LambdaFunctionConfigurations,
				preExisting.LambdaFunctionConfigurations)
		}
		if want := []awsclient.TopicConfiguration{weirTopic}; !reflect.DeepEqual(
			got.Configuration.TopicConfigurations, want) {
			t.Errorf("TopicConfigurations = %+v, want %+v", got.Configuration.TopicConfigurations, want)
		}
		// The regression this field was added for: the caller never mentioned
		// EventBridge, it only appended a topic to what it read, so the marker
		// it read must still be enabled afterwards.
		if got.Configuration.EventBridgeConfiguration == nil {
			t.Error("EventBridgeConfiguration is nil after Weir added its topic to what it read, want it " +
				"still non-nil: the marker was enabled before Weir touched the bucket, and a put that " +
				"drops it disables EventBridge delivery on a live bucket")
		}
	})

	t.Run("sending only Weir's own topic destroys them, as real S3 does", func(t *testing.T) {
		f := seed(t)

		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{
				Bucket: bucket,
				Configuration: awsclient.NotificationConfiguration{
					TopicConfigurations: []awsclient.TopicConfiguration{weirTopic},
				},
			}); err != nil {
			t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
		}

		got, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
		}
		if n := len(got.Configuration.QueueConfigurations); n != 0 {
			t.Errorf("QueueConfigurations holds %d entries after a put that omitted them, want 0: the "+
				"fake must not quietly merge, or WR-019 would never be forced to carry them forward", n)
		}
		if n := len(got.Configuration.LambdaFunctionConfigurations); n != 0 {
			t.Errorf("LambdaFunctionConfigurations holds %d entries after a put that omitted them, want 0", n)
		}
		if got.Configuration.EventBridgeConfiguration != nil {
			t.Error("EventBridgeConfiguration is non-nil after a put that omitted it, want nil: the fake " +
				"must not quietly keep the marker alive, or WR-019 would never be forced to carry it " +
				"forward")
		}
	})
}

// TestS3NotificationConfigurationIsPerBucket: WR-019 may wire more than one
// bucket, and configuring one must not disturb another.
func TestS3NotificationConfigurationIsPerBucket(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	perBucket := map[string]string{"bucket-one": "arn:one", "bucket-two": "arn:two"}
	for bucket, arn := range perBucket {
		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{
				Bucket: bucket,
				Configuration: awsclient.NotificationConfiguration{
					TopicConfigurations: []awsclient.TopicConfiguration{{TopicArn: arn}},
				},
			}); err != nil {
			t.Fatalf("PutBucketNotificationConfiguration(%q) returned error %v, want nil", bucket, err)
		}
	}

	for bucket, wantArn := range perBucket {
		got, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("GetBucketNotificationConfiguration(%q) returned error %v, want nil", bucket, err)
		}
		if len(got.Configuration.TopicConfigurations) != 1 {
			t.Fatalf("bucket %q has %d topic configurations, want 1",
				bucket, len(got.Configuration.TopicConfigurations))
		}
		if arn := got.Configuration.TopicConfigurations[0].TopicArn; arn != wantArn {
			t.Errorf("bucket %q topic ARN = %q, want %q", bucket, arn, wantArn)
		}
	}
}

// TestS3GetBucketNotificationConfigurationReturnsASnapshot: a configuration
// already handed to a caller must not change under it when someone else writes
// a new one. This is what makes a "compare what I read against what I want"
// test meaningful — the "what I read" value has to stay stable.
func TestS3GetBucketNotificationConfigurationReturnsASnapshot(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	original := awsclient.NotificationConfiguration{
		TopicConfigurations: []awsclient.TopicConfiguration{{ID: "original", TopicArn: "arn:a"}},
	}
	if _, err := f.PutBucketNotificationConfiguration(ctx,
		awsclient.PutBucketNotificationConfigurationInput{Bucket: "b", Configuration: original}); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
	}

	snapshot, err := f.GetBucketNotificationConfiguration(ctx,
		awsclient.GetBucketNotificationConfigurationInput{Bucket: "b"})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
	}

	if _, err := f.PutBucketNotificationConfiguration(ctx,
		awsclient.PutBucketNotificationConfigurationInput{
			Bucket: "b",
			Configuration: awsclient.NotificationConfiguration{
				TopicConfigurations: []awsclient.TopicConfiguration{{ID: "replacement", TopicArn: "arn:b"}},
			},
		}); err != nil {
		t.Fatalf("second PutBucketNotificationConfiguration returned error %v, want nil", err)
	}

	if !reflect.DeepEqual(snapshot.Configuration, original) {
		t.Errorf("the previously returned configuration changed to %+v after a later put, want it to "+
			"stay %+v", snapshot.Configuration, original)
	}
}

// notificationConfigWithSpareCapacity builds the value both aliasing tests
// below work with, freshly each call so a test can mutate one copy and still
// compare against a pristine one.
//
// The TopicConfigurations slice is deliberately created with spare capacity
// (len 1, cap 4): that is the realistic shape of a slice a caller assembled
// with make-then-append, and it is the shape that makes aliasing bugs visible,
// because a later append by the caller writes into the same backing array the
// fake would be holding. The entry carries a non-nil *NotificationFilter and a
// non-empty Events slice on purpose — the configuration owns three separate
// pieces of indirection (the topic slice, each Events slice, each Filter
// pointer), and a copy that duplicates only some of them still leaks.
func notificationConfigWithSpareCapacity() awsclient.NotificationConfiguration {
	topics := make([]awsclient.TopicConfiguration, 1, 4)
	topics[0] = awsclient.TopicConfiguration{
		ID:       "weir-uploads",
		TopicArn: "arn:aws:sns:local:000000000000:weir-events",
		Events:   []string{"s3:ObjectCreated:Put"},
		Filter:   &awsclient.NotificationFilter{Prefix: "incoming/", Suffix: ".json"},
	}
	return awsclient.NotificationConfiguration{TopicConfigurations: topics}
}

// TestS3PutBucketNotificationConfigurationDoesNotAliasCallerInput is a
// regression test for a real bug this suite found: the fake stored the
// caller's NotificationConfiguration by value, which copies the slice HEADER
// and the Filter POINTER but not what they point at — so the fake's stored
// state and the caller's input shared the same backing array and the same
// filter struct.
//
// That matters beyond tidiness. Real S3 serializes this configuration over the
// wire, so nothing a caller does to its own input after the call can reach
// S3's stored state. WR-019's ensure loop reuses and extends a desired
// configuration across reconciles; against an aliasing fake, a mutation made
// after the put would retroactively rewrite what the fake claims was stored,
// and the resulting test would either pass for the wrong reason or fail in a
// way that looks like a bug in the reconciler.
//
// Every mutation below happens strictly AFTER the put returns, so a correct
// implementation must be indifferent to all of them.
func TestS3PutBucketNotificationConfigurationDoesNotAliasCallerInput(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	input := notificationConfigWithSpareCapacity()
	want := notificationConfigWithSpareCapacity()

	if _, err := f.PutBucketNotificationConfiguration(ctx,
		awsclient.PutBucketNotificationConfigurationInput{
			Bucket: "weir-input", Configuration: input,
		}); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
	}

	// (1) Mutate the topic entry in place, through the caller's slice.
	input.TopicConfigurations[0].ID = "clobbered-id"
	input.TopicConfigurations[0].TopicArn = "arn:clobbered"
	// (2) Mutate the Events backing array, which a shallow struct copy shares.
	input.TopicConfigurations[0].Events[0] = "s3:ObjectRemoved:*"
	// (3) Mutate through the shared *NotificationFilter.
	input.TopicConfigurations[0].Filter.Prefix = "clobbered/"
	input.TopicConfigurations[0].Filter.Suffix = ".clobbered"
	// (4) Append into the slice's spare capacity: with an aliased slice the
	// caller is writing into the fake's own backing array.
	input.TopicConfigurations = append(input.TopicConfigurations,
		awsclient.TopicConfiguration{ID: "appended-after-the-put", TopicArn: "arn:appended"})

	got, err := f.GetBucketNotificationConfiguration(ctx,
		awsclient.GetBucketNotificationConfigurationInput{Bucket: "weir-input"})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
	}
	if !reflect.DeepEqual(got.Configuration, want) {
		t.Errorf("stored configuration = %+v after the caller mutated the input it had already passed "+
			"in, want it unchanged at %+v: PutBucketNotificationConfiguration must deep-copy the "+
			"configuration (the topic slice, each Events slice and each *NotificationFilter), because "+
			"real S3 cannot be reached through the caller's own memory",
			got.Configuration, want)
	}
	// Spelled out separately, so a failure says which piece of indirection leaked.
	if n := len(got.Configuration.TopicConfigurations); n != 1 {
		t.Fatalf("stored configuration has %d topic configurations, want 1: the append the caller made "+
			"after the put must not be visible to the fake", n)
	}
	stored := got.Configuration.TopicConfigurations[0]
	if stored.Events[0] != "s3:ObjectCreated:Put" {
		t.Errorf("stored Events[0] = %q, want %q: the Events slice was shared with the caller",
			stored.Events[0], "s3:ObjectCreated:Put")
	}
	if stored.Filter == nil {
		t.Fatal("stored Filter is nil, want the filter that was put")
	}
	if stored.Filter == input.TopicConfigurations[0].Filter {
		t.Error("stored Filter is the same pointer the caller passed in, want a copy")
	}
	if stored.Filter.Prefix != "incoming/" || stored.Filter.Suffix != ".json" {
		t.Errorf("stored Filter = %+v, want {Prefix:incoming/ Suffix:.json}: the *NotificationFilter "+
			"was shared with the caller", *stored.Filter)
	}
}

// TestS3GetBucketNotificationConfigurationDoesNotAliasReturnedConfiguration is
// the other half of the same regression: a caller that adjusts what it just
// read — WR-019 legitimately does exactly that, reading the live configuration
// and adding Weir's topic entry to it before putting it back — must not be
// editing the fake's stored state in place. If it were, the fake would report
// the desired configuration as already present and the ensure logic would look
// idempotent while having written nothing.
//
// Two successive Gets must also be independent of each other, which is why the
// second Get is what gets asserted.
func TestS3GetBucketNotificationConfigurationDoesNotAliasReturnedConfiguration(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	want := notificationConfigWithSpareCapacity()
	if _, err := f.PutBucketNotificationConfiguration(ctx,
		awsclient.PutBucketNotificationConfigurationInput{
			Bucket: "weir-input", Configuration: notificationConfigWithSpareCapacity(),
		}); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
	}

	first, err := f.GetBucketNotificationConfiguration(ctx,
		awsclient.GetBucketNotificationConfigurationInput{Bucket: "weir-input"})
	if err != nil {
		t.Fatalf("first GetBucketNotificationConfiguration returned error %v, want nil", err)
	}

	// A caller reshaping what it read, the way an ensure loop would.
	first.Configuration.TopicConfigurations[0].ID = "clobbered-id"
	first.Configuration.TopicConfigurations[0].Events[0] = "s3:ObjectRemoved:*"
	first.Configuration.TopicConfigurations[0].Filter.Prefix = "clobbered/"
	first.Configuration.TopicConfigurations = append(first.Configuration.TopicConfigurations,
		awsclient.TopicConfiguration{ID: "added-by-the-caller"})

	second, err := f.GetBucketNotificationConfiguration(ctx,
		awsclient.GetBucketNotificationConfigurationInput{Bucket: "weir-input"})
	if err != nil {
		t.Fatalf("second GetBucketNotificationConfiguration returned error %v, want nil", err)
	}
	if !reflect.DeepEqual(second.Configuration, want) {
		t.Errorf("configuration read back = %+v after the caller mutated an earlier result, want it "+
			"unchanged at %+v: GetBucketNotificationConfiguration must return a deep copy",
			second.Configuration, want)
	}
	if f2, f1 := second.Configuration.TopicConfigurations[0].Filter,
		first.Configuration.TopicConfigurations[0].Filter; f2 == f1 {
		t.Error("two Get calls returned the same *NotificationFilter pointer, want an independent copy " +
			"per call")
	}
}

// foreignNotificationConfigWithSpareCapacity is the queue/lambda counterpart of
// notificationConfigWithSpareCapacity: the destinations Weir never creates but
// must round-trip faithfully, in the shape that exposes aliasing — slices with
// spare capacity (len 1, cap 4), a non-empty Events slice and a non-nil
// *NotificationFilter per entry.
func foreignNotificationConfigWithSpareCapacity() awsclient.NotificationConfiguration {
	queues := make([]awsclient.QueueConfiguration, 1, 4)
	queues[0] = awsclient.QueueConfiguration{
		ID:       "someone-elses-queue",
		QueueArn: "arn:aws:sqs:local:000000000000:not-weirs",
		Events:   []string{"s3:ObjectCreated:Post"},
		Filter:   &awsclient.NotificationFilter{Prefix: "raw/", Suffix: ".csv"},
	}

	lambdas := make([]awsclient.LambdaFunctionConfiguration, 1, 4)
	lambdas[0] = awsclient.LambdaFunctionConfiguration{
		ID:                "someone-elses-lambda",
		LambdaFunctionArn: "arn:aws:lambda:local:000000000000:function:thumbnailer",
		Events:            []string{"s3:ObjectCreated:Put"},
		Filter:            &awsclient.NotificationFilter{Prefix: "uploads/", Suffix: ".png"},
	}

	return awsclient.NotificationConfiguration{
		QueueConfigurations:          queues,
		LambdaFunctionConfigurations: lambdas,
	}
}

// TestS3NotificationConfigurationDeepCopiesQueueAndLambdaConfigurations extends
// the two aliasing regressions above to the destination kinds that were added
// so a Get -> modify -> Put round-trip preserves them.
//
// The stakes are higher for these than for Weir's own topic entry: WR-019 reads
// them, holds them across the modify step, and writes them straight back
// without inspecting them. If the fake shared its backing arrays or filter
// pointers with the caller, a reconciler bug that mutated somebody else's
// destination in place would be invisible here — the fake's "stored" state
// would agree with the mutated value it was handed — and would only show up
// against real S3, as a deleted or rewritten notification on a live bucket.
//
// All four pieces of indirection are exercised per kind (the entry itself, the
// Events backing array, the *NotificationFilter, and an append into the slice's
// spare capacity), covering the paths cloneNotificationConfiguration walks for
// QueueConfigurations and LambdaFunctionConfigurations.
func TestS3NotificationConfigurationDeepCopiesQueueAndLambdaConfigurations(t *testing.T) {
	ctx := context.Background()

	const bucket = "weir-input"

	t.Run("Put does not alias the caller's input", func(t *testing.T) {
		f := fake.NewS3()

		input := foreignNotificationConfigWithSpareCapacity()
		want := foreignNotificationConfigWithSpareCapacity()

		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{Bucket: bucket, Configuration: input}); err != nil {
			t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
		}

		// Every mutation below happens strictly AFTER the put returned.
		input.QueueConfigurations[0].ID = "clobbered-id"
		input.QueueConfigurations[0].QueueArn = "arn:clobbered"
		input.QueueConfigurations[0].Events[0] = "s3:ObjectRemoved:*"
		input.QueueConfigurations[0].Filter.Prefix = "clobbered/"
		input.QueueConfigurations = append(input.QueueConfigurations,
			awsclient.QueueConfiguration{ID: "appended-after-the-put"})

		input.LambdaFunctionConfigurations[0].ID = "clobbered-id"
		input.LambdaFunctionConfigurations[0].LambdaFunctionArn = "arn:clobbered"
		input.LambdaFunctionConfigurations[0].Events[0] = "s3:ObjectRemoved:*"
		input.LambdaFunctionConfigurations[0].Filter.Suffix = ".clobbered"
		input.LambdaFunctionConfigurations = append(input.LambdaFunctionConfigurations,
			awsclient.LambdaFunctionConfiguration{ID: "appended-after-the-put"})

		got, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
		}
		if !reflect.DeepEqual(got.Configuration, want) {
			t.Errorf("stored configuration = %+v after the caller mutated the input it had already passed "+
				"in, want it unchanged at %+v: the queue and lambda configurations must be deep-copied "+
				"the same way the topic configurations are", got.Configuration, want)
		}
		if q := got.Configuration.QueueConfigurations; len(q) == 1 &&
			q[0].Filter == input.QueueConfigurations[0].Filter {
			t.Error("the stored QueueConfigurations[0].Filter is the same pointer the caller passed in, " +
				"want a copy")
		}
		if l := got.Configuration.LambdaFunctionConfigurations; len(l) == 1 &&
			l[0].Filter == input.LambdaFunctionConfigurations[0].Filter {
			t.Error("the stored LambdaFunctionConfigurations[0].Filter is the same pointer the caller " +
				"passed in, want a copy")
		}
	})

	t.Run("Get does not alias the returned configuration", func(t *testing.T) {
		f := fake.NewS3()

		want := foreignNotificationConfigWithSpareCapacity()
		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{
				Bucket: bucket, Configuration: foreignNotificationConfigWithSpareCapacity(),
			}); err != nil {
			t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
		}

		first, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("first GetBucketNotificationConfiguration returned error %v, want nil", err)
		}

		// A caller reshaping what it read, the way an ensure loop would.
		first.Configuration.QueueConfigurations[0].ID = "clobbered-id"
		first.Configuration.QueueConfigurations[0].Events[0] = "s3:ObjectRemoved:*"
		first.Configuration.QueueConfigurations[0].Filter.Prefix = "clobbered/"
		first.Configuration.QueueConfigurations = append(first.Configuration.QueueConfigurations,
			awsclient.QueueConfiguration{ID: "added-by-the-caller"})

		first.Configuration.LambdaFunctionConfigurations[0].ID = "clobbered-id"
		first.Configuration.LambdaFunctionConfigurations[0].Events[0] = "s3:ObjectRemoved:*"
		first.Configuration.LambdaFunctionConfigurations[0].Filter.Suffix = ".clobbered"
		first.Configuration.LambdaFunctionConfigurations = append(
			first.Configuration.LambdaFunctionConfigurations,
			awsclient.LambdaFunctionConfiguration{ID: "added-by-the-caller"})

		second, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("second GetBucketNotificationConfiguration returned error %v, want nil", err)
		}
		if !reflect.DeepEqual(second.Configuration, want) {
			t.Errorf("configuration read back = %+v after the caller mutated an earlier result, want it "+
				"unchanged at %+v: Get must return a deep copy of every destination kind",
				second.Configuration, want)
		}
		if second.Configuration.QueueConfigurations[0].Filter ==
			first.Configuration.QueueConfigurations[0].Filter {
			t.Error("two Get calls returned the same QueueConfigurations *NotificationFilter pointer, " +
				"want an independent copy per call")
		}
		if second.Configuration.LambdaFunctionConfigurations[0].Filter ==
			first.Configuration.LambdaFunctionConfigurations[0].Filter {
			t.Error("two Get calls returned the same LambdaFunctionConfigurations *NotificationFilter " +
				"pointer, want an independent copy per call")
		}
	})
}

// TestS3NotificationConfigurationEventBridgeMarkerIsIndependentOfTheCaller is
// the EventBridge slice of the same aliasing discipline as the two tests above,
// deliberately much shorter — and asserting something different, because the
// marker is a FIELD-LESS struct and that changes what is observable at all.
//
// Note on what is NOT asserted here: pointer inequality. Go permits distinct
// zero-size allocations to share an address, and gc does exactly that (every
// &EventBridgeConfiguration{} lands on runtime.zerobase), so
// `clone.EventBridgeConfiguration != input.EventBridgeConfiguration` is FALSE
// even for a correct, freshly allocating deep copy. Asserting it would test the
// language, not the fake, and would fail against correct code. It is also moot:
// a struct with no fields has nothing that could be mutated through a shared
// pointer.
//
// What IS observable — and what a caller can really get wrong — is the marker's
// presence. WR-019 reads the live configuration and reshapes it; flipping the
// marker on its own copy (or on the value it already handed to Put) must not
// reach the fake's stored state, exactly as it cannot reach real S3's. Both
// directions are covered: locally disabling a stored-enabled marker, and locally
// enabling a stored-absent one.
func TestS3NotificationConfigurationEventBridgeMarkerIsIndependentOfTheCaller(t *testing.T) {
	ctx := context.Background()

	const bucket = "weir-input"

	t.Run("clearing the marker on a value already put keeps it stored", func(t *testing.T) {
		f := fake.NewS3()

		input := awsclient.NotificationConfiguration{
			TopicConfigurations:      []awsclient.TopicConfiguration{{ID: "weir-uploads", TopicArn: "arn:a"}},
			EventBridgeConfiguration: &awsclient.EventBridgeConfiguration{},
		}
		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{
				Bucket: bucket, Configuration: input,
			}); err != nil {
			t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
		}

		// Strictly AFTER the put returned.
		input.EventBridgeConfiguration = nil

		got, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
		}
		if got.Configuration.EventBridgeConfiguration == nil {
			t.Error("stored EventBridgeConfiguration is nil after the caller cleared the marker on the " +
				"input it had already passed in, want it still enabled")
		}
	})

	t.Run("setting the marker on a value already read does not enable it", func(t *testing.T) {
		f := fake.NewS3()

		if _, err := f.PutBucketNotificationConfiguration(ctx,
			awsclient.PutBucketNotificationConfigurationInput{
				Bucket: bucket,
				Configuration: awsclient.NotificationConfiguration{
					TopicConfigurations: []awsclient.TopicConfiguration{{ID: "weir-uploads", TopicArn: "arn:a"}},
				},
			}); err != nil {
			t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
		}

		first, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("first GetBucketNotificationConfiguration returned error %v, want nil", err)
		}
		if first.Configuration.EventBridgeConfiguration != nil {
			t.Fatalf("EventBridgeConfiguration = %+v on a bucket that never had it enabled, want nil",
				first.Configuration.EventBridgeConfiguration)
		}

		// A caller reshaping what it read, the way an ensure loop would.
		first.Configuration.EventBridgeConfiguration = &awsclient.EventBridgeConfiguration{}

		second, err := f.GetBucketNotificationConfiguration(ctx,
			awsclient.GetBucketNotificationConfigurationInput{Bucket: bucket})
		if err != nil {
			t.Fatalf("second GetBucketNotificationConfiguration returned error %v, want nil", err)
		}
		if second.Configuration.EventBridgeConfiguration != nil {
			t.Error("EventBridgeConfiguration is non-nil after the caller enabled the marker on an earlier " +
				"result, want nil: Get must hand back a value the caller cannot write through")
		}
	})
}

// TestS3PutBucketNotificationConfigurationLeavesStateAloneWhenItFails: WR-019's
// error path ("SNS wiring failed, requeue and try again") must be testable, and
// that means a failed put cannot half-apply.
func TestS3PutBucketNotificationConfigurationLeavesStateAloneWhenItFails(t *testing.T) {
	ctx := context.Background()
	f := fake.NewS3()

	original := awsclient.NotificationConfiguration{
		TopicConfigurations: []awsclient.TopicConfiguration{{ID: "original", TopicArn: "arn:a"}},
	}
	if _, err := f.PutBucketNotificationConfiguration(ctx,
		awsclient.PutBucketNotificationConfigurationInput{Bucket: "b", Configuration: original}); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration returned error %v, want nil", err)
	}

	f.InjectError(fake.S3MethodPutBucketNotificationConfiguration, errInjected, 1)
	if _, err := f.PutBucketNotificationConfiguration(ctx,
		awsclient.PutBucketNotificationConfigurationInput{
			Bucket: "b",
			Configuration: awsclient.NotificationConfiguration{
				TopicConfigurations: []awsclient.TopicConfiguration{{ID: "should-not-land"}},
			},
		}); !errors.Is(err, errInjected) {
		t.Fatalf("PutBucketNotificationConfiguration error = %v, want one matching errInjected", err)
	}

	got, err := f.GetBucketNotificationConfiguration(ctx,
		awsclient.GetBucketNotificationConfigurationInput{Bucket: "b"})
	if err != nil {
		t.Fatalf("GetBucketNotificationConfiguration returned error %v, want nil", err)
	}
	if !reflect.DeepEqual(got.Configuration, original) {
		t.Errorf("configuration = %+v after a failed put, want it unchanged at %+v", got.Configuration, original)
	}
}

// --- concurrency ----------------------------------------------------------

// TestS3ConcurrentUseIsRaceFree is the -race check for the S3 fake. WR-022 runs
// workers with bounded concurrency against one shared client, so the fake will
// genuinely be hit from several goroutines at once; without its mutex the
// PutObjects map writes below are a textbook concurrent map write (a hard
// runtime crash, not merely a race report).
//
// InjectError is called concurrently on purpose: it takes a different lock from
// the method bodies, so this exercises that pairing too. The assertion is
// scheduling-independent — exactly one goroutine's write is rejected, so the
// remaining keys are all recorded — rather than depending on which goroutine
// lost.
func TestS3ConcurrentUseIsRaceFree(t *testing.T) {
	const goroutines = 64

	ctx := context.Background()
	f := fake.NewS3()
	f.Buckets = []awsclient.Bucket{{Name: "weir-results"}}

	var (
		mu       sync.Mutex
		failures int
		wg       sync.WaitGroup
		start    = make(chan struct{})
	)

	// One injected failure, so an error can surface mid-burst.
	f.InjectError(fake.S3MethodPutObject, errInjected, 1)

	wg.Add(goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			<-start // release everyone at once, to maximize contention

			key := "out/object-" + strings.Repeat("x", i%4) + "-" + strconv.Itoa(i)
			_, err := f.PutObject(ctx, awsclient.PutObjectInput{
				Bucket: "weir-results", Key: key, Body: []byte(strconv.Itoa(i)),
			})
			if err != nil {
				mu.Lock()
				failures++
				mu.Unlock()
			}

			if _, err := f.ListBuckets(ctx, awsclient.ListBucketsInput{}); err != nil {
				t.Errorf("ListBuckets returned error %v, want nil", err)
			}
			if _, err := f.PutBucketNotificationConfiguration(ctx,
				awsclient.PutBucketNotificationConfigurationInput{
					Bucket: "weir-results",
					Configuration: awsclient.NotificationConfiguration{
						TopicConfigurations: []awsclient.TopicConfiguration{{ID: strconv.Itoa(i)}},
					},
				}); err != nil {
				t.Errorf("PutBucketNotificationConfiguration returned error %v, want nil", err)
			}
			if _, err := f.GetBucketNotificationConfiguration(ctx,
				awsclient.GetBucketNotificationConfigurationInput{Bucket: "weir-results"}); err != nil {
				t.Errorf("GetBucketNotificationConfiguration returned error %v, want nil", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if failures != 1 {
		t.Errorf("%d of %d concurrent PutObject calls failed, want exactly 1 (the single injected error)",
			failures, goroutines)
	}
	if got, want := len(f.PutObjects["weir-results"]), goroutines-1; got != want {
		t.Errorf("recorded %d objects, want %d (every goroutine but the one that hit the injected "+
			"error)", got, want)
	}
}

// --- helpers --------------------------------------------------------------

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
