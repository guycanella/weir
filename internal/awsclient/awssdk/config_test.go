// Package awssdk_test also covers what WR-017's LocalStack smoke test cannot
// cover: where each client sends its requests when Config.EndpointURL is set,
// and — for S3 — how it *addresses* them.
//
// Neither guarantee survives the smoke test alone, for two different reasons.
//
// Endpoint override (all three services). The smoke test runs with
// AWS_ENDPOINT_URL exported by the Makefile's test-integration target, and
// aws-sdk-go-v2's config.LoadDefaultConfig resolves that env var into
// aws.Config.BaseEndpoint before Config's apply*Options helpers run. So its
// requests reach LocalStack whether or not Weir ever applied Config.EndpointURL
// itself — it cannot tell the two apart. The tests here can: isolateAWSEnv
// clears every ambient AWS_ENDPOINT_URL*, leaving Config.EndpointURL as the
// only thing that could point a client at the httptest stub.
//
// Path-style addressing (S3 only). The smoke test proves bucket-scoped calls
// work against a running LocalStack, which on a normal host also implies
// path-style addressing — a virtual-host-style request would resolve
// "<bucket>.localhost" and fail. But that inference depends on the host's
// resolver: on a machine that wildcards "*.localhost" to 127.0.0.1, a client
// missing UsePathStyle would pass the smoke test anyway. The S3 test below
// removes DNS from the picture entirely and inspects the request line instead.
//
// Both properties are therefore pinned deterministically, in the fast unit
// suite, with no LocalStack required.
package awssdk_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/awssdk"
)

// isolateAWSEnv makes SDK configuration loading hermetic: static dummy
// credentials so signing succeeds offline, no IMDS lookups, no ambient
// profile, and no ambient endpoint override that could mask the one under
// test. Without this, results would depend on the developer's ~/.aws.
func isolateAWSEnv(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	for _, name := range []string{"config", "credentials"} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("write empty AWS %s file: %v", name, err)
		}
	}

	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_REGION", "")

	// The service-specific overrides matter as much as the global one:
	// aws-sdk-go-v2 resolves AWS_ENDPOINT_URL_<SERVICE> into
	// aws.Config.BaseEndpoint on its own, before Config's apply*Options
	// helpers run. Left set, they would make the endpoint-override tests
	// below pass even if Weir never applied Config.EndpointURL at all.
	for _, service := range []string{"S3", "SNS", "SQS"} {
		t.Setenv("AWS_ENDPOINT_URL_"+service, "")
	}
}

// capturedRequest records the parts of an inbound request that reveal how the
// client addressed it. Guarded by a mutex because the handler runs on the
// server's goroutine and the assertions run on the test's.
type capturedRequest struct {
	mu     sync.Mutex
	seen   bool
	method string
	host   string
	path   string
	query  string
}

func (c *capturedRequest) record(r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = true
	c.method = r.Method
	c.host = r.Host
	c.path = r.URL.Path
	c.query = r.URL.RawQuery
}

func (c *capturedRequest) snapshot() capturedRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return capturedRequest{seen: c.seen, method: c.method, host: c.host, path: c.path, query: c.query}
}

func TestNewClientsRejectsMissingRegion(t *testing.T) {
	isolateAWSEnv(t)

	clients, err := awssdk.NewClients(t.Context(), awssdk.Config{EndpointURL: "http://localhost:4566"})
	if err == nil {
		t.Fatal("NewClients with an empty Region returned no error; a missing region must fail fast")
	}
	if clients != nil {
		t.Errorf("NewClients returned %+v alongside an error; it must return nil on failure", clients)
	}
	if !strings.Contains(err.Error(), "Region") {
		t.Errorf("error %q does not name the offending field (Region)", err)
	}
}

func TestNewClientsBuildsAllThreeAdapters(t *testing.T) {
	isolateAWSEnv(t)

	clients, err := awssdk.NewClients(t.Context(), awssdk.Config{Region: "us-east-2"})
	if err != nil {
		t.Fatalf("NewClients with a region and no endpoint override: unexpected error: %v", err)
	}

	// Each adapter must be usable as the Weir-owned interface its consumers
	// depend on (WR-016), so an accidental signature drift breaks here rather
	// than at the first call site.
	var (
		_ awsclient.S3Client  = clients.S3
		_ awsclient.SNSClient = clients.SNS
		_ awsclient.SQSClient = clients.SQS
	)

	if clients.S3 == nil || clients.SNS == nil || clients.SQS == nil {
		t.Fatalf("NewClients returned incomplete Clients: %+v", clients)
	}
}

// TestS3UsesPathStyleAddressingWhenEndpointOverridden is the hermetic guard for
// the one S3-specific part of Config: when EndpointURL is set, the client must
// address buckets path-style ("<endpoint>/<bucket>/<key>") rather than
// virtual-host style ("<bucket>.<endpoint-host>/<key>"). Virtual-host style is
// the SDK default and is unresolvable against LocalStack, which would break
// PutObject (WR-023) and the bucket notification configuration calls (WR-019).
//
// The stub endpoint deliberately uses the DNS name "localhost" rather than
// httptest's own 127.0.0.1: the SDK cannot use virtual-host addressing against
// an *IP* endpoint and silently falls back to path-style, so an IP-based stub
// would make this test pass even with UsePathStyle removed. With a DNS-name
// endpoint the test discriminates either way — a client missing UsePathStyle
// either fails to connect at all (no wildcard "*.localhost" resolution, so the
// request never arrives) or arrives carrying a bucket-prefixed Host header.
// Both are asserted below.
func TestS3UsesPathStyleAddressingWhenEndpointOverridden(t *testing.T) {
	isolateAWSEnv(t)

	const (
		bucket = "weir-addressing-bucket"
		key    = "nested/prefix/object.txt"
	)

	captured := &capturedRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.record(r)
		w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	endpoint := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)
	if endpoint == srv.URL {
		t.Fatalf("httptest server URL %q is not 127.0.0.1-based; this test needs a DNS-name endpoint to be meaningful", srv.URL)
	}

	clients, err := awssdk.NewClients(t.Context(), awssdk.Config{Region: "us-east-2", EndpointURL: endpoint})
	if err != nil {
		t.Fatalf("NewClients(endpoint=%q): unexpected error: %v", endpoint, err)
	}

	if _, err := clients.S3.PutObject(t.Context(), awsclient.PutObjectInput{
		Bucket:      bucket,
		Key:         key,
		Body:        []byte("payload"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("S3.PutObject against the stub endpoint %q: unexpected error: %v\n"+
			"a DNS lookup failure for %q.localhost means the client addressed the bucket virtual-host style, i.e. UsePathStyle was not applied",
			endpoint, err, bucket)
	}

	got := captured.snapshot()
	if !got.seen {
		t.Fatalf("the stub endpoint %q received no request; Config.EndpointURL was not applied to the S3 client", endpoint)
	}
	if got.method != http.MethodPut {
		t.Errorf("request method = %q, want %q", got.method, http.MethodPut)
	}
	if want := "/" + bucket + "/" + key; got.path != want {
		t.Errorf("request path = %q, want %q — the bucket must appear in the path (path-style addressing)", got.path, want)
	}
	if strings.HasPrefix(got.host, bucket+".") {
		t.Errorf("request Host = %q, want a host without the %q prefix — virtual-host-style addressing does not resolve against LocalStack", got.host, bucket)
	}
	if !strings.Contains(endpoint, got.host) {
		t.Errorf("request Host = %q, want the override endpoint's host from %q", got.host, endpoint)
	}
}

// TestSNSAndSQSApplyEndpointOverride pins, per service, that Config.EndpointURL
// is actually threaded into the SNS and SQS clients — the same guarantee the S3
// test above gives, which SNS and SQS otherwise had no coverage for.
//
// The integration smoke test cannot stand in for this. It runs with
// AWS_ENDPOINT_URL exported by the Makefile, and aws-sdk-go-v2's
// config.LoadDefaultConfig resolves that env var into aws.Config.BaseEndpoint
// by itself, before Config.applySNSOptions/applySQSOptions ever run. So the
// smoke test reaches LocalStack whether or not Weir applied its own override:
// gutting either helper to a no-op leaves it green. isolateAWSEnv makes this
// test the opposite — with every ambient AWS_ENDPOINT_URL* cleared, the *only*
// thing that can steer a request to the stub server is Config.EndpointURL
// having been applied by the helper under test. Without it the SDK resolves the
// real regional endpoint, the stub sees nothing, and the test fails.
//
// One representative call per service is enough: the override is applied once,
// per client, at construction — it is not per-operation.
func TestSNSAndSQSApplyEndpointOverride(t *testing.T) {
	const (
		wantTopicARN = "arn:aws:sns:us-east-2:000000000000:weir-endpoint-topic"
		wantQueueURL = "http://sqs.us-east-2.localhost:4566/000000000000/weir-endpoint-queue"
	)

	tests := []struct {
		name string
		// respond writes a minimal, protocol-correct success body for the
		// operation under test, so the call completes and the assertion can
		// distinguish "request never arrived" from "request arrived".
		respond func(w http.ResponseWriter)
		// call issues the one operation through the Weir-owned interface and
		// returns the response field the adapter maps out of the stub's body.
		call    func(t *testing.T, clients *awssdk.Clients) (string, error)
		want    string
		service string
	}{
		{
			name:    "SNS CreateTopic",
			service: "SNS",
			// SNS speaks the AWS Query protocol: XML response envelope.
			respond: func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", "text/xml")
				_, _ = io.WriteString(w, `<CreateTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">`+
					`<CreateTopicResult><TopicArn>`+wantTopicARN+`</TopicArn></CreateTopicResult>`+
					`<ResponseMetadata><RequestId>7a50221f-3774-11df-a9b7-05d48da6f042</RequestId></ResponseMetadata>`+
					`</CreateTopicResponse>`)
			},
			call: func(t *testing.T, clients *awssdk.Clients) (string, error) {
				out, err := clients.SNS.CreateTopic(t.Context(), awsclient.CreateTopicInput{Name: "weir-endpoint-topic"})
				return out.TopicArn, err
			},
			want: wantTopicARN,
		},
		{
			name:    "SQS GetQueueUrl",
			service: "SQS",
			// SQS speaks AWS JSON 1.0.
			respond: func(w http.ResponseWriter) {
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				_, _ = io.WriteString(w, `{"QueueUrl":"`+wantQueueURL+`"}`)
			},
			call: func(t *testing.T, clients *awssdk.Clients) (string, error) {
				out, err := clients.SQS.GetQueueUrl(t.Context(), awsclient.GetQueueUrlInput{Name: "weir-endpoint-queue"})
				return out.QueueUrl, err
			},
			want: wantQueueURL,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateAWSEnv(t)

			captured := &capturedRequest{}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				captured.record(r)
				tc.respond(w)
			}))
			defer srv.Close()

			stub, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("parse httptest server URL %q: %v", srv.URL, err)
			}

			clients, err := awssdk.NewClients(t.Context(), awssdk.Config{Region: "us-east-2", EndpointURL: srv.URL})
			if err != nil {
				t.Fatalf("NewClients(endpoint=%q): unexpected error: %v", srv.URL, err)
			}

			got, err := tc.call(t, clients)
			if err != nil {
				t.Fatalf("%s against the stub endpoint %q: unexpected error: %v\n"+
					"an error mentioning %s.us-east-2.amazonaws.com means the %s client resolved the real regional endpoint, i.e. Config.EndpointURL was not applied",
					tc.name, srv.URL, err, strings.ToLower(tc.service), tc.service)
			}

			seen := captured.snapshot()
			if !seen.seen {
				t.Fatalf("the stub endpoint %q received no request; Config.EndpointURL was not applied to the %s client",
					srv.URL, tc.service)
			}
			if seen.host != stub.Host {
				t.Errorf("request Host = %q, want the override endpoint's host %q — the %s client addressed a different endpoint",
					seen.host, stub.Host, tc.service)
			}
			if seen.method != http.MethodPost {
				t.Errorf("request method = %q, want %q", seen.method, http.MethodPost)
			}
			// Proves the response the adapter mapped came from the stub, not
			// from anywhere else the client might have reached.
			if got != tc.want {
				t.Errorf("%s returned %q, want %q (the value served by the stub endpoint)", tc.name, got, tc.want)
			}
		})
	}
}
