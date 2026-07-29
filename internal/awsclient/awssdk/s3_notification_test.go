// This file covers what neither the LocalStack smoke test nor a pure unit test
// on the mapping helpers can cover: the *bytes* the S3 adapter puts on the wire
// for PutBucketNotificationConfiguration.
//
// The specific hazard is S3's <Filter> element. A *types.NotificationConfigurationFilter
// that is non-nil but carries no filter rules serializes to an empty <Filter>
// element rather than being omitted, and real S3 can reject such a request
// (MalformedXML) even though every Weir-level field looks correct. That failure
// mode is invisible to a helper-level unit test (a non-nil pointer with an empty
// rule slice looks harmless in Go) and invisible to LocalStack (which is lenient
// about the same XML real S3 refuses). Only the serialized request shows it, so
// these tests capture the request body from an httptest stub and assert on the
// XML itself.
//
// Reuses config_test.go's isolateAWSEnv so no ambient AWS_ENDPOINT_URL* or
// ~/.aws profile can steer the request anywhere but the stub.
package awssdk_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/guycanella/weir/internal/awsclient"
	"github.com/guycanella/weir/internal/awsclient/awssdk"
)

// capturedBody records the raw request body the stub received. Guarded by a
// mutex because the handler runs on the server's goroutine and the assertions
// run on the test's.
type capturedBody struct {
	mu   sync.Mutex
	seen bool
	body string
}

func (c *capturedBody) record(r *http.Request) {
	body, err := io.ReadAll(r.Body)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = true
	if err != nil {
		c.body = "<read error: " + err.Error() + ">"
		return
	}
	c.body = string(body)
}

func (c *capturedBody) snapshot() (seen bool, body string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen, c.body
}

// The XML shapes below mirror only the parts of S3's
// PutBucketNotificationConfiguration request document these tests assert on.
// Filter is a pointer precisely so "element absent" and "element present but
// empty" are distinguishable — that distinction is the whole point here.
type notificationConfigurationXML struct {
	XMLName             xml.Name                `xml:"NotificationConfiguration"`
	TopicConfigurations []topicConfigurationXML `xml:"TopicConfiguration"`
}

type topicConfigurationXML struct {
	ID     string     `xml:"Id"`
	Topic  string     `xml:"Topic"`
	Events []string   `xml:"Event"`
	Filter *filterXML `xml:"Filter"`
}

type filterXML struct {
	S3Key struct {
		Rules []filterRuleXML `xml:"FilterRule"`
	} `xml:"S3Key"`
}

type filterRuleXML struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

// TestPutBucketNotificationConfigurationSerializesFilterOnlyWhenSet pins the
// boundary of when a <Filter> element is emitted at all.
//
// The regression it guards: a zero-value *awsclient.NotificationFilter (non-nil
// pointer, both Prefix and Suffix empty) must be treated exactly like a nil
// filter and produce *no* <Filter> element. Mapping it to a non-nil SDK filter
// with an empty rule list emits `<Filter><S3Key></S3Key></Filter>`, which is
// not what the caller asked for and which real S3 may reject outright.
//
// The two neighbouring cases are asserted in the same table so the boundary is
// pinned from both sides: nil must stay omitted (it always was — locking it in
// so it cannot regress), and a filter with any rule set must still be emitted
// in full (so the fix cannot be "over-corrected" into stripping every filter).
func TestPutBucketNotificationConfigurationSerializesFilterOnlyWhenSet(t *testing.T) {
	const (
		bucket   = "weir-notification-bucket"
		topicARN = "arn:aws:sns:us-east-2:000000000000:weir-events"
		configID = "weir-managed"
	)

	tests := []struct {
		name   string
		filter *awsclient.NotificationFilter
		// wantRules nil means: no <Filter> element may appear in the request
		// body at all. Non-nil means the element must appear and carry exactly
		// these rules, in this order.
		wantRules []filterRuleXML
	}{
		{
			name:      "nil filter emits no Filter element",
			filter:    nil,
			wantRules: nil,
		},
		{
			// The reported bug: a caller that says "no filtering" by passing an
			// empty struct rather than nil must get the same wire format.
			name:      "zero-value filter emits no Filter element",
			filter:    &awsclient.NotificationFilter{},
			wantRules: nil,
		},
		{
			name:      "prefix-only filter emits a prefix rule",
			filter:    &awsclient.NotificationFilter{Prefix: "incoming/"},
			wantRules: []filterRuleXML{{Name: "prefix", Value: "incoming/"}},
		},
		{
			name:      "suffix-only filter emits a suffix rule",
			filter:    &awsclient.NotificationFilter{Suffix: ".json"},
			wantRules: []filterRuleXML{{Name: "suffix", Value: ".json"}},
		},
		{
			name:   "prefix and suffix emit both rules",
			filter: &awsclient.NotificationFilter{Prefix: "incoming/", Suffix: ".json"},
			wantRules: []filterRuleXML{
				{Name: "prefix", Value: "incoming/"},
				{Name: "suffix", Value: ".json"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			isolateAWSEnv(t)

			captured := &capturedBody{}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				captured.record(r)
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			clients, err := awssdk.NewClients(t.Context(), awssdk.Config{Region: "us-east-2", EndpointURL: srv.URL})
			if err != nil {
				t.Fatalf("NewClients(endpoint=%q): unexpected error: %v", srv.URL, err)
			}

			_, err = clients.S3.PutBucketNotificationConfiguration(t.Context(), awsclient.PutBucketNotificationConfigurationInput{
				Bucket: bucket,
				Configuration: awsclient.NotificationConfiguration{
					TopicConfigurations: []awsclient.TopicConfiguration{{
						ID:       configID,
						TopicArn: topicARN,
						Events:   []string{"s3:ObjectCreated:*"},
						Filter:   tc.filter,
					}},
				},
			})
			if err != nil {
				t.Fatalf("S3.PutBucketNotificationConfiguration against the stub endpoint %q: unexpected error: %v", srv.URL, err)
			}

			seen, body := captured.snapshot()
			if !seen {
				t.Fatalf("the stub endpoint %q received no request; nothing was serialized to assert on", srv.URL)
			}

			// Blunt guard first: with no filter expected, the substring must
			// not appear anywhere — this catches a self-closing `<Filter/>` and
			// an empty `<Filter></Filter>` alike, whatever nesting they carry.
			if tc.wantRules == nil && strings.Contains(body, "<Filter") {
				t.Errorf("request body contains a <Filter> element for filter %#v, want it omitted entirely;\n"+
					"an empty <Filter> element is not equivalent to no filter and real S3 may reject it.\nbody: %s",
					tc.filter, body)
			}

			var doc notificationConfigurationXML
			if err := xml.Unmarshal([]byte(body), &doc); err != nil {
				t.Fatalf("request body is not the expected NotificationConfiguration XML: %v\nbody: %s", err, body)
			}

			if len(doc.TopicConfigurations) != 1 {
				t.Fatalf("request body has %d TopicConfiguration elements, want 1\nbody: %s", len(doc.TopicConfigurations), body)
			}
			topic := doc.TopicConfigurations[0]

			// Confirms the document under assertion really is the one this test
			// sent, so the Filter assertions below are not read off some other
			// request the SDK happened to make.
			if topic.ID != configID {
				t.Errorf("TopicConfiguration Id = %q, want %q\nbody: %s", topic.ID, configID, body)
			}
			if topic.Topic != topicARN {
				t.Errorf("TopicConfiguration Topic = %q, want %q\nbody: %s", topic.Topic, topicARN, body)
			}
			if want := []string{"s3:ObjectCreated:*"}; !slices.Equal(topic.Events, want) {
				t.Errorf("TopicConfiguration Events = %v, want %v\nbody: %s", topic.Events, want, body)
			}

			if tc.wantRules == nil {
				if topic.Filter != nil {
					t.Errorf("TopicConfiguration.Filter is present (%d rule(s)) for filter %#v, want the element omitted\nbody: %s",
						len(topic.Filter.S3Key.Rules), tc.filter, body)
				}
				return
			}

			if topic.Filter == nil {
				t.Fatalf("TopicConfiguration.Filter is absent for filter %#v, want rules %v — a filter the caller set must reach S3\nbody: %s",
					tc.filter, tc.wantRules, body)
			}
			got := topic.Filter.S3Key.Rules
			if len(got) != len(tc.wantRules) {
				t.Fatalf("Filter has %d rule(s) %v, want %d %v\nbody: %s", len(got), got, len(tc.wantRules), tc.wantRules, body)
			}
			for i, want := range tc.wantRules {
				if got[i] != want {
					t.Errorf("Filter rule[%d] = %+v, want %+v\nbody: %s", i, got[i], want, body)
				}
			}
		})
	}
}
