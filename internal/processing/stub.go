package processing

import (
	"encoding/json"

	"github.com/guycanella/weir/internal/events"
)

// DefaultContentType is the Content-Type used for a result object when
// Config.ContentType is left unset. DefaultStub always produces this
// content type.
const DefaultContentType = "application/json"

// ResultSuffix is appended to an event's object key to derive its result
// object key (see OutputKey).
const ResultSuffix = ".result.json"

// OutputKey derives the output-bucket key for evt's result, namespaced by
// evt.Bucket so that two events with the same object key but different
// source buckets never collide in a shared output bucket. It is a pure,
// total, deterministic function: see stub_test.go for the exact contract
// (nested paths, non-ASCII, an already-suffixed key, the empty key) and
// helpers_test.go/process_test.go for why the derivation ignores every
// other field besides evt.Bucket and evt.Key.
func OutputKey(evt events.Event) string {
	return evt.Bucket + "/" + evt.Key + ResultSuffix
}

// defaultStubResult is the JSON shape DefaultStub produces. It is a plain
// struct fed through encoding/json rather than built via string
// concatenation, so object keys containing quotes, backslashes or newlines
// are escaped correctly.
type defaultStubResult struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	Size      int64  `json:"size"`
	ETag      string `json:"etag"`
	VersionID string `json:"versionId"`
	EventName string `json:"eventName"`
}

// DefaultStub is the demo's default processing body: it derives the result
// purely from the event's metadata, since awsclient.S3Client exposes no
// GetObject and the uploaded object's content is therefore unreachable from
// here (a deliberate scope boundary for this demo). It is total,
// deterministic, and never errors.
func DefaultStub(evt events.Event) ([]byte, error) {
	return json.Marshal(defaultStubResult{
		Bucket:    evt.Bucket,
		Key:       evt.Key,
		Size:      evt.Size,
		ETag:      evt.ETag,
		VersionID: evt.VersionID,
		EventName: evt.EventName,
	})
}
