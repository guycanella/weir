package processing

import (
	"encoding/json"

	"github.com/guycanella/weir/internal/events"
	"github.com/guycanella/weir/internal/idempotency"
)

// DefaultContentType is the Content-Type used for a result object when
// Config.ContentType is left unset. DefaultStub always produces this
// content type.
const DefaultContentType = "application/json"

// ResultSuffix is appended to an event's object key to derive its result
// object key (see OutputKey).
const ResultSuffix = ".result.json"

// OutputKey derives the output-bucket key for evt's result as a fixed
// "results/" prefix followed by the opaque identity hash
// idempotency.Key(evt.Bucket, evt.Key, evt.VersionID, evt.ETag) and
// ResultSuffix. It deliberately does NOT mirror evt.Bucket/evt.Key into the
// output path: a source key can be up to 1024 bytes (S3's own object-key
// limit) and a bucket name up to 63 bytes, so path-mirroring the source
// identity into the result key could itself exceed S3's 1024-byte
// object-key limit, causing PutObject to fail permanently for that result
// (an unrecoverable, non-retryable error) with no way to write the result
// at all. Using only the fixed prefix and the hash bounds every output key
// to a small constant length — "results/" (8 bytes) + 64 hex chars +
// ResultSuffix (12 bytes) = 84 bytes — regardless of how long the source
// key or bucket name is, well under the 1024-byte limit with enormous
// headroom.
//
// The identity-hash component is unchanged from before: it is still exactly
// idempotency.Key(evt.Bucket, evt.Key, evt.VersionID, evt.ETag), so the
// collision-avoidance properties already established are preserved —
// two events with the same object key but different source buckets never
// collide in a shared output bucket, and two events with the same object
// key but different content (a different VersionID/ETag) never collide
// either.
//
// The VersionID/ETag component is required because this project's SQS queue
// is standard, not FIFO: delivery order across distinct writes to the same
// key is not guaranteed. Without it, an older version's event processed
// after a newer one would silently overwrite the newer result with stale
// data — a silent, hard-to-detect data-loss bug. Including it means every
// distinct (bucket, key, versionID, etag) tuple gets its own result object,
// so an out-of-order redelivery can never clobber a newer result.
//
// Because idempotency.Key deliberately excludes EventName/EventTime/Size,
// redeliveries of the SAME write (which vary only in those fields) still
// correctly collapse onto a single output key. The tradeoff is that this
// project no longer has a single stable "latest result" path per source
// key; unbounded small-object growth in the output bucket over repeated
// overwrites is an accepted, later-fixable storage/lifecycle concern, not
// addressed here.
//
// The accepted cost of dropping path-mirroring: results are no longer
// browsable-by-path in the output bucket — you cannot infer which source
// object a result came from just by looking at its key. A real deployment
// wanting that would need a separate index/manifest, out of scope here.
//
// It is a pure, total, deterministic function: see stub_test.go for the
// exact contract and helpers_test.go/process_test.go for further detail.
func OutputKey(evt events.Event) string {
	return "results/" + idempotency.Key(evt.Bucket, evt.Key, evt.VersionID, evt.ETag) + ResultSuffix
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
