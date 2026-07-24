package events

import "strings"

// WatchConfig is a pipeline's watch target: the bucket and key prefix from
// the CR's spec.source (DOCUMENTATION.md §3.4).
type WatchConfig struct {
	Bucket string
	Prefix string
}

// matches decides whether a pipeline watching cfg cares about evt: same
// bucket, key under the prefix (literal string prefix, matching S3's own
// semantics), and an object-creation event. Event-name filtering lives here
// rather than in the parser because it's a routing concern, not a parsing
// one: a delete event parses fine, it's just out of scope for an
// upload->process pipeline.
func matches(evt Event, cfg WatchConfig) bool {
	if evt.Bucket != cfg.Bucket {
		return false
	}
	if !strings.HasPrefix(evt.Key, cfg.Prefix) {
		return false
	}
	return strings.HasPrefix(evt.EventName, "ObjectCreated:")
}
