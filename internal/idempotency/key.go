// Package idempotency contains the pure decision logic for recognizing a
// re-delivered S3 event as a duplicate of one already processed (WR-008).
package idempotency

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// Key derives a deterministic, collision-resistant key from the
// identity of an S3 object write: its bucket, key, version ID and ETag.
//
// The function is total and pure: it never errors or panics, performs no
// I/O, and depends on no clock or randomness, so identical inputs always
// produce an identical output.
//
// Fields are combined via length-prefixed framing rather than delimiter-
// joined concatenation: each field is preceded by its length as a fixed
// 8-byte big-endian prefix before being written into the hash. This makes
// every field boundary explicit and unambiguous, so no byte within a
// field's content — including any candidate separator character, an empty
// string, or an embedded NUL — can be misread as a boundary between two
// fields. Two distinct tuples therefore cannot collide due to ambiguous
// field boundaries or content "straddling" a boundary. This eliminates that
// specific class of collision; SHA-256 itself remains collision-resistant,
// not collision-proof, over its full (unbounded) input space.
func Key(bucket, key, versionID, etag string) string {
	h := sha256.New()

	for _, field := range []string{bucket, key, versionID, etag} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		h.Write(length[:])
		h.Write([]byte(field))
	}

	return hex.EncodeToString(h.Sum(nil))
}
