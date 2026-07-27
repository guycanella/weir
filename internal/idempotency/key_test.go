package idempotency

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

// This file pins the pure idempotency-key decision for WR-008: given the
// identity of an S3 object write, produce a deterministic key that lets a
// re-delivered event be recognized as a duplicate (WR-010 consumes this).
//
// Design decisions this suite pins down for the implementer (Julia):
//
//   - Signature: idempotencyKey(bucket, key, versionID, etag string) string.
//     It takes exactly the four IDENTITY fields, not the whole events.Event.
//     That is deliberate: EventName, EventTime and Size are NOT identity. The
//     same object write can be re-delivered with a different EventTime (SQS
//     redelivery), so feeding them in would make redeliveries look distinct —
//     precisely the bug this task exists to prevent. Keeping them out of the
//     signature makes that property structural rather than something a test
//     has to police.
//
//   - The function is TOTAL and pure: no error return, no panic, no clock, no
//     randomness, no I/O. Any four strings — including all-empty — yield a
//     key. Unversioned buckets legitimately produce versionID == "".
//
//   - Output format: 64 lowercase hex characters (a SHA-256 digest, hex
//     encoded). Two properties matter and are asserted below: a BOUNDED,
//     constant length and a SAFE alphabet. Raw S3 object keys are up to 1024
//     bytes of arbitrary UTF-8 (spaces, slashes, control characters), so the
//     key must not be a concatenation of the inputs — it has to be usable
//     verbatim as a cache/map/label/record key downstream.
//
//   - Golden digests are asserted in exactly ONE place,
//     TestIdempotencyKeyGoldenVectors, which pins the concrete framing so a
//     silent format change cannot invalidate persisted keys. Every other test
//     here is behavioral and framing-agnostic on purpose: they specify
//     determinism, output format and structural distinctness without dictating
//     an encoding. See that test's comment for why the change-detector
//     tradeoff is worth paying once and only once.
//
//   - UNAMBIGUOUS FIELD FRAMING IS THE LOAD-BEARING PROPERTY — see
//     TestIdempotencyKeyDelimiterCollisionSafety, which is the main reason
//     this suite exists. Joining the fields with a separator and hashing the
//     result is only safe if no field can contain the separator, and S3
//     object keys can contain essentially anything. The robust construction
//     is unambiguous framing — length-prefix each field, or hash each field
//     separately and combine the digests — NOT hunting for a byte you believe
//     cannot appear in the input.
//
//     Stated precisely, because the distinction matters: framing each field
//     unambiguously makes the PREIMAGE ENCODING injective, so two distinct
//     tuples cannot collide by having content straddle an ambiguous field
//     boundary. That class of collision is eliminated structurally, and the
//     tests below exercise it exhaustively. It does not make idempotencyKey
//     itself injective — SHA-256 maps an unbounded input space onto 256 bits,
//     so distinct tuples can in theory share a digest. What the function
//     offers overall is collision RESISTANCE, not collision freedom; what the
//     framing offers is the removal of boundary-ambiguity collisions, which
//     is the part an implementation actually controls and the part these
//     tests can prove.

// identity is one (bucket, key, versionID, etag) tuple: the full input to
// idempotencyKey.
type identity struct {
	name      string
	bucket    string
	key       string
	versionID string
	etag      string
}

func (id identity) call() string {
	return idempotencyKey(id.bucket, id.key, id.versionID, id.etag)
}

func (id identity) tuple() [4]string {
	return [4]string{id.bucket, id.key, id.versionID, id.etag}
}

// corpus is the shared set of identities used by the determinism, format and
// global-distinctness tests. Every entry is a DISTINCT tuple, so every entry
// must hash to a distinct key.
func corpus() []identity {
	return []identity{
		// --- the canonical happy path (example CR: bucket "uploads", prefix "raw/") ---
		{
			name:      "canonical versioned object",
			bucket:    "uploads",
			key:       "raw/video1.mp4",
			versionID: "3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",
			etag:      "d41d8cd98f00b204e9800998ecf8427e",
		},

		// --- one field varied at a time from the canonical case ---
		{
			name:      "different bucket only",
			bucket:    "uploads-staging",
			key:       "raw/video1.mp4",
			versionID: "3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",
			etag:      "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:      "different key only",
			bucket:    "uploads",
			key:       "raw/video2.mp4",
			versionID: "3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",
			etag:      "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:      "different versionID only (overwrite of the same object)",
			bucket:    "uploads",
			key:       "raw/video1.mp4",
			versionID: "PHtexPGjH2y.zBgT8LmB7wwLI2mpbz.k",
			etag:      "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:      "different etag only",
			bucket:    "uploads",
			key:       "raw/video1.mp4",
			versionID: "3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",
			etag:      "9e107d9d372bb6826bd81d3542a419d6",
		},

		// --- unversioned bucket: versionID is legitimately empty ---
		{
			name:      "unversioned object (empty versionID)",
			bucket:    "uploads",
			key:       "raw/video1.mp4",
			versionID: "",
			etag:      "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:      "unversioned object, different etag (same object re-uploaded)",
			bucket:    "uploads",
			key:       "raw/video1.mp4",
			versionID: "",
			etag:      "9e107d9d372bb6826bd81d3542a419d6",
		},
		{
			name:      "S3 literal 'null' versionID is not the same as an empty one",
			bucket:    "uploads",
			key:       "raw/video1.mp4",
			versionID: "null",
			etag:      "d41d8cd98f00b204e9800998ecf8427e",
		},

		// --- field-value swaps: position must matter, not just the multiset ---
		{
			name:      "bucket and key values swapped",
			bucket:    "raw/video1.mp4",
			key:       "uploads",
			versionID: "3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",
			etag:      "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:      "versionID present, etag empty",
			bucket:    "uploads",
			key:       "raw/report.csv",
			versionID: "abc123",
			etag:      "",
		},
		{
			name:      "versionID empty, etag holds the same value",
			bucket:    "uploads",
			key:       "raw/report.csv",
			versionID: "",
			etag:      "abc123",
		},

		// --- realistic S3 object-key shapes ---
		{
			name:      "key with spaces (decoded from the '+'/%20 wire form)",
			bucket:    "uploads",
			key:       "raw/Q1 2026 report final.pdf",
			versionID: "",
			etag:      "0b26e313ed4a7ca6904b0e9369e5b957",
		},
		{
			name:      "deeply nested key",
			bucket:    "uploads",
			key:       "raw/2026/07/27/tenant-42/batch-0007/video1.mp4",
			versionID: "",
			etag:      "0b26e313ed4a7ca6904b0e9369e5b957",
		},
		{
			name:      "unicode key",
			bucket:    "uploads",
			key:       "raw/relatórios/ação-日本語-🎬.mp4",
			versionID: "",
			etag:      "0b26e313ed4a7ca6904b0e9369e5b957",
		},
		{
			name:      "key with an embedded newline (legal in an S3 key)",
			bucket:    "uploads",
			key:       "raw/weird\nname.txt",
			versionID: "",
			etag:      "0b26e313ed4a7ca6904b0e9369e5b957",
		},
		{
			name:      "key differing only by case (S3 keys are case-sensitive)",
			bucket:    "uploads",
			key:       "raw/Video1.mp4",
			versionID: "",
			etag:      "0b26e313ed4a7ca6904b0e9369e5b957",
		},
		{
			name:      "multipart etag (hex with a part-count suffix)",
			bucket:    "uploads",
			key:       "raw/big.mp4",
			versionID: "",
			etag:      "9bb58f26192e4ba00f01e2e7b136bbd8-42",
		},
		{
			name:      "quoted etag as some SDKs surface it",
			bucket:    "uploads",
			key:       "raw/big.mp4",
			versionID: "",
			etag:      `"9bb58f26192e4ba00f01e2e7b136bbd8-42"`,
		},
		{
			name:      "maximum-length (1024-byte) object key",
			bucket:    "uploads",
			key:       strings.Repeat("k", 1024),
			versionID: "",
			etag:      "0b26e313ed4a7ca6904b0e9369e5b957",
		},

		// --- degenerate but legal inputs: the function must stay total ---
		{
			name:      "all fields empty",
			bucket:    "",
			key:       "",
			versionID: "",
			etag:      "",
		},
		{
			name:      "only bucket set",
			bucket:    "uploads",
			key:       "",
			versionID: "",
			etag:      "",
		},
		{
			name:      "only key set",
			bucket:    "",
			key:       "raw/video1.mp4",
			versionID: "",
			etag:      "",
		},
	}
}

// TestIdempotencyKeyIsDeterministic is the "same input -> same key" half of the
// Done-when: repeated calls with identical inputs must return byte-identical
// keys, within a call and across calls. Any hidden clock, random salt, or
// map-iteration order in the implementation shows up here.
func TestIdempotencyKeyIsDeterministic(t *testing.T) {
	for _, id := range corpus() {
		t.Run(id.name, func(t *testing.T) {
			first := id.call()
			if first == "" {
				t.Fatalf("idempotencyKey(%q, %q, %q, %q) = empty string, want a key",
					id.bucket, id.key, id.versionID, id.etag)
			}
			for i := 0; i < 100; i++ {
				if got := id.call(); got != first {
					t.Fatalf("call #%d: idempotencyKey(%q, %q, %q, %q) = %q, want %q (not deterministic)",
						i+2, id.bucket, id.key, id.versionID, id.etag, got, first)
				}
			}
		})
	}
}

// TestIdempotencyKeyFormat pins the output contract: a constant-width,
// lowercase-hex digest. This is what makes the key safe to use verbatim as a
// downstream map/cache/record key regardless of what arbitrary bytes the S3
// object key contained.
func TestIdempotencyKeyFormat(t *testing.T) {
	// 64 lowercase hex chars == SHA-256, hex encoded.
	format := regexp.MustCompile(`^[0-9a-f]{64}$`)

	for _, id := range corpus() {
		t.Run(id.name, func(t *testing.T) {
			got := id.call()
			if !format.MatchString(got) {
				t.Errorf("idempotencyKey(%q, %q, %q, %q) = %q, want 64 lowercase hex chars matching %s",
					id.bucket, id.key, id.versionID, id.etag, got, format)
			}
		})
	}
}

// TestIdempotencyKeyGoldenVectors is deliberately different in kind from every
// other test in this file, and that difference is the point.
//
// The rest of the suite is BEHAVIORAL: it constrains determinism, output shape
// and structural distinctness while leaving the framing to the implementer.
// This test is a CROSS-VERSION STABILITY PIN. By asserting literal digests it
// fixes the concrete encoding:
//
//	sha256( for each field, in the order [bucket, key, versionID, etag]:
//	          uint64(len(fieldBytes)) written as 8 bytes, BIG-endian
//	          followed by fieldBytes, the raw UTF-8 of the field )
//	then hex-encoded, lowercase
//
// which means it also pins the hash algorithm, the field ORDER, the prefix
// WIDTH (8 bytes, not 4), the BYTE ORDER (big-endian, not little), the fact
// that the prefix counts UTF-8 BYTES and not runes, and the ABSENCE of any
// extra framing (no salt, no separator, no version tag, no terminator).
//
// Why pay the "change detector" cost here, when the rest of the suite
// deliberately avoids it? Because from WR-010 onward these keys are PERSISTED
// in a seen-events store. The digest stops being an internal detail the moment
// it becomes the primary key of durable state that outlives the process. A
// silent change to the framing — reordering two fields, narrowing the prefix to
// 4 bytes, switching endianness, prepending a schema tag — would leave every
// other test in this file green, because all of them still hold under the new
// encoding. But on upgrade every already-processed event would hash to a new
// key, miss in the store, and be reprocessed: a full replay storm, which is
// exactly the failure this task exists to prevent. So the format is pinned
// here, and any INTENTIONAL change to it must break this test loudly and be
// handled as a versioned migration of the store rather than slipping through.
//
// The expected digests below were computed INDEPENDENTLY of the Go
// implementation: the byte stream was constructed by hand and hashed with
// Python's hashlib, GNU coreutils sha256sum and openssl, all three agreeing.
// They are a specification, not an echo of whatever the code happens to emit.
func TestIdempotencyKeyGoldenVectors(t *testing.T) {
	cases := []struct {
		identity
		want string
	}{
		{
			// The vector the format hangs on: all four fields exercised, a
			// legitimately empty field (unversioned bucket), and multibyte
			// UTF-8 in the object key — accented Latin, CJK and an
			// astral-plane emoji, so the key's byte length (41) differs from
			// its rune length (31). A rune-counting or UTF-16-counting length
			// prefix would produce a different digest and fail right here.
			identity: identity{
				name:      "multibyte UTF-8 key, unversioned bucket, multipart etag",
				bucket:    "weir-uploads",
				key:       "raw/relatórios/ação-日本語-🎬.mp4",
				versionID: "",
				etag:      "9bb58f26192e4ba00f01e2e7b136bbd8-42",
			},
			want: "acce830f88b6f7f6cbf094ab2e12ff2e00411a2959c2029efac27a0596283a5b",
		},
		{
			// All four fields non-empty and of different lengths, which pins
			// field ORDER most sharply: permuting any two fields changes the
			// byte stream and therefore the digest.
			identity: identity{
				name:      "all four fields non-empty (pins field order)",
				bucket:    "uploads",
				key:       "raw/video1.mp4",
				versionID: "3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",
				etag:      "d41d8cd98f00b204e9800998ecf8427e",
			},
			want: "ce7ebc632f9654cd1da4fd9f9e4cd970a12b655246e4f1dcac431f22a42c8b97",
		},
		{
			// The degenerate case pins that empty fields are FRAMED, not
			// skipped: the preimage is four 8-byte zero prefixes and nothing
			// else, i.e. 32 NUL bytes. Its digest is the widely published
			// SHA-256 of 32 zero bytes, so this literal is verifiable at a
			// glance against any external reference.
			identity: identity{
				name:      "all fields empty (pins that empty fields are framed, not omitted)",
				bucket:    "",
				key:       "",
				versionID: "",
				etag:      "",
			},
			want: "66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.call(); got != tc.want {
				t.Errorf("idempotencyKey(%q, %q, %q, %q) = %q, want %q\n"+
					"the key framing changed. If this was intentional, note that keys are persisted "+
					"from WR-010 on: every previously processed event will hash differently and be "+
					"reprocessed, so the change needs a versioned migration of the seen-events store, "+
					"not just a new literal here",
					tc.bucket, tc.key, tc.versionID, tc.etag, got, tc.want)
			}
		})
	}

	// Guard the guard: a future edit that "simplifies" the multibyte vector's
	// inputs would silently drop the UTF-8 coverage the golden digest is there
	// to protect. Assert the vector still is what its comment claims.
	t.Run("multibyte vector still carries multibyte content", func(t *testing.T) {
		key := cases[0].key
		if byteLen, runeLen := len(key), utf8.RuneCountInString(key); byteLen == runeLen {
			t.Errorf("golden vector key %q is pure ASCII (%d bytes == %d runes); "+
				"it must contain multibyte UTF-8 for this vector to pin byte-length framing",
				key, byteLen, runeLen)
		}
	})
}

// TestIdempotencyKeyDistinctInputsProduceDistinctKeys is the "distinct inputs
// -> distinct keys" half of the Done-when, checked globally: every pair of
// distinct tuples in the corpus must produce distinct keys. Written as a
// collision map rather than a baseline comparison so that varying any single
// field, swapping two fields, or moving a value between versionID and etag all
// get caught by the same assertion.
func TestIdempotencyKeyDistinctInputsProduceDistinctKeys(t *testing.T) {
	seen := make(map[string]identity)

	for _, id := range corpus() {
		got := id.call()
		if prev, dup := seen[got]; dup {
			t.Errorf("collision: %q (%q, %q, %q, %q) and %q (%q, %q, %q, %q) both produced key %q",
				prev.name, prev.bucket, prev.key, prev.versionID, prev.etag,
				id.name, id.bucket, id.key, id.versionID, id.etag,
				got)
			continue
		}
		seen[got] = id
	}
}

// TestIdempotencyKeyDelimiterCollisionSafety is the sharp edge of this task.
//
// The tempting implementation is strings.Join([]string{bucket, key, versionID,
// etag}, sep) followed by a hash. That is ambiguous for EVERY choice of sep,
// because content can straddle the separator: with sep == "/", the tuples
//
//	(bucket "a", key "b/c", version "v", etag "e")
//	(bucket "a/b", key "c",  version "v", etag "e")
//
// both join to "a/b/c/v/e" — two different object identities collapsing to one
// key, which means a genuinely new event gets silently swallowed as a duplicate
// and never processed. Losing work is far worse than reprocessing it.
//
// The table below generates that straddle across every plausible separator
// choice (including the "surely a key can't contain this" ones: NUL, control
// bytes, newline) and across all three field boundaries, plus the
// "skip empty fields" variant. The only way to pass the whole table is
// unambiguous framing — length-prefixed fields, or per-field hashing — so this
// test drives the design rather than just checking it.
func TestIdempotencyKeyDelimiterCollisionSafety(t *testing.T) {
	separators := []struct {
		name string
		sep  string
	}{
		{"empty (bare concatenation)", ""},
		{"slash", "/"},
		{"colon", ":"},
		{"pipe", "|"},
		{"hyphen", "-"},
		{"underscore", "_"},
		{"hash", "#"},
		{"dot", "."},
		{"comma", ","},
		{"space", " "},
		{"newline", "\n"},
		{"tab", "\t"},
		{"NUL byte", "\x00"},
		{"unit separator", "\x1f"},
		{"double colon", "::"},
		{"multi-char sentinel", "|::|"},
	}

	// Each boundary is a pair of tuple builders that a naive sep-join would
	// flatten to the same string.
	boundaries := []struct {
		name string
		// mid is the fragment that straddles the boundary.
		build func(sep, mid string) (a, b identity)
	}{
		{
			name: "content straddles the bucket/key boundary",
			build: func(sep, mid string) (identity, identity) {
				return identity{bucket: "a", key: mid + sep + "c", versionID: "v", etag: "e"},
					identity{bucket: "a" + sep + mid, key: "c", versionID: "v", etag: "e"}
			},
		},
		{
			name: "content straddles the key/versionID boundary",
			build: func(sep, mid string) (identity, identity) {
				return identity{bucket: "a", key: "b" + sep + mid, versionID: "v", etag: "e"},
					identity{bucket: "a", key: "b", versionID: mid + sep + "v", etag: "e"}
			},
		},
		{
			name: "content straddles the versionID/etag boundary",
			build: func(sep, mid string) (identity, identity) {
				return identity{bucket: "a", key: "b", versionID: "v" + sep + mid, etag: "e"},
					identity{bucket: "a", key: "b", versionID: "v", etag: mid + sep + "e"}
			},
		},
	}

	mids := []struct {
		name string
		mid  string
	}{
		{"non-empty straddling fragment", "b"},
		{"empty straddling fragment", ""},
	}

	for _, sep := range separators {
		for _, boundary := range boundaries {
			for _, m := range mids {
				if sep.sep == "" && m.mid == "" {
					// Degenerate: with nothing to move across the boundary and
					// no separator, both builders produce the identical tuple,
					// so there is no distinct pair to compare.
					continue
				}
				name := fmt.Sprintf("sep=%s/%s/%s", sep.name, boundary.name, m.name)
				t.Run(name, func(t *testing.T) {
					a, b := boundary.build(sep.sep, m.mid)
					if a.tuple() == b.tuple() {
						t.Fatalf("bad test case: both tuples are identical (%q)", a.tuple())
					}
					keyA, keyB := a.call(), b.call()
					if keyA == keyB {
						t.Errorf("collision: (%q, %q, %q, %q) and (%q, %q, %q, %q) both produced key %q; "+
							"the field framing is ambiguous for separator %q",
							a.bucket, a.key, a.versionID, a.etag,
							b.bucket, b.key, b.versionID, b.etag,
							keyA, sep.sep)
					}
				})
			}
		}
	}

	// Separate shape: an implementation that omits empty fields (rather than
	// framing them) collapses these two distinct identities. An unversioned
	// object whose etag is X is NOT the same object as a versioned object
	// whose versionID is X.
	t.Run("empty field is framed, not omitted", func(t *testing.T) {
		withVersion := identity{bucket: "a", key: "b", versionID: "x", etag: ""}
		withETag := identity{bucket: "a", key: "b", versionID: "", etag: "x"}
		if got, other := withVersion.call(), withETag.call(); got == other {
			t.Errorf("collision: versionID=%q/etag=%q and versionID=%q/etag=%q both produced key %q",
				withVersion.versionID, withVersion.etag, withETag.versionID, withETag.etag, got)
		}
	})
}

// TestIdempotencyKeyUnversionedObjectIsStableAndDistinct isolates the
// unversioned-bucket case called out in the Done-when discussion: versionID ==
// "" is a normal, expected input (not an error), it must be deterministic, and
// it must not be conflated with the same object carrying a real versionID.
func TestIdempotencyKeyUnversionedObjectIsStableAndDistinct(t *testing.T) {
	const (
		bucket = "uploads"
		key    = "raw/video1.mp4"
		etag   = "d41d8cd98f00b204e9800998ecf8427e"
	)

	unversioned := idempotencyKey(bucket, key, "", etag)
	if unversioned == "" {
		t.Fatal("idempotencyKey with an empty versionID returned an empty key, want a valid key")
	}
	if again := idempotencyKey(bucket, key, "", etag); again != unversioned {
		t.Errorf("empty versionID is not deterministic: got %q then %q", unversioned, again)
	}

	versioned := idempotencyKey(bucket, key, "PHtexPGjH2y.zBgT8LmB7wwLI2mpbz.k", etag)
	if versioned == unversioned {
		t.Errorf("empty and non-empty versionID produced the same key %q for the same bucket/key/etag",
			unversioned)
	}
}
