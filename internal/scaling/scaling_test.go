package scaling

import (
	"math"
	"testing"
)

// TestDesiredReplicas pins the behavior of the pure scaling-decision function
// desiredReplicas(backlog int, cfg ScalingConfig) int (DOCUMENTATION.md §3.4,
// §6.1). It is the functional core of ADR-002 (scale on backlog, from zero):
// given the current queue depth and the pipeline's scaling config, return the
// number of worker replicas the reconciler should drive the Deployment to.
//
// Design decisions this table pins down (so the implementer implements exactly
// this, not a guess):
//
//   - Rounding is CEILING. We never under-provision: if backlog=31 and each
//     replica targets 30 messages, one replica would leave 1 message unserved
//     and the backlog grows, so we round up to 2. Under-provisioning is the
//     costlier failure mode for a backlog regulator, so ties/remainders always
//     round toward more capacity.
//   - Clamps: the raw ceiling is clamped into [Min, Max]. Min wins when the
//     computed value is below it (including the scale-to-zero case Min=0), Max
//     wins when the computed value exceeds it.
//   - Non-positive backlog (0 or, defensively, negative) yields Min. A queue
//     depth is a physical count that cannot really be negative, but the pure
//     function must be total and deterministic for any int, so we treat any
//     non-positive backlog as "no work" → Min.
type testCase struct {
	name    string
	backlog int
	cfg     ScalingConfig
	want    int
}

func TestDesiredReplicas(t *testing.T) {
	// The canonical config from the example CR in DOCUMENTATION.md §3.4.
	docCfg := ScalingConfig{Min: 0, Max: 20, PerReplica: 30}

	cases := []testCase{
		// --- zero / non-positive backlog → Min branch ---
		{
			name:    "zero backlog scales to zero when Min is zero",
			backlog: 0,
			cfg:     docCfg,
			want:    0,
		},
		{
			name:    "zero backlog returns Min when Min is non-zero",
			backlog: 0,
			cfg:     ScalingConfig{Min: 2, Max: 20, PerReplica: 30},
			want:    2,
		},
		{
			name:    "negative backlog is treated as no work and returns Min",
			backlog: -5,
			cfg:     ScalingConfig{Min: 1, Max: 20, PerReplica: 30},
			want:    1,
		},
		{
			name:    "negative backlog with Min zero returns zero",
			backlog: -100,
			cfg:     docCfg,
			want:    0,
		},

		// --- scale-from-zero: any work with Min=0 needs at least one replica ---
		{
			name:    "single message from idle needs one replica",
			backlog: 1,
			cfg:     docCfg,
			want:    1,
		},

		// --- ceiling rounding (the load-bearing design decision) ---
		{
			name:    "backlog below perReplica rounds up to one replica",
			backlog: 29,
			cfg:     docCfg,
			want:    1,
		},
		{
			name:    "backlog equal to perReplica is exactly one replica",
			backlog: 30,
			cfg:     docCfg,
			want:    1,
		},
		{
			name:    "one over perReplica rounds up to two replicas",
			backlog: 31,
			cfg:     docCfg,
			want:    2,
		},
		{
			name:    "backlog equal to two perReplica units is exactly two replicas",
			backlog: 60,
			cfg:     docCfg,
			want:    2,
		},
		{
			name:    "remainder just below a multiple still rounds up",
			backlog: 89, // 89/30 = 2.966..., ceil -> 3
			cfg:     docCfg,
			want:    3,
		},
		{
			name:    "perReplica of one maps each message to a replica",
			backlog: 5,
			cfg:     ScalingConfig{Min: 0, Max: 20, PerReplica: 1},
			want:    5,
		},

		// --- Min clamp: computed value below Min ---
		{
			name:    "computed below Min is clamped up to Min",
			backlog: 5, // ceil(5/30) = 1, but Min = 3
			cfg:     ScalingConfig{Min: 3, Max: 20, PerReplica: 30},
			want:    3,
		},
		{
			name:    "computed exactly at Min returns Min",
			backlog: 30, // ceil(30/30) = 1 == Min
			cfg:     ScalingConfig{Min: 1, Max: 20, PerReplica: 30},
			want:    1,
		},

		// --- Max clamp: computed value above Max ---
		{
			name:    "backlog far above capacity is clamped to Max",
			backlog: 10000, // ceil(10000/30) = 334, clamped to 20
			cfg:     docCfg,
			want:    20,
		},
		{
			name:    "backlog exactly at Max capacity returns Max",
			backlog: 600, // Max(20) * perReplica(30) = 600 -> 20
			cfg:     docCfg,
			want:    20,
		},
		{
			name:    "backlog one over Max capacity is clamped to Max",
			backlog: 601, // ceil(601/30) = 21, clamped to 20
			cfg:     docCfg,
			want:    20,
		},
		{
			name:    "backlog one below Max capacity is not clamped",
			backlog: 570, // ceil(570/30) = 19, within [0,20]
			cfg:     docCfg,
			want:    19,
		},
		{
			// Regression: the ceiling formula (backlog+PerReplica-1)/PerReplica
			// overflows int at extreme backlogs and wraps negative, which would
			// wrongly clamp to Min. An enormous backlog must clamp to Max.
			name:    "backlog at math.MaxInt clamps to Max without overflow",
			backlog: math.MaxInt,
			cfg:     docCfg,
			want:    20,
		},

		// --- pass-through: Min <= computed <= Max ---
		{
			name:    "computed strictly inside the clamp range passes through",
			backlog: 150, // ceil(150/30) = 5
			cfg:     docCfg,
			want:    5,
		},

		// --- pinned replica count: Min == Max ---
		{
			name:    "fixed replicas (Min==Max) ignore backlog when there is work",
			backlog: 100000,
			cfg:     ScalingConfig{Min: 5, Max: 5, PerReplica: 30},
			want:    5,
		},
		{
			name:    "fixed replicas (Min==Max) still return that value at zero backlog",
			backlog: 0,
			cfg:     ScalingConfig{Min: 5, Max: 5, PerReplica: 30},
			want:    5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := desiredReplicas(tc.backlog, tc.cfg)
			if got != tc.want {
				t.Errorf("desiredReplicas(%d, %+v) = %d, want %d",
					tc.backlog, tc.cfg, got, tc.want)
			}
		})
	}
}
