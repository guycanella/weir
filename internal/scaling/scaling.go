// Package scaling holds the pure decision core for backlog-driven scaling
// (ADR-002): given a queue depth, decide how many worker replicas should run.
package scaling

// ScalingConfig captures the scaling knobs from a ProcessingPipeline's spec:
// the replica floor and ceiling, and how many backlog messages each replica
// is expected to handle.
type ScalingConfig struct {
	Min        int
	Max        int
	PerReplica int
}

// desiredReplicas computes the replica count for the given backlog, rounding
// up on any remainder (see the test file's header comment for the full
// rationale) and clamping the result into [cfg.Min, cfg.Max]. cfg.PerReplica
// is assumed positive; CR spec validation (WR-009) enforces that upstream.
// The ceiling is computed via quotient+remainder rather than
// (backlog+PerReplica-1)/PerReplica: the latter overflows for backlog near
// math.MaxInt, wrapping negative and defeating the Max clamp below.
func desiredReplicas(backlog int, cfg ScalingConfig) int {
	if backlog <= 0 {
		return cfg.Min
	}

	raw := backlog / cfg.PerReplica
	if backlog%cfg.PerReplica != 0 {
		raw++
	}

	if raw < cfg.Min {
		return cfg.Min
	}
	if raw > cfg.Max {
		return cfg.Max
	}
	return raw
}
