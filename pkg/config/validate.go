package config

import (
	"fmt"
	"strings"
)

// Validate checks the process configuration for internally-consistent, usable
// values. It does NOT touch the queue tree (that is ValidateTree in the engine,
// M2) — this is the process-knob invariant check only.
func (c ProcessConfig) Validate() error {
	// These values are interpolated into generated CEL string literals (VAP/MAP
	// policies, see pkg/admission); a quote, backslash or control character would
	// break out of the literal, so reject them up front.
	for name, v := range map[string]string{
		"scheduler-name":           c.SchedulerName,
		"node-pool-label-selector": c.NodePool.LabelSelector,
		"node-pool-taint-key":      c.NodePool.TaintKey,
	} {
		if err := checkCELSafe(v); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if strings.TrimSpace(c.SchedulerName) == "" {
		return fmt.Errorf("scheduler-name must not be empty")
	}
	switch c.NodePool.Mode {
	case NodePoolExclusive, NodePoolShared:
	default:
		return fmt.Errorf("node-pool mode must be exclusive|shared, got %q", c.NodePool.Mode)
	}
	if strings.TrimSpace(c.NodePool.LabelSelector) == "" {
		// The pool label is the source of truth for capacity; without it the
		// label-filtered informer would consider the whole cluster (q20).
		return fmt.Errorf("node-pool-label-selector must not be empty (it bounds the capacity pool)")
	}
	if c.NodePool.Mode == NodePoolExclusive && strings.TrimSpace(c.NodePool.TaintKey) == "" {
		return fmt.Errorf("node-pool-taint-key must be set in exclusive mode (it enforces exclusivity)")
	}
	switch c.TreeInvalidityPolicy {
	case InvaliditySubtreeLocal, InvalidityWholeTree:
	default:
		return fmt.Errorf("tree-invalidity-policy must be subtree|whole, got %q", c.TreeInvalidityPolicy)
	}
	if c.GangScheduleTimeout <= 0 {
		return fmt.Errorf("gang-schedule-timeout must be positive, got %s", c.GangScheduleTimeout)
	}
	return nil
}

// checkCELSafe rejects characters that could terminate or escape a quoted CEL
// string literal.
func checkCELSafe(v string) error {
	for _, r := range v {
		if r == '\'' || r == '"' || r == '\\' || r == '`' || r < 0x20 || r == 0x7f {
			return fmt.Errorf("contains forbidden character %q (would be interpolated into a CEL literal)", r)
		}
	}
	return nil
}
