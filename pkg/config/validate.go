package config

import (
	"fmt"
	"strings"
)

// Validate checks the process configuration for internally-consistent, usable
// values. It does NOT touch the queue tree (that is ValidateTree in the engine,
// M2) — this is the process-knob invariant check only.
func (c ProcessConfig) Validate() error {
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
