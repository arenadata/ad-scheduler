/*
Package config holds ad-scheduler's global/process configuration.

Per decision q29 there is no ConfigMap and no config CRD: process-level settings
(scheduler name, dedicated node pool, metric-dimension allowlist, feature flags)
are supplied as Deployment flags/env and owned by the cluster-admin. The queue
tree itself is NOT here — it is assembled from namespaced `Queue` CRDs by
controller/queue (see pkg/framework). This package owns only the process knobs
and the K8s-agnostic invariant checks.
*/
package config

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

// NodePoolMode selects how the dedicated node pool is shared (decision q20).
type NodePoolMode string

const (
	// NodePoolExclusive is the MVP default: pool nodes carry a NoSchedule taint
	// so only our tolerating pods land there; capacity is deterministic.
	NodePoolExclusive NodePoolMode = "exclusive"
	// NodePoolShared (post-MVP, gated at M6) lets pool nodes be shared with the
	// default-scheduler; requires optimistic concurrency + PreBind re-validate.
	NodePoolShared NodePoolMode = "shared"
)

// TreeInvalidityPolicy selects how controller/queue reacts to an invalid node
// while assembling the tree (decision q31).
type TreeInvalidityPolicy string

const (
	// InvaliditySubtreeLocal (default) quarantines only the affected subtree.
	InvaliditySubtreeLocal TreeInvalidityPolicy = "subtree"
	// InvalidityWholeTree rejects the whole candidate and keeps last-good.
	InvalidityWholeTree TreeInvalidityPolicy = "whole"
)

// NodePoolConfig describes the dedicated node pool (decision q20).
type NodePoolConfig struct {
	// LabelSelector selects pool membership; it is the source of truth for
	// cluster capacity (e.g. "scheduler.arenadata.io/name=ad-scheduler").
	LabelSelector string
	// Mode is exclusive (MVP) or shared (post-MVP).
	Mode NodePoolMode
	// TaintKey is the key of the NoSchedule taint enforcing exclusivity
	// (e.g. "scheduler.arenadata.io/dedicated").
	TaintKey string
}

// PoolLabelValue splits the pool LabelSelector into its single membership label
// key and value (the "key=value" form the MutatingAdmissionPolicy stamps and the
// off-pool Filter matches). It is the single source both the MAP codegen and its
// sync test read, so the stamped nodeSelector cannot drift from the capacity
// label. ok is false when the selector is not a bare key=value pair (e.g. a set-
// based selector), in which case the MAP cannot be generated from it.
func (n NodePoolConfig) PoolLabelValue() (label, value string, ok bool) {
	kv := strings.SplitN(n.LabelSelector, "=", 2)
	if len(kv) != 2 {
		return "", "", false
	}
	label, value = strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
	if label == "" || value == "" {
		return "", "", false
	}
	return label, value, true
}

// ProcessConfig is the full set of process-level knobs (decision q29).
type ProcessConfig struct {
	// SchedulerName is the scheduler-name this process is responsible for.
	SchedulerName string
	// NodePool is the dedicated node pool selection/enforcement.
	NodePool NodePoolConfig
	// MetricDimensionAllowlist bounds which resource dimensions are exported as
	// metric labels (TSDB cardinality guard, incl. dra/*).
	MetricDimensionAllowlist []string
	// GangScheduleTimeout is the default gang admission TTL (= Permit-Wait).
	GangScheduleTimeout time.Duration
	// ReservationDelay is the node-reservation grace before preemption kicks in.
	ReservationDelay time.Duration
	// TreeInvalidityPolicy is the default reaction to an invalid tree node.
	TreeInvalidityPolicy TreeInvalidityPolicy
	// ReconcileDebounce coalesces bursts of Queue changes before a rebuild.
	ReconcileDebounce time.Duration
	// DRAFailClosed controls unresolvable DRA requests (allocationMode:All /
	// firstAvailable): true rejects the pod, false (default) accounts what is
	// statically countable and lets the rest through (decision q11).
	DRAFailClosed bool
	// NodeSortPolicy is how AdCapacity's Score ranks feasible nodes by DRF
	// dominant utilisation: "spread" (default) or "binpacking".
	NodeSortPolicy string
	// AutoCountQuota, when true, makes the reconciler ensure a count-only
	// ResourceQuota (a DoS backstop on pod count) in every participating
	// namespace. Off by default — the cluster-admin owns quotas (q27); this is an
	// opt-in convenience distinct from the resource envelope.
	AutoCountQuota bool
	// CountPodsLimit is the count/pods hard limit for the auto count-quota.
	CountPodsLimit int64
	// PendingTTL, when > 0, makes the reconciler evict our pods that have stayed
	// unschedulable longer than this (a DoS/stuck-pod backstop). 0 = off (default).
	PendingTTL time.Duration
	// AutoscaleDemand, when true, makes the reconciler publish aggregate node
	// demand for admitted-but-unplaceable gangs as Cluster-Autoscaler
	// ProvisioningRequests (best-effort-atomic), so a gang gets its N nodes
	// provisioned as one unit instead of the pool growing member-by-member.
	// Off by default — needs the ProvisioningRequest CRD + CA configured with
	// --enable-provisioning-requests.
	AutoscaleDemand bool
}

// Defaults returns a ProcessConfig with the documented default knobs.
func Defaults() ProcessConfig {
	return ProcessConfig{
		SchedulerName: "ad-scheduler",
		NodePool: NodePoolConfig{
			LabelSelector: "scheduler.arenadata.io/name=ad-scheduler",
			Mode:          NodePoolExclusive,
			TaintKey:      "scheduler.arenadata.io/dedicated",
		},
		GangScheduleTimeout:  300 * time.Second,
		ReservationDelay:     2 * time.Second,
		TreeInvalidityPolicy: InvaliditySubtreeLocal,
		ReconcileDebounce:    500 * time.Millisecond,
		NodeSortPolicy:       "spread",
		CountPodsLimit:       5000,
	}
}

// RegisterFlags binds ProcessConfig fields to fs, starting from c's current
// values as defaults. Call with a fresh ProcessConfig{} filled from Defaults()
// to get the documented defaults on the command line.
func (c *ProcessConfig) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.SchedulerName, "scheduler-name", c.SchedulerName, "scheduler-name this process is responsible for")
	fs.StringVar(&c.NodePool.LabelSelector, "node-pool-label-selector", c.NodePool.LabelSelector, "label selector for dedicated pool membership (source of truth for capacity)")
	fs.Func("node-pool-mode", "dedicated node pool mode: exclusive|shared", func(v string) error {
		switch NodePoolMode(v) {
		case NodePoolExclusive, NodePoolShared:
			c.NodePool.Mode = NodePoolMode(v)
			return nil
		default:
			return fmt.Errorf("must be exclusive|shared, got %q", v)
		}
	})
	fs.StringVar(&c.NodePool.TaintKey, "node-pool-taint-key", c.NodePool.TaintKey, "taint key enforcing pool exclusivity (NoSchedule)")
	fs.DurationVar(&c.GangScheduleTimeout, "gang-schedule-timeout", c.GangScheduleTimeout, "default gang admission TTL")
	fs.DurationVar(&c.ReservationDelay, "reservation-delay", c.ReservationDelay, "node reservation grace before preemption")
	fs.DurationVar(&c.ReconcileDebounce, "reconcile-debounce", c.ReconcileDebounce, "debounce window for coalesced tree rebuilds")
	fs.Func("tree-invalidity-policy", "invalid-node policy: subtree|whole", func(v string) error {
		switch TreeInvalidityPolicy(v) {
		case InvaliditySubtreeLocal, InvalidityWholeTree:
			c.TreeInvalidityPolicy = TreeInvalidityPolicy(v)
			return nil
		default:
			return fmt.Errorf("must be subtree|whole, got %q", v)
		}
	})
}
