package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment variable names the Deployment sets to configure the process
// (decision q29 — cluster-admin owned). They override Defaults(); anything unset
// keeps its default. Env is used instead of custom command-line flags because
// the kube-scheduler command owns its own flag set and rejects unknown flags.
const (
	EnvSchedulerName    = "AD_SCHEDULER_NAME"
	EnvNodePoolSelector = "AD_NODE_POOL_LABEL_SELECTOR"
	EnvNodePoolTaintKey = "AD_NODE_POOL_TAINT_KEY"
	EnvNodePoolMode     = "AD_NODE_POOL_MODE"
	EnvDRAFailClosed    = "AD_DRA_FAIL_CLOSED"
	EnvReservationDelay = "AD_RESERVATION_DELAY"
	EnvGangTimeout      = "AD_GANG_SCHEDULE_TIMEOUT"
	EnvNodeSortPolicy   = "AD_NODE_SORT_POLICY"
	EnvMetricAllowlist  = "AD_METRIC_DIM_ALLOWLIST"
	EnvAutoCountQuota   = "AD_AUTO_COUNT_QUOTA"
	EnvCountPodsLimit   = "AD_COUNT_PODS_LIMIT"
	EnvPendingTTL       = "AD_PENDING_TTL"
	EnvAutoscaleDemand  = "AD_AUTOSCALE_DEMAND"
)

// FromEnv returns Defaults() with any AD_* environment overrides applied. The
// capacity plugin calls this in its factory so all extension points share one
// consistent process configuration.
func FromEnv() ProcessConfig {
	c := Defaults()
	if v := os.Getenv(EnvSchedulerName); v != "" {
		c.SchedulerName = v
	}
	if v := os.Getenv(EnvNodePoolSelector); v != "" {
		c.NodePool.LabelSelector = v
	}
	if v := os.Getenv(EnvNodePoolTaintKey); v != "" {
		c.NodePool.TaintKey = v
	}
	if v := os.Getenv(EnvNodePoolMode); v != "" {
		switch NodePoolMode(v) {
		case NodePoolExclusive, NodePoolShared:
			c.NodePool.Mode = NodePoolMode(v)
		}
	}
	if v := os.Getenv(EnvDRAFailClosed); v == "true" || v == "1" {
		c.DRAFailClosed = true
	}
	if v := os.Getenv(EnvReservationDelay); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			c.ReservationDelay = d
		}
	}
	if v := os.Getenv(EnvGangTimeout); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.GangScheduleTimeout = d
		}
	}
	if v := os.Getenv(EnvNodeSortPolicy); v != "" {
		c.NodeSortPolicy = v
	}
	if v := os.Getenv(EnvMetricAllowlist); v != "" {
		c.MetricDimensionAllowlist = nil
		for d := range strings.SplitSeq(v, ",") {
			if d = strings.TrimSpace(d); d != "" {
				c.MetricDimensionAllowlist = append(c.MetricDimensionAllowlist, d)
			}
		}
	}
	if v := os.Getenv(EnvAutoCountQuota); v == "true" || v == "1" {
		c.AutoCountQuota = true
	}
	if v := os.Getenv(EnvCountPodsLimit); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			c.CountPodsLimit = n
		}
	}
	if v := os.Getenv(EnvPendingTTL); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.PendingTTL = d
		}
	}
	if v := os.Getenv(EnvAutoscaleDemand); v == "true" || v == "1" {
		c.AutoscaleDemand = true
	}
	return c
}
