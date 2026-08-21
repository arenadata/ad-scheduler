package config

import (
	"flag"
	"testing"
	"time"
)

func TestDefaultsAreValid(t *testing.T) {
	if err := Defaults().Validate(); err != nil {
		t.Fatalf("default config must validate: %v", err)
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name  string
		mutfn func(*ProcessConfig)
	}{
		{"empty scheduler name", func(c *ProcessConfig) { c.SchedulerName = "" }},
		{"empty pool label", func(c *ProcessConfig) { c.NodePool.LabelSelector = "" }},
		{"bad mode", func(c *ProcessConfig) { c.NodePool.Mode = "weird" }},
		{"exclusive without taint", func(c *ProcessConfig) { c.NodePool.TaintKey = "" }},
		{"bad invalidity policy", func(c *ProcessConfig) { c.TreeInvalidityPolicy = "nope" }},
		{"non-positive gang timeout", func(c *ProcessConfig) { c.GangScheduleTimeout = 0 }},
	}
	for _, tc := range cases {
		c := Defaults()
		tc.mutfn(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected validation error, got nil", tc.name)
		}
	}
}

func TestSharedModeWithoutTaintIsValid(t *testing.T) {
	c := Defaults()
	c.NodePool.Mode = NodePoolShared
	c.NodePool.TaintKey = ""
	if err := c.Validate(); err != nil {
		t.Fatalf("shared mode may omit the taint key: %v", err)
	}
}

func TestRegisterFlags(t *testing.T) {
	c := Defaults()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c.RegisterFlags(fs)
	args := []string{
		"--scheduler-name=my-sched",
		"--node-pool-mode=shared",
		"--gang-schedule-timeout=90s",
		"--tree-invalidity-policy=whole",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.SchedulerName != "my-sched" {
		t.Errorf("scheduler-name = %q", c.SchedulerName)
	}
	if c.NodePool.Mode != NodePoolShared {
		t.Errorf("mode = %q", c.NodePool.Mode)
	}
	if c.GangScheduleTimeout != 90*time.Second {
		t.Errorf("gang timeout = %s", c.GangScheduleTimeout)
	}
	if c.TreeInvalidityPolicy != InvalidityWholeTree {
		t.Errorf("invalidity policy = %q", c.TreeInvalidityPolicy)
	}
	// untouched flags keep their defaults
	if c.NodePool.LabelSelector != Defaults().NodePool.LabelSelector {
		t.Errorf("label selector should keep default, got %q", c.NodePool.LabelSelector)
	}
}

func TestRegisterFlagsRejectsBadEnum(t *testing.T) {
	c := Defaults()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(new(nopWriter))
	c.RegisterFlags(fs)
	if err := fs.Parse([]string{"--node-pool-mode=bogus"}); err == nil {
		t.Fatal("bogus mode must fail at parse")
	}
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestFromEnvDurationOverrides(t *testing.T) {
	t.Setenv(EnvReservationDelay, "20s")
	t.Setenv(EnvGangTimeout, "45s")
	t.Setenv(EnvNodeSortPolicy, "binpacking")
	t.Setenv(EnvAutoCountQuota, "true")
	t.Setenv(EnvCountPodsLimit, "1234")
	t.Setenv(EnvPendingTTL, "90s")
	c := FromEnv()
	if c.ReservationDelay != 20*time.Second {
		t.Errorf("ReservationDelay = %v, want 20s", c.ReservationDelay)
	}
	if c.PendingTTL != 90*time.Second {
		t.Errorf("PendingTTL = %v, want 90s", c.PendingTTL)
	}
	if !c.AutoCountQuota || c.CountPodsLimit != 1234 {
		t.Errorf("auto-count-quota env not applied: %v/%d", c.AutoCountQuota, c.CountPodsLimit)
	}
	if c.NodeSortPolicy != "binpacking" {
		t.Errorf("NodeSortPolicy = %q, want binpacking", c.NodeSortPolicy)
	}
	if c.GangScheduleTimeout != 45*time.Second {
		t.Errorf("GangScheduleTimeout = %v, want 45s", c.GangScheduleTimeout)
	}
	// invalid values are ignored (keep defaults).
	t.Setenv(EnvReservationDelay, "not-a-duration")
	if d := FromEnv().ReservationDelay; d != Defaults().ReservationDelay {
		t.Errorf("invalid duration should keep default, got %v", d)
	}
}
