package framework

import (
	"testing"
	"time"
)

func TestGangTimeoutVerdict(t *testing.T) {
	const (
		mrTO  = 10 * time.Second // membersReady
		schTO = 30 * time.Second // schedule (Hard)
		min   = 3
	)
	cases := []struct {
		name             string
		elapsed          time.Duration
		mr, sch          time.Duration
		created, bound   int
		wantFail         bool
		wantReasonSubstr string
	}{
		{"neither deadline reached", 5 * time.Second, mrTO, schTO, 1, 0, false, ""},
		{"members not materialized in time", 12 * time.Second, mrTO, schTO, 2, 0, true, "materialized"},
		{"materialized in time, still binding", 12 * time.Second, mrTO, schTO, 3, 1, false, ""},
		{"materialized but never assembled", 31 * time.Second, mrTO, schTO, 3, 2, true, "assembled"},
		{"fully assembled by schedule TO", 31 * time.Second, mrTO, schTO, 3, 3, false, ""},
		{"membersReady disabled, schedule fails", 31 * time.Second, 0, schTO, 1, 0, true, "assembled"},
		{"membersReady disabled, within schedule", 20 * time.Second, 0, schTO, 1, 0, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fail, reason := gangTimeoutVerdict(c.elapsed, c.mr, c.sch, c.created, c.bound, min)
			if fail != c.wantFail {
				t.Fatalf("fail = %v; want %v (reason %q)", fail, c.wantFail, reason)
			}
			if c.wantReasonSubstr != "" && !contains(reason, c.wantReasonSubstr) {
				t.Fatalf("reason %q does not mention %q", reason, c.wantReasonSubstr)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
