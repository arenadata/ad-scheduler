package placement

import (
	"errors"
	"testing"

	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

func routingTree(t *testing.T) *queue.QueueManager {
	t.Helper()
	m, err := queue.NewManager(&queue.Spec{
		Children: []*queue.Spec{
			{Name: "team", Children: []*queue.Spec{
				{Name: "spark", ServiceAccounts: []string{"spark-sa"}},
				{Name: "default", ServiceAccounts: []string{"*"}}, // legacy alias
			}},
			// flag namespace uses the explicit spec.default marker instead of ["*"].
			{Name: "flag", Children: []*queue.Spec{
				{Name: "spark", ServiceAccounts: []string{"spark-sa"}},
				{Name: "fallback", Default: true},
			}},
			{Name: "solo", ServiceAccounts: []string{"solo-sa"}}, // namespace node is itself a leaf
		},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return m
}

func TestResolve(t *testing.T) {
	m := routingTree(t)
	cases := []struct {
		ns, sa string
		want   string
		err    bool
	}{
		{"team", "spark-sa", "root.team.spark", false},    // exact
		{"team", "unmapped", "root.team.default", false},  // ["*"] alias catches it
		{"flag", "spark-sa", "root.flag.spark", false},    // exact wins over default marker
		{"flag", "unmapped", "root.flag.fallback", false}, // spec.default catches it
		{"solo", "solo-sa", "root.solo", false},           // exact on namespace-leaf
		{"solo", "other", "", true},                       // no default queue -> Reject
		{"absent", "x", "", true},                         // namespace not onboarded
	}
	for _, c := range cases {
		got, err := Resolve(m, util.Identity{Namespace: c.ns, ServiceAccount: c.sa})
		if c.err {
			if !errors.Is(err, ErrNoQueue) {
				t.Errorf("(%s,%s): want ErrNoQueue, got %v/%q", c.ns, c.sa, err, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("(%s,%s) = %q,%v; want %q", c.ns, c.sa, got, err, c.want)
		}
	}
}
