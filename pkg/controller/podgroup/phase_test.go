package podgroup

import "testing"

func TestObservedPhase(t *testing.T) {
	cases := []struct {
		name string
		o    Observed
		want string
	}{
		{"not admitted -> pending", Observed{MinMember: 3}, PhasePending},
		{"admitted, assembling -> scheduling", Observed{Admitted: true, Bound: 1, MinMember: 3}, PhaseScheduling},
		{"minMember placed -> running", Observed{Admitted: true, Bound: 3, MinMember: 3}, PhaseRunning},
		{"all succeeded -> completed", Observed{Admitted: true, Bound: 3, Succeeded: 3, MinMember: 3}, PhaseCompleted},
		{"failed wins over running", Observed{Admitted: true, Bound: 3, MinMember: 3, Failed: true}, PhaseFailed},
		{"failed wins over pending", Observed{Failed: true, MinMember: 3}, PhaseFailed},
		{"completed wins over running (partial succeed still running)", Observed{Admitted: true, Bound: 3, Succeeded: 2, MinMember: 3}, PhaseRunning},
		{"barrier-only (minMember 0) admitted -> scheduling", Observed{Admitted: true, MinMember: 0}, PhaseScheduling},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.o.Phase(); got != c.want {
				t.Fatalf("Phase() = %q; want %q", got, c.want)
			}
		})
	}
}
