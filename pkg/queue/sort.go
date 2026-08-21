package queue

// SortPolicy selects how a leaf orders its runnable applications. It is the
// engine-side of the single process-wide QueueSort (PrioritySort is disabled):
// the queuesort plugin builds an AppOrder per schedulable unit and ranks with
// Less, so gang members cluster and the policy decides the rest.
type SortPolicy int

const (
	// SortFIFO orders by submit time (oldest first). Default.
	SortFIFO SortPolicy = iota
	// SortFair orders by DRF dominant share (least-satisfied first).
	SortFair
	// SortStateAware orders like FIFO; the "one starting app at a time" gate is
	// enforced separately at admission, not in the comparator.
	SortStateAware
)

// ParseSortPolicy maps the CRD's applicationSortPolicy string to a SortPolicy.
// Unknown/empty falls back to FIFO.
func ParseSortPolicy(s string) SortPolicy {
	switch s {
	case "fair":
		return SortFair
	case "stateaware":
		return SortStateAware
	default:
		return SortFIFO
	}
}

// AppOrder is the pod-free read-model the comparator ranks. The queuesort plugin
// builds one per schedulable unit. Gang members share a GangID so they sort as
// one contiguous block, and the block's representative key (GangPriority,
// GangSubmitNanos, GangID) makes the whole gang move together. For fair policy,
// DominantShare is the owning application's aggregate share — identical across a
// gang's members so the block ranks consistently.
type AppOrder struct {
	DominantShare   float64 // app dominant share (SortFair); caller computes
	Priority        int32
	SubmitNanos     int64
	UID             string
	GangID          string // "" = not a gang member
	GangPriority    int32
	GangSubmitNanos int64
}

// blockID identifies the contiguous ordering block a unit belongs to: its gang,
// or itself when solo. Units with equal blockID are ranked among themselves;
// units with different blockID are ranked by reprLess.
func (a AppOrder) blockID() string {
	if a.GangID != "" {
		return "g/" + a.GangID
	}
	return "p/" + a.UID
}

// repr returns the block's representative (priority, submitNanos): the gang's for
// a member, the unit's own otherwise.
func (a AppOrder) repr() (int32, int64) {
	if a.GangID != "" {
		return a.GangPriority, a.GangSubmitNanos
	}
	return a.Priority, a.SubmitNanos
}

// Less reports whether a should be scheduled before b under policy. It is a
// strict weak ordering (usable as a sort.Slice / heap comparator):
//
//   - different blocks are ranked by the policy key first (FIFO/StateAware:
//     submit time; Fair: dominant share), then a deterministic fallback of
//     (priority desc, submit asc, blockID asc);
//   - same block (same gang) members are ranked by (submit asc, UID asc) so the
//     gang stays contiguous and internally stable.
func Less(policy SortPolicy, a, b AppOrder) bool {
	ba, bb := a.blockID(), b.blockID()
	if ba == bb {
		if a.SubmitNanos != b.SubmitNanos {
			return a.SubmitNanos < b.SubmitNanos
		}
		return a.UID < b.UID
	}
	pa, sa := a.repr()
	pb, sb := b.repr()
	// Policy key is primary.
	switch policy {
	case SortFair:
		if a.DominantShare != b.DominantShare {
			return a.DominantShare < b.DominantShare // least-satisfied first
		}
	default: // SortFIFO, SortStateAware
		if sa != sb {
			return sa < sb // oldest block first
		}
	}
	// Mandatory fallback: priority desc, submit asc, blockID asc (stable).
	if pa != pb {
		return pa > pb
	}
	if sa != sb {
		return sa < sb
	}
	return ba < bb
}
