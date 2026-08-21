package queue

import "strings"

// ACL is a queue submit/admin access list. Authorization is by namespace
// (decision: SA is a routing filter, not a security boundary — the boundary is
// the namespace + native RBAC). The wire form mirrors YuniKorn: a space- or
// comma-separated list of principals, where "*" means "everyone". Here a
// principal is a namespace name; "*" is the wildcard.
//
// An empty ACL means "inherit the parent's effective ACL" — never "deny all".
// EffectiveSubmitACL / EffectiveAdminACL resolve inheritance by walking up.
type ACL struct {
	all        bool                // "*" present -> allow any namespace
	namespaces map[string]struct{} // explicit allowed namespaces
}

// ParseACL parses the wire form. Tokens are split on spaces and commas; "*"
// anywhere makes the ACL allow-all. An empty/whitespace string yields the zero
// ACL, which IsEmpty reports true for (so it inherits).
func ParseACL(s string) ACL {
	a := ACL{}
	for _, tok := range strings.FieldsFunc(s, func(r rune) bool { return r == ' ' || r == ',' || r == '\t' }) {
		if tok == "*" {
			a.all = true
			continue
		}
		if a.namespaces == nil {
			a.namespaces = map[string]struct{}{}
		}
		a.namespaces[tok] = struct{}{}
	}
	return a
}

// IsEmpty reports whether the ACL carries no rule (so it should inherit).
func (a ACL) IsEmpty() bool { return !a.all && len(a.namespaces) == 0 }

// Allows reports whether the given namespace is authorized by this ACL.
func (a ACL) Allows(namespace string) bool {
	if a.all {
		return true
	}
	_, ok := a.namespaces[namespace]
	return ok
}

// effectiveSubmitACL returns the nearest non-empty submitACL walking up from this
// queue, or the zero ACL if none is set anywhere (deny — a queue with no submit
// rule on its whole path admits nobody, fail-closed).
func (q *Queue) effectiveSubmitACL() ACL {
	for n := q; n != nil; n = n.parent {
		if !n.submitACL.IsEmpty() {
			return n.submitACL
		}
	}
	return ACL{}
}

func (q *Queue) effectiveAdminACL() ACL {
	for n := q; n != nil; n = n.parent {
		if !n.adminACL.IsEmpty() {
			return n.adminACL
		}
	}
	return ACL{}
}

// CanSubmit reports whether a pod in the given namespace may submit to this
// queue, per the effective (inherited) submit ACL.
func (q *Queue) CanSubmit(namespace string) bool { return q.effectiveSubmitACL().Allows(namespace) }

// CanAdmin reports whether the given namespace may administer this queue.
func (q *Queue) CanAdmin(namespace string) bool { return q.effectiveAdminACL().Allows(namespace) }
