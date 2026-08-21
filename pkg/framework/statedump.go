package framework

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// EnvDebugAddr overrides the address of the introspection server (/statedump,
// /metrics). Empty/unset uses the default below.
const EnvDebugAddr = "AD_DEBUG_ADDR"

// EnvStatedumpToken guards /statedump (it exposes the full tenant/queue tree, so
// it is not an open endpoint): unset disables /statedump (404), set requires a
// matching `Authorization: Bearer <token>`. /metrics stays open (non-sensitive).
const EnvStatedumpToken = "AD_STATEDUMP_TOKEN"

const defaultDebugAddr = ":8089"

// QueueDump is one queue's live accounting, as served by /statedump.
type QueueDump struct {
	Path            string            `json:"path"`
	Leaf            bool              `json:"leaf"`
	Guaranteed      resource.Resource `json:"guaranteed,omitempty"`
	Max             resource.Resource `json:"max,omitempty"`
	Allocated       resource.Resource `json:"allocated,omitempty"`
	Allocating      resource.Resource `json:"allocating,omitempty"`
	Pending         resource.Resource `json:"pending,omitempty"`
	Headroom        resource.Resource `json:"headroom,omitempty"`
	ServiceAccounts []string          `json:"serviceAccounts,omitempty"`
	// Fence is the effective preemption-fence status: explicit
	// spec.preemption.policy=fence or an implicit top-level (namespace) fence.
	Fence bool `json:"fence,omitempty"`
}

// GangDump is one admitted gang's virtual reservation.
type GangDump struct {
	Key  string            `json:"key"`
	Leaf string            `json:"leaf"`
	Held resource.Resource `json:"held"`
}

// StateDump is the full introspection snapshot of the engine.
type StateDump struct {
	Profile       string            `json:"profile"`
	Generation    uint64            `json:"generation"`
	Ready         bool              `json:"ready"`
	Queues        []QueueDump       `json:"queues"`
	Gangs         []GangDump        `json:"gangs,omitempty"`
	PoolAvailable resource.Resource `json:"poolAvailable"`
}

// StateDump builds the introspection snapshot from the live tree, cache and gang
// ledger. Safe to call concurrently with scheduling.
func (e *Engine) StateDump() StateDump {
	sd := StateDump{
		Profile:       e.profile,
		Generation:    e.Snapshot().Generation(),
		Ready:         e.Ready(),
		PoolAvailable: e.cache.PoolAvailable(),
	}
	if t := e.Tree(); t != nil {
		for _, q := range t.All() {
			sd.Queues = append(sd.Queues, QueueDump{
				Path:            q.Path(),
				Leaf:            q.IsLeaf(),
				Guaranteed:      nonEmpty(q.Guaranteed()),
				Max:             nonEmpty(q.Max()),
				Allocated:       nonEmpty(q.Allocated()),
				Allocating:      nonEmpty(q.Allocating()),
				Pending:         nonEmpty(q.Pending()),
				Headroom:        nonEmpty(q.PathHeadroom()),
				ServiceAccounts: q.ServiceAccounts(),
				Fence:           q.IsFence(),
			})
		}
	}
	e.mu.Lock()
	for key, gb := range e.gangBook {
		sd.Gangs = append(sd.Gangs, GangDump{Key: key, Leaf: gb.leaf, Held: gb.minRes.Clone()})
	}
	e.mu.Unlock()
	sort.Slice(sd.Gangs, func(i, j int) bool { return sd.Gangs[i].Key < sd.Gangs[j].Key })
	return sd
}

func nonEmpty(r resource.Resource) resource.Resource {
	if r.IsEmpty() {
		return nil
	}
	return r
}

// StartDebugServer serves /statedump (JSON) and /metrics (Prometheus text) for
// introspection, on AD_DEBUG_ADDR (default :8089). It runs until ctx is done.
func (e *Engine) StartDebugServer(ctx context.Context) {
	addr := os.Getenv(EnvDebugAddr)
	if addr == "" {
		addr = defaultDebugAddr
	}
	statedumpToken := os.Getenv(EnvStatedumpToken)
	mux := http.NewServeMux()
	mux.HandleFunc("/statedump", func(w http.ResponseWriter, r *http.Request) {
		// Not an open endpoint: disabled unless a token is configured, then gated
		// on a matching bearer token (constant-time compare).
		if statedumpToken == "" {
			http.NotFound(w, r)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(statedumpToken)) != 1 {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(e.StateDump())
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(e.metricsText()))
	})
	// The kube-scheduler framework metrics (scheduler_schedule_attempts_total,
	// scheduling_attempt_duration_seconds, …) from the default registry, served
	// unauthenticated here so a perf harness can read the scheduler's true
	// decision rate without the :10259 delegated-authz dance.
	mux.Handle("/scheduler-metrics", legacyregistry.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		klog.InfoS("ad-scheduler: introspection server", "addr", addr, "paths", "/statedump,/metrics")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.ErrorS(err, "ad-scheduler: introspection server stopped")
		}
	}()
}

// metricsText renders per-queue gauges in Prometheus text exposition format.
// Hand-rolled to avoid a client dependency; one series per (queue, dimension).
func (e *Engine) metricsText() string {
	var b strings.Builder
	sd := e.StateDump()
	fmt.Fprintf(&b, "# HELP ad_scheduler_ready 1 if the queue tree is built.\n# TYPE ad_scheduler_ready gauge\nad_scheduler_ready %d\n", b2i(sd.Ready))
	fmt.Fprintf(&b, "# HELP ad_scheduler_generation Monotonic tree rebuild id.\n# TYPE ad_scheduler_generation counter\nad_scheduler_generation %d\n", sd.Generation)
	fmt.Fprintf(&b, "# HELP ad_gangs_admitted Currently-admitted gangs holding a reservation.\n# TYPE ad_gangs_admitted gauge\nad_gangs_admitted %d\n", e.GangCount())
	fmt.Fprintf(&b, "# HELP ad_reclaim_evictions_total Pods evicted by the reclaim controller.\n# TYPE ad_reclaim_evictions_total counter\nad_reclaim_evictions_total %d\n", e.ReclaimEvictions())

	// Cardinality guard (decision q29): if an allowlist is configured, only those
	// resource dimensions become metric labels — keeps TSDB series bounded when a
	// cluster has many dra/<class> or extended-resource dimensions.
	var allow map[string]struct{}
	if al := e.config.MetricDimensionAllowlist; len(al) > 0 {
		allow = make(map[string]struct{}, len(al))
		for _, d := range al {
			allow[d] = struct{}{}
		}
	}
	emit := func(name, help string, pick func(QueueDump) resource.Resource) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
		for _, q := range sd.Queues {
			dims := pick(q)
			keys := make([]string, 0, len(dims))
			for d := range dims {
				if allow != nil {
					if _, ok := allow[d]; !ok {
						continue
					}
				}
				keys = append(keys, d)
			}
			sort.Strings(keys)
			for _, d := range keys {
				fmt.Fprintf(&b, "%s{queue=%q,dim=%q} %d\n", name, q.Path, d, dims[d])
			}
		}
	}
	emit("ad_queue_max", "Queue max (millicores for cpu).", func(q QueueDump) resource.Resource { return q.Max })
	emit("ad_queue_guaranteed", "Queue guaranteed floor.", func(q QueueDump) resource.Resource { return q.Guaranteed })
	emit("ad_queue_allocated", "Queue confirmed allocation.", func(q QueueDump) resource.Resource { return q.Allocated })
	emit("ad_queue_allocating", "Queue in-flight reservation.", func(q QueueDump) resource.Resource { return q.Allocating })
	emit("ad_queue_pending", "Queue pending demand.", func(q QueueDump) resource.Resource { return q.Pending })
	emit("ad_queue_headroom", "Queue remaining headroom.", func(q QueueDump) resource.Resource { return q.Headroom })

	// DRF dominant share: the queue's tightest utilisation of its own ceiling
	// (max over dimensions of used/max) — the quantity DRF fair-ordering keys on.
	// An unbounded dimension with usage yields +Inf (valid Prometheus).
	fmt.Fprintf(&b, "# HELP ad_queue_dominant_share DRF dominant share of a queue's used (allocated+allocating) vs its max.\n# TYPE ad_queue_dominant_share gauge\n")
	for _, q := range sd.Queues {
		share := resource.DominantShare(resource.Add(q.Allocated, q.Allocating), q.Max)
		fmt.Fprintf(&b, "ad_queue_dominant_share{queue=%q} %g\n", q.Path, share)
	}
	return b.String()
}

func b2i(v bool) int {
	if v {
		return 1
	}
	return 0
}
