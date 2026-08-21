package framework

import (
	"net/http"
)

// HealthHandler serves the scheduler's liveness and readiness probes.
//
//   - /healthz (liveness) is always 200 once the process is up: the goroutine
//     serving HTTP is alive.
//   - /readyz (readiness) is 200 only after the engine has completed its first
//     successful queue-tree build (Engine.Ready) AND every wired readiness gate
//     reports synced. Until then it is 503, so a load balancer / leader takeover
//     does not send pods to a scheduler that would reject them all (q30/q28).
type HealthHandler struct {
	engine *Engine
	// synced is an optional extra gate (e.g. informer HasSynced); nil means
	// "no extra gate", so readiness depends on the engine alone. Wired in M1
	// once informers exist.
	synced func() bool
}

// NewHealthHandler builds a HealthHandler for engine. syncedGate may be nil.
func NewHealthHandler(engine *Engine, syncedGate func() bool) *HealthHandler {
	return &HealthHandler{engine: engine, synced: syncedGate}
}

// Register installs /healthz and /readyz on mux.
func (h *HealthHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.healthz)
	mux.HandleFunc("/readyz", h.readyz)
}

func (h *HealthHandler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *HealthHandler) readyz(w http.ResponseWriter, _ *http.Request) {
	if !h.ready() {
		http.Error(w, "not ready: queue tree not built (fail-closed, q30)", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// ready reports overall readiness: engine has built a tree and every extra gate
// (if any) is satisfied.
func (h *HealthHandler) ready() bool {
	if h.engine == nil || !h.engine.Ready() {
		return false
	}
	if h.synced != nil && !h.synced() {
		return false
	}
	return true
}
