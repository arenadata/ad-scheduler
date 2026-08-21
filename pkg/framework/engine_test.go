package framework

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/arenadata/ad-scheduler/pkg/config"
)

func TestGetOrInitEngineIsOncePerProfile(t *testing.T) {
	resetEnginesForTest()
	cfg := config.Defaults()

	a := GetOrInitEngine("ad-scheduler", cfg)
	b := GetOrInitEngine("ad-scheduler", cfg)
	if a != b {
		t.Fatal("same profile must return the same engine instance")
	}
	c := GetOrInitEngine("other", cfg)
	if c == a {
		t.Fatal("different profiles must be different engines")
	}

	// concurrent callers still get exactly one instance
	resetEnginesForTest()
	var wg sync.WaitGroup
	got := make([]*Engine, 32)
	for i := range got {
		wg.Add(1)
		go func(i int) { defer wg.Done(); got[i] = GetOrInitEngine("p", cfg) }(i)
	}
	wg.Wait()
	for i := 1; i < len(got); i++ {
		if got[i] != got[0] {
			t.Fatal("concurrent GetOrInitEngine produced multiple instances")
		}
	}
}

func TestNilTreeRejectsAll(t *testing.T) {
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())

	// Fresh engine: no tree built -> fail-closed reject, not ready.
	if e.HasTree() {
		t.Fatal("a fresh engine must have no tree")
	}
	if e.Ready() {
		t.Fatal("a fresh engine must not be ready")
	}
	if err := e.Admit(); !errors.Is(err, ErrRejectAll) {
		t.Fatalf("nil tree must reject-all, got %v", err)
	}

	// After a build the engine admits and is ready.
	e.SwapSnapshot(&Snapshot{generation: 1})
	if !e.HasTree() || !e.Ready() {
		t.Fatal("engine must have a tree and be ready after a build")
	}
	if err := e.Admit(); err != nil {
		t.Fatalf("built tree must admit, got %v", err)
	}
	if e.Snapshot().Generation() != 1 {
		t.Fatalf("snapshot generation = %d", e.Snapshot().Generation())
	}
}

func TestReadinessNeverRegresses(t *testing.T) {
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())
	e.SwapSnapshot(&Snapshot{generation: 1})
	if !e.Ready() {
		t.Fatal("should be ready after first build")
	}
	// an explicit nil swap (teardown) must not un-ready (last-good semantics)
	e.SwapSnapshot(nil)
	if !e.Ready() {
		t.Fatal("readiness must not regress on a nil swap")
	}
}

func TestHealthEndpoints(t *testing.T) {
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())
	mux := http.NewServeMux()
	NewHealthHandler(e, nil).Register(mux)

	// liveness is always ok
	if code := do(mux, "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", code)
	}
	// readiness 503 before a build...
	if code := do(mux, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before build = %d, want 503", code)
	}
	// ...200 after a build
	e.SwapSnapshot(&Snapshot{generation: 1})
	if code := do(mux, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz after build = %d, want 200", code)
	}
}

func TestReadinessRespectsSyncedGate(t *testing.T) {
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())
	e.SwapSnapshot(&Snapshot{generation: 1})
	synced := false
	mux := http.NewServeMux()
	NewHealthHandler(e, func() bool { return synced }).Register(mux)

	if code := do(mux, "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz with unsynced informers = %d, want 503", code)
	}
	synced = true
	if code := do(mux, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz with synced informers = %d, want 200", code)
	}
}

func do(mux *http.ServeMux, path string) int {
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}
