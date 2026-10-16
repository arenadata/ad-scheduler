# ad-scheduler — build/test/lint targets. See TODO.md M0.

GO      ?= go
PKGS    := ./...
K8S_VERSION ?=
E2E_TIMEOUT ?= 10m
E2E_FOCUS   ?=
# kube-apiserver/etcd version for envtest; keep in step with k8s.io/kubernetes.
ENVTEST_K8S_VERSION ?= 1.37.0
SETUP_ENVTEST ?= $(GO) tool setup-envtest
E2E_ARGS = -count=1 -timeout $(E2E_TIMEOUT) ./test/e2e/... -v \
	$(if $(E2E_FOCUS),-args -ginkgo.focus='$(E2E_FOCUS)')

.PHONY: all build test e2e e2e-cluster e2e-smoke race vet fmt fmt-check ci update-k8s-deps verify-k8s-deps clean

all: ci

## build: compile everything.
build:
	$(GO) build $(PKGS)

## test: run all tests.
test:
	$(GO) test -count=1 $$(go list $(PKGS) | grep -v /test)

## e2e: run the end-to-end suite on envtest (local kube-apiserver + etcd from
##   setup-envtest); the suite starts fake nodes, a fake kubelet and the scheduler.
##   make e2e E2E_FOCUS=gang   # only specs matching the regexp
e2e:
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" \
	  $(GO) test $(E2E_ARGS)

## e2e-cluster: run the e2e suite against the current kubeconfig context (kind),
##   which must already have CRDs, RBAC, admission and the scheduler deployed.
e2e-cluster:
	USE_EXISTING_CLUSTER=true $(GO) test $(E2E_ARGS)

## e2e-smoke: kubectl-driven smoke test of the core scheduling path.
##   make e2e-smoke                 # throwaway envtest control plane (default)
##   make e2e-smoke SMOKE_ENV=kind  # current kubeconfig context (kind)
SMOKE_ENV ?= envtest
e2e-smoke:
	SMOKE_ENV=$(SMOKE_ENV) ENVTEST_K8S_VERSION=$(ENVTEST_K8S_VERSION) bash test/e2e/smoke.sh

## race: run all tests (except e2e) with the race detector.
race:
	$(GO) test -race -count=1 $$(go list $(PKGS) | grep -v /test)

## vet: static checks.
vet:
	$(GO) vet $(PKGS)

## fmt: format the tree in place.
fmt:
	gofmt -w .

## fmt-check: fail if anything is unformatted.
fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

## ci: the full gate (matches the M0 CI task).
ci: fmt-check vet build test race e2e-smoke

## update-k8s-deps: pin k8s.io/kubernetes + staging to one minor.
##   make update-k8s-deps K8S_VERSION=1.31.4
update-k8s-deps:
	@if [ -z "$(K8S_VERSION)" ]; then echo "set K8S_VERSION, e.g. make update-k8s-deps K8S_VERSION=1.31.4"; exit 2; fi
	hack/update-k8s-deps.sh $(K8S_VERSION)

## verify-k8s-deps: fail if the go.mod replace block drifted from K8S_VERSION.
verify-k8s-deps: update-k8s-deps
	@if ! git diff --quiet -- go.mod go.sum; then \
	  echo "go.mod/go.sum drift from k8s $(K8S_VERSION) — run 'make update-k8s-deps' and commit"; \
	  git --no-pager diff -- go.mod go.sum; exit 1; \
	fi

clean:
	rm -rf bin
