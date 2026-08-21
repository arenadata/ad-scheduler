# ad-scheduler — build/test/lint targets. See TODO.md M0.

GO      ?= go
PKGS    := ./...
K8S_VERSION ?=

.PHONY: all build test race vet fmt fmt-check ci update-k8s-deps verify-k8s-deps clean

all: ci

## build: compile everything.
build:
	$(GO) build $(PKGS)

## test: run all tests.
test:
	$(GO) test -count=1 $$(go list $(PKGS) | grep -v /test)

## race: run all tests with the race detector.
race:
	$(GO) test -race -count=1 $(PKGS)

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
ci: fmt-check vet build test race

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
