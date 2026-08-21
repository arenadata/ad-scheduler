#!/usr/bin/env bash
#
# update-k8s-deps.sh — pin k8s.io/kubernetes and all its staging modules to one
# minor. See TODO.md M0 / risk #1 §10.1.
#
# k8s.io/kubernetes is not meant to be imported as a library: its own go.mod
# declares every staging module (k8s.io/api, apimachinery, client-go,
# component-base, kube-scheduler, ...) as v0.0.0 with *relative-path* replace
# directives that only resolve inside the monorepo. An external consumer must
# replicate the full replace set, pinning each staging module to the version
# tagged for the same k8s release (k8s.io/kubernetes vX.Y.Z <-> staging v0.Y.Z).
#
# Usage:  hack/update-k8s-deps.sh 1.31.4
#         K8S_VERSION=1.31.4 hack/update-k8s-deps.sh
#
# One version in, a consistent replace block out — no hand-editing ~30 lines.
set -euo pipefail

K8S_VERSION="${1:-${K8S_VERSION:-}}"
if [[ -z "${K8S_VERSION}" ]]; then
  echo "usage: $0 <k8s-version, e.g. 1.31.4>   (or set K8S_VERSION)" >&2
  exit 2
fi
K8S_VERSION="${K8S_VERSION#v}" # tolerate a leading v

if [[ ! "${K8S_VERSION}" =~ ^1\.([0-9]+)\.([0-9]+)$ ]]; then
  echo "error: expected a k8s version like 1.31.4, got '${K8S_VERSION}'" >&2
  exit 2
fi
MINOR="${BASH_REMATCH[1]}"
PATCH="${BASH_REMATCH[2]}"
# Staging modules are tagged v0.<minor>.<patch> for k8s v1.<minor>.<patch>.
STAGING_VER="v0.${MINOR}.${PATCH}"
CORE_VER="v${K8S_VERSION}"

cd "$(git rev-parse --show-toplevel)"

echo ">> resolving staging modules from k8s.io/kubernetes@${CORE_VER}"
# Download the module and read its go.mod; the staging deps are exactly the
# k8s.io/* requires pinned at v0.0.0 (the monorepo's placeholder version).
CORE_GOMOD="$(go mod download -json "k8s.io/kubernetes@${CORE_VER}" | sed -n 's/.*"GoMod": *"\([^"]*\)".*/\1/p')"
if [[ -z "${CORE_GOMOD}" || ! -f "${CORE_GOMOD}" ]]; then
  echo "error: could not download k8s.io/kubernetes@${CORE_VER} go.mod" >&2
  exit 1
fi

# Staging module paths are exactly the k8s.io/* requires pinned at v0.0.0 (the
# monorepo placeholder). Parse the machine-readable require block with jq
# (go mod edit -json splits Path/Version across lines, so a line grep misses
# them). Portable to bash 3.2 (macOS): a while-read loop, not mapfile.
command -v jq >/dev/null 2>&1 || { echo "error: jq is required" >&2; exit 1; }
STAGING=()
while IFS= read -r mod; do
  [ -n "${mod}" ] && STAGING+=("${mod}")
done < <(
  go mod edit -json "${CORE_GOMOD}" \
    | jq -r '.Require[]? | select(.Version=="v0.0.0") | select(.Path|startswith("k8s.io/")) | .Path'
)

if [[ "${#STAGING[@]}" -eq 0 ]]; then
  echo "error: found no staging modules at v0.0.0 in ${CORE_GOMOD} — layout changed?" >&2
  exit 1
fi
echo ">> ${#STAGING[@]} staging modules -> ${STAGING_VER}"

echo ">> pinning core: k8s.io/kubernetes -> ${CORE_VER}"
go mod edit -require="k8s.io/kubernetes@${CORE_VER}"
# Also replace the core module to itself at CORE_VER so MVS cannot silently bump
# it to a newer minor whose staging packages our v0.<minor> replaces would lack.
go mod edit -replace="k8s.io/kubernetes=k8s.io/kubernetes@${CORE_VER}"

for mod in "${STAGING[@]}"; do
  echo "   replace ${mod} => ${mod} ${STAGING_VER}"
  go mod edit -replace="${mod}=${mod}@${STAGING_VER}"
done

echo ">> go mod tidy"
go mod tidy

echo ">> done. k8s pinned to ${CORE_VER} / staging ${STAGING_VER}."
echo "   (CI should assert the replace block matches this version — see Makefile 'verify-k8s-deps'.)"
