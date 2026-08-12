# Tiltfile — Weir local dev inner loop (kind + LocalStack)
#
# Scope for WR-004 (deliberately cut down, per CLAUDE.md's MVP discipline):
# this file manages only the environment scaffolding that exists today —
# the kind cluster, LocalStack, and a hello-pod smoke check. It does NOT
# wire up `ko build` or any operator/worker image or manifest, because
# that code doesn't exist yet:
#   - cmd/operator, cmd/worker: land in WR-011 onward
#   - `ko build`: lands in WR-026 (see Makefile's docker-build target)
#   - config/ (kubebuilder CRD/RBAC manifests): lands in WR-011+
#   - the Helm chart: lands in WR-051
# Extending this Tiltfile with those pieces is those tasks' job, not
# WR-004's — see the TODO markers below for where each one hooks in.
#
# `make deploy-local` is the non-Tilt equivalent entrypoint for this same
# scope (WR-004's Done-when treats "tilt up" and "make deploy-local" as
# interchangeable) — both bring up kind + LocalStack + the hello pod.
#
# Known startup-ordering constraint (found in external review, WR-004):
# Tilt parses the whole Tiltfile up front — including every k8s_yaml()/
# k8s_resource() call — before any local_resource actually runs. That
# parse step needs a resolvable kubeconfig current-context to talk to the
# Kubernetes API (to discover the hello-pod's live status, etc). On a
# genuinely clean machine that has never run `make kind-up`/`kind create
# cluster`, there is no kind-<cluster> context yet, so a bare `tilt up`
# can fail before the 'kind-cluster' local_resource below ever gets a
# chance to create one and switch kubectl's current-context to it.
# `resource_deps` (below) only orders execution *after* Tilt has already
# parsed the file — it cannot fix a failure that happens during parsing
# itself. So, for this WR-004-scoped Tiltfile specifically:
#   - `tilt up` is NOT yet equivalent to `make deploy-local` from a truly
#     clean machine (no kind cluster, no kubeconfig context for it).
#   - Run `make kind-up` once first (idempotent — see the Makefile), THEN
#     `tilt up`, and the two entrypoints behave the same from there on.
#
# Context lock (found in external review, WR-004 round 2 — CodeRabbit;
# hardened in round 3 after a follow-up CodeRabbit finding):
# `allow_k8s_contexts('kind-$(KIND_CLUSTER)')` below (literally
# 'kind-weir', matching the Makefile's `KIND_CLUSTER ?= weir` and the
# `kind-$(KIND_CLUSTER)` context name kind creates) declares the one
# context this Tiltfile is meant to run against. On its own, though,
# allow_k8s_contexts is an ALLOWLIST addition, not an exclusive lock:
#   - Tilt already refuses, by default, to apply against a current-context
#     that doesn't *look* local — names outside its built-in safe patterns
#     (roughly: kind-*, minikube, docker-desktop, docker-for-desktop, k3d-*,
#     microk8s, rancher-desktop, colima) are blocked unless explicitly
#     allowlisted via allow_k8s_contexts. That default heuristic protects
#     against the worst case — a real/cloud context (EKS/GKE/etc) as
#     current-context — independently of this line.
#   - But declaring 'kind-weir' does not, by itself, stop Tilt from running
#     against some *other* kind cluster (e.g. a stray 'kind-other-project'
#     context) or minikube/docker-desktop, if one of those happens to be
#     the current-context instead — those are already on Tilt's default
#     safe list independent of this declaration.
# That gap is why the `k8s_context()`/`fail()` check immediately below
# exists: it reads the actual current-context at parse time and hard-fails
# before allow_k8s_contexts (or anything else) runs if it isn't exactly
# 'kind-weir'. Combined, the two lines below give an exclusive lock:
#   - the fail-fast check refuses to proceed against any other context
#     (kind or not, "safe-looking" or not);
#   - allow_k8s_contexts then allowlists the one context the check just
#     confirmed we're on, satisfying Tilt's own default safety heuristic.
# Caveat: this still doesn't *switch* the developer's context to kind-weir
# for them — it only refuses to run against the wrong one, which is the
# correct fail-safe behavior (silently applying to the wrong cluster would
# be worse than stopping and telling the developer to switch).
# Tilt isn't installed in this environment, so this hasn't been
# live-verified against a real `tilt up` run — treat the above as a
# documentation-accurate best effort from Tilt's public API reference
# (k8s_context() and fail() are both documented Tilt Starlark built-ins),
# not a tested guarantee.

# --- kind cluster + LocalStack -----------------------------------------
# Restrict Tilt to the context this project actually expects (see the
# "Context lock" note above) instead of trusting whatever kubeconfig
# current-context happens to be set at parse time.
if k8s_context() != 'kind-weir':
    fail('Tilt must run with Kubernetes context kind-weir')
allow_k8s_contexts('kind-weir')

# Tilt doesn't reimplement cluster/container lifecycle here — it shells
# out to the same `make` targets every other agent and human uses, so
# there's exactly one place (the Makefile) that knows how to bring these
# up idempotently and detect drift (e.g. kind-up's node-image check).
local_resource(
    'kind-cluster',
    cmd='make kind-up',
    labels=['infra'],
)

local_resource(
    'localstack',
    cmd='make localstack-up',
    resource_deps=['kind-cluster'],
    labels=['infra'],
)

# --- hello-pod smoke check ----------------------------------------------
# Minimal in-cluster liveness check standing in for the operator/worker
# until they exist. Tilt gives this a live status + port-forward in its UI.
k8s_yaml('hack/hello-pod.yaml')
k8s_resource(
    'hello',
    port_forwards=['8080:8080'],
    resource_deps=['kind-cluster', 'localstack'],
    labels=['smoke-test'],
)

# --- TODO: hooks for later phases ----------------------------------------
# WR-026 (ko build) — worker half DONE: `make docker-build` builds
#   cmd/worker with ko into the local Docker daemon, and `make worker-pod-up`
#   / `make worker-pod-down` (see hack/worker-pod.yaml) build+load+apply a
#   ko://-referenced smoke-check Pod straight into kind via
#   KO_DOCKER_REPO=kind.local. Those two are deliberately plain `make`
#   targets, not wired into this Tiltfile yet, because worker-pod-up needs a
#   pre-existing LocalStack queue/bucket this task doesn't auto-provision
#   (see hack/worker-pod.yaml's header) — wiring a `local_resource`/
#   `k8s_yaml(kustomize(...))` here that rebuilds cmd/worker on every save
#   is left for whichever task automates that provisioning (WR-027) or wires
#   the real worker Deployment (WR-030+), so `tilt up` can depend on it
#   cleanly instead of racing a missing queue.
#   `cmd/operator`'s ko/Tilt wiring remains open — lands whenever that
#   binary's own task addresses it.
# WR-011+ (operator scaffold): once kubebuilder scaffolds config/, add
#   k8s_yaml(kustomize('config/default')) (or the CRD/RBAC/manager split
#   kubebuilder generates) alongside/instead of hack/hello-pod.yaml.
# WR-036 (KEDA): install the KEDA Helm release here (or leave it to
#   `make deploy-local`/a dedicated target — decide when that task lands).
# WR-051 (Helm chart): once the chart is packaged, swap the raw
#   k8s_yaml/kustomize calls above for a helm() call against charts/weir.
