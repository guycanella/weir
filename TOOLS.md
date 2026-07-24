# Weir — Pinned Toolchain & Version Matrix

This file is the single source of truth for **which versions of the toolchain are mutually
compatible** for local development (kind + LocalStack, $0). It exists because kubebuilder,
controller-runtime, KEDA, and the Kubernetes/envtest binaries evolve independently and each
has its own compatibility window — see CLAUDE.md's "Version discipline" note and
DOCUMENTATION.md's version caveat. **Consult this file before scaffolding or upgrading;
don't guess versions from memory (WR-002).**

Last verified: 2026-07-23 (WR-002), against upstream release notes at that date. LocalStack pin
corrected 2026-07-24 (WR-004) after the original `2026.06.3` pin turned out to be Pro-gated —
see the LocalStack row below.

## The pinned set

| Tool | Pinned version | Why this one |
|------|-----------------|--------------|
| Go | `1.26.5` | Matches the installed toolchain (`go version` → `go1.26.5`) recorded in `go.mod` since WR-001. New enough for every tool below; Go's backward compatibility means a newer patch toolchain building against an older `go` directive is safe. |
| kubebuilder | `v4.14.0` | Last release on the controller-runtime v0.23.x / Kubernetes 1.35 generation, before v4.15.0 moved to Kubernetes 1.36 support. Picked over the newer v4.15.0 specifically to stay inside KEDA's tested window (below). |
| controller-runtime | `v0.23.3` | The version kubebuilder v4.14.0 scaffolds; built against `k8s.io/*` v1.35. Latest patch on the v0.23 line. |
| controller-tools (`controller-gen`) | `v0.20.1` | Paired with the same v1.35 generation (`envtest-v1.35.0` was tagged the same day as `controller-tools v0.20.0`); v0.20.1 is the latest bugfix patch on that line. |
| `setup-envtest` (the installer binary) | `v0.0.0-20260305142021-f9589b9f2b9d` (exact pseudo-version, not the `release-0.23` branch) | `setup-envtest` has no semver tags — it's installed off a release branch, which is mutable. Pinning the exact pseudo-version `go install` resolved (the commit on `release-0.23` at pin time) makes `make tools` reproducible: re-running it later installs the same code, not whatever `release-0.23` has moved to. Bump deliberately, not implicitly. |
| Kubernetes / envtest | `1.35.0` | `setup-envtest use 1.35.0` — matches controller-runtime v0.23.3's tested API version, and sits inside KEDA's supported window. |
| KEDA | `2.20.1` (Helm chart) | Latest KEDA release. Its documented compatibility window is Kubernetes **v1.33–v1.35** (N-2 tested policy, https://keda.sh/docs/2.20/operate/cluster/) — this is the constraint that drove every other pin below Kubernetes 1.36. |
| kind | `v0.32.0` | Already installed locally (`kind version` → `v0.32.0 go1.26.5`). Its *default* node image is `kindest/node:v1.36.1`, which is **outside** KEDA's tested window — see the override below. |
| kind node image | `kindest/node:v1.35.5` (tag, not digest — see note) | Overrides kind v0.32.0's default (1.36.1) so the local cluster's control-plane version matches the Kubernetes 1.35 generation everything else above is pinned to. |
| LocalStack | `localstack/localstack:4.14.0` | The original WR-002 pin (`2026.06.3`) turned out to be broken: LocalStack ended free Community Docker distribution on 2026-03-23 — since then `localstack/localstack` and `localstack/localstack-pro` are the *same* image on Docker Hub and it unconditionally requires a paid `LOCALSTACK_AUTH_TOKEN` (confirmed live: the container exits with code 55, "License activation failed... LocalStack pro features can only be used with a valid license", regardless of which `SERVICES` are requested — not a Lambda-only gate). `4.14.0` is the last free, no-auth-token, semver-tagged Community release before that cutover, confirmed live to start cleanly (`healthy` status) and serve S3, SNS, SQS, and Lambda (including a real Lambda invocation via the Docker-executor path) under `SERVICES=s3,sns,sqs,lambda`. That Docker-executor path is why `make localstack-up` mounts the host `/var/run/docker.sock` into the container — a deliberate, scoped, local-dev-only exception (see the comment above the mount in `Makefile`). |

## Why Kubernetes 1.35, not the newest 1.36

kind's newest default and kubebuilder's newest release both moved to Kubernetes 1.36 shortly
before this pin was made. KEDA 2.20 — the autoscaler ADR-002 depends on — only documents
support up to Kubernetes 1.35 (N-2 from whatever is current for KEDA). Rather than run a
cluster version KEDA doesn't claim to support, this pin holds the whole stack one Kubernetes
minor version back, on 1.35, where kubebuilder, controller-runtime, controller-tools/envtest,
and KEDA are all confirmed compatible. Revisit this pin (and this file) when KEDA extends its
tested window to 1.36+ — likely around the task that installs KEDA (WR-036) or later.

## Deliberately deferred (not over-engineering this file)

- **Digest-pinning the kind node image** (`kindest/node:v1.35.5@sha256:...`) is kind's own
  recommended best practice for reproducibility, but the digest wasn't hand-verified against
  a trusted source at pin time. Tag-pinning (`v1.35.5`) is good enough for local dev now;
  add the digest when CI hardening lands (WR-005) if reproducibility issues show up.
- **controller-gen / setup-envtest exact `go install` tags** are recorded here and wired into
  the Makefile (`make tools`), but will be reconciled against whatever `go.mod` kubebuilder
  v4.14.0 actually scaffolds in WR-011 — kubebuilder is the source of truth for its own
  generated `go.mod`, this file is the pre-scaffold plan.
- **ARM64 LocalStack image variant** isn't pinned separately; Docker Hub resolves the
  multi-arch manifest for `4.14.0` automatically.
- **`localstack/localstack:4.14.0` gets no further updates or security patches** — it's the
  last tag before LocalStack's Community distribution ended, so there's no newer free tag to
  move to without paying for a license. Same trade-off already accepted for the kind node
  image and `setup-envtest` pins above: correctness/compatibility now, at the cost of drift
  over time. Revisit if/when a viable free alternative appears (a fork, a different tool, or
  LocalStack reversing course), or when a real AWS account is in scope anyway for `[cloud]`
  tasks and LocalStack becomes optional rather than the default.
- **LocalStack's port (4566) is bound to `127.0.0.1` only, never `0.0.0.0`** — combined with the
  `/var/run/docker.sock` mount above, an unauthenticated bind on all interfaces would let any
  host on the network reach a container-spawning API; never widen this binding, and never run
  this stack on a host executing untrusted code.

## Where this is wired

- `Makefile`: `ENVTEST_K8S_VERSION`, `KIND_NODE_IMAGE`, `CONTROLLER_GEN_VERSION`,
  `SETUP_ENVTEST_VERSION`, `KEDA_VERSION`, `LOCALSTACK_IMAGE` variables reference the pins
  above; `make tools`, `make envtest`, `make kind-up`, and `make localstack-up` consume them.
- kubebuilder itself (`v4.14.0`) is not a Makefile variable — it is a one-time scaffolding
  tool invoked directly (`kubebuilder init` / `kubebuilder create api`) in WR-011 onward, not
  something `make` installs or runs repeatedly.
