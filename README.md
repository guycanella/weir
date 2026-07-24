# Weir

A Kubernetes operator (Go) for event-driven processing pipelines. You declare a
`ProcessingPipeline` custom resource; Weir provisions the queue, runs the workers, and
scales them from zero against the queue backlog — identically on kind + LocalStack ($0)
and on real AWS.

`DOCUMENTATION.md` (design + ADRs), `IMPLEMENTATION.md` (task backlog), and `PROGRESS.md`
(execution diary) are this project's internal document triad, used as local working
documents during development. They're intentionally gitignored/untracked, so a fresh clone
of this repo will not contain them — see `TOOLS.md` for the pinned toolchain/version matrix.

## Development

All dev workflows go through `make` targets, so behavior is identical across contributors
and agents (see `Makefile`'s own header comment). Run `make help` for the full list with
descriptions; the targets below are the ones this repo's day-to-day loop depends on.

Several targets are wired ahead of the phase that fully implements them, and fall into one
of two deliberately different patterns:

- **Phase-gated no-op stubs** (`make test`, `make manifests`, `make generate`, `make cover`):
  when the thing they depend on doesn't exist yet on this empty skeleton (no Go packages,
  no `controller-gen`/kubebuilder types, no `cover.out`), they print what's missing and
  which task lands it, then **exit 0**. That keeps CI/local runs green on a repo that
  hasn't reached that phase yet. This no-op path only fires on a *successful, genuinely
  empty* discovery — e.g. `make test` first checks that `go list ./...` itself succeeded
  before treating an empty package list as "nothing to test yet"; a malformed `go.mod`,
  build errors, or a missing Go toolchain still fail loudly with a nonzero exit, they are
  never swallowed into the no-op message.
- **Hard requirement guards** (`make lint`, `make docker-build`, `make test-integration`):
  when a required tool or service is missing, they **fail loudly** (nonzero exit) with an
  actionable message telling you what to install/start and which task wires it up for real.
  This is intentional — silently skipping linting or an image build would hide a real
  problem, not just a not-yet-scaffolded phase.

Both patterns are expected today, not a bug — the "Status today" column below says which
one applies to each target.

| Target | What it does | Status today |
|--------|--------------|---------------|
| `make build` | `go build ./...` — compiles every package. | Works now. |
| `make test` | `go test ./... -race -coverprofile=cover.out` — the fast unit-test / TDD inner loop. | Phase-gated no-op stub — exits 0 only when `go list ./...` *succeeds* with zero packages, so CI stays green on the empty skeleton; a broken `go.mod`/build error from `go list` still fails loudly with a nonzero exit instead of being masked as a no-op. |
| `make cover` | `go tool cover -func=cover.out` — coverage summary from the last `make test` run. | Phase-gated no-op stub — exits 0 with a message if `cover.out` doesn't exist yet (i.e. before any packages exist to test). |
| `make test-integration` | Runs `-tags=integration` tests against kind + LocalStack. | Hard requirement guard — fails loudly if `kind` isn't installed or LocalStack isn't running (needs `make deploy-local`); fully exercised once WR-004 lands. |
| `make lint` | Runs `golangci-lint run`. | Hard requirement guard — fails loudly if `golangci-lint` isn't on `PATH` locally; installed separately in CI (WR-005). |
| `make manifests` | Generates CRD + RBAC manifests via `controller-gen`. | Phase-gated no-op stub — exits 0 until `make tools` (controller-gen) and the kubebuilder API types WR-011 scaffolds exist. |
| `make generate` | Generates deepcopy code via `controller-gen`. | Phase-gated no-op stub — same as `manifests`, plus needs `hack/boilerplate.go.txt`, which lands with `kubebuilder init` in WR-011. |
| `make docker-build` | Builds the worker/operator images with `ko`. | Hard requirement guard — fails loudly if `ko` isn't on `PATH`; wired for real in WR-026. |
| `make deploy-local` | Brings up the full local stack: kind cluster + LocalStack (+ operator, once Helm-installable). | Partially works — kind + LocalStack come up; KEDA (WR-036) and the Helm-installed operator (WR-051) are still TODOs printed by the target itself. |
| `make undeploy-local` | Tears down the full local stack (LocalStack container + kind cluster). | Works now. |

Other useful targets: `make fmt`, `make vet`, `make tidy`, `make clean`, `make tools`
(installs pinned `controller-gen` / `setup-envtest` into `./bin` — see `TOOLS.md`),
`make envtest` (envtest binaries for controller tests, WR-034), and `make kind-up` /
`make kind-down` / `make localstack-up` / `make localstack-down` (the individual pieces
`deploy-local`/`undeploy-local` compose).

### Conventions

- Local-first, $0 by default: everything above runs against kind + LocalStack. Anything
  touching real AWS is a separate, explicitly gated `[cloud]` task — never run through
  these targets.
- Prefer `make <target>` over raw `go`/`kind`/`docker` invocations so every contributor and
  every agent gets identical behavior.
