# CLAUDE.md — Weir

Operational contract for this repo. Loaded into every session. This file holds the **rules and pointers**; the detailed design lives in the three documents below. When they conflict, this file's golden rules win.

## What Weir is

A Kubernetes operator (Go) for event-driven processing pipelines. You declare a `ProcessingPipeline` custom resource; Weir provisions the queue, runs the workers, and scales them from zero against the queue backlog — identically on kind + LocalStack ($0) and on real AWS. It *measures the backlog and regulates the flow*.

## The document triad — single sources of truth

| File | Role | Who edits |
|------|------|-----------|
| `DOCUMENTATION.md` | The **why** — design, architecture, ADRs, stack rationale. | Humans + docs agent. Decisions become ADRs here. |
| `IMPLEMENTATION.md` | The **what** — the 66 `WR-NNN` tasks, dependencies, Definition of Done. Stable spec. | Rarely edited (only to add/reshape tasks). |
| `PROGRESS.md` | The **status** — execution diary: status board + append-only log. | **Orchestrator only.** |

## Golden rules (non-negotiable)

1. **Only the orchestrator writes `PROGRESS.md`.** Specialist agents return results; the orchestrator is the sole scribe. This is enforced by tool grants in the agent definitions — specialists are not given write access to `PROGRESS.md`.
2. **Every unit of work maps to a `WR-NNN` task.** Commits follow **Conventional Commits** with the task id in a `Refs: WR-NNN` footer (see Conventions). Check a task's `Depends on` in `PROGRESS.md` before starting it.
3. **TDD where it belongs (ADR-003).** Write tests first for the pure decision core and for non-trivial reconciler logic. Do *not* force TDD on IaC, framework glue, or AWS wiring — those get integration tests.
4. **Local-first, $0 by default.** All routine work runs on kind + LocalStack. Anything touching real AWS is a `[cloud]` task and **requires explicit human confirmation** before running (cloud commands are permission-gated to `ask`).
5. **Least privilege, no static keys.** Scoped RBAC; IRSA for pod credentials; never hardcode AWS keys.
6. **MVP first, resist over-engineering.** Ship the walking skeleton before stretch goals. Cutting scope on purpose is correct; document what was cut and why.
7. **Decisions vs blockers.** An architecture decision that changes the design → promote to a new ADR in `DOCUMENTATION.md` *and* log it in `PROGRESS.md`. A mere execution blocker (a version clash, a flaky tool) → log in `PROGRESS.md` only.

## The task workflow — `/start-task WR-NNN`

The orchestrator drives this loop:

1. Read the task from `IMPLEMENTATION.md`; verify every `Depends on` is `DONE` in `PROGRESS.md`.
2. Write `status: IN PROGRESS` (with timestamp + task id) to `PROGRESS.md`. This is the recovery point.
3. Dispatch to specialists in the right order. For `[TDD]` tasks: **tester writes the failing test first**, then the coder implements until green.
4. Internal review pass (reviewer agent) against the ADRs and conventions.
5. **Review gate — wait for a verdict before finalizing.**
   - *Now (manual mode):* orchestrator pauses and reports "ready for review"; the human runs the external reviewer (Opus/Codex) in another terminal and returns `pode finalizar` (PASS) or findings (FAIL).
   - *Later (automated mode):* orchestrator invokes the external reviewer CLI as a subprocess and parses a JSON verdict `{ "verdict": "PASS|FAIL", "findings": [...] }`. Same gate, different verdict source.
6. On **PASS**: security agent runs the final scan → orchestrator authors the `WR-NNN:` commit message and the PR title/description and hands them to the human — it never runs `git commit`/`git push` or opens/merges the PR itself → writes `status: IN PROGRESS` with Notes `awaiting merge` to `PROGRESS.md`. Once the human confirms the PR is merged to `main`, orchestrator writes `status: DONE` + summary + which agents acted + which model reviewed.
7. On **FAIL**: log findings; return to the coder to fix (re-review the diff only). **Cap at 3 iterations**; on the cap, stop and escalate to the human. `high` severity blocks; `low` is a note, not a blocker.

## Agent roster

Main thread = **orchestrator**. Specialists are subagents (`.claude/agents/`), each with a restricted tool set and its own model tier. The external reviewer is a separate CLI (not a subagent).

| Agent | Role | Model tier |
|-------|------|-----------|
| orchestrator | routes tasks, owns `PROGRESS.md`, runs the loop; creates the task branch but never commits/pushes/opens or merges PRs — authors that text for the human | capable (e.g. Sonnet 5) |
| tester | writes tests first (TDD); runs the suite | robust (test design is hard) |
| go | all Go code (operator, worker, loadgen, SDK) | lighter coder (Sonnet 5 / Gemini Flash) |
| kubernetes | CRD, reconcile, RBAC, KEDA, Helm | lighter coder |
| terraform | `.tf` files, LocalStack/AWS toggle, destroy discipline | lighter coder |
| aws-advisor | cloud/IAM semantics (IRSA, visibility timeout, redrive) — advises, doesn't own files | capable |
| reviewer (internal) | Go idiom, ADR adherence, over-engineering check | robust |
| security | design-time check + final scan (RBAC, trivy, cosign, SBOM) | robust |
| docs | keeps the triad in sync; drafts ADRs | lighter |
| **external reviewer (CLI)** | cross-model code review gate (Opus 4.8 / Codex) | most robust |

Start with the minimum roster that closes a `[TDD][local]` task end-to-end — orchestrator, tester, go, reviewer, docs — and add the rest as the phases demand them.

## Commands

```bash
make build              # compile
make test               # unit tests (fast)
make test-integration   # kind + LocalStack e2e
make lint               # golangci-lint
make manifests generate # CRD/RBAC/deepcopy (kubebuilder)
make deploy-local        # bring up kind + LocalStack + operator
make undeploy-local
```

Prefer `make` targets over raw commands so behavior is consistent across agents.

## Conventions

- **Commits:** follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/). Format: `<type>[optional scope]: <description>`, with an optional body and footers.
  - **Types:** `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`. Scope is an optional noun for the area, e.g. `feat(operator):`, `test(scaling):`.
  - **Task traceability:** put the task id in a footer — `Refs: WR-NNN` (git-trailer style). Keep the description imperative and lowercase; no trailing period.
  - **Breaking changes:** append `!` before the colon (`feat!:`) and/or add a `BREAKING CHANGE:` footer.
  - Example:
    ```
    feat(operator): scale workers from queue backlog

    Reconcile now reads SQS depth and derives replicas via desiredReplicas().

    Refs: WR-031
    ```
- **Branches:** `wr-NNN-short-slug`.
- **Go style:** idiomatic Go; **functional core, imperative shell** — pure decision logic separated from I/O (see ADR-003).
- **Event routing:** `S3 → SNS → SQS` (ADR-001), not EventBridge.
- **Scaling:** backlog-driven via KEDA, scale-to-zero (ADR-002).
- **Repo layout:** `cmd/` (binaries), `internal/` (logic), `api/` (CRD types), `config/` (manifests), `test/`, `hack/`.

## Environments

- **Local (default, $0):** kind + LocalStack. The AWS SDK reads `AWS_ENDPOINT_URL` and dummy `test` creds from settings — no real account touched.
- **Cloud (`[cloud]` tasks only, gated):** real EKS/AWS. Real credentials must come from your shell or `.claude/settings.local.json` (gitignored), **never** committed. Follow the apply → demo → destroy runbook; confirm before every `terraform apply`/`destroy`.

## Version discipline

kubebuilder, controller-runtime, KEDA, and the Kubernetes/envtest binaries have a compatibility matrix. **WR-002 pins the compatible set** — consult it before scaffolding or upgrading; don't guess versions from memory.

## Read next

`DOCUMENTATION.md` (design + ADRs) · `IMPLEMENTATION.md` (task backlog) · `PROGRESS.md` (current status). Branding assets: `weir-brand.svg`, `weir-logo.svg`, `weir-mark.svg`, `weir-favicon.svg`.