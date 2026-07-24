# CLAUDE.md — Weir

Operational contract for this repo. Loaded into every session. This file holds the **rules and pointers**; the detailed design lives in the three documents below. When they conflict, this file's golden rules win.

## What Weir is

A Kubernetes operator (Go) for event-driven processing pipelines. You declare a `ProcessingPipeline` custom resource; Weir provisions the queue, runs the workers, and scales them from zero against the queue backlog — identically on kind + LocalStack ($0) and on real AWS. It *measures the backlog and regulates the flow*.

## The document triad — single sources of truth

| File | Role | Who edits |
|------|------|-----------|
| `DOCUMENTATION.md` | The **why** — design, architecture, ADRs, stack rationale. | Humans + docs agent. Decisions become ADRs here. |
| `IMPLEMENTATION.md` | The **what** — the 66 `WR-NNN` tasks, dependencies, Definition of Done. Stable spec. | Rarely edited (only to add/reshape tasks). |
| `PROGRESS.md` | The **status** — execution diary: status board + append-only log. | **Orchestrator only** (the main thread running a task). |

## Golden rules (non-negotiable)

1. **Only the orchestrator writes `PROGRESS.md`.** Specialist agents return results; the orchestrator (the main thread running the task) is the sole scribe. This is enforced by **instruction**, not tool grants: coders legitimately need write access to files, so the tools field can't scope out a single file — specialists are told to return results and never touch `PROGRESS.md`.
2. **Every unit of work maps to a `WR-NNN` task.** Commits follow **Conventional Commits** with the task id in a `Refs: WR-NNN` footer (see Conventions). Check a task's `Depends on` in `PROGRESS.md` before starting it.
3. **TDD where it belongs (ADR-003).** Write tests first for the pure decision core and for non-trivial reconciler logic. Do *not* force TDD on IaC, framework glue, or AWS wiring — those get integration tests.
4. **Local-first, $0 by default.** All routine work runs on kind + LocalStack. Anything touching real AWS is a `[cloud]` task and **requires explicit human confirmation** before running (cloud commands are permission-gated to `ask`).
5. **Least privilege, no static keys.** Scoped RBAC; IRSA for pod credentials; never hardcode AWS keys.
6. **MVP first, resist over-engineering.** Ship the walking skeleton before stretch goals. Cutting scope on purpose is correct; document what was cut and why.
7. **Decisions vs blockers.** An architecture decision that changes the design → promote to a new ADR in `DOCUMENTATION.md` *and* log it in `PROGRESS.md`. A mere execution blocker (a version clash, a flaky tool) → log in `PROGRESS.md` only.

## Orchestration model — who dispatches whom

Task delivery is driven by the **main thread** (this session) via `/start-task WR-NNN`, **not** by a subagent. This is deliberate and load-bearing: Claude Code subagents are **one level deep** — a subagent cannot spawn another subagent. Only the main thread can dispatch. So the fan-out to specialists must happen from here.

- The **main thread is the orchestrator**: it creates the branch, dispatches specialists, runs the review gate, owns `PROGRESS.md`, and authors the commit/PR hand-off.
- The specialists in `.claude/agents/` are **leaf subagents**: the main thread dispatches them; they never dispatch each other.
- A subagent only receives the **prompt string** as its context — it does not see this conversation, files already read, or decisions already made. So **every dispatch must be a self-contained briefing**: task id, `Done when`, branch name, file paths, relevant ADRs, and any decision made this session that affects the specialist's work. Over-brief rather than under-brief.

The full step-by-step workflow lives in `.claude/commands/start-task.md` and loads only when you run the command, so it doesn't weigh on ordinary sessions.

## The task workflow — `/start-task WR-NNN`

The main thread (orchestrator) drives this loop:

1. **Branch check.** Read `PROGRESS.md` first. If `WR-NNN` is already `IN PROGRESS` there with a recorded branch, and the current branch matches it, this is a resumed session — continue from wherever the log left off; being off `main` is expected, not an error. Otherwise (a fresh start): if on `main`, create and switch to the task branch `<type>/WR-NNN/<scope>` (type from the task's nature; scope a kebab-case slug from the task description). If on neither `main` nor the recorded resume branch, stop and ask the human to switch first — never silently adopt an unrelated branch as the task branch.
2. Read the task from `IMPLEMENTATION.md`; verify every `Depends on` is `DONE` in `PROGRESS.md`.
3. Write `status: IN PROGRESS` (with timestamp + task id + branch) to `PROGRESS.md`. This is the recovery point.
4. Dispatch to specialists in the right order, each with a self-contained briefing. For `[TDD]` tasks: **tester writes the failing test first**, then the coder implements until green.
5. Internal review pass (reviewer agent) against the ADRs and conventions.
6. **Review gate — wait for a verdict before finalizing.**
   - *Now (manual mode):* orchestrator pauses and reports "ready for review"; the human runs the external reviewer (Opus/Codex) in another terminal and returns `pode finalizar` (PASS) or findings (FAIL).
   - *Later (automated mode):* orchestrator invokes the external reviewer CLI as a subprocess and parses a JSON verdict `{ "verdict": "PASS|FAIL", "findings": [...] }`. Same gate, different verdict source.
7. On **PASS**: security agent runs the final scan → orchestrator presents a handoff summary (what was implemented, decisions & deviations, tests, review result) → authors the `WR-NNN:` commit message and the PR title/description and hands them to the human — it never runs `git commit`/`git push` or opens/merges the PR itself → writes `status: IN PROGRESS` with Notes `awaiting merge` to `PROGRESS.md`. Once the human confirms the PR is merged to `main`, orchestrator writes `status: DONE` + summary + which agents acted + which model reviewed.
8. On **FAIL**: log findings, then route each one to the specialist who owns the affected artifact — Go code to the go specialist, CRD/reconcile/RBAC/KEDA/Helm to the kubernetes specialist, `.tf`/LocalStack toggle to the terraform specialist, test-coverage gaps to the tester, security findings to the security specialist, docs/ADR findings to the docs specialist — rather than a single generic "the coder"; a task can span more than one owner. Re-review the diff only. **Cap at 3 iterations**; on the cap, stop and escalate to the human. `high` severity blocks; `low` is a note, not a blocker.

## Agent roster

Main thread = **orchestrator** (it dispatches; it is not itself a subagent). Specialists are subagents (`.claude/agents/`), each with a restricted tool set and its own model tier. The external reviewer is a separate CLI (not a subagent).

| Agent | Role | Model tier |
|-------|------|-----------|
| orchestrator (main thread) | routes tasks, owns `PROGRESS.md`, runs the loop; creates the task branch but never commits/pushes/opens or merges PRs — authors that text for the human | the session model (Sonnet 5) |
| tester | writes tests first (TDD); runs the suite | robust (test design is hard) |
| go | all Go code (operator, worker, loadgen, SDK) | lighter coder (Sonnet 5 / Gemini Flash) |
| kubernetes | CRD, reconcile, RBAC, KEDA, Helm | lighter coder |
| terraform | `.tf` files, LocalStack/AWS toggle, destroy discipline | lighter coder |
| aws-advisor | cloud/IAM semantics (IRSA, visibility timeout, redrive) — advises, doesn't own files | capable |
| reviewer (internal) | Go idiom, ADR adherence, over-engineering check | robust |
| security | design-time check + final scan (RBAC, trivy, cosign, SBOM) | robust |
| docs | keeps the triad in sync; drafts ADRs | lighter |
| **external reviewer (CLI)** | cross-model code review gate (Opus 4.8 / Codex) | most robust |

Start with the minimum roster that closes a `[TDD][local]` task end-to-end — tester, go, reviewer, docs — and add the rest as the phases demand them.

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
  - **No attribution trailer:** never add `Co-authored-by` or any "generated with" line to a commit or PR.
  - **Breaking changes:** append `!` before the colon (`feat!:`) and/or add a `BREAKING CHANGE:` footer.
  - Example:

    ```text
    feat(operator): scale workers from queue backlog

    Reconcile now reads SQS depth and derives replicas via desiredReplicas().

    Refs: WR-031
    ```

- **Branches:** `<type>/WR-NNN/<scope>` — the same Conventional-Commit type as the task's commit, then the task id, then a short kebab-case slug from the task description. E.g. `feat/WR-006/desired-replicas`, `chore/WR-003/makefile`. The orchestrator creates the branch off `main` at the start of `/start-task`.
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
