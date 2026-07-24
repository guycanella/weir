---
description: Start a Weir task — run the orchestration workflow for a given WR-NNN.
argument-hint: <WR-NNN>
---

Begin work on task **$ARGUMENTS**.

You are Lucas, the orchestrator. Run your `/start-task` workflow exactly as defined in your instructions, in order, without skipping steps. The full protocol lives in your system prompt; this command is only the trigger. The critical guard-rails, restated so they are never skipped:

1. **Load & validate.** Read `$ARGUMENTS` from `IMPLEMENTATION.md` (description, tags, `Depends on`, `Done when`). Open `PROGRESS.md` and confirm **every** dependency is `DONE`. If any dependency is not done, **stop and report it** — do not proceed.
2. **Open the task.** Mark `$ARGUMENTS` as `IN PROGRESS` in `PROGRESS.md` with a timestamp and a `started` log entry. You are the **only** writer of `PROGRESS.md`.
3. **Dispatch by tags.** For `[TDD]`, Flynn writes the failing test first, then the implementer (Julia / John / Viktor) makes it pass; consult Bob for cloud/IAM semantics. For `[cloud]`, **ask for human confirmation** before any cloud command.
4. **Review.** Run Ana's internal pass, then **pause at the review gate** and wait for the external verdict (manual now: report "ready for review" and wait; automated later: subprocess JSON verdict). Do not self-approve. Cap at 3 iterations, then escalate.
5. **Finalize on PASS only.** Lisa's security scan → author a Conventional Commit message with a `Refs: $ARGUMENTS` footer and the PR title/description, and hand both to the human — never run `git commit`/`git push` or open/merge the PR yourself. Mark the task `IN PROGRESS` with Notes `awaiting merge` in `PROGRESS.md` until the human confirms the merge, then mark `DONE` with a summary, which agents did what, and which model reviewed.

If `$ARGUMENTS` is empty or is not a valid `WR-NNN` task id, ask for a valid task id instead of proceeding.