# Code review

How to review a pull request in this repo, whether a person or an agent does it. [PRD.md](PRD.md) is the spec. A review checks the diff against the PRD and this page, not against what the author says the diff does.

## Setup

- Review in a fresh session, not the one that wrote the change. Use the exact base and head commits, the PR description, and the PRD.
- Run the check commands from [AGENTS.md](../AGENTS.md) on the head commit. If you can't run them, stop and report what stopped you instead of giving a verdict. If the PR changes only docs, or the repo has no checks yet, say so.
- An agent reviewer reads and reports. It doesn't push to the PR branch, approve on GitHub, or merge. The repo owner merges.

## What to check

Check only the areas the diff touches. Read the code around each change, not just the changed lines.

1. **PRD and code agree.** A change to the lifecycle table, the invariants, a design decision, the API, or the error codes updates `docs/PRD.md` in the same PR. Review a PRD edit like a changed test assertion: does the new rule still agree with every other row and invariant? An edit to an invariant, a lifecycle row, or a decision needs the owner's sign-off, so list it under owner decisions.
2. **Lifecycle and invariants.** For each state change in the diff, find the lifecycle row that allows it and check who may do it. Check that each state change appends exactly one event in the same transaction (invariant 8), terminal states stay final (3), `attempts` never exceeds `max_attempts` (5), and a `retry: manual` task never returns to `ready` without a requeue (4).
3. **Leases and fencing.** Every claim-scoped write checks holder, epoch, and lease expiry against the server's clock, inside the same transaction as the write. A stale write fails with `claim_lost` and changes nothing: no state change and no event (invariant 2). Expiry is applied when a task is read or claimed, not by a timer (D4). Look for check-then-act: a read outside the transaction followed by a write. Check the boundary at exactly `lease_expires_at`.
4. **Idempotency.** A repeated `request_id` returns the first result and appends no event (invariant 7). Ingest dedupes on (source, dedupe key) under concurrent deliveries, through a constraint or a transaction, not a read followed by an insert (6). The first memo write wins (10). Event sequence numbers stay gap-free when a transaction rolls back, and a cursor resumes completely after a restart (9).
5. **Security.** An ingest path verifies the source's signature, and rejects replays, before it creates or cancels anything. Titles and payloads from sources stay in untrusted fields: never copied into fields the hub sets, never used to build SQL or shell commands, and never presented to agents as instructions. Every endpoint checks the agent key's queue and action scope in the server, so HTTP, MCP, and the CLI behave the same (D14). Keys are stored hashed and never logged. Tests and fixtures hold no real credentials. A new dependency needs a reason and can't be native (D12).
6. **Tests.** Tests come from the PRD, not from the code. Changes to claims, leases, ingest, or events test the risky sequences listed in AGENTS.md, not just the happy path. Name one plausible wrong implementation, such as `<` instead of `<=` at the lease boundary or fencing on epoch alone, and check that a test would fail on it. Tests reach the behavior through the interface callers use: the store's API now, and HTTP or the CLI against a real `serve` process for the PRD's acceptance checks. A bug fix comes with a test that fails without the fix. When the lifecycle table changes, the generated-sequence model changes with it and stays independent of the store code. Tests use a temporary database, fake sources, and an injectable clock instead of sleeps. A failing generated seed and its minimized trace stay as a regression case.
7. **Schema, events, and rollback.** Once someone has a database, its tables, constraints, stored JSON, and each event's `type` and `data` are a compatibility surface: watchers and bridges read events through the cursor, and invariant 8 needs old events to keep replaying. A change to any of them needs a migration, or a stated decision that there is none and what happens to an existing database. Check the PR's rollback and blast-radius notes against the code: reverting the code doesn't undo data the change already wrote.
8. **Boundaries and patterns.** New code follows the existing code that does the same job instead of starting a parallel version of it. Only the storage adapter uses Node-specific APIs (D12). The CLI and MCP stay thin clients of the HTTP API (D14). Commands in AGENTS.md and the README still work.

Don't report style preferences. If you have seen the same finding on an earlier PR, say so and propose a test or check that catches it, so the next review doesn't have to.

## Code comments

Code comments are deleted by default. Only these stay:

- License headers.
- Behavior forced by something outside this repo that we can't change, such as a `node:sqlite` limitation, a provider's webhook format, or a protocol. Link the source.
- Doc comments that define a public API contract.
- A link to an issue or a PRD decision or invariant, for a constraint the code can't express.
- A lint suppression for a rule that is wrong or only about style.

Every other comment is a blocker: narration of the change, commented-out code, banners, TODOs, and explanations of our own code. If our own code needs a comment to be understood, rename, extract, or add a type so it doesn't. Turn a "do not remove" or "important" comment into a test, type, or check, then delete it. A `@ts-ignore`, `@ts-expect-error`, or `eslint-disable` on a rule that catches real bugs is a blocker: fix the code instead. If you aren't sure an exception applies, it doesn't. New Markdown files nobody asked for are blockers too.

## What the review produces

```text
Verdict: HAS_MERGE_BLOCKERS | MERGEABLE_WITH_COMMENTS | MERGEABLE
Checks: <commands run on <head sha>, and the result>
Owner decisions: none | <each PRD rule or decision the owner must accept>
Findings:
- [blocker|comment] <file:line> <rule: PRD invariant, row, or decision, or a section above>
  <what goes wrong, with a concrete sequence or failing input>
  <smallest fix, as a suggestion block when it is clear>
```

If there are no findings, write `Findings: none`. Mark anything you suspect but couldn't confirm as uncertain.

## Verdict

The verdict follows from the findings, so reviewers who agree on the findings agree on the verdict:

| Verdict | When |
| --- | --- |
| `HAS_MERGE_BLOCKERS` | At least one blocker. Fix it and rerun the checks before merging. |
| `MERGEABLE_WITH_COMMENTS` | No blockers and at least one comment. The author may merge without changing anything, and answers or files each comment. |
| `MERGEABLE` | No findings, and the checks passed on the head commit. |

A finding is a **blocker** when any of these holds. Everything else is a comment.

- It breaks a PRD invariant or lifecycle row, or the code and the PRD disagree. A plausible break of an invariant or a security rule that you couldn't rule out also counts: ask for the test that rules it out.
- Changed claim, lease, ingest, or event code lacks a risky-sequence test, or its tests would pass the plausible wrong implementation.
- It breaks a rule under Security.
- It adds a code comment, suppression, or Markdown file that [Code comments](#code-comments) doesn't allow.
- It changes the schema or an event's shape without a migration or a stated decision about existing databases.
- A check fails on the head commit.

The verdict covers the diff. Owner decisions are separate: a PR with open owner decisions doesn't merge until the owner accepts them, whatever the verdict.
