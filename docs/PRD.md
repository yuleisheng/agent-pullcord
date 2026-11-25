# agent-pullcord: product requirements and design

Status: draft for review. No code yet. Decisions marked **Proposed** need an owner's sign-off before the first code PR.

## Problem

In an agent software factory, work starts from events in many services: a PagerDuty incident, a Sentry issue, a failed CI run, a Slack report. Each agent platform now has its own triggers. Cursor and Devin both ship PagerDuty triggers, and Claude Code, Factory, and Amp take webhooks. PagerDuty, incident.io, and Datadog each run their own AI responder.

When several of these are wired to the same source, nothing coordinates them. Each one fires independently, so two agents can investigate the same incident or apply conflicting fixes. Adding a source or an agent means wiring it to every agent or every source.

## Goal

agent-pullcord ("the hub" below) is a small, headless, self-hosted service between sources and agents:

- **Sources in.** It receives events from any service by webhook and turns each event into a task.
- **Agents out.** Any agent, from any vendor, pulls a task, holds it under a lease, works it, and settles it.
- **One owner at a time.** A fenced lease guarantees that at most one agent can act on a task's state at a time.

Adding a source must not require changing any agent, and adding an agent must not require changing any source.

### Requirements

| Requirement | What it means here |
| --- | --- |
| Minimal | One npm package with no native dependencies, using Node's built-in SQLite. Runs in a small VM or sandbox with only Node, and no other services. |
| Robust with many agents | The invariants under [Task lifecycle](#task-lifecycle) hold under concurrent claims, lease expiry, retries, cancellation, and a hub crash. |
| Vendor neutral | No dependency on any agent or cloud vendor. Agents use HTTP, MCP, or the CLI. Only the source adapters know about a specific service. |
| Easy setup | A person or an agent goes from download to a first claimed task in a few commands, with no external accounts beyond the source being connected. |
| Secure | Verified sources only, scoped per-agent keys, an append-only audit log, and payloads treated as untrusted. See [Security](#security). |

### Non-goals for v1

- A UI. The hub is headless. Its API lets anyone build one.
- Running agents. Agents are external; the hub never executes model calls or tools.
- Correlating events across sources with AI. v1 dedupes within a source only.
- High availability or multiple nodes.
- Task dependencies, and agents creating tasks (see [Later](#later)).
- Creating webhook subscriptions in providers automatically.

## Design decisions

The task model follows [Vercel Board](https://github.com/vercel/board)'s tasks. The storage and durability ideas follow [Pi Durable](https://earendil.com/posts/pi-durable/). This project shares no code with either.

| # | Decision | Why |
| --- | --- | --- |
| D1 | One process owns the database. Agents and people attach over HTTP. | Serializes claims without distributed locking. Pi Durable uses the same rule. Never run two hub processes on one database. |
| D2 | SQLite by default, behind a small storage interface with a conformance test suite. | No server to install. Postgres or another store can be added later if the suite passes. |
| D3 | A claim is a lease with an epoch. Every write made under a claim carries that epoch and is rejected if the epoch is stale or the lease has expired. | Fencing stops an agent that stalled past its lease from settling a task that someone else now holds. Taken from Board. |
| D4 | Lease expiry is checked against the server's clock whenever a task is read or claimed. A sweeper only records the transition sooner. | Correctness does not depend on a timer firing. |
| D5 | Each task kind declares `retry: safe` or `retry: manual`. When a lease expires on a manual task, the task becomes `interrupted`, not `ready`. | Fencing protects the hub's state, not the outside world. A lost lease halfway through a rollback must not start a second rollback. Taken from Pi Durable's `replay: "safe"`. |
| D6 | Every write accepts a `request_id`. Repeating a write with the same ID returns the original result and changes nothing. Ingest is idempotent on (source, dedupe key). | Clients and providers retry after timeouts and crashes. Taken from Pi Durable's `requestId`. |
| D7 | Every state change appends one event, in the same transaction. Events carry a gap-free sequence number that clients use as a cursor. | The audit log can never disagree with the state. The same log drives watchers and bridges. |
| D8 | Watchers read state plus a cursor, then long-poll for events after the cursor. The global cursor and complete event feed require all-queue access and the `events` action. | A client that restarts resumes from its saved cursor and misses nothing. Queue-scoped keys cannot infer other queues' activity from the global cursor. v1 needs no streaming infrastructure. |
| D9 | Memos: a small value stored on a task, where the first write wins. | Approvals and other decisions survive restarts instead of being asked again. Taken from Pi Durable. |
| D10 | Write-back to sources (a PagerDuty note, a GitHub comment), escalation, and notifications are bridges. A bridge is a consumer of the event log, not part of the core. | Keeps the core small. Bridges can fail and resume without affecting task state. |
| D11 | Each agent has its own API key, scoped to queues and actions, stored hashed, and revocable. No shared passwords. | Every claim and every event names the agent that acted. |
| D12 | TypeScript on Node 22 or later, using Node's built-in `node:sqlite`, published to npm. Only the SQLite adapter and thin transport/bootstrap adapter may use Node-specific APIs. Core request handlers stay independent of both adapters. A standalone binary can come later, built with `bun build --compile` and a `bun:sqlite` adapter. | Vercel Board and Pi Durable both work this way: npm packages on Node with `node:sqlite`. Board's package has three dependencies and no native modules, so `npx` works with nothing to compile. Pi also ships Bun-compiled binaries for its coding agent, for machines without Node. Risk: Node still flags `node:sqlite` as experimental (Board hides the warning), so pin and test the Node versions we support. |
| D13 | Apache-2.0 license. | Permissive, with a patent grant. |
| D14 | All logic lives in the server. The CLI and the MCP endpoint are thin clients of the HTTP API. | Every interface behaves the same, and adding an interface is cheap. |
| D15 | The name is `agent-pullcord`, and the CLI is `pullcord`. | In Toyota's factories, any worker pulls the andon cord to signal a problem, and someone comes to own it. That is this service's job in a software factory. "Pull" also names the design: agents pull tasks. As of 2026-11-25, `agent-pullcord` was unused on npm and GitHub. "event-hub" collided with Azure Event Hubs, and `agent-andon` was already an npm package for AI coding agents. |

## Task model

A **task** has these fields:

- **Identity:** `id`, `queue` (for example `incidents/checkout`), `kind`, `source`, `dedupe_key`, `correlation_key`
- **Content:** `title`, `severity`, `labels`, `source_url`, `payload` (raw, untrusted)
- **Retry policy:** `retry` (`safe` or `manual`), `attempts`, `max_attempts`
- **Claim:** `state`, `holder`, `epoch`, `lease_expires_at`, `ready_at` (when the task last became `ready`)
- **Outcome:** `result`, `reason`
- **Adapter:** the name and version of the adapter that made the task

An **event** has these fields: `seq`, `task_id`, `type`, `actor`, `request_id`, `data`, and `at`.

A **memo** is keyed by (`task_id`, `key`) and holds a JSON value.

### Task lifecycle

| From | Action | To | Who |
| --- | --- | --- | --- |
| (none) | ingest | `ready` | a verified source |
| `ready` | claim | `claimed` (epoch + 1, attempts + 1) | an agent with the queue's claim scope |
| `claimed` | renew | `claimed` (lease extended) | the holder, with the current epoch |
| `claimed` | complete | `completed` | the holder, with the current epoch |
| `claimed` | release | `ready` (attempts − 1) | the holder, with the current epoch |
| `claimed` | fail, retryable, attempts < max | `ready` | the holder, with the current epoch |
| `claimed` | fail, not retryable or attempts = max | `failed` | the holder, with the current epoch |
| `claimed` | block | `blocked` (the claim ends; attempts − 1) | the holder, with the current epoch |
| `claimed` | lease expires, `retry: safe`, attempts < max | `ready` | the hub |
| `claimed` | lease expires, `retry: safe`, attempts = max | `failed` | the hub |
| `claimed` | lease expires, `retry: manual` | `interrupted` | the hub |
| `blocked` | unblock | `ready` | a person or an admin key |
| `interrupted` | requeue | `ready` (attempts − 1) | a person or an admin key |
| any state that isn't terminal | cancel | `canceled` | an admin key, or the source (for example, PagerDuty resolves the incident) |

`completed`, `failed`, and `canceled` are terminal.

A claim counts an attempt. A release or a block gives it back, because the agent handed the task back instead of trying and failing. A requeue gives it back too, because a person decided to try again. This keeps every `ready` task below `max_attempts`, so a claim never pushes `attempts` past it.

A queue claim takes the matching task that has been `ready` longest, by `ready_at`, then by `id`. An exact-ID claim only considers the named task. Every move into `ready` sets `ready_at`, so a released, retried, unblocked, or requeued task goes to the back of the queue. An agent that keeps releasing one task can't starve the others in a queue claim.

Defaults: `retry: manual`, `max_attempts` 3, and a lease of 15 minutes. A lease can be 10 seconds to 1 hour, and the holder renews it.

### Invariants

These are the expectations tests are written from. Change one only through a reviewed change to this document.

1. **Single holder.** At any moment, at most one (holder, epoch) pair can make a claim-scoped write that succeeds.
2. **Fencing.** A claim-scoped write with a stale epoch, or after the lease has expired, fails with `claim_lost` and changes nothing: no state change and no event.
3. **Terminal is final.** No action moves a task out of `completed`, `failed`, or `canceled`.
4. **Manual tasks never retry themselves.** After its lease expires, a `retry: manual` task reaches `ready` only through a requeue by a person or an admin key.
5. **Bounded attempts.** `attempts` never exceeds `max_attempts`, and a `ready` task is always below it. A `retry: safe` task that runs out of attempts ends in `failed`; a `retry: manual` one waits in `interrupted` for a person.
6. **Idempotent ingest.** Any number of deliveries of the same (source, dedupe key), concurrent or not, create at most one task.
7. **Exactly-once writes.** A successful write repeated with the same `request_id` by the same actor returns the first result and appends no event. A failed write, or a claim that found nothing, isn't recorded, so repeating it runs it again. Reusing a `request_id` for a different write fails with `request_id_reused`.
8. **State matches the log.** Every state change appends exactly one event in the same transaction. Replaying the events from the start reproduces every task's current state.
9. **Complete cursor.** `GET /events?after=c` returns every event with `seq > c`, in order, with no gaps. This holds across a hub restart.
10. **First memo wins.** Once a memo key has a value, later writes to that key return the stored value and change nothing.

**Assumption:** lease times come from the server's wall clock. If the clock jumps backward, leases last longer. That doesn't break invariants 1 or 2, because the epoch still fences writes.

## API v1

Agent endpoints take `Authorization: Bearer <key>`. Task JSON uses snake_case fields; timestamps are Unix milliseconds. Titles and payloads appear only inside `untrusted`.

- **Ingest:**
  - `POST /ingest/:source`: verifies the source's signature, maps the event to a task or a cancel, and answers `202`.
- **Read** (requires the `read` action):
  - `GET /tasks?queue=&state=&label=`: returns `{ tasks }` with tasks in the key's queues. Only keys with the `*` queue grant and the `events` action also get `cursor`, the current global event cursor. An explicit `queue` must be within scope; a slash-terminated queue matches its descendants. Repeat `label=key=value` to require multiple labels.
  - `GET /tasks/:id`: returns one task with its `memos` object. Out-of-scope and missing IDs both return `404 not_found`.
- **Watch** (requires the `*` queue grant and the `events` action):
  - `GET /events?after=<cursor>&wait=<seconds>`: returns `{ events, cursor }` with the complete, unfiltered feed after `after` (a non-negative integer, default 0). Events use `seq`, `task_id`, `type`, `actor`, `request_id`, `from`, `to`, `data`, and `at`. `cursor` is the last returned `seq`, or the requested `after` when no events are returned. The server waits for events for up to `wait` (0–30 whole seconds, default 0). A watcher that takes its initial snapshot from `GET /tasks` also needs `read`.
- **Task writes** require a JSON object with a non-empty `request_id`, the relevant action, and a queue grant. Bodies are limited to 64 KiB. **Claim and holder writes** return the resulting task.
  - `POST /tasks/claim` with `{ request_id, queue, labels?, wait?, lease? }`: claims the match that has been ready longest, or waits up to `wait` (0–30 whole seconds, default 0). A `queue` ending in `/`, such as `incidents/`, matches every queue that starts with it; any other `queue` matches exactly. The requested selector must be within the key's grant. A match has every label in the `labels` object. If there is no match, returns `409 nothing_ready`. `wait` limits retries, not the identity of the write for `request_id` deduplication.
  - To claim only one task, send `{ request_id, id, wait?, lease? }` to the same endpoint with a positive integer `id` and no `queue` or `labels`. The same claim grant and lease rules apply; `read` is not required. Missing and out-of-scope IDs both return `404 not_found`. An in-scope task that is not ready returns `409 nothing_ready` after any wait; the server never substitutes another task.
  - `POST /tasks/:id/renew`, `/complete`, `/fail`, `/release`, `/block` also take the `epoch` returned by claim. The actor comes from the key, not the body; missing and out-of-scope IDs both return `404 not_found`. `renew` accepts optional `lease`; `complete` accepts an optional string `result`; `fail` and `block` require a string `reason`; `fail` accepts optional boolean `retry` (default false). On claim and renew, `lease` is in whole seconds and uses the lifecycle bounds and default.
- **Other task writes** do not require a claim or epoch. Missing and out-of-scope IDs both return `404 not_found`.
  - `POST /tasks/:id/log` with `{ request_id, text }`: appends a non-empty note and returns the event. Requires `log`. Only keys with the `*` queue grant and `events` action receive `seq` in the response.
  - `POST /tasks/:id/memo` with `{ request_id, key, value }`: first write wins; returns `{ value }` with the stored JSON value, including `null`. Numbers anywhere within `value` must be finite. A non-finite number returns `400 invalid_argument` without storing a value, request receipt, or event. Requires `memo`. To read without writing, use the `memos` in `GET /tasks/:id` with `read`.
  - `POST /tasks/:id/unblock`, `/requeue`, `/cancel`: require `admin` and return the resulting task. `cancel` also takes a non-empty `reason`.

### CLI

The CLI is `pullcord`. It reads `PULLCORD_URL` and `PULLCORD_KEY` from the environment. It keeps each claim's epoch in a file only the user can read, so commands after `claim` take just the task ID. `--json` prints machine-readable output and errors.

**Setup, for people:**
- `pullcord init [--db <path>]`: creates the database and prints a one-time admin key.
- `pullcord serve [--listen <addr>] [--public-url <url>]`: runs the hub.
- `pullcord sources add <pagerduty|github|generic> --queue <queue> [--retry safe|manual]`: prints the webhook URL and the secret to paste into the provider.
- `pullcord keys create <agent> --queues <glob> [--can <actions>]`: prints the key once, with an MCP config snippet.
- `pullcord keys list`, `pullcord keys revoke <agent>`.

**Work, for agents:**
- `pullcord claim <queue> [--label k=v] [--wait 30s] [--lease 15m]`: claims the match that has been ready longest and prints it.
- `pullcord renew <id>`, `pullcord complete <id> ["result"]`, `pullcord fail <id> "reason" [--retry]`, `pullcord release <id>`, `pullcord block <id> "question"`.
- `pullcord run <queue|id> -- <command>`: claims a task and runs the command with `PULLCORD_TASK` set, renewing at a third of the lease. Exit 0 completes the task, any other exit fails it, and Ctrl-C releases it. Taken from Board's `tasks run`.
- `pullcord log <id> "note"`, `pullcord memo <id> <key> [value]`.

**Oversight, for people:**
- `pullcord tasks [--queue] [--state]`, `pullcord show <id>`.
- `pullcord unblock <id>`, `pullcord requeue <id>`, `pullcord cancel <id> "reason"`.
- `pullcord events [--after <cursor>] [--follow]`.

### MCP

The hub serves MCP itself at `/mcp`, over streamable HTTP, authenticated with the agent's key. Agents need nothing installed: they add a URL and a key.

- **Tools** mirror the agent commands: claim, renew, complete, fail, release, block, log, memo, list, and show.
- **Epochs** are kept by the MCP session, so tools take only a task ID.
- **`claim` waits** at most 30 seconds, like the HTTP API. An agent that needs to wait longer calls it again.
- **`run` is CLI only.**

**Errors** are JSON with a stable `code`, for example `claim_lost`, `task_claimed`, `nothing_ready`, `unauthorized`, `forbidden`, `invalid_transition`, `not_found`, `invalid_argument`, `request_id_reused`, or `store_locked` (another process owns the database). A missing or invalid agent key returns `401 unauthorized`; insufficient action or explicit queue scope returns `403 forbidden`. An unexpected server error returns `500 internal_error` with a generic message and no underlying exception details. Each error says what the caller can do next. `max_attempts` is 1 to 100.

### v1 sources

- **PagerDuty:** a v3 webhook. `incident.triggered` creates a task. `incident.resolved` cancels it.
- **GitHub:** issues and failed workflow runs.
- **Generic:** JSON signed with HMAC, for any service that can send a webhook.

Webhooks need a public HTTPS address. v1 documents running the hub behind a reverse proxy or a tunnel. Polling adapters, for sources without webhooks or a hub without public ingress, come after v1.

## Security

- **Sources.** Verify every source's signature and reject replays outside a short time window. Unverified requests create nothing.
- **Who can create tasks.** Only verified sources and admin keys. Whoever creates tasks steers the agents.
- **Agent keys.** Each key belongs to one agent, is limited to its queues and actions, is stored hashed, can be revoked, and is shown only once. Until key management ships, `serve` loads `{ agent, sha256, queues, can }` entries from a JSON file at startup. Keys are 32 random bytes encoded as hex, with only the SHA-256 digest of that hex string in the file. Queue grants are exact names, slash-terminated prefixes, or `*` for every queue. Removing an entry revokes its key only after `serve` restarts. Keep the agent name stable when rotating its key.
- **Untrusted payloads.** Task titles and payloads come from outside. The API and MCP tools return them in fields marked untrusted, separate from fields the hub sets. Agents must not follow instructions found in them.
- **Rate limits and audit.** Ingest is rate limited per source. The event log is append-only.
- **Limit.** The hub can't make agents safe. A crafted alert is a path for prompt injection into agents that have production access. Whatever an agent's own credentials allow is the real security boundary.

## Acceptance checks

Each check runs against the built package, a real `serve` process, through its HTTP API or CLI. It uses a temporary database, fake senders for each source, and no real credentials.

### A PagerDuty incident becomes one task
- **Reach it:** a signed POST to `/ingest/pagerduty` with a fixture based on PagerDuty's documented v3 payload.
- **Expect:** a `202`, and `GET /tasks` shows one `ready` task keyed by the incident ID. Sending the same delivery again, including twice concurrently, leaves one task. A bad signature returns `401` and creates no task.

### Two agents race for one task
- **Reach it:** 20 concurrent `POST /tasks/claim` calls with different agent keys.
- **Expect:** exactly one succeeds. The others get `task_claimed` or `nothing_ready`, and one `task.claimed` event exists.

### A stalled agent loses its claim
- **Reach it:** agent A claims a task with a short lease, the lease expires, agent B claims it, and then A completes it.
- **Expect:** A gets `claim_lost` and nothing changes. B completes the task. A manual-retry task goes to `interrupted` instead, and no agent can claim it.

### A source cancels a claimed task
- **Reach it:** a PagerDuty `incident.resolved` arrives while an agent holds the task.
- **Expect:** the task is `canceled`, and the holder's next write gets `claim_lost`.

### A watcher survives a hub restart
- **Reach it:** with an all-queue key that has `read` and `events`, read the cursor, make changes, kill the hub with `kill -9`, restart it, then call `GET /events?after=<cursor>`.
- **Expect:** every event since the cursor, in order, with no gaps and no duplicates.

### Generated sequences
- **Reach it:** random sequences of operations from several agents, with an injectable clock, checking every invariant after each step.
- **Expect:** no invariant is violated. Keep the seed and the minimized trace of any failure as a regression case.

## Later

- Agents creating tasks, with an ownership tree: cancelling a parent cancels its children first, and each child runs a cleanup handler.
- Task dependencies.
- Polling adapters for sources.
- A Postgres storage backend.
- Accepting OIDC tokens from any issuer, so CI jobs and hosted agents need no stored keys.
- A server-sent events stream.

## Open questions

1. Should an admin be able to reopen a `failed` task, or should a new task be created instead? v1 keeps terminal states final.
