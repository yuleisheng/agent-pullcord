# agent-pullcord

A headless, self-hosted hub that turns signed webhooks into tasks for coding agents. Agents can work over HTTP, Streamable HTTP MCP, or the `pullcord` CLI. The hub accepts claim-scoped writes only from the current lease holder.

## Install from a checkout

Use Node 22.18 or later. The first npm release is not yet published. In the source checkout, build and install the same tarball used by the acceptance checks:

```sh
npm ci
npm pack
npm install --global ./agent-pullcord-1.0.0.tgz
```

## Claim your first task without a provider account

From a new working directory:

```sh
pullcord init
pullcord keys create demo-agent --queues demo/
pullcord sources add generic --queue demo/local
pullcord serve
```

Save the admin key separately. For this demo, use the **demo-agent key** and the **Generic signing secret**, not the admin key. Both are printed when created, not by `keys list`. The hub stores only key digests; the source file contains live signing secrets and should stay owner-readable only.

In another Bash shell on the same machine, send a signed fixture and claim it:

```sh
read -rsp 'Demo agent key: ' PULLCORD_KEY; printf '\n'
read -rsp 'Generic signing secret: ' DEMO_SOURCE_SECRET; printf '\n'
export PULLCORD_KEY DEMO_SOURCE_SECRET
export PULLCORD_URL=http://127.0.0.1:7337

node --input-type=module <<'JS'
import { createHmac } from 'node:crypto'
const body = JSON.stringify({ timestamp: Date.now(), dedupe_key: 'demo-1', title: 'Verify the demo' })
const signature = createHmac('sha256', process.env.DEMO_SOURCE_SECRET).update(body).digest('hex')
const response = await fetch(`${process.env.PULLCORD_URL}/ingest/generic`, {
  method: 'POST', body,
  headers: { 'Content-Type': 'application/json', 'X-Pullcord-Signature': `sha256=${signature}` },
})
if (response.status !== 202) throw new Error(`Webhook rejected (${response.status}): ${await response.text()}`)
JS
unset DEMO_SOURCE_SECRET

pullcord --json claim demo/local
pullcord complete 1 'Demo verified'
```

Use the returned task ID instead of `1` if you started with an existing database. `pullcord --help` lists the commands. `pullcord run <queue|id> -- <command>` leases and runs a command locally; a decimal selector is an exact task ID. If renewal loses the claim, `run` stops the command and does not settle the task.

## Connect a source or an agent

- Put the hub behind an HTTPS reverse proxy or tunnel before exposing it outside its host. It listens on loopback by default. Pass `pullcord serve --public-url https://hub.example.com` and use the same base in `PULLCORD_URL` when printing source URLs or MCP snippets. A proxy base with a path prefix must strip that prefix upstream. Forward the original body, signature and authorization headers, and MCP session/version headers; allow at least 30 seconds for long-polling requests.
- For [GitHub](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), run `pullcord sources add github --queue incidents/ci`. Use the printed URL and secret in a JSON webhook subscribed to issues or workflow runs. For [PagerDuty](https://docs.pagerduty.com/developer/verifying-webhook-signatures), create a v3 subscription to `/ingest/pagerduty` for triggered and resolved incidents, then run `pullcord sources add pagerduty --queue incidents/on-call --secret-file <path>` with its provider-issued secret. Protect and remove the temporary secret file after import. The [source contracts](docs/PRD.md#v1-sources) specify exactly which events create work and the five-minute freshness limit, which also rejects late provider retries.
- Give each agent its own scoped key. For a Streamable HTTP MCP client, use the URL and bearer header from `pullcord keys create`. For the CLI, set `PULLCORD_URL` and `PULLCORD_KEY`. Restart `serve` after adding a source, issuing a key, or revoking one with `pullcord keys revoke <agent>`. Use `--db`, `--keys`, and `--sources` to select paths other than the default `pullcord.db`, `pullcord-keys.json`, and `pullcord-sources.json`.
- If a crashed setup command leaves a `<config-path>.pullcord-lock` file, stop all setup commands using that config and ensure none can start during recovery. Remove the named lock file, then retry the setup command.
- For a watcher, use a key with the `*` queue grant and `events` (plus `read` for a task snapshot). Save the cursor and reconnect with `pullcord --json events --after <cursor> --follow` for one JSON event per line. The [HTTP API](docs/PRD.md#api-v1) exposes the same operations and cursor.

## Checks

```sh
npm run typecheck && npm test && npm run test:acceptance
```

The acceptance suite installs the tarball offline and runs the installed `pullcord serve` against temporary databases and fake webhook senders. It does not publish the package.
