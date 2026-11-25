# agent-pullcord

A small, headless, self-hosted service between event sources and coding agents. It turns events from services such as PagerDuty and GitHub into tasks. Any agent can claim a task under a fenced lease, work it, and settle it, so two agents never act on the same event.

Status: early. The HTTP server supports signed source webhooks, task reads and writes, and event watching. The agent CLI and MCP aren't implemented yet. See [docs/PRD.md](docs/PRD.md) for the design.

## Run the HTTP server locally

Use Node 22.18 or later. Generate a key for a local agent and put only its digest in the server's configuration:

```sh
umask 077
export PULLCORD_KEY="$(node -p "require('node:crypto').randomBytes(32).toString('hex')")"
node -e '
  const sha256 = require("node:crypto").createHash("sha256").update(process.env.PULLCORD_KEY).digest("hex")
  console.log(JSON.stringify([{ agent: "local-agent", sha256, queues: ["incidents/"], can: ["read", "claim", "complete"] }]))
' > pullcord-keys.json
env -u PULLCORD_KEY npm run serve -- --keys pullcord-keys.json --db pullcord.db
```

From a shell with that same key, list tasks:

```sh
printf 'Authorization: Bearer %s\n' "$PULLCORD_KEY" | curl --fail-with-body -H @- http://127.0.0.1:7337/tasks
```

A new database returns an empty list. Use an existing task-store database to read populated tasks. The global event cursor appears only for keys with both `"*"` queue access and the `events` action, in addition to `read`. Save that cursor and call `GET /events?after=<cursor>&wait=30` with an all-queue `events` key to follow changes.

The server listens on loopback by default; use TLS at a reverse proxy before exposing it outside the machine. Keys are loaded at startup. To revoke a key, remove its entry from the file and restart the server.

To connect a source, save a JSON array of `{name, provider, queue, secret, retry?}` entries in `pullcord-sources.json` or pass `--sources <path>` to `serve`. Keep the file owner-readable only; it contains live signing secrets. Restart after edits. Send JSON to `/ingest/<name>` at your public HTTPS address. The [PRD](docs/PRD.md#v1-sources) specifies each signature header, payload, and the five-minute freshness window. Use the secret [provided by PagerDuty](https://docs.pagerduty.com/developer/verifying-webhook-signatures) for its v3 webhook; for [GitHub](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries), choose a separate webhook secret and subscribe to issue and workflow run events.

Claim with `POST /tasks/claim` and `{"queue":"incidents/demo","request_id":"demo-claim-1"}` as JSON. To finish, use `POST /tasks/:id/complete` with the returned task's `id` in the URL and its `epoch` in the JSON body, alongside a new `request_id`. Reuse the same request ID when retrying the same write.
