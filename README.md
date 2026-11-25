# agent-pullcord

A small, headless, self-hosted service between event sources and coding agents. It turns events from services such as PagerDuty and GitHub into tasks. Any agent can claim a task under a fenced lease, work it, and settle it, so two agents never act on the same event.

Status: early. The HTTP server can list tasks, claim them, and perform holder writes. Source webhooks, event watching, the agent CLI, and MCP aren't implemented yet. See [docs/PRD.md](docs/PRD.md) for the design.

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

A new database returns an empty list. Use an existing task-store database to read populated tasks. The global event cursor appears only for keys with both `"*"` queue access and the `events` action, in addition to `read`. The event endpoint is not implemented yet.

The server listens on loopback by default; use TLS at a reverse proxy before exposing it outside the machine. Keys are loaded at startup. To revoke a key, remove its entry from the file and restart the server.

To try claiming before webhooks are available, stop the server, seed a local task, and start the server again:

```sh
node --input-type=module -e '
  import { openStore } from "./src/store.ts"
  const store = openStore({ path: "pullcord.db" })
  store.ingest({ queue: "incidents/demo", kind: "incident", source: "demo", dedupeKey: "one", title: "Check the demo" }, { actor: "source:demo" })
  store.close()
'
```

Claim with `POST /tasks/claim` and `{"queue":"incidents/demo","request_id":"demo-claim-1"}` as JSON. To finish, use `POST /tasks/:id/complete` with the returned task's `id` in the URL and its `epoch` in the JSON body, alongside a new `request_id`. Reuse the same request ID when retrying the same write.
