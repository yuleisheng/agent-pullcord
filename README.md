# agent-pullcord

A small, headless, self-hosted service between event sources and coding agents. It turns events from services such as PagerDuty and GitHub into tasks. Any agent can claim a task under a fenced lease, work it, and settle it, so two agents never act on the same event.

Status: early. The task store and authenticated HTTP task reads are available. The agent CLI, MCP endpoint, and source webhooks aren't implemented yet. See [docs/PRD.md](docs/PRD.md) for the design.

## Run the HTTP server locally

Use Node 22.18 or later. Generate a key for a local reader and put only its digest in the server's configuration:

```sh
umask 077
export PULLCORD_KEY="$(node -p "require('node:crypto').randomBytes(32).toString('hex')")"
node -e '
  const sha256 = require("node:crypto").createHash("sha256").update(process.env.PULLCORD_KEY).digest("hex")
  console.log(JSON.stringify([{ agent: "local-reader", sha256, queues: ["incidents/"], can: ["read"] }]))
' > pullcord-keys.json
env -u PULLCORD_KEY npm run serve -- --keys pullcord-keys.json --db pullcord.db
```

From a shell with that same key, list tasks:

```sh
printf 'Authorization: Bearer %s\n' "$PULLCORD_KEY" | curl --fail-with-body -H @- http://127.0.0.1:7337/tasks
```

A new database returns an empty list. Use an existing task-store database to read populated tasks. The server listens on loopback by default; use TLS at a reverse proxy before exposing it outside the machine. To revoke a key, remove its entry from the file and restart the server.
