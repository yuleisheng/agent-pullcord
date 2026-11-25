# agent-pullcord

A small, headless, self-hosted service between event sources and coding agents. It turns events from services such as PagerDuty and GitHub into tasks. Any agent can claim a task under a fenced lease, work it, and settle it, so two agents never act on the same event.

Status: design. Start with [docs/PRD.md](docs/PRD.md).
