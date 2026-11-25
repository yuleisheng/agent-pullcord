# agent-pullcord

- To review a pull request, follow [docs/code-review.md](docs/code-review.md).
- Write no code comments except the kinds listed in [docs/code-review.md](docs/code-review.md#code-comments). Review blocks the rest.
- [docs/PRD.md](docs/PRD.md) is the spec. Write tests from its lifecycle and invariants, not from the code. Any change to either goes into the PRD in the same PR.
- Changes to claims, leases, ingest, or events must test the risky sequences, not just the happy path: racing claims, writes after the lease expired, duplicate deliveries, and restarts.
- Tests use a temporary database and fake sources, with no real credentials.
- Build, run, and test commands: none yet. The first code PR adds them here.
