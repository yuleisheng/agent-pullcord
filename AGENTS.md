# agent-pullcord

- To review a pull request, follow [docs/code-review.md](docs/code-review.md).
- Write no code comments except the kinds listed in [docs/code-review.md](docs/code-review.md#code-comments). Review blocks the rest.
- [docs/PRD.md](docs/PRD.md) is the spec. Write tests from its lifecycle and invariants, not from the code. Any change to either goes into the PRD in the same PR.
- Changes to claims, leases, ingest, or events must test the risky sequences, not just the happy path: racing claims, writes after the lease expired, duplicate deliveries, and restarts. `test/lifecycle.test.ts` checks the store against a model of the lifecycle table; change the model when the table changes.
- Tests use a temporary database and fake sources, with no real credentials.
- Setup: `npm ci` (Node 22.18 or later). Check: `npm run typecheck && npm test`.
