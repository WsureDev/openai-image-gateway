# OpenAI Image Gateway

Use Node.js 24+ and TypeScript. Run `npm ci`, `npm run typecheck`, `npm test`, and `npm run format:check` for implementation changes. No model code belongs here.

Tests must use fake HTTP workers. Do not change or contact existing model services as part of tests. Model inference requires an explicitly scoped user request.

The independent `openai-image-worker-adapter` repository vendors the same protocol source and contract. Maintain one current contract with identical copies. Do not add compatibility branches, migration guides or historical status to documentation. Do not import the other repository at runtime.

Preserve persistent idempotency, uncertain reservations, client/management token separation and model-agnostic routing. Per user policy, worker registration, heartbeats, events and Adapter traffic are unauthenticated on the trusted LAN; leases and task ownership remain required for correctness. No automatic inference retry after an ambiguous transport failure. Container start/rebuild/configuration changes require the user's Container Change Notice and authorization rules.
