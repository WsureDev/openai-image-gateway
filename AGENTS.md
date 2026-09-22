# OpenAI Image Gateway

Use Node.js 24+ and TypeScript. Run `npm ci`, `npm run typecheck`, `npm test`, and `npm run format:check` for implementation changes. No model code belongs here.

Tests must use fake HTTP workers. Do not change or contact existing model services as part of tests. Real model integration is a separately authorized validation phase after the worker optimization is complete.

The independent `openai-image-worker-adapter` repository vendors the same protocol source and contract. Keep these copies identical when changing v1. Version incompatible wire changes explicitly. Do not import the other repository at runtime.

Preserve persistent idempotency, uncertain reservations, token separation and model-agnostic routing. No automatic inference retry after an ambiguous transport failure. Container start/rebuild/configuration changes require the user's Container Change Notice and authorization rules.
