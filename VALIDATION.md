# Validation — 2026-09-22

Runtime: Node.js 24.18.0. Dependencies installed independently from package-lock.json using npm ci. No symlinks to another repository's dependencies.

Passed:

- TypeScript typecheck, build, Prettier check.
- 15 Gateway integration tests with temporary loopback HTTP workers.
- OpenAI SDK model listing and image generation against Gateway.
- Official MCP SDK initialization, tool enumeration, and worker query.
- Three-worker capacity isolation, model-based routing, multipart binary preservation.
- Concurrent/persistent idempotency; mismatched request rejection.
- Lease expiry, old-token fencing, task-event replay deduplication.
- Ambiguous dispatch reconciliation without duplicate inference; Gateway restart recovery.
- Queue timeout, real-time SSE, late duplicate SSE replay, Gateway-owned image URLs, body limits.
- Cross-repository smoke: three real Adapter runtimes with injected fake backends; six jobs distributed across all three; successful events persisted.
- Adapter test suite: 4 tests, using only fake backends/Gateway and fixture GPU CSV.
- Protocol source, Markdown contract and JSON Schema are byte-identical in both repositories.

Not performed:

- No live Qwen/Hunyuan image inference, API requests to existing model services, or GPU telemetry commands.
- No modifications to the existing qwen-image API, production model/container workloads, or client configuration.
- Adapter containers have not been built or started.

Deployment acceptance completed on 2026-09-22:

- Built the Gateway image from a digest-pinned Node 24 Alpine base with npm ci.
- Started only `openai-image-gateway` using the standard root compose.yaml.
- Docker healthcheck passed; non-root user, read-only root filesystem, dedicated data volume and restart policy verified through Docker inspection.
- Default Docker address pools were exhausted; the Gateway received an isolated, non-overlapping 10.253.80.0/24 Compose subnet. Existing networks were unchanged.
- Read-only verification against the deployed Gateway passed: liveness, generation authentication, management scope, models, status, metrics, and official MCP SDK initialization/list/call for all eight exposed tools.
- Zero registered workers and an empty model list were confirmed. /readyz returned the expected 503 while /livez returned 200.
- The three independent credentials are stored only in ignored, mode-0600 .env; no credentials were printed or committed.

Next phase after worker optimization completes: inspect the optimized worker's finalized API and capability limits, align the Qwen connector, then validate single-request generation/editing and finally three-GPU concurrency. The current connector follows the previously inspected legacy /health + /generate shape only. Uncertain model execution is intentionally not auto-retried.
