import { GatewayService } from './gateway.js';
import { buildServer } from './server.js';
import { StateStore } from './store.js';
import { origin } from './protocol.js';
import type { GatewayConfig } from './types.js';
function secret(name: string): string {
  const v = process.env[name];
  if (!v || v.length < 24) throw new Error(`${name} must contain at least 24 characters`);
  return v;
}
function number(name: string, fallback: number): number {
  const n = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(n) || n <= 0) throw new Error(`${name} must be a positive integer`);
  return n;
}
const port = number('GATEWAY_PORT', 8188);
const config: GatewayConfig = {
  host: process.env.GATEWAY_HOST ?? '127.0.0.1',
  port,
  state_path: process.env.GATEWAY_STATE_PATH ?? './data/gateway.sqlite',
  public_url: origin(process.env.GATEWAY_PUBLIC_URL ?? `http://127.0.0.1:${port}`),
  generation_token: secret('GATEWAY_GENERATION_TOKEN'),
  management_token: secret('GATEWAY_MANAGEMENT_TOKEN'),
  default_model: process.env.GATEWAY_DEFAULT_MODEL,
  heartbeat_ttl_ms: number('GATEWAY_HEARTBEAT_TTL_MS', 30000),
  worker_request_timeout_ms: number('GATEWAY_WORKER_REQUEST_TIMEOUT_MS', 2700000),
  max_request_bytes: number('GATEWAY_MAX_REQUEST_BYTES', 64 * 1024 * 1024),
  max_response_bytes: number('GATEWAY_MAX_RESPONSE_BYTES', 128 * 1024 * 1024),
  max_queued_jobs: number('GATEWAY_MAX_QUEUED_JOBS', 100),
  queue_timeout_ms: number('GATEWAY_QUEUE_TIMEOUT_MS', 300000),
  poll_ms: 1000,
  allowed_worker_origins: process.env.GATEWAY_ALLOWED_WORKER_ORIGINS?.split(',')
    .map((value) => value.trim())
    .filter(Boolean)
    .map(origin),
};
const store = new StateStore(config.state_path),
  gateway = new GatewayService(config, store),
  server = buildServer(gateway);
gateway.start();
server.listen(port, config.host, () =>
  console.log(`OpenAI Image Gateway listening on ${config.host}:${port}`),
);
async function stop() {
  server.close();
  await gateway.stop();
  server.closeAllConnections();
  store.close();
}
process.once('SIGINT', () => {
  void stop();
});
process.once('SIGTERM', () => {
  void stop();
});
