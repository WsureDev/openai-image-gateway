// Read-only deployment verification. Credentials stay in .env and are never printed.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { parseEnv } from 'node:util';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const config = { ...process.env, ...parseEnv(await readFile('.env', 'utf8')) };
const base = config.GATEWAY_PUBLIC_URL;
assert.ok(base, 'GATEWAY_PUBLIC_URL is required');
async function request(path, token) {
  return fetch(new URL(path, base), {
    headers: token ? { authorization: `Bearer ${token}` } : {},
    signal: AbortSignal.timeout(5000),
    redirect: 'error',
  });
}
const live = await request('/livez');
assert.equal(live.status, 200);
assert.deepEqual(await live.json(), { status: 'ok' });
const unauthorized = await request('/v1/models');
assert.equal(unauthorized.status, 401);
assert.equal((await request('/gateway/v1/status')).status, 401);
// Invalid payloads exercise the public internal routes without changing registry state.
for (const [path, method, payload, expected] of [
  ['/control/v1/workers/register', 'POST', {}, 400],
  [
    '/control/v1/workers/__auth_probe__/heartbeat',
    'PUT',
    {
      lease_id: '00000000-0000-4000-8000-000000000000',
      sequence: 1,
      state: 'ready',
      load: { in_flight: 0, queue_depth: 0 },
    },
    409,
  ],
  [
    '/control/v1/workers/__auth_probe__/events',
    'POST',
    {
      lease_id: '00000000-0000-4000-8000-000000000000',
      events: [],
    },
    409,
  ],
]) {
  const response = await fetch(new URL(path, base), {
    method,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(5000),
  });
  assert.equal(response.status, expected);
}
const models = await request('/v1/models', config.GATEWAY_GENERATION_TOKEN);
assert.equal(models.status, 200);
const inventory = await models.json();
assert.equal(inventory.object, 'list');
const wrongScope = await request('/gateway/v1/status', config.GATEWAY_GENERATION_TOKEN);
assert.equal(wrongScope.status, 401);
const status = await request('/gateway/v1/status', config.GATEWAY_MANAGEMENT_TOKEN);
assert.equal(status.status, 200);
const snapshot = await status.json();
const ready = await request('/readyz');
assert.equal(ready.status, inventory.data.length ? 200 : 503);
const metrics = await request('/metrics', config.GATEWAY_MANAGEMENT_TOKEN);
assert.equal(metrics.status, 200);
assert.match(await metrics.text(), /image_gateway_workers/);
const client = new Client({ name: 'deployment-verification', version: '1.0.0' });
try {
  await client.connect(
    new StreamableHTTPClientTransport(new URL('/mcp', base), {
      requestInit: { headers: { authorization: `Bearer ${config.GATEWAY_MANAGEMENT_TOKEN}` } },
    }),
  );
  const { tools } = await client.listTools();
  assert.equal(tools.length, 11);
  const toolArguments = {
    workers_list: {},
    workers_get: { worker_id: '__deployment_unregistered__' },
    workers_history: { worker_id: '__deployment_unregistered__' },
    jobs_list: {},
    jobs_get: { job_id: '__deployment_unregistered__' },
    scheduler_snapshot: {},
    scheduler_explain: { job_id: '__deployment_unregistered__' },
    metrics_snapshot: {},
    adapter_configs_list: {},
    adapter_config_get: {
      adapter_type: '__deployment_unknown__',
      image_id: '__deployment_unknown__',
    },
  };
  for (const tool of tools) {
    if (tool.name === 'adapter_config_set') continue;
    assert.ok(Object.hasOwn(toolArguments, tool.name));
    const result = await client.callTool({ name: tool.name, arguments: toolArguments[tool.name] });
    assert.ok(!result.isError, `${tool.name} must succeed`);
  }
  console.log(
    JSON.stringify(
      {
        status: 'passed',
        base_url: base,
        worker_count: snapshot.workers.length,
        model_count: inventory.data.length,
        readiness_http: ready.status,
        mcp_tools: tools.length,
        checked: [
          'liveness',
          'generation_auth',
          'management_scope',
          'internal_routes_without_auth',
          'models',
          'status',
          'metrics',
          'MCP SDK',
        ],
      },
      null,
      2,
    ),
  );
} finally {
  await client.close();
}
