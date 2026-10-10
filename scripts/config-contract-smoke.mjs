// Exercises the real Gateway, MCP and Qwen Adapter against a fake HTTP model only.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { GatewayService } from '../dist/src/gateway.js';
import { StateStore } from '../dist/src/store.js';
import { buildServer as buildGateway } from '../dist/src/server.js';

const adapterRoot = process.argv[2];
if (!adapterRoot) throw new Error('provide the built Adapter repository path');
const importAdapter = (path) => import(pathToFileURL(resolve(adapterRoot, path)).href);
const { AdapterService, buildServer: buildAdapter } = await importAdapter('dist/src/server.js');
const { QwenBackend } = await importAdapter('dist/src/backends/qwen/backend.js');
const root = await mkdtemp(join(tmpdir(), 'adapter-config-contract-'));
const generation = 'fake-generation-token-at-least-24';
const management = 'fake-management-token-at-least-24';
const model = 'qwen-image-2.1';
const listen = async (server) => {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return `http://127.0.0.1:${server.address().port}`;
};
const close = async (server) => {
  if (!server.listening) return;
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
};
const until = async (condition) => {
  const deadline = Date.now() + 5000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('registration timed out');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
const received = [];
let duringGeneration;
const worker = createServer((req, res) => {
  void (async () => {
    if (req.url === '/health') {
      res.end(JSON.stringify({ status: 'ok', tier: 'sr' }));
      return;
    }
    assert.equal(req.url, '/generate');
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const payload = JSON.parse(Buffer.concat(chunks).toString());
    received.push(payload);
    if (duringGeneration) {
      const hook = duringGeneration;
      duringGeneration = undefined;
      await hook();
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
    res.end(
      JSON.stringify({ image_base64: 'ZmFrZQ==', width: payload.width, height: payload.height }),
    );
  })().catch((e) => {
    res.writeHead(500);
    res.end(JSON.stringify({ error: e.message }));
  });
});
const config = {
  host: '127.0.0.1',
  port: 0,
  state_path: join(root, 'gateway.sqlite'),
  public_url: 'http://127.0.0.1:1',
  generation_token: generation,
  management_token: management,
  heartbeat_ttl_ms: 1000,
  worker_request_timeout_ms: 5000,
  max_request_bytes: 1048576,
  max_response_bytes: 1048576,
  max_queued_jobs: 20,
  queue_timeout_ms: 3000,
  poll_ms: 20,
};
const store = new StateStore(config.state_path);
const gateway = new GatewayService(config, store);
const http = buildGateway(gateway);
const adapters = [];
const client = new Client({ name: 'config-contract-test', version: '1' });
let pulls = 0;
http.on('request', (req) => {
  if (req.url?.startsWith('/control/v1/config?')) pulls++;
});
try {
  config.public_url = await listen(http);
  const workerUrl = await listen(worker);
  gateway.start();
  for (const [index, defaults] of [28, 40].entries()) {
    const ac = {
      host: '127.0.0.1',
      port: 0,
      public_url: 'http://127.0.0.1:1',
      worker_id: `config-adapter-${index}`,
      gateway_url: config.public_url,
      state_path: join(root, `adapter-${index}.sqlite`),
      max_request_bytes: 1048576,
      max_response_bytes: 1048576,
      slots: 1,
      request_timeout_ms: 5000,
      heartbeat_interval_ms: 30,
      resources: { node_id: `fake-node-${index}` },
    };
    const backend = new QwenBackend(workerUrl, model, undefined, { defaultSteps: defaults });
    const service = new AdapterService(ac, backend);
    const ah = buildAdapter(service);
    adapters.push({ service, http: ah });
    ac.public_url = await listen(ah);
    service.control.start();
    await until(
      () => gateway.workerSnapshots().filter((w) => w.state === 'ready').length === index + 1,
    );
  }
  assert.equal(pulls, 0); // registration and heartbeats do not fetch runtime config
  assert.equal(gateway.adapterConfigs().length, 1);
  assert.equal(gateway.adapterConfig('qwen', model).config.steps, 28);
  await client.connect(
    new StreamableHTTPClientTransport(new URL('/mcp', config.public_url), {
      requestInit: { headers: { authorization: `Bearer ${management}` } },
    }),
  );
  const set = async (steps) => {
    const result = await client.callTool({
      name: 'adapter_config_set',
      arguments: {
        adapter_type: 'qwen',
        image_id: model,
        config: { steps, steps_mode: 'default' },
      },
    });
    assert.ok(!result.isError, JSON.stringify(result));
  };
  const generate = async (fields = {}, key) => {
    const response = await fetch(new URL('/v1/images/generations', config.public_url), {
      method: 'POST',
      headers: {
        authorization: `Bearer ${generation}`,
        'content-type': 'application/json',
        ...(key ? { 'idempotency-key': key } : {}),
      },
      body: JSON.stringify({ model, prompt: 'configuration contract fixture', ...fields }),
    });
    assert.equal(response.status, 200);
    return response.json();
  };
  await set(42);
  await Promise.all([generate(), generate()]);
  assert.deepEqual(
    received.map((p) => p.steps),
    [42, 42],
  );
  assert.equal(pulls, 2);
  assert.equal(new Set(gateway.jobs().map((j) => j.worker_id)).size, 2);
  await set(44);
  duringGeneration = () => set(46);
  const result = await generate({ n: 2 }, 'batch-config');
  assert.deepEqual(
    received.slice(2).map((p) => p.steps),
    [44, 44],
  );
  assert.equal(pulls, 3);
  assert.deepEqual(await generate({ n: 2 }, 'batch-config'), result);
  assert.equal(pulls, 3); // completed idempotent requests are replayed without a config GET
  await generate();
  assert.equal(received.at(-1).steps, 46);
  assert.equal(pulls, 4);
  const direct = await fetch(
    new URL('/v1/images/generations', adapters[0].service.config.public_url),
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model, prompt: 'direct fixture' }),
    },
  );
  assert.equal(direct.status, 200);
  await direct.arrayBuffer();
  assert.equal(received.at(-1).steps, 28);
  assert.equal(pulls, 4);
  assert.deepEqual(
    adapters.map((a) => a.service.backend.models()[0].metadata.default_steps),
    [28, 40],
  );
  assert.equal(gateway.adapterConfigs().length, 1);
  console.log(
    'PASS: MCP updates reach both Qwen Adapters on the next request; one pull per batch, no refetch on replay, direct/local defaults unchanged. Fake model only.',
  );
} finally {
  await client.close();
  for (const a of adapters) {
    await a.service.stop();
    await close(a.http);
  }
  await gateway.stop();
  await close(http);
  await close(worker);
  store.close();
  await rm(root, { recursive: true, force: true });
}
