// Starts only temporary local HTTP processes with a fake backend. No model access.
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { GatewayService } from '../dist/src/gateway.js';
import { StateStore } from '../dist/src/store.js';
import { buildServer as buildGateway } from '../dist/src/server.js';
const adapterRoot = process.argv[2];
if (!adapterRoot)
  throw new Error('usage: node scripts/adapter-contract-smoke.mjs /path/to/built/adapter');
const { AdapterService, buildServer: buildAdapter } = await import(
  pathToFileURL(resolve(adapterRoot, 'dist/src/server.js')).href
);
const root = await mkdtemp(join(tmpdir(), 'image-contract-smoke-'));
const token = 'smoke-test-secret-at-least-24';
const listen = async (server) => {
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  return `http://127.0.0.1:${server.address().port}`;
};
const config = {
  host: '127.0.0.1',
  port: 0,
  state_path: join(root, 'gateway.sqlite'),
  public_url: 'http://127.0.0.1:1',
  generation_token: token,
  management_token: token,

  heartbeat_ttl_ms: 1000,
  worker_request_timeout_ms: 2000,
  max_request_bytes: 1048576,
  max_response_bytes: 1048576,
  max_queued_jobs: 20,
  queue_timeout_ms: 3000,
  poll_ms: 20,
};
const store = new StateStore(config.state_path),
  gateway = new GatewayService(config, store),
  server = buildGateway(gateway),
  adapters = [];
let calls = 0;
try {
  config.public_url = await listen(server);
  gateway.start();
  for (let i = 0; i < 3; i++) {
    const ac = {
      host: '127.0.0.1',
      port: 0,
      public_url: 'http://127.0.0.1:1',
      worker_id: `smoke-${i}`,
      gateway_url: config.public_url,

      state_path: join(root, `adapter-${i}.sqlite`),
      max_request_bytes: 1048576,
      max_response_bytes: 1048576,
      slots: 1,
      request_timeout_ms: 2000,
      heartbeat_interval_ms: 20,
      resources: { node_id: `node-${i}`, gpu_ids: [`fake-gpu-${i}`] },
    };
    const service = new AdapterService(ac, {
      models: () => [
        { id: 'fake-image', operations: ['images.generations', 'images.edits'], streaming: false },
      ],
      ready: async () => true,
      execute: async () => {
        calls++;
        await new Promise((r) => setTimeout(r, 70));
        return {
          status: 200,
          content_type: 'application/json',
          body: JSON.stringify({ created: 1, data: [{ b64_json: 'ZmFrZQ==' }] }),
        };
      },
    });
    const http = buildAdapter(service);
    ac.public_url = await listen(http);
    adapters.push({ service, http });
    service.control.start();
  }
  const until = async (predicate) => {
    const deadline = Date.now() + 5000;
    while (!predicate()) {
      if (Date.now() > deadline) throw new Error('condition timed out');
      await new Promise((r) => setTimeout(r, 20));
    }
  };
  await until(
    () =>
      gateway.workerSnapshots().length === 3 &&
      gateway.workerSnapshots().every((w) => w.state === 'ready'),
  );
  const responses = await Promise.all(
    Array.from({ length: 6 }, () =>
      fetch(config.public_url + '/v1/images/generations', {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({ model: 'fake-image', prompt: 'smoke' }),
      }),
    ),
  );
  assert.ok(responses.every((r) => r.status === 200));
  assert.equal(calls, 6);
  assert.equal(new Set(gateway.jobs().map((j) => j.worker_id)).size, 3);
  await until(() => store.events().filter((e) => e.state === 'succeeded').length === 6);
  assert.ok(!JSON.stringify(gateway.workerSnapshots()).includes(token));
  console.log(
    'PASS: three independent Adapter runtimes self-register; six jobs execute; durable task events arrive. No model runtime contacted.',
  );
} finally {
  for (const { service, http } of adapters) {
    await service.stop();
    http.closeAllConnections();
    await new Promise((r) => http.close(r));
  }
  await gateway.stop();
  server.closeAllConnections();
  await new Promise((r) => server.close(r));
  store.close();
  await rm(root, { recursive: true, force: true });
}
