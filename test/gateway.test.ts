import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import OpenAI from 'openai';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { GatewayService } from '../src/gateway.js';
import { StateStore } from '../src/store.js';
import { buildServer } from '../src/server.js';
import type { GatewayConfig } from '../src/types.js';
import {
  PROTOCOL,
  fingerprint,
  type ImageInput,
  type ImageOutput,
  type Task,
} from '../src/protocol.js';
const generation = 'generation-secret-at-least-24',
  enrollment = 'enrollment-secret-at-least-24',
  management = 'management-secret-at-least-24',
  apiToken = 'worker-api-secret-at-least-24';
const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function until(check: () => boolean, ms = 3000) {
  const end = Date.now() + ms;
  while (!check()) {
    if (Date.now() > end) throw new Error('condition timeout');
    await pause(10);
  }
}
async function listen(server: Server) {
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  return `http://127.0.0.1:${(server.address() as { port: number }).port}`;
}
async function close(server: Server) {
  server.closeAllConnections();
  await new Promise<void>((r) => server.close(() => r()));
}
async function fixture(t: TestContext, options: Partial<GatewayConfig> = {}) {
  const root = await mkdtemp(join(tmpdir(), 'openai-gateway-test-'));
  const config: GatewayConfig = {
    host: '127.0.0.1',
    port: 0,
    state_path: join(root, 'state.sqlite'),
    public_url: 'http://127.0.0.1:1',
    generation_token: generation,
    enrollment_token: enrollment,
    management_token: management,
    heartbeat_ttl_ms: 3000,
    worker_request_timeout_ms: 2000,
    max_request_bytes: 1024 * 1024,
    max_response_bytes: 4 * 1024 * 1024,
    max_queued_jobs: 100,
    queue_timeout_ms: 2000,
    poll_ms: 20,
    ...options,
  };
  let store = new StateStore(config.state_path),
    g = new GatewayService(config, store),
    server = buildServer(g),
    url = await listen(server);
  config.public_url = url;
  g.start();
  const workers: Server[] = [];
  const timers: NodeJS.Timeout[] = [];
  t.after(async () => {
    await g.stop();
    await close(server);
    await Promise.all(workers.map(close));
    for (const timer of timers) clearTimeout(timer);
    store.close();
    await rm(root, { recursive: true, force: true });
  });
  const call = (path: string, body?: unknown, token = generation, method = body ? 'POST' : 'GET') =>
    fetch(url + path, {
      method,
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined,
    });
  async function worker(
    id: string,
    opts: {
      model?: string;
      delay?: number;
      stream?: boolean;
      disconnect?: boolean;
      instance?: string;
    } = {},
  ) {
    let calls = 0,
      active = 0,
      peak = 0;
    const rawBodies: Buffer[] = [],
      results = new Map<string, { state: string; result?: ImageOutput }>();
    const output: ImageOutput = {
      status: 200,
      content_type: 'application/json',
      body: JSON.stringify({
        created: 1,
        data: [{ b64_json: Buffer.from('fake-png').toString('base64') }],
        marker: id,
      }),
    };
    const ws = createServer(async (req, res) => {
      assert.equal(req.headers.authorization, `Bearer ${apiToken}`);
      if (req.url?.startsWith('/worker/v1/tasks/')) {
        const value = results.get(req.url.split('/').at(-1)!);
        res.writeHead(value ? 200 : 404, { 'content-type': 'application/json' });
        res.end(JSON.stringify(value ?? {}));
        return;
      }
      calls++;
      active++;
      peak = Math.max(peak, active);
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(Buffer.from(c));
      rawBodies.push(Buffer.concat(chunks));
      const task = String(req.headers['x-gateway-job-id']);
      results.set(task, { state: 'running' });
      if (opts.disconnect) req.socket.destroy();
      if (opts.stream) {
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        res.write(
          'event: image_generation.partial_image\ndata: {"type":"image_generation.partial_image","b64_json":"partial","partial_image_index":0}\n\n',
        );
      }
      const timer = setTimeout(() => {
        active--;
        results.set(task, { state: 'succeeded', result: output });
        if (opts.disconnect) return;
        if (opts.stream)
          res.end(
            'event: image_generation.completed\ndata: {"type":"image_generation.completed","b64_json":"ZmFrZQ=="}\n\n',
          );
        else {
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(output.body);
        }
      }, opts.delay ?? 5);
      timers.push(timer);
    });
    workers.push(ws);
    const endpoint = await listen(ws);
    const spec = {
      protocol_version: PROTOCOL,
      worker_id: id,
      instance_id: opts.instance ?? 'instance-one',
      endpoint: { base_url: endpoint, protocol: 'openai-image/v1', api_token: apiToken },
      models: [
        {
          id: opts.model ?? 'alpha-image',
          operations: ['images.generations', 'images.edits'],
          streaming: opts.stream ?? false,
          estimated_duration_ms: 100,
        },
      ],
      capacity: { slots: 1, max_queue: 0 },
      resources: { node_id: id, gpu_ids: ['gpu-1'] },
    };
    const registered = await call('/control/v1/workers/register', spec, enrollment);
    assert.equal(registered.status, 201);
    const lease = (await registered.json()) as { lease_id: string; worker_token: string };
    let sequence = 0;
    const beat = (leaseId = lease.lease_id) =>
      call(
        `/control/v1/workers/${id}/heartbeat`,
        {
          lease_id: leaseId,
          sequence: ++sequence,
          state: 'ready',
          load: { in_flight: 0, queue_depth: 0 },
        },
        lease.worker_token,
        'PUT',
      );
    assert.equal((await beat()).status, 200);
    return {
      id,
      endpoint,
      spec,
      lease,
      beat,
      output,
      results,
      rawBodies,
      get calls() {
        return calls;
      },
      get peak() {
        return peak;
      },
      get active() {
        return active;
      },
    };
  }
  return {
    config,
    call,
    worker,
    get g() {
      return g;
    },
    get store() {
      return store;
    },
    get url() {
      return url;
    },
    async restart() {
      await g.stop();
      await close(server);
      store.close();
      store = new StateStore(config.state_path);
      g = new GatewayService(config, store);
      server = buildServer(g);
      url = await listen(server);
      config.public_url = url;
      g.start();
    },
  };
}
const request = { model: 'alpha-image', prompt: 'test', steps: 17 };
function submit(
  f: Awaited<ReturnType<typeof fixture>>,
  body: Record<string, unknown> = request,
  key?: string,
) {
  return fetch(f.url + '/v1/images/generations', {
    method: 'POST',
    headers: {
      authorization: `Bearer ${generation}`,
      'content-type': 'application/json',
      ...(key ? { 'idempotency-key': key } : {}),
    },
    body: JSON.stringify(body),
  });
}

test('three independent workers execute concurrently; heartbeat cannot erase reservations', async (t) => {
  const f = await fixture(t),
    workers = await Promise.all(
      ['local', 'lan-1', 'lan-2'].map((id) => f.worker(id, { delay: 120 })),
    );
  const requests = Array.from({ length: 6 }, () => submit(f));
  await until(() => workers.every((w) => w.active === 1));
  await Promise.all(workers.map((w) => w.beat()));
  const responses = await Promise.all(requests);
  assert.ok(responses.every((r) => r.status === 200));
  for (const w of workers) {
    assert.equal(w.calls, 2);
    assert.equal(w.peak, 1);
  }
  assert.equal(f.g.jobs().filter((j) => j.state === 'succeeded').length, 6);
});
test('different models route by advertised capability with standard OpenAI SDK', async (t) => {
  const f = await fixture(t),
    a = await f.worker('alpha'),
    b = await f.worker('beta', { model: 'beta-image' });
  const client = new OpenAI({ apiKey: generation, baseURL: f.url + '/v1', maxRetries: 0 });
  assert.deepEqual(
    (await client.models.list()).data.map((m) => m.id),
    ['alpha-image', 'beta-image'],
  );
  const output = await client.images.generate({
    model: 'beta-image',
    prompt: 'test',
    response_format: 'b64_json',
  });
  assert.equal(output.data?.length, 1);
  assert.equal(a.calls, 0);
  assert.equal(b.calls, 1);
  const failed = await submit(f, { ...request, model: 'unknown' });
  assert.equal(failed.status, 503);
});
test('multipart edits route using the model field and preserve reference bytes/extensions', async (t) => {
  const f = await fixture(t),
    a = await f.worker('alpha');
  const form = new FormData();
  form.append('model', 'alpha-image');
  form.append('prompt', 'edit');
  form.append('steps', '21');
  form.append(
    'image[]',
    new Blob([Buffer.from([0, 255, 1, 13, 10])], { type: 'image/png' }),
    'test.png',
  );
  const r = await fetch(f.url + '/v1/images/edits', {
    method: 'POST',
    headers: { authorization: `Bearer ${generation}` },
    body: form,
  });
  assert.equal(r.status, 200);
  assert.ok(a.rawBodies[0].includes(Buffer.from([0, 255, 1, 13, 10])));
  assert.ok(a.rawBodies[0].includes(Buffer.from('21')));
});
test('idempotency survives simultaneous requests and Gateway restart; conflicts are rejected', async (t) => {
  const f = await fixture(t),
    a = await f.worker('alpha', { delay: 60 });
  const rs = await Promise.all([submit(f, request, 'same'), submit(f, request, 'same')]);
  assert.equal(rs[0].headers.get('x-gateway-job-id'), rs[1].headers.get('x-gateway-job-id'));
  assert.equal(a.calls, 1);
  assert.equal((await submit(f, { ...request, prompt: 'different' }, 'same')).status, 409);
  const first = await rs[0].text();
  await f.restart();
  const retry = await submit(f, request, 'same');
  assert.equal(await retry.text(), first);
  assert.equal(a.calls, 1);
});
test('MCP SDK initializes and calls read-only tools; credentials and prompts are excluded', async (t) => {
  const f = await fixture(t);
  await f.worker('alpha');
  await submit(f);
  const client = new Client({ name: 'test', version: '1' });
  const transport = new StreamableHTTPClientTransport(new URL(f.url + '/mcp'), {
    requestInit: { headers: { authorization: `Bearer ${management}` } },
  });
  t.after(() => client.close());
  await client.connect(transport);
  const list = await client.listTools();
  assert.equal(list.tools.length, 8);
  assert.ok(list.tools.every((tool) => tool.annotations?.readOnlyHint));
  const result = await client.callTool({ name: 'workers_list', arguments: {} });
  const text = JSON.stringify(result);
  assert.ok(text.includes('alpha'));
  assert.ok(!text.includes(apiToken));
  assert.equal(
    (await f.call('/mcp', { jsonrpc: '2.0', id: 1, method: 'tools/list' }, generation)).status,
    401,
  );
});
test('lease expiry removes routing, requires re-registration, and rejects stale lease IDs', async (t) => {
  const f = await fixture(t, { heartbeat_ttl_ms: 120 }),
    w = await f.worker('alpha');
  assert.equal((await w.beat('00000000-0000-4000-8000-000000000000')).status, 409);
  await pause(150);
  assert.equal((await w.beat()).status, 409);
  assert.equal((await submit(f)).status, 503);
  assert.equal((await f.call('/readyz')).status, 503);
});
test('uncertain dispatch is reconciled without resubmission and keeps the execution slot', async (t) => {
  const f = await fixture(t),
    w = await f.worker('alpha', { delay: 120, disconnect: true });
  const response = await submit(f, request, 'recover');
  assert.equal(response.status, 502);
  assert.equal(f.g.jobs()[0].state, 'uncertain');
  const cap = f.g.workerSnapshots()[0].capacity as { gateway_reserved: number };
  assert.equal(cap.gateway_reserved, 1);
  await until(() => f.g.jobs()[0].state === 'succeeded');
  assert.equal((await submit(f, request, 'recover')).status, 200);
  assert.equal(w.calls, 1);
});
test('running jobs become uncertain on restart and reconcile against the same worker instance', async (t) => {
  const f = await fixture(t),
    w = await f.worker('alpha');
  const input: ImageInput = {
    operation: 'images.generations',
    fields: request,
    files: [],
    multipart: false,
  };
  const task: Task = {
    id: 'seeded-job',
    key: 'restart-key',
    fingerprint: fingerprint(input),
    model: 'alpha-image',
    operation: 'images.generations',
    state: 'running',
    worker_id: 'alpha',
    instance_id: 'instance-one',
    created_at: Date.now(),
    updated_at: Date.now(),
    started_at: Date.now(),
  };
  f.store.create(task, input);
  w.results.set(task.id, { state: 'succeeded', result: w.output });
  await f.restart();
  assert.equal(f.g.jobs()[0].state, 'uncertain');
  const r = await f.call('/control/v1/workers/register', w.spec, enrollment);
  const e = (await r.json()) as { lease_id: string; worker_token: string };
  await f.call(
    '/control/v1/workers/alpha/heartbeat',
    { lease_id: e.lease_id, sequence: 1, state: 'ready', load: { in_flight: 0, queue_depth: 0 } },
    e.worker_token,
    'PUT',
  );
  await until(() => f.g.jobs()[0].state === 'succeeded');
  assert.equal(w.calls, 0);
});
test('queue expiry returns a bounded error and never dispatches the expired task', async (t) => {
  const f = await fixture(t, { queue_timeout_ms: 40 }),
    w = await f.worker('alpha', { delay: 160 });
  const first = submit(f);
  await until(() => w.active === 1);
  const second = await submit(f);
  assert.equal(second.status, 504);
  await first;
  assert.equal(w.calls, 1);
});
test('SSE is delivered before worker completion and retains original event types', async (t) => {
  const f = await fixture(t),
    w = await f.worker('alpha', { delay: 180, stream: true });
  const response = await submit(f, { ...request, stream: true });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type') ?? '', /text\/event-stream/);
  const reader = response.body!.getReader(),
    first = await reader.read();
  assert.ok(Buffer.from(first.value!).toString().includes('partial_image'));
  assert.equal(w.active, 1);
  let rest = '';
  for (;;) {
    const part = await reader.read();
    if (part.done) break;
    rest += Buffer.from(part.value).toString();
  }
  assert.ok(rest.includes('image_generation.completed'));
});
test('response_format=url serves a Gateway-owned image independent of worker addresses', async (t) => {
  const f = await fixture(t);
  await f.worker('alpha');
  const r = await submit(f, { ...request, response_format: 'url' });
  const result = (await r.json()) as { data: Array<{ url: string }> };
  assert.ok(result.data[0].url.startsWith(f.url + '/v1/images/files/'));
  const image = await fetch(result.data[0].url);
  assert.equal(image.status, 200);
  assert.equal(await image.text(), 'fake-png');
  assert.equal(JSON.stringify(result).includes('b64_json'), false);
});
test('malformed registration, control auth and events ownership are enforced', async (t) => {
  const f = await fixture(t),
    w = await f.worker('alpha');
  assert.equal((await f.call('/control/v1/workers/register', {}, enrollment)).status, 400);
  assert.equal((await f.call('/control/v1/workers/register', w.spec, generation)).status, 401);
  const response = await f.call(
    '/control/v1/workers/alpha/events',
    {
      lease_id: w.lease.lease_id,
      events: [{ sequence: 1, task_id: 'not-owned', state: 'running', occurred_at: Date.now() }],
    },
    w.lease.worker_token,
  );
  assert.equal(response.status, 409);
  assert.equal((await f.call('/gateway/v1/jobs', undefined, generation)).status, 401);
});

test('events replay is idempotent and worker re-registration fences the old control token', async (t) => {
  const f = await fixture(t),
    w = await f.worker('alpha');
  const r = await submit(f);
  const id = r.headers.get('x-gateway-job-id')!;
  const body = {
    lease_id: w.lease.lease_id,
    events: [{ sequence: 1, task_id: id, state: 'succeeded', occurred_at: Date.now() }],
  };
  assert.deepEqual(
    await (await f.call('/control/v1/workers/alpha/events', body, w.lease.worker_token)).json(),
    { accepted: 1 },
  );
  assert.deepEqual(
    await (await f.call('/control/v1/workers/alpha/events', body, w.lease.worker_token)).json(),
    { accepted: 0 },
  );
  const replacement = await f.call(
    '/control/v1/workers/register',
    { ...w.spec, instance_id: 'replacement-instance' },
    enrollment,
  );
  assert.equal(replacement.status, 201);
  assert.equal((await w.beat()).status, 401);
});
test('a late duplicate stream gets the complete replay rather than a missing first event', async (t) => {
  const f = await fixture(t),
    w = await f.worker('alpha', { delay: 120, stream: true });
  const first = await submit(f, { ...request, stream: true }, 'stream-key');
  await until(() => w.active === 1);
  const duplicate = submit(f, { ...request, stream: true }, 'stream-key');
  const a = await first.text(),
    second = await duplicate,
    b = await second.text();
  assert.equal(a, b);
  assert.equal(w.calls, 1);
  assert.match(b, /partial_image/);
});

test('oversized image requests receive HTTP 413 without dispatching', async (t) => {
  const f = await fixture(t, { max_request_bytes: 32 });
  const response = await submit(f, { ...request, prompt: 'x'.repeat(1024) });
  assert.equal(response.status, 413);
  assert.equal(f.g.jobs().length, 0);
});
