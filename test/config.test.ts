import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { GatewayService } from '../src/gateway.js';
import { StateStore } from '../src/store.js';
import { buildServer } from '../src/server.js';
import type { GatewayConfig } from '../src/types.js';
import { PROTOCOL } from '../src/protocol.js';

const config: GatewayConfig = {
  host: '127.0.0.1',
  port: 8188,
  state_path: '',
  public_url: 'http://127.0.0.1:8188',
  generation_token: 'generation-secret-at-least-24',
  management_token: 'management-secret-at-least-24',
  heartbeat_ttl_ms: 30000,
  worker_request_timeout_ms: 300000,
  max_request_bytes: 1024 * 1024,
  max_response_bytes: 1024 * 1024,
  max_queued_jobs: 10,
  queue_timeout_ms: 30000,
  poll_ms: 1000,
};

function registration(workerId: string, steps: number) {
  return {
    protocol_version: PROTOCOL,
    worker_id: workerId,
    instance_id: `${workerId}-instance`,
    endpoint: {
      base_url: `http://127.0.0.1:${workerId === 'worker-a' ? 56514 : 56515}`,
      protocol: 'openai-image/v1',
    },
    models: [
      {
        id: 'qwen-image-2.1',
        operations: ['images.generations', 'images.edits'],
        streaming: true,
        metadata: {
          adapter_type: 'qwen',
          config_schema: { type: 'object', properties: { steps: { type: 'integer' } } },
          config_defaults: { steps, steps_mode: 'default' },
        },
      },
    ],
    capacity: { slots: 1, max_queue: 0 },
    resources: { node_id: workerId, gpu_ids: [`${workerId}-gpu`] },
  };
}

test('adapter config is seeded once from registration and remains a single current record', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'gateway-config-test-'));
  const state = join(dir, 'state.sqlite');
  let store = new StateStore(state);
  let gateway = new GatewayService({ ...config, state_path: state }, store);
  try {
    assert.equal(gateway.adapterConfigs().length, 0);
    assert.throws(() => gateway.setAdapterConfig('missing', 'image', {}), /register an adapter/);
    gateway.register(registration('worker-a', 28));
    assert.deepEqual(gateway.adapterConfig('qwen', 'qwen-image-2.1')?.config, {
      steps: 28,
      steps_mode: 'default',
    });
    gateway.register(registration('worker-b', 40));
    assert.deepEqual(gateway.adapterConfigs(), [
      {
        adapter_type: 'qwen',
        image_id: 'qwen-image-2.1',
        schema: { type: 'object', properties: { steps: { type: 'integer' } } },
        config: { steps: 28, steps_mode: 'default' },
      },
    ]);
    gateway.setAdapterConfig('qwen', 'qwen-image-2.1', {
      steps: 40,
      steps_mode: 'default',
    });
    assert.equal(
      (gateway.adapterConfig('qwen', 'qwen-image-2.1')?.config as { steps: number }).steps,
      40,
    );
    const server = buildServer(gateway);
    try {
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const address = server.address() as { port: number };
      const response = await fetch(
        `http://127.0.0.1:${address.port}/control/v1/config?adapter_type=qwen&image_id=qwen-image-2.1`,
      );
      assert.equal(response.status, 200);
      assert.deepEqual((await response.json()).config, { steps: 40, steps_mode: 'default' });
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    await gateway.stop();
    store.close();
    store = new StateStore(state);
    gateway = new GatewayService({ ...config, state_path: state }, store);
    gateway.register(registration('worker-a', 12));
    assert.deepEqual(gateway.adapterConfig('qwen', 'qwen-image-2.1')?.config, {
      steps: 40,
      steps_mode: 'default',
    });
    assert.equal(gateway.adapterConfigs().length, 1);
    assert.throws(() => gateway.setAdapterConfig('qwen', 'qwen-image-2.1', []), /JSON object/);
    const extra = registration('unknown-backend', 4);
    const unknownBackend = {
      ...extra,
      models: [
        {
          ...extra.models[0],
          id: 'another-model',
          metadata: {
            adapter_type: 'another-backend',
            config_schema: { type: 'object', properties: { tiles: { type: 'integer' } } },
            config_defaults: { tiles: 6 },
          },
        },
      ],
    };
    gateway.register(unknownBackend);
    assert.deepEqual(gateway.adapterConfig('another-backend', 'another-model')?.config, {
      tiles: 6,
    });
    assert.equal(gateway.adapterConfigs().length, 2);
    const invalid = registration('bad-config', 15);
    assert.throws(
      () =>
        gateway.register({
          ...invalid,
          models: [
            { ...invalid.models[0], metadata: { adapter_type: 'qwen', config_defaults: {} } },
          ],
        }),
      /configuration metadata/,
    );
    assert.equal(gateway.registry.workers.has('bad-config'), false);
    assert.equal(gateway.adapterConfigs().length, 2);
  } finally {
    await gateway.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});
