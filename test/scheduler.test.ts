import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { StateStore } from '../src/store.js';
import { selectCandidate, type RankedCandidate } from '../src/scheduler.js';
import type { ImageInput, Task } from '../src/protocol.js';

const input: ImageInput = {
  operation: 'images.generations',
  multipart: false,
  files: [],
  fields: { model: 'test', prompt: 'test', size: '1024x1024', n: 1 },
};
let sequence = 0;
function job(worker: string, instance = 'one'): Task {
  return {
    id: `job-${++sequence}`,
    key: '',
    fingerprint: 'test',
    worker_id: worker,
    instance_id: instance,
    model: 'test',
    operation: 'images.generations',
    state: 'running',
    created_at: 1,
    updated_at: 1,
    started_at: 10,
  };
}
function candidate(id: string, ms: number, extra: Partial<RankedCandidate> = {}): RankedCandidate {
  return {
    worker_id: id,
    eligible: true,
    in_flight: 0,
    slots: 1,
    estimated_ms: ms,
    estimate_source: 'exact_history',
    sample_count: 3,
    last_dispatch: 1,
    dispatches_since_last: 0,
    ...extra,
  };
}

test('idle capacity outranks historical speed, unavailable workers cannot win', () => {
  const result = selectCandidate([
    candidate('fast-partly-busy', 1, { in_flight: 1, slots: 2 }),
    candidate('idle', 1000),
    candidate('offline', 0, { eligible: false }),
    candidate('uncertain', 0, { eligible: false }),
  ]);
  assert.equal(result.selected?.worker_id, 'idle');
  assert.equal(result.reason, 'fastest_history');
  assert.equal(selectCandidate([candidate('full', 1, { eligible: false })]).reason, 'no_capacity');
});

test('cold starts and periodic probes are bounded; otherwise fastest observed idle worker wins', () => {
  const fast = candidate('fast', 10, { last_dispatch: 5 });
  const slow = candidate('slow', 100, { last_dispatch: 1 });
  const fresh = candidate('new', 60000, {
    sample_count: 0,
    last_dispatch: 0,
    estimate_source: 'unmeasured',
  });
  assert.equal(selectCandidate([fast, slow, fresh]).reason, 'new_worker_probe');
  assert.equal(selectCandidate([fast, slow, fresh]).selected?.worker_id, 'new');
  fresh.last_dispatch = 6;
  assert.equal(selectCandidate([fresh, slow, fast]).selected?.worker_id, 'fast');
  slow.dispatches_since_last = 20;
  assert.equal(selectCandidate([fast, slow]).selected?.worker_id, 'slow');
  assert.equal(selectCandidate([fast, slow]).reason, 'periodic_probe');
  assert.equal(
    selectCandidate([candidate('busy-new', 1, { in_flight: 1, slots: 2, last_dispatch: 0 }), fast])
      .selected?.worker_id,
    'fast',
  );
});

test('persisted recent history adapts to speed changes and isolates model operations and instances', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'gateway-speed-test-'));
  const path = join(dir, 'state.sqlite');
  let store = new StateStore(path);
  try {
    function observe(worker: string, duration: number, request = input) {
      const task = job(worker);
      task.operation = request.operation;
      store.dispatched(task, request);
      store.recordDuration(task, duration);
    }
    observe('fast', 100);
    observe('slow', 500);
    const estimate = (id: string, request = input, instance = 'one') =>
      store.estimate(id, instance, 'test', request.operation, request, 60000);
    assert.equal(estimate('fast').estimated_ms, 100);
    for (let i = 0; i < 20; i++) observe('fast', 1000);
    assert.equal(estimate('fast').sample_count, 20);
    assert.equal(estimate('fast').estimated_ms, 1000);
    assert.equal(
      selectCandidate([
        candidate('fast', 1, estimate('fast')),
        candidate('slow', 1, { ...estimate('slow'), dispatches_since_last: 0 }),
      ]).selected?.worker_id,
      'slow',
    );
    const otherSize = { ...input, fields: { ...input.fields, size: '2048x2048', n: 2 } };
    const fallback = estimate('slow', otherSize);
    assert.equal(fallback.estimate_source, 'operation_history');
    assert.equal(fallback.estimated_ms, 1000);
    observe('slow', 1400, otherSize);
    assert.equal(estimate('slow', otherSize).estimate_source, 'exact_history');
    assert.equal(estimate('slow', otherSize).estimated_ms, 1400);
    assert.equal(estimate('fast', input, 'different-instance').sample_count, 0);
    assert.equal(estimate('fast', { ...input, operation: 'images.edits' }).sample_count, 0);
    const before = estimate('slow');
    store.close();
    store = new StateStore(path);
    assert.deepEqual(estimate('slow'), before);
  } finally {
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test('existing success records seed history once; failures and uncertain attempts do not become fast samples', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'gateway-speed-seed-'));
  const path = join(dir, 'state.sqlite');
  let store = new StateStore(path);
  try {
    const good = { ...job('worker'), state: 'succeeded' as const, completed_at: 310 };
    const failed = { ...job('worker'), state: 'failed' as const, completed_at: 11 };
    store.create(good, input);
    store.create(failed, input);
    store.close();
    store = new StateStore(path);
    const estimate = () =>
      store.estimate('worker', 'one', 'test', 'images.generations', input, 60000);
    assert.equal(estimate().sample_count, 1);
    assert.equal(estimate().estimated_ms, 300);
    const uncertain = job('worker');
    store.create(uncertain, input);
    store.dispatched(uncertain, input);
    // A recovered result must not teach the scheduler the outage duration.
    store.save({ ...uncertain, state: 'succeeded', completed_at: 100000 });
    store.close();
    store = new StateStore(path);
    assert.equal(estimate().sample_count, 1);
    assert.equal(estimate().estimated_ms, 300);
  } finally {
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});
