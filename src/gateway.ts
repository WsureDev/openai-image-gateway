import { randomUUID, randomBytes } from 'node:crypto';
import { EventEmitter } from 'node:events';
import {
  ApiError,
  errorOutput,
  fingerprint,
  encodeImage,
  boundedResponse,
  eventsSchema,
  type ImageInput,
  type ImageOutput,
  type Task,
} from './protocol.js';
import { StateStore } from './store.js';
import { WorkerRegistry, type Worker } from './worker-registry.js';
import type { GatewayConfig, Decision } from './types.js';

export class GatewayService {
  readonly registry: WorkerRegistry;
  private readonly bus = new EventEmitter();
  private readonly active = new Map<string, AbortController>();
  private readonly runs = new Set<Promise<void>>();
  private readonly liveStreams = new Set<string>();
  private timer?: NodeJS.Timeout;
  private stopped = false;
  private ticking = false;
  constructor(
    readonly config: GatewayConfig,
    readonly store: StateStore,
  ) {
    this.registry = new WorkerRegistry(config.heartbeat_ttl_ms, config.allowed_worker_origins);
    this.bus.setMaxListeners(1000);
    for (const job of store.tasks())
      if (['dispatching', 'running'].includes(job.state)) {
        job.state = 'uncertain';
        job.updated_at = Date.now();
        job.error_code = 'gateway_restarted';
        store.save(job);
      }
  }
  start(): void {
    this.timer = setInterval(() => {
      void this.tick().catch(() => this.storageFailure());
    }, this.config.poll_ms).unref();
  }
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    for (const controller of this.active.values()) controller.abort();
    await Promise.allSettled([...this.runs]);
    this.bus.emit('stop');
  }
  private reserved(id: string): number {
    return this.store
      .tasks()
      .filter(
        (t) => t.worker_id === id && ['dispatching', 'running', 'uncertain'].includes(t.state),
      ).length;
  }
  register(value: unknown): unknown {
    const w = this.registry.register(value);
    this.store.worker(w.spec.worker_id, this.registry.snapshot(w, this.reserved(w.spec.worker_id)));
    return {
      worker_id: w.spec.worker_id,
      lease_id: w.lease_id,
      worker_token: w.token,
      heartbeat_interval_ms: Math.max(10, Math.floor(this.config.heartbeat_ttl_ms / 3)),
      lease_ttl_ms: this.config.heartbeat_ttl_ms,
    };
  }
  heartbeat(id: string, header: string | undefined, body: unknown): unknown {
    const w = this.registry.heartbeat(id, header, body);
    this.store.worker(id, this.registry.snapshot(w, this.reserved(id)));
    this.pump();
    return { accepted_sequence: w.sequence, lease_expires_at: w.expires };
  }
  events(id: string, header: string | undefined, body: unknown): unknown {
    const v = eventsSchema.safeParse(body);
    if (!v.success) throw new ApiError(400, 'invalid_events', v.error.message);
    const w = this.registry.authenticate(id, header, v.data.lease_id);
    let accepted = 0;
    for (const event of v.data.events) {
      const task = this.store.task(event.task_id);
      if (!task || task.worker_id !== id || task.instance_id !== w.spec.instance_id)
        throw new ApiError(
          409,
          'task_owner_mismatch',
          'event task is not assigned to this worker instance',
        );
      if (this.store.event(id, w.spec.instance_id, event)) accepted++;
    }
    return { accepted };
  }
  workerSnapshots(): Record<string, unknown>[] {
    const snapshots = new Map<string, Record<string, unknown>>(
      this.store.workers().map((w) => [String(w.worker_id), { ...w, state: 'offline' }]),
    );
    for (const w of this.registry.workers.values())
      snapshots.set(w.spec.worker_id, this.registry.snapshot(w, this.reserved(w.spec.worker_id)));
    return [...snapshots.values()];
  }
  models(): unknown[] {
    const ids = new Set<string>();
    for (const w of this.registry.workers.values())
      if (w.expires > Date.now() && ['ready', 'busy'].includes(w.state))
        for (const m of w.spec.models) ids.add(m.id);
    return [...ids].sort().map((id) => ({ id, object: 'model', created: 0, owned_by: 'adapter' }));
  }
  ready(): boolean {
    return !this.stopped && this.models().length > 0;
  }
  status(): unknown {
    const tasks = this.store.tasks();
    return {
      workers: this.workerSnapshots(),
      models: this.models(),
      queue: {
        pending: tasks.filter((t) => t.state === 'queued').length,
        running: tasks.filter((t) => ['running', 'dispatching'].includes(t.state)).length,
        uncertain: tasks.filter((t) => t.state === 'uncertain').length,
      },
      observed_at: Date.now(),
    };
  }
  jobs(
    filter: { worker_id?: string; status?: string; limit?: number } = {},
  ): Omit<Task, 'key' | 'fingerprint'>[] {
    return this.store
      .tasks()
      .filter(
        (t) =>
          (!filter.worker_id || t.worker_id === filter.worker_id) &&
          (!filter.status || t.state === filter.status),
      )
      .reverse()
      .slice(0, Math.min(1000, Math.max(1, filter.limit || 100)))
      .map(({ key, fingerprint, ...task }) => task);
  }
  enqueue(input: ImageInput, key = ''): Task {
    if (this.stopped) throw new ApiError(503, 'shutting_down', 'Gateway is shutting down');
    if (key.length > 256)
      throw new ApiError(400, 'invalid_idempotency_key', 'idempotency key too long');
    const hash = fingerprint(input),
      previous = key ? this.store.byKey(key) : undefined;
    if (previous) {
      if (previous.fingerprint !== hash)
        throw new ApiError(
          409,
          'idempotency_conflict',
          'idempotency key was used for a different request',
        );
      return previous;
    }
    const candidates = [...this.registry.workers.values()].filter(
      (w) =>
        w.expires > Date.now() &&
        ['ready', 'busy'].includes(w.state) &&
        w.spec.models.some(
          (m) =>
            m.id === input.fields.model &&
            m.operations.includes(input.operation) &&
            (!input.fields.stream || m.streaming),
        ),
    );
    if (!candidates.length)
      throw new ApiError(
        503,
        'no_eligible_workers',
        'no online worker supports the requested model, operation and streaming mode',
      );
    if (input.fields.stream && input.fields.response_format === 'url')
      throw new ApiError(400, 'stream_url_unsupported', 'streaming responses use base64 images');
    if (
      this.store.tasks().filter((t) => t.state === 'queued').length >= this.config.max_queued_jobs
    )
      throw new ApiError(429, 'queue_full', 'Gateway queue is full');
    const now = Date.now(),
      job: Task = {
        id: randomUUID(),
        key,
        fingerprint: hash,
        model: String(input.fields.model),
        operation: input.operation,
        state: 'queued',
        worker_id: '',
        instance_id: '',
        created_at: now,
        updated_at: now,
      };
    this.store.create(job, input);
    queueMicrotask(() => {
      try {
        this.pump();
      } catch {
        this.storageFailure();
      }
    });
    return job;
  }
  async wait(
    id: string,
    stream?: { head: (type: string) => void; chunk: (bytes: Buffer) => void },
  ): Promise<ImageOutput> {
    const existing = this.store.result(id);
    if (existing) return existing;
    if (this.store.task(id)?.state === 'uncertain' && !this.active.has(id))
      return errorOutput(
        new ApiError(
          503,
          'task_uncertain',
          'execution outcome is being reconciled; retry with the same idempotency key',
        ),
      );
    // Only the first subscriber receives a live stream. Duplicate callers get the
    // complete persisted SSE body, so a late join cannot miss its initial events.
    if (stream) {
      if (this.liveStreams.has(id)) stream = undefined;
      else this.liveStreams.add(id);
    }
    return new Promise((resolve) => {
      const done = (result: ImageOutput) => {
        cleanup();
        resolve(result);
      };
      const stop = () =>
        done(errorOutput(new ApiError(503, 'shutting_down', 'Gateway is shutting down')));
      const cleanup = () => {
        this.bus.off(`done:${id}`, done);
        this.bus.off(`head:${id}`, head);
        this.bus.off(`chunk:${id}`, chunk);
        this.bus.off('stop', stop);
      };
      const head = (type: string) => stream?.head(type),
        chunk = (bytes: Buffer) => stream?.chunk(bytes);
      this.bus.on(`done:${id}`, done);
      this.bus.on(`head:${id}`, head);
      this.bus.on(`chunk:${id}`, chunk);
      this.bus.on('stop', stop);
    });
  }
  private profile(worker: string, job: Task, input: ImageInput): string {
    return JSON.stringify([
      worker,
      job.model,
      job.operation,
      input.fields.size ?? 'auto',
      input.fields.n ?? 1,
    ]);
  }
  private choose(job: Task, input: ImageInput): { worker?: Worker; decision: Decision } {
    let chosen: Worker | undefined,
      best = Infinity;
    const candidates: Decision['candidates'] = [];
    for (const w of this.registry.workers.values()) {
      const m = w.spec.models.find((m) => m.id === job.model);
      let reason: string | undefined;
      if (!m) reason = 'model_not_supported';
      else if (!m.operations.includes(job.operation)) reason = 'operation_not_supported';
      else if (input.fields.stream && !m.streaming) reason = 'streaming_not_supported';
      else if (w.expires <= Date.now() || !['ready', 'busy'].includes(w.state))
        reason = 'worker_not_ready';
      else if (Math.max(w.reported, this.reserved(w.spec.worker_id)) >= w.spec.capacity.slots)
        reason = 'capacity_full';
      const duration =
        this.store.duration(this.profile(w.spec.worker_id, job, input)) ??
        (m?.estimated_duration_ms ?? 60_000) * Number(input.fields.n ?? 1);
      candidates.push({
        worker_id: w.spec.worker_id,
        eligible: !reason,
        reason,
        estimated_ms: duration,
      });
      if (
        !reason &&
        (duration < best ||
          (duration === best && w.spec.worker_id < (chosen?.spec.worker_id ?? '')))
      ) {
        chosen = w;
        best = duration;
      }
    }
    return {
      worker: chosen,
      decision: {
        job_id: job.id,
        selected_worker_id: chosen?.spec.worker_id ?? '',
        created_at: Date.now(),
        candidates,
      },
    };
  }
  private pump(): void {
    if (this.stopped) return;
    for (const job of this.store.tasks().filter((t) => t.state === 'queued')) {
      if (Date.now() - job.created_at > this.config.queue_timeout_ms) {
        this.complete(
          job,
          errorOutput(new ApiError(504, 'queue_timeout', 'task expired waiting for capacity')),
        );
        continue;
      }
      const input = this.store.input(job.id),
        { worker, decision } = this.choose(job, input);
      if (!worker) continue;
      job.worker_id = worker.spec.worker_id;
      job.instance_id = worker.spec.instance_id;
      job.state = 'dispatching';
      job.started_at = Date.now();
      job.updated_at = Date.now();
      job.predicted_duration_ms = decision.candidates.find(
        (c) => c.worker_id === job.worker_id,
      )?.estimated_ms;
      this.store.save(job);
      this.store.decision(decision);
      const controller = new AbortController();
      this.active.set(job.id, controller);
      const run = this.run(job, input, worker, controller).catch(() => this.storageFailure());
      this.runs.add(run);
      void run.finally(() => {
        this.runs.delete(run);
        this.active.delete(job.id);
        try {
          this.pump();
        } catch {
          this.storageFailure();
        }
      });
    }
  }
  private async run(
    job: Task,
    input: ImageInput,
    w: Worker,
    controller: AbortController,
  ): Promise<void> {
    job.state = 'running';
    this.store.save(job);
    const timeout = setTimeout(
      () => controller.abort(),
      this.config.worker_request_timeout_ms,
    ).unref();
    try {
      const upstream = structuredClone(input);
      if (upstream.fields.response_format === 'url') upstream.fields.response_format = 'b64_json';
      const encoded = encodeImage(upstream),
        response = await fetch(
          `${w.spec.endpoint.base_url}/v1/${job.operation.replace('.', '/')}`,
          {
            method: 'POST',
            redirect: 'error',
            headers: {
              ...encoded.headers,
              authorization: `Bearer ${w.spec.endpoint.api_token}`,
              'x-gateway-job-id': job.id,
              'idempotency-key': job.id,
            },
            body: encoded.body,
            signal: controller.signal,
          },
        );
      const type = response.headers.get('content-type') ?? 'application/json';
      const streaming =
        Boolean(input.fields.stream) && response.ok && type.includes('text/event-stream');
      if (streaming) this.bus.emit(`head:${job.id}`, type);
      const bytes = await boundedResponse(
        response,
        this.config.max_response_bytes,
        streaming ? (b) => this.bus.emit(`chunk:${job.id}`, b) : undefined,
      );
      if (response.ok && input.fields.stream && !streaming)
        throw new ApiError(
          502,
          'invalid_worker_response',
          'worker did not return the advertised SSE stream',
        );
      this.complete(
        job,
        this.materialize(job, input, {
          status: response.status,
          content_type: type,
          body: bytes.toString(),
        }),
      );
    } catch (error) {
      job.state = 'uncertain';
      job.updated_at = Date.now();
      job.error_code = 'execution_uncertain';
      job.error_message = 'worker connection ended without a confirmed result';
      const output = errorOutput(
        new ApiError(
          502,
          'execution_uncertain',
          'worker result is uncertain; retry using the same idempotency key',
        ),
      );
      this.store.save(job, output);
      this.bus.emit(`done:${job.id}`, output);
    } finally {
      clearTimeout(timeout);
    }
  }
  private materialize(job: Task, input: ImageInput, output: ImageOutput): ImageOutput {
    if (output.status < 200 || output.status >= 300) return output;
    if (output.content_type.includes('text/event-stream')) {
      const frames = output.body.split(/\r?\n\r?\n/).flatMap((frame) => {
        const data = frame
          .split(/\r?\n/)
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice(5).trimStart())
          .join('\n');
        try {
          const parsed = data ? JSON.parse(data) : undefined;
          return parsed && typeof parsed === 'object' ? [parsed] : [];
        } catch {
          return [];
        }
      });
      const completedType =
        job.operation === 'images.edits' ? 'image_edit.completed' : 'image_generation.completed';
      if (
        frames.some((frame) => frame.type === 'error' || frame.error) ||
        !frames.some((frame) => frame.type === completedType)
      )
        return errorOutput(
          new ApiError(502, 'stream_incomplete', 'worker stream ended without a completed image'),
        );
      return output;
    }
    let result: Record<string, unknown>;
    try {
      result = JSON.parse(output.body);
    } catch {
      return errorOutput(
        new ApiError(502, 'invalid_worker_response', 'worker returned invalid JSON'),
      );
    }
    if (
      !result ||
      typeof result !== 'object' ||
      !Array.isArray(result.data) ||
      !result.data.length ||
      result.data.some((item) => !item || typeof item.b64_json !== 'string' || !item.b64_json)
    )
      return errorOutput(
        new ApiError(502, 'invalid_worker_response', 'adapter must return base64 image data'),
      );
    if (input.fields.response_format === 'url') {
      result.data = result.data.map((item: Record<string, unknown>) => {
        const id = randomBytes(32).toString('hex'),
          format = String(result.output_format ?? input.fields.output_format ?? 'png');
        const mime =
          format === 'jpeg' ? 'image/jpeg' : format === 'webp' ? 'image/webp' : 'image/png';
        this.store.artifact(
          id,
          Buffer.from(String(item.b64_json), 'base64'),
          mime,
          Date.now() + 3600_000,
        );
        const { b64_json, ...rest } = item;
        return { ...rest, url: `${this.config.public_url}/v1/images/files/${id}` };
      });
    }
    return { ...output, body: JSON.stringify(result) };
  }
  private complete(job: Task, output: ImageOutput): void {
    const ok = output.status >= 200 && output.status < 300;
    job.state = ok ? 'succeeded' : 'failed';
    job.completed_at = Date.now();
    job.updated_at = Date.now();
    if (!ok) job.error_code = 'request_failed';
    else {
      delete job.error_code;
      delete job.error_message;
    }
    this.store.save(job, output);
    if (ok && job.started_at)
      this.store.sample(
        this.profile(job.worker_id, job, this.store.input(job.id)),
        Date.now() - job.started_at,
      );
    this.bus.emit(`done:${job.id}`, output);
    this.liveStreams.delete(job.id);
  }
  private storageFailure(): void {
    console.error('Gateway background persistence failed; new dispatch is disabled.');
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    for (const controller of this.active.values()) controller.abort();
    this.bus.emit('stop');
  }
  async tick(): Promise<void> {
    if (this.stopped || this.ticking) return;
    this.ticking = true;
    try {
      this.registry.expire();
      for (const job of this.store.tasks()) {
        if (
          ['dispatching', 'running'].includes(job.state) &&
          this.registry.workers.get(job.worker_id)?.expires! <= Date.now()
        ) {
          job.state = 'uncertain';
          this.store.save(job);
        }
        if (job.state !== 'uncertain' || this.active.has(job.id)) continue;
        const w = this.registry.workers.get(job.worker_id);
        if (!w || w.expires <= Date.now()) continue;
        // A new runtime cannot prove what the previous model process executed.
        if (w.spec.instance_id !== job.instance_id) continue;
        try {
          const response = await fetch(
            `${w.spec.endpoint.base_url}/worker/v1/tasks/${encodeURIComponent(job.id)}`,
            {
              headers: { authorization: `Bearer ${w.spec.endpoint.api_token}` },
              signal: AbortSignal.timeout(2000),
              redirect: 'error',
            },
          );
          if (!response.ok) continue;
          const remote = JSON.parse(
            (await boundedResponse(response, this.config.max_response_bytes)).toString(),
          ) as { state: string; result?: ImageOutput };
          if (
            ['succeeded', 'failed'].includes(remote.state) &&
            remote.result &&
            Number.isInteger(remote.result.status) &&
            typeof remote.result.body === 'string' &&
            typeof remote.result.content_type === 'string'
          )
            this.complete(job, this.materialize(job, this.store.input(job.id), remote.result));
        } catch {
          /* Keep uncertain reservations until there is affirmative evidence. */
        }
      }
      this.pump();
    } finally {
      this.ticking = false;
    }
  }
}
