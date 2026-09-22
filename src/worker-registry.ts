import { randomUUID } from 'node:crypto';
import {
  ApiError,
  origin,
  registrationSchema,
  heartbeatSchema,
  type Registration,
  type Heartbeat,
} from './protocol.js';
export interface Worker {
  spec: Registration;
  lease_id: string;
  expires: number;
  heartbeat_at: number;
  sequence: number;
  state: Heartbeat['state'] | 'stale';
  reported: number;
  resources: Record<string, unknown>;
}
export class WorkerRegistry {
  readonly workers = new Map<string, Worker>();
  constructor(
    private ttl: number,
    private allow?: string[],
  ) {}
  register(value: unknown): Worker {
    const parsed = registrationSchema.safeParse(value);
    if (!parsed.success) throw new ApiError(400, 'invalid_registration', parsed.error.message);
    const spec = parsed.data;
    spec.endpoint.base_url = origin(spec.endpoint.base_url);
    if (this.allow?.length && !this.allow.includes(spec.endpoint.base_url))
      throw new ApiError(403, 'endpoint_not_allowed', 'worker endpoint is not allowed');
    if (new Set(spec.models.map((m) => m.id)).size !== spec.models.length)
      throw new ApiError(400, 'duplicate_model', 'duplicate model IDs');
    // Physical device identities are optional; when advertised they must not be double-counted.
    for (const other of this.workers.values())
      if (
        other.spec.worker_id !== spec.worker_id &&
        other.expires > Date.now() &&
        spec.resources.node_id &&
        other.spec.resources.node_id === spec.resources.node_id &&
        spec.resources.gpu_ids?.some((id) => other.spec.resources.gpu_ids?.includes(id))
      )
        throw new ApiError(409, 'duplicate_gpu', 'GPU is already registered by another worker');
    const worker: Worker = {
      spec,
      lease_id: randomUUID(),
      expires: Date.now() + this.ttl,
      heartbeat_at: Date.now(),
      sequence: -1,
      state: 'starting',
      reported: 0,
      resources: spec.resources,
    };
    this.workers.set(spec.worker_id, worker);
    return worker;
  }
  currentLease(id: string, lease: string): Worker {
    const w = this.workers.get(id);
    if (!w) throw new ApiError(409, 'unknown_worker', 'worker must register');
    if (lease !== w.lease_id || w.expires <= Date.now())
      throw new ApiError(409, 'expired_lease', 'worker must register a new lease');
    return w;
  }
  heartbeat(id: string, value: unknown): Worker {
    const parsed = heartbeatSchema.safeParse(value);
    if (!parsed.success) throw new ApiError(400, 'invalid_heartbeat', parsed.error.message);
    const v = parsed.data,
      w = this.currentLease(id, v.lease_id);
    if (v.sequence <= w.sequence)
      throw new ApiError(409, 'stale_sequence', 'heartbeat sequence must increase');
    w.sequence = v.sequence;
    w.expires = Date.now() + this.ttl;
    w.heartbeat_at = Date.now();
    w.state = v.state;
    w.reported = v.load.in_flight;
    if (v.resources) w.resources = { ...w.resources, ...v.resources };
    return w;
  }
  expire(): void {
    for (const w of this.workers.values()) if (w.expires <= Date.now()) w.state = 'stale';
  }
  snapshot(w: Worker, reserved = 0): Record<string, unknown> {
    return {
      worker_id: w.spec.worker_id,
      instance_id: w.spec.instance_id,
      state: w.expires <= Date.now() ? 'stale' : w.state,
      models: w.spec.models,
      resources: w.resources,
      labels: w.spec.labels,
      capacity: {
        slots: w.spec.capacity.slots,
        in_flight: Math.max(reserved, w.reported),
        gateway_reserved: reserved,
        reported_in_flight: w.reported,
        queue_depth: 0,
      },
      heartbeat_at: w.heartbeat_at,
      lease_expires_at: w.expires,
    };
  }
}
