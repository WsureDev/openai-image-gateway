import { createHash, timingSafeEqual } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import Busboy from 'busboy';
import { z } from 'zod';

export const PROTOCOL = 'openai-image-worker/v1';
export const operationSchema = z.enum(['images.generations', 'images.edits']);
export type Operation = z.infer<typeof operationSchema>;
const id = z.string().regex(/^[A-Za-z0-9._:-]{1,128}$/);
export const modelSchema = z.object({
  id: z.string().min(1).max(256),
  operations: z.array(operationSchema).min(1),
  estimated_duration_ms: z.number().positive().max(86_400_000).optional(),
  streaming: z.boolean().default(false),
  metadata: z.record(z.unknown()).optional(),
});
export type Model = z.infer<typeof modelSchema>;
export const registrationSchema = z.object({
  protocol_version: z.literal(PROTOCOL),
  worker_id: id,
  instance_id: id,
  endpoint: z.object({
    base_url: z.string().url(),
    protocol: z.literal('openai-image/v1'),
  }),
  models: z.array(modelSchema).min(1).max(100),
  capacity: z.object({ slots: z.number().int().min(1).max(64), max_queue: z.literal(0) }),
  resources: z
    .object({
      node_id: id.optional(),
      gpu_ids: z.array(z.string().min(1)).optional(),
      gpus: z.array(z.record(z.unknown())).optional(),
    })
    .passthrough()
    .default({}),
  labels: z.record(z.string()).default({}),
});
export type Registration = z.infer<typeof registrationSchema>;
export const heartbeatSchema = z.object({
  lease_id: z.string().uuid(),
  sequence: z.number().int().nonnegative(),
  state: z.enum(['starting', 'ready', 'busy', 'draining', 'offline']),
  load: z.object({ in_flight: z.number().int().nonnegative(), queue_depth: z.literal(0) }),
  resources: z.record(z.unknown()).optional(),
});
export type Heartbeat = z.infer<typeof heartbeatSchema>;
export const eventSchema = z.object({
  sequence: z.number().int().positive(),
  task_id: id,
  state: z.enum(['running', 'succeeded', 'failed', 'uncertain']),
  occurred_at: z.number().int().nonnegative(),
});
export const eventsSchema = z.object({
  lease_id: z.string().uuid(),
  events: z.array(eventSchema).max(100),
});
export type TaskEvent = z.infer<typeof eventSchema>;
export interface Enrollment {
  worker_id: string;
  lease_id: string;
  heartbeat_interval_ms: number;
  lease_ttl_ms: number;
}
export interface ImageFile {
  field: string;
  name: string;
  mime: string;
  base64: string;
}
export interface ImageInput {
  operation: Operation;
  fields: Record<string, unknown>;
  files: ImageFile[];
  multipart: boolean;
}
export interface ImageOutput {
  status: number;
  content_type: string;
  body: string;
}
export type TaskState = 'queued' | 'dispatching' | 'running' | 'succeeded' | 'failed' | 'uncertain';
export interface Task {
  id: string;
  key: string;
  fingerprint: string;
  model: string;
  operation: Operation;
  state: TaskState;
  worker_id: string;
  instance_id: string;
  created_at: number;
  updated_at: number;
  started_at?: number;
  completed_at?: number;
  error_code?: string;
  error_message?: string;
  predicted_duration_ms?: number;
}
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}
export function errorOutput(error: unknown): ImageOutput {
  const e =
    error instanceof ApiError
      ? error
      : new ApiError(500, 'internal_error', 'internal service error');
  return {
    status: e.status,
    content_type: 'application/json',
    body: JSON.stringify({
      error: {
        message: e.message,
        type: e.status >= 500 ? 'server_error' : 'invalid_request_error',
        param: null,
        code: e.code,
      },
    }),
  };
}
export function requireToken(header: string | undefined, token: string): void {
  const supplied = Buffer.from(header?.replace(/^Bearer /, '') ?? '');
  const expected = Buffer.from(token);
  if (
    !token ||
    !header?.startsWith('Bearer ') ||
    supplied.length !== expected.length ||
    !timingSafeEqual(supplied, expected)
  )
    throw new ApiError(401, 'unauthorized', 'valid bearer token required');
}
export function origin(value: string): string {
  const url = new URL(value);
  if (
    !['http:', 'https:'].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== '/'
  )
    throw new ApiError(
      400,
      'invalid_endpoint',
      'endpoint must be an HTTP(S) origin without credentials or a path',
    );
  if (
    url.hostname === '0.0.0.0' ||
    url.hostname === '[::]' ||
    url.hostname.startsWith('169.254.') ||
    url.hostname.startsWith('[fe80:')
  )
    throw new ApiError(400, 'invalid_endpoint', 'endpoint must be an address reachable by Gateway');
  return url.origin;
}
export function fingerprint(input: ImageInput): string {
  const stable = (v: unknown): unknown =>
    Array.isArray(v)
      ? v.map(stable)
      : v && typeof v === 'object'
        ? Object.fromEntries(
            Object.entries(v)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([k, x]) => [k, stable(x)]),
          )
        : v;
  return createHash('sha256')
    .update(JSON.stringify(stable(input)))
    .digest('hex');
}
export async function readBytes(req: IncomingMessage, limit: number): Promise<Buffer> {
  if (Number(req.headers['content-length'] ?? 0) > limit) {
    req.resume();
    throw new ApiError(413, 'body_too_large', 'request exceeds body limit');
  }
  const parts: Buffer[] = [];
  let length = 0;
  for await (const chunk of req.iterator({ destroyOnReturn: false })) {
    const part = Buffer.from(chunk);
    length += part.length;
    if (length > limit) {
      req.resume();
      throw new ApiError(413, 'body_too_large', 'request exceeds body limit');
    }
    parts.push(part);
  }
  return Buffer.concat(parts);
}
export async function readJson(req: IncomingMessage, limit = 1024 * 1024): Promise<unknown> {
  try {
    return JSON.parse((await readBytes(req, limit)).toString());
  } catch (e) {
    if (e instanceof ApiError) throw e;
    throw new ApiError(400, 'invalid_json', 'invalid JSON body');
  }
}
export async function parseImage(
  req: IncomingMessage,
  operation: Operation,
  limit: number,
  defaultModel?: string,
): Promise<ImageInput> {
  const type = req.headers['content-type'] ?? '';
  const bytes = await readBytes(req, limit);
  let fields: Record<string, unknown> = {};
  const files: ImageFile[] = [];
  const multipart = type.toLowerCase().startsWith('multipart/form-data');
  if (multipart) {
    await new Promise<void>((resolve, reject) => {
      let parser: ReturnType<typeof Busboy>;
      try {
        parser = Busboy({
          headers: { 'content-type': type },
          limits: { files: 20, fields: 100, fileSize: limit, fieldSize: limit, parts: 120 },
        });
      } catch {
        reject(new ApiError(400, 'invalid_multipart', 'invalid multipart boundary'));
        return;
      }
      const fail = () => reject(new ApiError(413, 'multipart_limit', 'multipart limits exceeded'));
      parser.on('file', (field, stream, info) => {
        const parts: Buffer[] = [];
        stream.on('data', (b: Buffer) => parts.push(b));
        stream.on('limit', fail);
        stream.on('error', reject);
        stream.on('end', () =>
          files.push({
            field,
            name: info.filename,
            mime: info.mimeType,
            base64: Buffer.concat(parts).toString('base64'),
          }),
        );
      });
      parser.on('field', (field, value, info) => {
        if (info.valueTruncated) fail();
        else if (field in fields)
          reject(new ApiError(400, 'duplicate_field', `duplicate field: ${field}`));
        else fields[field] = value;
      });
      parser.on('filesLimit', fail);
      parser.on('fieldsLimit', fail);
      parser.on('partsLimit', fail);
      parser.on('error', () =>
        reject(new ApiError(400, 'invalid_multipart', 'malformed multipart body')),
      );
      parser.on('close', resolve);
      parser.end(bytes);
    });
    if (typeof fields.n === 'string') fields.n = Number(fields.n);
    if (fields.stream === 'true' || fields.stream === 'false')
      fields.stream = fields.stream === 'true';
  } else if (type.toLowerCase().startsWith('application/json')) {
    try {
      fields = JSON.parse(bytes.toString());
    } catch {
      throw new ApiError(400, 'invalid_json', 'invalid JSON body');
    }
    if (!fields || Array.isArray(fields) || typeof fields !== 'object')
      throw new ApiError(400, 'invalid_request', 'body must be a JSON object');
  } else
    throw new ApiError(
      415,
      'unsupported_content_type',
      'use application/json or multipart/form-data',
    );
  if (!fields.model && defaultModel) fields.model = defaultModel;
  if (typeof fields.model !== 'string' || !fields.model.trim())
    throw new ApiError(400, 'model_required', 'model is required');
  if (typeof fields.prompt !== 'string' || !fields.prompt.trim())
    throw new ApiError(400, 'prompt_required', 'prompt is required');
  if (
    fields.n !== undefined &&
    (!Number.isInteger(fields.n) || Number(fields.n) < 1 || Number(fields.n) > 100)
  )
    throw new ApiError(400, 'invalid_n', 'n must be an integer from 1 to 100');
  if (fields.stream !== undefined && typeof fields.stream !== 'boolean')
    throw new ApiError(400, 'invalid_stream', 'stream must be boolean');
  if (
    fields.response_format !== undefined &&
    !['url', 'b64_json'].includes(String(fields.response_format))
  )
    throw new ApiError(400, 'invalid_response_format', 'response_format must be url or b64_json');
  if (
    operation === 'images.edits' &&
    !files.some((f) => /^(image|images)(\[\]|\[\d+\])?$/.test(f.field)) &&
    !fields.image &&
    !fields.images
  )
    throw new ApiError(400, 'image_required', 'image is required for edits');
  return { operation, fields, files, multipart };
}
export function encodeImage(input: ImageInput): {
  body: BodyInit;
  headers: Record<string, string>;
} {
  if (!input.multipart)
    return { body: JSON.stringify(input.fields), headers: { 'content-type': 'application/json' } };
  const form = new FormData();
  for (const [key, value] of Object.entries(input.fields))
    form.append(key, typeof value === 'object' ? JSON.stringify(value) : String(value));
  for (const file of input.files)
    form.append(
      file.field,
      new Blob([Buffer.from(file.base64, 'base64')], { type: file.mime }),
      file.name,
    );
  return { body: form, headers: {} };
}
export async function boundedResponse(
  response: Response,
  limit: number,
  chunk?: (bytes: Buffer) => void,
): Promise<Buffer> {
  const parts: Buffer[] = [];
  let size = 0;
  if (response.body)
    for await (const value of response.body) {
      const part = Buffer.from(value);
      size += part.length;
      if (size > limit)
        throw new ApiError(502, 'response_too_large', 'worker response exceeds limit');
      parts.push(part);
      chunk?.(part);
    }
  return Buffer.concat(parts);
}
