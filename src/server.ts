import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { ApiError, errorOutput, requireToken, readJson, parseImage } from './protocol.js';
import type { GatewayService } from './gateway.js';
import { handleMcp } from './mcp.js';
function json(res: ServerResponse, status: number, value: unknown): void {
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(value));
}
export function buildServer(g: GatewayService) {
  const server = createServer((req, res) => {
    void handle(req, res, g).catch((e) => {
      const output = errorOutput(e);
      if (!res.headersSent) {
        res.writeHead(output.status, { 'content-type': output.content_type });
        res.end(output.body);
      } else res.end();
    });
  });
  server.requestTimeout = 0;
  server.headersTimeout = 30_000;
  return server;
}
async function handle(req: IncomingMessage, res: ServerResponse, g: GatewayService): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://gateway.invalid'),
    path = url.pathname,
    method = req.method;
  if (method === 'GET' && ['/livez', '/health', '/healthz'].includes(path)) {
    json(res, 200, { status: 'ok' });
    return;
  }
  if (method === 'GET' && path === '/readyz') {
    json(res, g.ready() ? 200 : 503, { ready: g.ready() });
    return;
  }
  const artifact = /^\/v1\/images\/files\/([a-f0-9]{64})$/.exec(path);
  if (method === 'GET' && artifact) {
    const file = g.store.getArtifact(artifact[1]);
    if (!file) throw new ApiError(404, 'image_not_found', 'image not found or expired');
    res.writeHead(200, {
      'content-type': file.mime,
      'cache-control': 'private, max-age=3600',
      'x-content-type-options': 'nosniff',
    });
    res.end(file.content);
    return;
  }
  if (path.startsWith('/control/')) {
    if (method === 'POST' && path === '/control/v1/workers/register') {
      json(res, 201, g.register(await readJson(req)));
      return;
    }
    const match = /^\/control\/v1\/workers\/([^/]+)\/(heartbeat|events)$/.exec(path);
    if (match && method === (match[2] === 'heartbeat' ? 'PUT' : 'POST')) {
      const body = await readJson(req);
      json(
        res,
        match[2] === 'events' ? 202 : 200,
        match[2] === 'heartbeat'
          ? g.heartbeat(decodeURIComponent(match[1]), body)
          : g.events(decodeURIComponent(match[1]), body),
      );
      return;
    }
  }
  if (path.startsWith('/gateway/') || path === '/mcp' || path === '/metrics') {
    requireToken(req.headers.authorization, g.config.management_token);
    if (path === '/mcp') {
      await handleMcp(req, res, g, method === 'POST' ? await readJson(req) : undefined);
      return;
    }
    if (method === 'GET' && path === '/metrics') {
      const jobs = g.jobs({ limit: 1000 });
      res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
      res.end(
        `# TYPE image_gateway_workers gauge\nimage_gateway_workers ${g.workerSnapshots().length}\n# TYPE image_gateway_jobs gauge\n${['queued', 'running', 'succeeded', 'failed', 'uncertain'].map((s) => `image_gateway_jobs{state="${s}"} ${jobs.filter((j) => j.state === s).length}`).join('\n')}\n`,
      );
      return;
    }
    if (method === 'GET' && path === '/gateway/v1/workers') {
      json(res, 200, { workers: g.workerSnapshots() });
      return;
    }
    if (method === 'GET' && path === '/gateway/v1/status') {
      json(res, 200, g.status());
      return;
    }
    if (method === 'GET' && path === '/gateway/v1/jobs') {
      json(res, 200, {
        jobs: g.jobs({
          worker_id: url.searchParams.get('worker_id') ?? undefined,
          status: url.searchParams.get('status') ?? undefined,
          limit: Number(url.searchParams.get('limit') ?? 100),
        }),
      });
      return;
    }
    const match = /^\/gateway\/v1\/jobs\/([^/]+)$/.exec(path);
    if (method === 'GET' && match) {
      const job = g.store.task(match[1]);
      if (!job) throw new ApiError(404, 'job_not_found', 'job not found');
      const { key, fingerprint, ...safe } = job;
      json(res, 200, { ...safe, scheduling: g.store.decisions(job.id) });
      return;
    }
  }
  if (path.startsWith('/v1/')) {
    requireToken(req.headers.authorization, g.config.generation_token);
    if (method === 'GET' && path === '/v1/models') {
      json(res, 200, { object: 'list', data: g.models() });
      return;
    }
    if (method === 'POST' && ['/v1/images/generations', '/v1/images/edits'].includes(path)) {
      const input = await parseImage(
        req,
        path.endsWith('edits') ? 'images.edits' : 'images.generations',
        g.config.max_request_bytes,
        g.config.default_model,
      );
      const job = g.enqueue(input, String(req.headers['idempotency-key'] ?? ''));
      res.setHeader('x-gateway-job-id', job.id);
      let sent = false;
      const result = await g.wait(
        job.id,
        input.fields.stream
          ? {
              head: (type) => {
                sent = true;
                res.writeHead(200, {
                  'content-type': type,
                  'cache-control': 'no-cache',
                  'x-accel-buffering': 'no',
                });
                res.flushHeaders();
              },
              chunk: (bytes) => {
                if (!res.destroyed) {
                  if (res.writableLength > g.config.max_response_bytes) res.destroy();
                  else res.write(bytes);
                }
              },
            }
          : undefined,
      );
      if (!res.destroyed) {
        if (!sent) {
          res.writeHead(result.status, {
            'content-type': result.content_type,
            'cache-control': 'no-store',
          });
          res.end(result.body);
        } else {
          if (result.status >= 400) res.write(`event: error\ndata: ${result.body}\n\n`);
          res.end();
        }
      }
      return;
    }
  }
  throw new ApiError(404, 'not_found', 'route not found');
}
