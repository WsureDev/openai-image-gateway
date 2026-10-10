import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { GatewayService } from './gateway.js';
import { ApiError } from './protocol.js';
export async function handleMcp(
  req: IncomingMessage,
  res: ServerResponse,
  gateway: GatewayService,
  body: unknown,
): Promise<void> {
  const server = new McpServer({ name: 'openai-image-gateway', version: '0.1.0' });
  const wrap = (value: unknown) => ({
    content: [{ type: 'text' as const, text: JSON.stringify(value) }],
    structuredContent: { data: value },
  });
  const annotations = { readOnlyHint: true, destructiveHint: false, openWorldHint: false };
  const writeAnnotations = { readOnlyHint: false, destructiveHint: false, openWorldHint: false };
  server.registerTool(
    'adapter_configs_list',
    {
      description: 'Current runtime configuration profiles learned from registered adapters.',
      inputSchema: {},
      annotations,
    },
    async () => wrap(gateway.adapterConfigs()),
  );
  server.registerTool(
    'adapter_config_get',
    {
      description: 'Inspect one current adapter configuration profile.',
      inputSchema: { adapter_type: z.string(), image_id: z.string() },
      annotations,
    },
    async ({ adapter_type, image_id }) =>
      wrap(gateway.adapterConfig(adapter_type, image_id) ?? null),
  );
  server.registerTool(
    'adapter_config_set',
    {
      description: 'Replace the current runtime configuration for one adapter and image.',
      inputSchema: {
        adapter_type: z.string(),
        image_id: z.string(),
        config: z.record(z.unknown()),
      },
      annotations: writeAnnotations,
    },
    async ({ adapter_type, image_id, config }) => {
      if (!gateway.adapterConfig(adapter_type, image_id))
        throw new ApiError(404, 'adapter_config_not_found', 'adapter config not found');
      gateway.setAdapterConfig(adapter_type, image_id, config);
      return wrap(gateway.adapterConfig(adapter_type, image_id));
    },
  );
  server.registerTool(
    'workers_list',
    {
      description: 'Registered image workers, GPU resources, leases and capacity.',
      inputSchema: {},
      annotations,
    },
    async () => wrap(gateway.workerSnapshots()),
  );
  server.registerTool(
    'workers_get',
    { description: 'Inspect one worker.', inputSchema: { worker_id: z.string() }, annotations },
    async ({ worker_id }) =>
      wrap(gateway.workerSnapshots().find((w) => w.worker_id === worker_id) ?? null),
  );
  server.registerTool(
    'workers_history',
    {
      description: 'Persisted worker task events.',
      inputSchema: { worker_id: z.string() },
      annotations,
    },
    async ({ worker_id }) => wrap(gateway.store.events(worker_id)),
  );
  server.registerTool(
    'jobs_list',
    {
      description: 'Job history without prompts, input files or credentials.',
      inputSchema: {
        worker_id: z.string().optional(),
        status: z.string().optional(),
        limit: z.number().int().min(1).max(1000).optional(),
      },
      annotations,
    },
    async (args) => wrap(gateway.jobs(args)),
  );
  server.registerTool(
    'jobs_get',
    {
      description: 'One job and its scheduling decision.',
      inputSchema: { job_id: z.string() },
      annotations,
    },
    async ({ job_id }) => {
      const stored = gateway.store.task(job_id);
      const safe = stored ? (({ key, fingerprint, ...job }) => job)(stored) : null;
      return wrap({ job: safe, scheduling: gateway.store.decisions(job_id) });
    },
  );
  server.registerTool(
    'scheduler_snapshot',
    { description: 'Queue, load and schedulable model inventory.', inputSchema: {}, annotations },
    async () => wrap(gateway.status()),
  );
  server.registerTool(
    'scheduler_explain',
    {
      description: 'Candidate eligibility and predicted duration at dispatch.',
      inputSchema: { job_id: z.string() },
      annotations,
    },
    async ({ job_id }) => wrap(gateway.store.decisions(job_id)),
  );
  server.registerTool(
    'metrics_snapshot',
    { description: 'Current cluster capacity and queue counters.', inputSchema: {}, annotations },
    async () => wrap(gateway.status()),
  );
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
    enableJsonResponse: true,
  });
  res.on('close', () => {
    void transport.close();
    void server.close();
  });
  await server.connect(transport);
  await transport.handleRequest(req, res, body);
}
