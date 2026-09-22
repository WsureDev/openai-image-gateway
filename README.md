# OpenAI Image Gateway

通用 TypeScript OpenAI 图片 API 聚合层。Worker Adapter 主动注册，Gateway 根据模型能力、容量和历史耗时分派任务；Qwen、Hunyuan 等模型实现不进入 Gateway。

当前已实现注册/租约、动态模型目录、全局持久队列、每 worker 槽位预留、幂等请求、掉线后的结果查询、调度历史、图片 URL 托管、SSE 转发、MCP 和 Prometheus 指标。状态使用 SQLite/WAL，运行时要求 Node.js 24+。

## 安装与测试

```bash
npm ci
npm run typecheck
npm test
npm run format:check
```

测试只启动回环地址的临时 HTTP fake worker，使用临时 SQLite 数据库，不加载模型，不使用 Docker 或 GPU。测试包括 OpenAI SDK 和官方 MCP SDK 客户端。

跨两个仓库的契约冒烟测试：先在两个仓库执行 `npm run build`，再从本仓库执行：

```bash
node scripts/adapter-contract-smoke.mjs /path/to/openai-image-worker-adapter
```

它启动三个真正的 Adapter 运行时，每个注入 fake backend，验证主动注册、六个任务并行调度、任务事件持久化；不会调用实际推理服务。

## 配置与运行

生产部署使用仓库根目录的标准 `compose.yaml`。复制 `.env.example` 为 `.env`，填入三组不同的强随机凭据；将 `GATEWAY_PUBLISH_IP` 和 `GATEWAY_PUBLIC_URL` 设为局域网可访问的本机地址：

```bash
docker compose config --quiet
docker compose up -d --build --wait
docker compose ps
```

Compose 项目及容器名为 `openai-image-gateway`，默认宿主机端口为 54160，容器端口为 8188；数据卷 `openai-image-gateway_gateway-state` 挂载到 `/data`。容器以 node 用户运行，根文件系统只读，设置自动重启和 `/livez` 健康检查。零 worker 时 `/readyz` 返回 503，不影响容器健康状态。

此主机的 Docker 默认地址池已耗尽，Compose 使用独立的 `GATEWAY_SUBNET`（默认 `10.253.80.0/24`）；部署到其他主机时应选择与当地路由不重叠的子网。可选 `GATEWAY_BUILD_PROXY`、`GATEWAY_BUILD_NETWORK` 只作用于镜像构建，不向运行中的 Gateway 注入代理。

部署后可从宿主机做只读验收，凭据从 `.env` 加载并保持不输出：

```bash
npm ci
node scripts/verify-deployment.mjs
```

本机开发也可以直接运行 Node；此时把 `GATEWAY_PUBLIC_URL` 设置为本机实际监听的 8188 端口：

```bash
npm run build
node --env-file=.env dist/src/main.js
```

默认仅监听 `127.0.0.1:8188`。LAN 场景配置监听地址、`GATEWAY_PUBLIC_URL`，并使用现有 TLS 入口或受控网络。Gateway 不配置固定 worker 列表；允许地址白名单可选。

| 配置                              | 含义                                          |
| --------------------------------- | --------------------------------------------- |
| GATEWAY_GENERATION_TOKEN          | OpenAI 图片客户端凭据，至少 24 字符           |
| GATEWAY_ENROLLMENT_TOKEN          | Worker 注册凭据，至少 24 字符                 |
| GATEWAY_MANAGEMENT_TOKEN          | 管理/MCP 凭据，至少 24 字符                   |
| GATEWAY_STATE_PATH                | SQLite 文件，默认 `./data/gateway.sqlite`     |
| GATEWAY_PUBLIC_URL                | 返回图片 URL 的外部地址                       |
| GATEWAY_ALLOWED_WORKER_ORIGINS    | 可选，逗号分隔的 Worker HTTP(S) origin 白名单 |
| GATEWAY_DEFAULT_MODEL             | 可选，省略 model 时使用；无默认值             |
| GATEWAY_HEARTBEAT_TTL_MS          | 默认 30000                                    |
| GATEWAY_WORKER_REQUEST_TIMEOUT_MS | 默认 2700000，覆盖完整响应体                  |
| GATEWAY_QUEUE_TIMEOUT_MS          | 默认 300000                                   |
| GATEWAY_MAX_QUEUED_JOBS           | 默认 100                                      |
| GATEWAY_MAX_REQUEST_BYTES         | 默认 64 MiB                                   |
| GATEWAY_MAX_RESPONSE_BYTES        | 默认 128 MiB                                  |

生成请求示例：

```bash
curl http://127.0.0.1:8188/v1/images/generations \
  -H "Authorization: Bearer $GATEWAY_GENERATION_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: request-001' \
  -d '{"model":"example-image","prompt":"a red cup","response_format":"b64_json"}'
```

客户端只需要 Gateway 的 base URL、API key、model ID。未知或没有在线可用能力的模型返回 503；忙碌但在线的模型进入持久队列。`n` 在一个 worker 中执行整个请求，不等同于跨卡 batch。多个请求可并行使用三张 GPU。

## 管理和 MCP

管理 REST：`/gateway/v1/workers`、`/gateway/v1/status`、`/gateway/v1/jobs`、`/gateway/v1/jobs/{id}`。任务查询不返回 prompt、输入文件和凭据。

`/mcp` 使用官方 TypeScript SDK 的 stateless Streamable HTTP transport。支持 tools：workers_list、workers_get、workers_history、jobs_list、jobs_get、scheduler_snapshot、scheduler_explain、metrics_snapshot。所有工具只读，使用管理 bearer token。

`/metrics` 同样需要管理凭据。`/livez`、`/health` 为进程存活，`/readyz` 表示至少一种模型在线。图片文件 URL 为随机能力 URL，生成后有效一小时。

## 持久化和调度边界

- 单实例 SQLite/WAL；队列、输入、结果、幂等映射、事件、调度记录持久化。
- FIFO 扫描队列；跳过当前无匹配容量的任务，避免阻塞其他模型。
- 可用槽中的候选按预计执行耗时选择，完成后用 EWMA 更新。按 worker/model/operation/size/n 分组；模型专用扩展参数不进入 Gateway 调度代码。
- 心跳负载不能冲掉 Gateway 自己预留的槽。超时、断线、进程重启后先查询结果，不盲目重复派单。
- uncertain 任务在原实例不可证明完成时会一直保留槽。新实例不能冒领旧实例结果；当前版本没有强制释放/取消模型任务的管理接口。
- 任务和输入/结果暂不自动清理，属于本地 MVP 的持久历史；大规模长期部署前需增加保留策略、磁盘配额和清理作业。图片访问有过期时间，但过期 BLOB 暂未做垃圾回收。
- SQLite 不用于 Gateway 多副本 HA。完整响应和有限大小的上传会进入内存；初始目标是少量 GPU 的局域网集群。

[统一 Worker 协议](contracts/openai-image-worker-v1.md) / [JSON Schema](contracts/openai-image-worker-v1.schema.json)。

## 交付状态

Gateway 已完成镜像构建、标准 Compose 部署和实际 HTTP/MCP 验收。部署凭据和本机发布地址保存在被 Git 忽略的 `.env`。现有 qwen-image API、ComfyUI 容器和模型进程未改动，Adapter 尚未部署；真实 Qwen 优化后负载的对接和 GPU 性能验收仍待进行。
