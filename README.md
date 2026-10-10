# OpenAI Image Gateway

通用 TypeScript OpenAI 图片 API 聚合层。Worker Adapter 主动注册，Gateway 根据模型能力、容量和历史耗时分派任务；Qwen、Hunyuan 等模型实现不进入 Gateway。

当前已实现注册/租约、动态模型目录、全局持久队列、每 worker 槽位预留、幂等请求、掉线后的结果查询、调度历史、图片 URL 托管、SSE 转发、MCP、Adapter 运行时配置和 Prometheus 指标。状态使用 SQLite/WAL，运行时要求 Node.js 24+。

## 安装与测试

本机开发和 CI 使用 `.nvmrc` 指定的 Node.js 26；使用 nvm 时，先执行 `nvm install` 和 `nvm use`。

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
node scripts/config-contract-smoke.mjs /path/to/openai-image-worker-adapter
```

注册与任务契约测试启动三个真正的 Adapter 运行时，每个注入 fake backend，验证主动注册、六个任务并行调度和任务事件持久化。配置契约测试启动两个 Qwen Adapter 和 fake HTTP 模型，验证共享配置、批量任务、幂等重放和直连默认值。两者都不会调用实际推理服务。

## 配置与运行

生产部署使用仓库根目录的标准 `compose.yaml`。复制 `.env.example` 为 `.env`，填入两组不同的客户端/管理凭据；将 `GATEWAY_PUBLISH_IP` 和 `GATEWAY_PUBLIC_URL` 设为局域网可访问的本机地址：

```bash
docker compose config --quiet
docker compose up -d --build --wait
docker compose ps
```

Compose 项目及容器名为 `openai-image-gateway`，默认宿主机端口为 54160，容器端口为 8188；数据卷 `openai-image-gateway_gateway-state` 挂载到 `/data`。容器以 node 用户运行，根文件系统只读，设置自动重启和 `/livez` 健康检查。零 worker 时 `/readyz` 返回 503，不影响容器健康状态。

Compose 使用独立的 `GATEWAY_SUBNET`（默认 `10.253.80.0/24`）；部署时应选择与本机路由不重叠的子网。可选 `GATEWAY_BUILD_PROXY`、`GATEWAY_BUILD_NETWORK` 只作用于镜像构建，不向运行中的 Gateway 注入代理。

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

## 内部调用

worker 使用 `openai-image-worker/v1` 自主注册，不需要注册 token。心跳和事件也无需 Authorization；Gateway 调用 Adapter 时不附带或转发客户端凭据。Adapter 只需配置 Gateway URL、自身可达地址和 worker ID。租约/任务归属校验继续保证调度正确性。

## 管理和 MCP

管理 REST：`/gateway/v1/workers`、`/gateway/v1/status`、`/gateway/v1/jobs`、`/gateway/v1/jobs/{id}`。任务查询不返回 prompt、输入文件和凭据。

`/mcp` 使用官方 TypeScript SDK 的 stateless Streamable HTTP transport。支持 worker、job、scheduler、metrics 查询，以及 `adapter_configs_list`、`adapter_config_get`、`adapter_config_set`。所有工具使用管理 bearer token；只有 `adapter_config_set` 写入配置。配置 schema 和默认值来自已注册 Adapter，Gateway 只保存每个 `adapter_type + image_id` 的唯一当前配置，不预设模型字段。`image_id` 就是注册模型的 `id`（客户端的 `model`）。首次注册初始化配置；后续注册或 Gateway 重启都不会重置它。

先通过 `adapter_config_get` 查看注册的 schema 和当前配置，再以完整 JSON 对象调用 `adapter_config_set`，例如：

```json
{
  "adapter_type": "qwen",
  "image_id": "qwen-image-2.1",
  "config": { "steps": 40, "steps_mode": "default" }
}
```

写入是整份替换，不合并旧字段；未知类型或模型无法创建配置，必须先由 Adapter 注册。Gateway 只检查配置为 JSON 对象，模型字段由具体 Adapter 在每次执行前校验；无效配置使请求在推理前失败。没有配置历史、多版本、digest、定时同步，也不修改 Adapter 环境变量或模型 Worker。

`/metrics` 同样需要管理凭据。`/livez`、`/health` 为进程存活，`/readyz` 表示至少一种模型在线。图片文件 URL 为随机能力 URL，生成后有效一小时。

## 持久化和调度边界

- 单实例 SQLite/WAL；队列、输入、结果、幂等映射、事件、调度记录持久化。
- FIFO 扫描队列；跳过当前无匹配容量的任务，避免阻塞其他模型。
- 调度先过滤离线、不支持请求和槽位已满的 Worker（包含 uncertain 预留），再优先完全空闲的 Worker；多槽 Worker 都在执行时，优先占用比例较低者。
- 同一空闲层内按近期历史速度选择：优先相同 worker实例/model/operation/size/n 的最多20个成功样本，EWMA 最近样本权重0.3。不包含排队时间，失败和无法确认执行时长的恢复任务不作为速度样本。
- 没有同尺寸样本时，回退到同实例、同模型、同操作的每图历史耗时，并按 n 估算；文生图和编辑分开，新实例重新学习。模型专用参数不进入 Gateway。
- 新 Worker 在该模型/操作下首次有空闲容量时获得一次探测任务。某 Worker 连续错过20次同模型/操作派单时，在同一空闲层获得一次重测机会；其余派单优先历史更快者。失败的探测也计为已尝试，避免反复占用探测机会。不为探测额外生成图片，使用实际到达的任务。
- 历史和派单次数持久化，重启不会固定回到某个 Worker。速度相同则优先较久没分配任务者，没有写死机器名或 GPU 优先级。最快者忙碌时使用其他空闲 Worker，不为等最快机器阻塞队列。
- MCP `scheduler_snapshot` / `metrics_snapshot` 公布当前策略；`scheduler_explain` 返回选择理由、每个候选的负载、预计耗时、样本数和估算来源，方便验证动态分配。
- Adapter 通过 `/control/v1/config` 在每次 Gateway 生图任务前拉取当前配置；Gateway 不在生图请求中注入模型特定字段。一次请求内的多张图共用本次配置，幂等重放不重新拉取；直连 Adapter 使用本地默认值。配置读取采用 `no-store`，拉取失败或配置无效时不会调用模型 Worker。
- 心跳负载不能冲掉 Gateway 自己预留的槽。超时、断线、进程重启后先查询结果，不盲目重复派单。
- uncertain 任务在原实例不可证明完成时会一直保留槽。新实例不能冒领旧实例结果；当前版本没有强制释放/取消模型任务的管理接口。
- 任务和输入/结果暂不自动清理，属于本地 MVP 的持久历史；大规模长期部署前需增加保留策略、磁盘配额和清理作业。图片访问有过期时间，但过期 BLOB 暂未做垃圾回收。
- SQLite 不用于 Gateway 多副本 HA。完整响应和有限大小的上传会进入内存；初始目标是少量 GPU 的局域网集群。

[统一 Worker 协议](contracts/worker-protocol.md) / [JSON Schema](contracts/worker-protocol.schema.json)。

## 当前部署

Gateway 和 Adapter 由 Compose 运行，worker 自主注册并上报状态。对外入口、凭据和发布地址来自 `.env`，该文件不进入 Git 或离线包。日常使用与接口验证见 [VALIDATION.md](VALIDATION.md)。
