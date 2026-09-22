# OpenAI Image Worker Protocol v1

此协议属于本项目，不是 OpenAI 官方发布的 worker 管理协议。两个独立仓库保存相同的 `src/protocol.ts` 和本契约副本。公共请求采用 OpenAI Images API；模型参数由 Adapter 校验。Gateway 不导入任何模型实现。

## 数据面

| 方法 | 路径                     | 语义                                            |
| ---- | ------------------------ | ----------------------------------------------- |
| GET  | `/v1/models`             | OpenAI 模型列表                                 |
| POST | `/v1/images/generations` | JSON 图片生成                                   |
| POST | `/v1/images/edits`       | multipart 编辑；也允许 Adapter 声明的 JSON 扩展 |

请求必须有 `model`、`prompt`。默认模型只在配置后补入。公共字段 `n`、`stream`、`response_format` 做基础校验，`size` 和其他扩展参数透传。multipart 保留文件字节、字段名和文件名。具体可用尺寸、步数、参考图限制、mask、quality 等由 Adapter 决定，不支持的参数应明确拒绝。

Adapter 接受 `response_format=b64_json`，成功非流式响应必须是 `{created, data:[{b64_json,...}],...}`。它不向 Gateway 返回内部文件 URL。客户端选择 `response_format=url` 时，Gateway 向 Adapter 请求 base64 并托管结果，返回具有一小时有效期的随机能力 URL。不要在外部日志中泄露这些 URL。

`stream=true` 只路由到声明 `streaming:true` 的模型。事件按 OpenAI 图片 SSE 格式透传，至少包含 `image_generation.completed` 或 `image_edit.completed`；部分图不是必须的。Gateway 不制造模型进度。流式 URL 格式暂不支持。流中错误或缺少 completed 事件记为失败。相同幂等键的迟到流式请求重放完整结果，避免漏掉早期事件。

Gateway -> Adapter 数据面使用独立 `Authorization: Bearer <api_token>`，以及：

- `X-Gateway-Job-Id`：全局任务 ID。
- `Idempotency-Key`：同一个全局任务 ID。

Adapter 必须以任务 ID/幂等键持久化去重，相同 ID 与不同请求返回 409。没有确认结果的任务不自动重跑。`n` 作为整个请求占一个槽，不拆到多卡；多卡并行来自多个独立请求。

## 注册与租约

`POST /control/v1/workers/register`，使用 enrollment bearer token：

```json
{
  "protocol_version": "openai-image-worker/v1",
  "worker_id": "host-a-gpu-0",
  "instance_id": "durable-adapter-instance-id",
  "endpoint": {
    "base_url": "http://worker-a.example:54162",
    "protocol": "openai-image/v1",
    "api_token": "replace-with-a-dedicated-strong-secret"
  },
  "models": [
    {
      "id": "example-image",
      "operations": ["images.generations", "images.edits"],
      "streaming": false,
      "estimated_duration_ms": 60000
    }
  ],
  "capacity": { "slots": 1, "max_queue": 0 },
  "resources": { "node_id": "host-a", "gpu_ids": ["GPU-example"] },
  "labels": { "location": "lan" }
}
```

201 响应：`worker_id`、`lease_id`、`worker_token`、`heartbeat_interval_ms`、`lease_ttl_ms`。注册后状态为 starting，首次就绪心跳后参与调度。worker_token 只用于控制面，api_token 只用于 Adapter 数据/查询面。Gateway 不持久化或向 MCP 返回这两个凭据。

同 worker_id 重新注册会替换租约并吊销旧 worker_token；已分配任务保留原 instance_id。Adapter 将 instance_id 持久化在自己的状态卷中。丢失状态卷/更换底层执行实例时应生成新的实例标识，旧实例的 uncertain 任务不会被当作新实例的任务。

每个 Adapter 应注册一个 GPU 执行单元，默认 capacity=1。若上报 node_id + gpu_ids，Gateway 拒绝把仍有有效租约的同一 GPU 重复注册为两个 worker。GPU 指标可选，缺失时不能推断 GPU 空闲。

## 心跳与任务事件

`PUT /control/v1/workers/{worker_id}/heartbeat`，使用 worker_token bearer：

```json
{
  "lease_id": "lease-uuid-returned-by-gateway",
  "sequence": 1,
  "state": "ready",
  "load": { "in_flight": 0, "queue_depth": 0 },
  "resources": { "gpus": [{ "id": "GPU-example", "memory_used_bytes": 1000 }] }
}
```

状态：starting、ready、busy、draining、offline。序号必须在同一租约中严格递增。租约过期返回 409，必须重新注册；不能用迟到心跳复活旧租约。默认 10 秒心跳/30 秒租约。Gateway 的计时以收到心跳的本机时钟为准。

有效占用为 `max(Adapter 报告占用, Gateway 持久化预留槽数)`。dispatching/running/uncertain 均占用槽；心跳中报告 0 不会消除已有任务的预留。

`POST /control/v1/workers/{worker_id}/events`：

```json
{
  "lease_id": "lease-uuid-returned-by-gateway",
  "events": [
    {
      "sequence": 1,
      "task_id": "gateway-job-id",
      "state": "succeeded",
      "occurred_at": 1790000000000
    }
  ]
}
```

事件序号与心跳序号独立。事件序号持久化，Gateway 按 worker_id + instance_id + sequence 去重。Adapter 维护 SQLite outbox，收到确认后标记已发送，网络失败重试。事件只能对应分配给当前 worker/instance 的任务。事件用于历史审计，不单凭 succeeded 事件释放执行槽；必须收齐结果或通过任务查询确认。

## Adapter 查询

除 `/livez` 外均使用 api_token。

- `GET /livez`：Adapter 进程存活。
- `GET /health`、`GET /readyz`：后端就绪探测；未就绪 503。
- `GET /worker/v1/status`：状态、model、load、resources、worker/instance 标识。
- `GET /worker/v1/tasks`：任务元数据。
- `GET /worker/v1/tasks/{id}`：任务及 `result:{status,content_type,body}`，供 Gateway 恢复结果。
- `GET /metrics`：Prometheus text 格式。

Gateway 重启会恢复 queued 请求，将未确认的执行标记为 uncertain。Adapter 重新注册同一个 instance 后，Gateway 查询任务并恢复成功/失败结果。网络超时、404 或更换 instance **不证明任务没有执行**，不会自动重提；相关槽保持预留。需要后续人工确认或引入能证明取消/失败的底层任务协议。这是防止重复生成的保守边界，不承诺跨模型进程 exactly-once。

## 范围与认证

单 Gateway + 本地 SQLite/WAL；不支持两个 Gateway 同时共享一个状态文件。公共生成、管理/MCP、enrollment、Adapter API 使用不同凭据。enrollment 持有者可声明下游地址，应视为可信管理者；可用 `GATEWAY_ALLOWED_WORKER_ORIGINS` 限制地址。跨主机部署应放在受控局域网或已有 TLS 反向代理后。

官方图片接口参考：

- https://developers.openai.com/api/reference/resources/images/methods/generate
- https://developers.openai.com/api/reference/resources/images/methods/edit

2026-09-22 开发验证只使用模拟 worker。Qwen connector 尚未对优化后的真实模型服务做验收。
