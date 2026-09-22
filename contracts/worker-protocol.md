# Worker 协议

Gateway 与 Adapter 使用唯一的 `openai-image-worker/v1` 契约。两个仓库中的 `src/protocol.ts` 和本目录契约保持一致。Gateway 只理解模型 ID、OpenAI 图片操作、执行容量和任务状态。

## 鉴权与职责

客户端访问 Gateway 的 OpenAI 图片接口需要 generation token，管理接口和 MCP 需要 management token。个人局域网内部的注册、心跳、事件、Adapter 生成和查询均无需凭据。Gateway 不向 Adapter 转发客户端 Authorization。

## OpenAI 图片接口

- `GET /v1/models`
- `POST /v1/images/generations`：JSON 请求。
- `POST /v1/images/edits`：multipart 文件或 Adapter 声明支持的 JSON 图片输入。

请求包含 model、prompt，Gateway 校验 n、stream、response_format 的基本类型，其他模型参数交由 Adapter 处理。multipart 文件字节、名称和字段保持完整。

Adapter 返回 `{created,data:[{b64_json,...}],...}`。Gateway 在 response_format=url 时托管图片，生成有效期一小时的随机能力 URL。stream=true 只路由到声明 streaming=true 的模型，使用 `image_generation.completed` / `image_edit.completed` 及可选 partial_image 事件。流式结果使用 base64。

Gateway 派发时发送 `X-Gateway-Job-Id`，并以同一全局任务 ID 作为 `Idempotency-Key`。Adapter 持久化任务去重和结果；相同 ID 携带不同请求返回 409。n 张图片由一个执行单元顺序完成，多个独立请求可以使用多个 GPU。

## 主动注册

`POST /control/v1/workers/register`：

```json
{
  "protocol_version": "openai-image-worker/v1",
  "worker_id": "host-a-gpu-0",
  "instance_id": "persistent-instance-id",
  "endpoint": { "base_url": "http://worker-a.example:54162", "protocol": "openai-image/v1" },
  "models": [
    {
      "id": "example-image",
      "operations": ["images.generations", "images.edits"],
      "streaming": true,
      "estimated_duration_ms": 60000
    }
  ],
  "capacity": { "slots": 1, "max_queue": 0 },
  "resources": { "node_id": "host-a", "gpu_ids": ["GPU-example"] }
}
```

201 响应包含 worker_id、lease_id、heartbeat_interval_ms、lease_ttl_ms。注册不签发凭据。实例先进入 starting，收到就绪心跳后参与调度。

worker_id 是稳定的执行单元标识。instance_id 保存在 Adapter 状态卷中。每次注册产生新的 lease_id，用于识别当前连接代际。每个 GPU 应只注册一个执行单元；node_id + gpu_ids 重复时拒绝计入两份容量。

## 心跳和事件

`PUT /control/v1/workers/{worker_id}/heartbeat`：

```json
{
  "lease_id": "registration-lease-uuid",
  "sequence": 1,
  "state": "ready",
  "load": { "in_flight": 0, "queue_depth": 0 },
  "resources": { "gpus": [{ "id": "GPU-example", "memory_used_bytes": 1000 }] }
}
```

state 为 starting、ready、busy、draining 或 offline。序号在租约中严格递增；租约无效、过期或序号倒退返回 409。默认每 10 秒心跳、30 秒租约，以 Gateway 接收时钟计时。

有效占用为 `max(Adapter 报告占用, Gateway 持久化预留槽)`。dispatching、running、uncertain 均占用槽；报告空闲不会清除 Gateway 已预留的执行权。

`POST /control/v1/workers/{worker_id}/events`：

```json
{
  "lease_id": "registration-lease-uuid",
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

事件序号与心跳序号独立。Adapter 使用持久化 outbox 重发未确认事件，Gateway 按 worker_id、instance_id、sequence 去重。任务必须属于该 worker 实例。收到完成事件后仍需确认结果，才释放任务槽。

Adapter 仅为 Gateway 派发的任务创建上报事件，直连任务保存在本地。Adapter 逐条提交并确认事件，accepted=1 表示新收录，accepted=0 表示已收录。task_owner_mismatch 或 invalid_events 会隔离对应事件，保留任务和拒绝记录并继续发送其他事件；未知错误、网络错误和不完整确认保持待重试。只有 expired_lease、unknown_worker、stale_sequence 触发重新注册。

## 查询与恢复

Adapter 提供 `/livez`、`/readyz`、`/health`、`/worker/v1/status`、`/worker/v1/tasks`、`/worker/v1/tasks/{id}` 和 `/metrics`，全部无需凭据。单任务查询包含状态，以及完成时的 `result:{status,content_type,body}`。

`/worker/v1/status` 提供 event_delivery.pending/rejected 和 control 状态；`/worker/v1/events/rejected` 返回最近的拒绝事件、原因和时间。事件隔离不修改推理结果或释放不确定任务的槽位。

Gateway 持久化队列、任务、输入、结果和调度决策。未确认执行在重启后进入 uncertain，通过原 worker 实例的任务查询恢复结果。超时、404、实例不匹配都不能证明任务没有执行，不自动重新推理；相关槽保持预留，避免重复生成。

当前采用单 Gateway、SQLite/WAL 和每 Adapter 一个执行槽。状态文件不能被多 Gateway 并发共享。队列超时、上传/响应体大小和等待数量均有限制。
