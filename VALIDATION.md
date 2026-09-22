# 验证

运行环境为 Node.js 24，依赖由 package-lock.json 和 npm ci 固定。

```bash
npm ci
npm run typecheck
npm test
npm run format:check
node scripts/adapter-contract-smoke.mjs /path/to/openai-image-worker-adapter
node scripts/verify-deployment.mjs
```

自动化测试使用临时 HTTP worker 和 SQLite，覆盖三执行单元并发、模型路由、multipart、幂等持久化、队列超时、租约、事件去重、断线恢复、SSE 和图片 URL。OpenAI SDK 和官方 MCP SDK 参与接口验证。

契约冒烟测试启动三个真实 Adapter 运行时并注入 fake backend，确认自主注册、六个 Gateway 任务分配、三个直连任务隔离和事件持久化。测试向各 Adapter 注入无归属积压事件，验证拒绝隔离、后续心跳和租约稳定。部署验证检查 HTTP 健康、内部免鉴权、对外凭据隔离、模型目录、指标及八个只读 MCP 工具。

生产服务由 Compose 管理。模型 worker 的容器 ID、启动时间与镜像用于确认 API 更新没有改动推理进程。包内不包含真实凭据、任务数据库或模型权重。
