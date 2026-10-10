# 验证

运行环境为 Node.js 24，依赖由 package-lock.json 和 npm ci 固定。

```bash
npm ci
npm run typecheck
npm test
npm run format:check
node scripts/adapter-contract-smoke.mjs /path/to/openai-image-worker-adapter
node scripts/config-contract-smoke.mjs /path/to/openai-image-worker-adapter
```

自动化测试使用临时 HTTP worker 和 SQLite，覆盖三执行单元并发、模型路由、multipart、幂等持久化、队列超时、租约、事件去重、断线恢复、SSE 和图片 URL。OpenAI SDK 和官方 MCP SDK 参与接口验证。

动态调度测试覆盖空闲优先、近期速度排序、首次探测、间隔重测、速度变化后的排序更新、无同尺寸样本时的每图回退、实例/操作隔离、重启持久化、失败样本排除，以及实际 HTTP 派单中最快 Worker 忙碌时使用其他空闲 Worker。该验证不向生产模型提交推理。

契约冒烟测试启动三个真实 Adapter 运行时并注入 fake backend，确认自主注册、六个 Gateway 任务分配、三个直连任务隔离和事件持久化。测试向各 Adapter 注入无归属积压事件，验证拒绝隔离、后续心跳和租约稳定。部署验证检查 HTTP 健康、内部免鉴权、对外凭据隔离、模型目录、指标及 11 个 MCP 工具，其中配置工具覆盖 Gateway 当前配置查询。

配置契约冒烟测试启动两个真实 Qwen Adapter、Gateway 和 fake HTTP 模型，验证不同本地默认值的 Adapter 共用首个注册配置；MCP 写入影响两台的下一次任务；批量生图共用一次读取；执行中配置更新、幂等重放和直连默认值互不干扰。部署验证脚本只调用只读工具，不调用配置写入工具。

部署后的只读验收需要本地 `.env` 指向目标 Gateway，不属于上述离线测试：

```bash
node scripts/verify-deployment.mjs
```

生产服务由 Compose 管理。模型 worker 的容器 ID、启动时间与镜像用于确认 API 更新没有改动推理进程。仓库不追踪真实凭据、任务数据库、日志、依赖或构建产物。
