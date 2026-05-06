# k8s-delete-interceptor v2

这是基于讨论结果重构的 v2 版本：Mongo 优先、共享 PVC 缓存、策略动态热加载、Web Console、Telegram 资源化、通知模板化、ServiceAccount 资产扫描、语义 diff 降噪、审计/通知/审批/回滚拆分。

## 核心原则

- Admission 请求路径只读内存 RuntimeConfig，不同步依赖 Mongo。
- Mongo 是优先数据源和 Web 查询数据源。
- 共享 PVC 保存 last-good 配置、回滚备份、Mongo 异常期间的审计缓冲队列。
- ConfigMap 只作为 bootstrap 默认配置。
- 未命中 ResourceScope 的资源默认只审计，不通知、不拦截。
- 创建、更新、删除统一走 ResourceScope -> ChangeClass -> Rule -> Decision。
- workload restart、managedFields/status/no effective change 默认只审计。
- 内置 Mongo 资源带固定 label 后默认禁止删除。

## 快速部署

1. 修改 `deploy/00-bootstrap.yaml` 里的 `storageClassName`、Mongo 密码、Webhook CA 注入方式。
2. 先部署 cert-manager，或替换为你的证书方案。
3. 执行：

```bash
kubectl apply -f deploy/00-bootstrap.yaml
kubectl apply -f deploy/10-mongodb.yaml
kubectl apply -f deploy/20-app.yaml
kubectl apply -f deploy/30-webhook.yaml
```

Web Console 默认 Service：`delete-interceptor.webhook-system.svc:8080`。
Webhook HTTPS：`delete-interceptor.webhook-system.svc:443`。

## 重要环境变量

- `CONFIG_PATH`: bootstrap 配置文件，默认 `/etc/config/runtime-config.yaml`
- `STATE_DIR`: 共享 PVC 路径，默认 `/var/lib/k8s-delete-interceptor`
- `MONGO_URI`: Mongo 连接串
- `MONGO_DATABASE`: Mongo database，默认 `k8s_delete_interceptor`
- `WEB_ADMIN_TOKEN`: 可选。设置后 Web API 需要 Header `Authorization: Bearer <token>`
- `WEB_BASE_URL`: 通知里展示的 Web 地址
- `TLS_CERT_FILE` / `TLS_KEY_FILE`: webhook TLS 证书

## 说明

这个包是完整可编译代码和 K8s 部署基础，但生产落地前仍建议结合你的集群证书、Ingress、OIDC 登录、备份策略、RWX 存储类做二次适配。

## Web Console v3 upgrade notes

This build extends the Web Console from a single read/write JSON page into an RBAC-driven operations console:

- Custom site name, subtitle, icon and default timezone through Web settings.
- User dropdown with login, logout and user switching. Username/password login is backed by RuntimeConfig `web_users`; `WEB_ADMIN_TOKEN` remains supported as a bootstrap superadmin token.
- Built-in RBAC roles: `superadmin`, `viewer`, `auditor`, `operator`, `rule_manager`. Roles map to granular permissions such as `rules:write`, `config:approve`, `datasources:write`, and `users:write`.
- Data sources are now a standalone navigation entry. Only one enabled + active data source is allowed.
- Cluster metadata endpoint automatically discovers namespaces, API resources, kinds, users and ServiceAccounts for dropdown selection.
- Historical events support time range, timezone, namespace, resource, kind, operation, decision and wildcard/regex matching for names and users.
- ServiceAccount page supports namespace filtering, collapsed details, and mounting an SA user string to an ActorGroup/security policy.
- Rule configuration supports form-based CREATE / UPDATE / DELETE policies and generates RuntimeConfig scopes/rules automatically.
- Important configuration changes create a pending config change request by default. Approvers can approve/reject in Web; Telegram notification is sent when Telegram is configured.
- Configuration versions can be listed, exported as YAML/JSON, and restored by submitting a restore change request.

Bootstrap login options:

```bash
# Backward compatible token login / superadmin bearer token
WEB_ADMIN_TOKEN='change-me'

# Optional username/password account inserted into default config on first boot
WEB_ADMIN_USERNAME='admin'
WEB_ADMIN_PASSWORD='change-me-too'

# Defaults to true. Set false only for local development if direct apply is desired.
CONFIG_CHANGE_REQUIRE_APPROVAL='true'
```

After a config mutation is submitted, check **变更审批** in the Web Console and approve it with a user that has `config:approve` or `*` permission.

## Web Console UX / RBAC Upgrade Notes

This build adds another round of Web Console improvements:

- Left navigation is icon-free and more compact.
- Data source health is shown in the top-right action bar instead of the lower-left sidebar.
- User login, switch-user and logout are grouped into a rounded user menu.
- Site settings are applied immediately and do not enter the approval workflow.
- Only business policy changes enter approval by default: rule changes, ServiceAccount policy mounts, full raw runtime config publishing and config version restore.
- Telegram now has a standalone navigation entry and API endpoints for Bot, Chat and approval-user settings.
- Cluster preview metadata is persisted and refreshed asynchronously to avoid expensive Kubernetes API calls on every page load.

Useful metadata tuning environment variables:

```bash
METADATA_REFRESH_INTERVAL=10m      # minimum enforced interval is 2m
METADATA_REFRESH_TIMEOUT=20s       # capped at 60s
METADATA_INITIAL_DELAY=20s         # delay before first background refresh after startup
```


## Telegram 全局队列与多副本消费

本版本将 Telegram 资源配置独立持久化到 MongoDB 的 `telegram_config` 集合，Web 页面更新 Bot、群/Chat、用户 ID 后会直接写入数据库，不再通过业务配置审批，也不依赖各 Pod 的本地运行配置缓存。发送通知时会实时读取数据库中的 Telegram 配置；如果 MongoDB 不可用，Telegram 通知不会直发，等待数据库恢复后继续消费队列。

通知消息统一写入 `telegram_notification_events` 集合，状态流转为：

- `pending`：未消费
- `sending`：消费中，带有 worker lease
- `sent`：消费完成
- `failed`：达到最大重试次数后的失败状态

多副本运行时，每个 Pod 会先探测是否有待消费事件；发现队列后随机竞争 `telegram_dispatcher` 分布式锁，只有抢到锁的 Pod 会启动本轮 Telegram 消费。队列被清空后释放锁；如果 Pod 异常退出，lease 到期后其他 Pod 会继续断点重试。

可用环境变量：

```bash
TELEGRAM_NOTIFY_MAX_WORKERS=8          # 最大并发 worker 数
TELEGRAM_NOTIFY_MIN_INTERVAL=1200ms    # 单 worker 发送间隔，避免 Telegram 限流
TELEGRAM_NOTIFY_MAX_ATTEMPTS=10        # 单条通知最大重试次数
TELEGRAM_NOTIFY_LEASE=2m               # 事件和 dispatcher 的 lease 时间
TELEGRAM_NOTIFY_IDLE_POLL=2s           # 无队列时探测间隔
TELEGRAM_NOTIFY_PROBE_JITTER=1500ms    # 多 Pod 随机竞争抖动
```

一个 Bot 可以配置 `token_env`、`token_envs`、`token`、`tokens`。程序会按可用 token 数自动提升消费 worker 数，但仍通过队列状态和 Mongo 原子抢占避免重复消费。生产环境建议使用 `token_env` / `token_envs`，不要在页面明文保存 token。
