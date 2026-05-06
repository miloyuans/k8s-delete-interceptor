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
