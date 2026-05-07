# k8s-delete-interceptor-v2 latest patched

## 本次升级重点

### 1. Telegram 交互窗口
- 新增 Telegram callback polling 后台循环：当存在未过期的交互通知时，会在单独分布式锁下持续处理 callback_query，不再依赖通知队列消费 session 是否还在。
- 同时保留 `/telegram/webhook`，如果 Telegram 已配置 webhook，getUpdates 会被 Telegram 拒绝，系统会记录日志并继续依赖 webhook 路径处理回调。
- 每条交互通知新增 `interactive`、`interaction_expires_at`、`interaction_closed_at` 字段。
- 交互完成、拒绝、审批、回滚、YAML 下载或窗口过期后，会编辑原 Telegram 消息并关闭交互窗口。
- 状态更新后的按钮不再保留“下载 YAML”的 callback，而是更新为“打开事件页下载 YAML”的 URL 按钮；交互进行中仍可直接点 Telegram 下载 YAML 摘要。
- 状态内容追加快速检索关键字：`event:<id>`、`rollback:<id>`、`fp:<fingerprint>`。

### 2. 重复事件抑制
- 新增 admission 事件业务指纹 `fingerprint`，基于集群、资源、用户、规则、决策、变更类型以及去掉 volatile 字段后的新旧对象生成。
- 默认 30 秒内同一 fingerprint 的重复 AdmissionReview 会被识别并抑制，避免多 Pod Deployment 或重复 apply 导致历史事件和 rollback YAML 重复刷屏。
- 去重窗口可在站点设置中调整，设置为 `0s` 可关闭。

### 3. 数据持久化生命周期
- RuntimeConfig 新增 `persistence` 配置，并在 Web “站点设置”中提供可视化编辑。
- 默认热库保留 `24h`，超过后迁移到 `<当前库>_cold` 冷库。
- 默认冷库追加保留 `24h`，即默认总保留约 `48h`，之后从冷库删除。
- 归档范围为运行期数据：`admission_events`、`rollback_backups`、`telegram_notification_events`、`config_change_requests`、`config_audit_events`、`admission_approval_grants`。
- 业务配置、用户、角色、规则、Telegram 配置不会被生命周期清理。

### 4. 删除审批超时同步
- 默认核心删除审批规则的 `approval.ttl_seconds` 改为 `0`，表示继承站点设置中的 `delete_approval_timeout`。
- 默认 `delete_approval_timeout` 与 Telegram 交互窗口一致为 `12h`。

### 5. YAML 下载修补
- Web 下载事件 YAML 时，如果当前对象为空，会自动回退到 `old_object`，适配 DELETE 事件。

## 验证
- 已执行 `gofmt -w *.go`。
- 已用 Go parser 解析所有 Go 文件，语法通过。
- 当前容器无法解析 `proxy.golang.org`，`go test ./...` 停在依赖下载 / go.sum 阶段；在有网络环境执行 `go mod tidy && go test ./...` 可完成完整构建验证。

## v3 patch

- 历史 v3 曾尝试由服务账号代执行 DELETE；v5 已废弃该方案，改为一次性授权原用户重试。
- 非 DELETE 类审批仍保留一次性重试授权，不虚假承诺可安全复放 UPDATE/CREATE。
- Telegram 状态更新中的查询关键字改成编号列表：event / rollback / fingerprint。
- Telegram 模板里的用户展示改为短用户名，例如 system:serviceaccount:kube-system:admin-milo 展示为 admin-milo。
- 状态更新里的 Web 链接改成 Markdown 超链：事件详细地址。
- 新增服务启动、关闭、Mongo 异常、Mongo 恢复通知。Mongo 不可用时通知会先落本地 PVC 队列，恢复后写入 Telegram 队列补发。
- 新增本地 system-notifications 队列目录；SERVICE_ACCOUNT_NAME 仅用于识别并阻断审计服务账号代删行为。

## v4 事件与 Telegram 交互优化

- 新增短事件 ID：`ev_<time>_<random>`，用于 Web 查询和 Telegram 快速定位。
- 历史事件默认只展示 `final=true` 的真实集群执行事件；审批等待、拒绝、拦截等未实际执行的事件只作为内部审批状态保存，直接按事件 ID 查询时仍可定位。
- Telegram 审批放行后不再代执行；原用户重试时 Admission 事件复用原事件 ID，并覆盖为最终事件，避免一笔删除出现多条历史记录。
- 修复审批放行后原 pending 事件覆盖最终回滚信息的问题：执行完成后优先重新读取最终事件，不再用旧 pending 状态覆盖 rollback_id。
- Telegram 通知内容压缩为编号列表，查询关键字只保留事件 ID，事件详情以 Markdown 超链展示。
- 回滚通知按钮改为底部一行并排：`回滚` 与 `下载 YAML`。
- Web 历史事件表格缩减为：时间、事件 ID、资源类型、用户、操作、回滚/YAML；用户只展示 ServiceAccount 名称。
- 历史事件筛选栏新增事件 ID 输入框，并固定悬浮在事件页顶部，便于滚动查看时调整查询参数。
- 时区下拉补齐 AWS 常用核心全球时区并去重。
- 顶部状态和刷新按钮改为紧凑图标化展示；用户菜单尺寸自适应小屏。
- 启动成功通知改为优先直接发送，Mongo 不可用时仍可基于本地配置尝试发送，失败则进入本地队列等待恢复后补发。

## v5 本轮升级说明

### Admission 审批执行边界
- 删除审批通过后不再由审计服务自身的 Kubernetes ServiceAccount 代执行 DELETE。
- Telegram/Web 审批只创建短期一次性授权，原始用户或原始自动化 ServiceAccount 必须在有效期内重新执行同一条删除命令。
- Webhook 在原用户重试时消费授权、复用原事件 ID，并只记录这一次真实执行的集群操作。
- 审计服务 ServiceAccount 发起 DELETE 会被默认硬阻断，避免审计程序获得或滥用集群删除能力。

### Telegram 通知文案
- 所有内置 Admission 通知模板移除 “1、2、3” 编号展示。
- 创建通知改为展示事件 ID，不再展示 Kubernetes Admission request UID。
- 状态变更文案改为“字段: 值”的简洁格式。
- 删除审批通过后的文案明确提示“请原用户在有效期内重新执行删除命令”。

### 默认规则边界
- 控制器 ServiceAccount 发起的 Pod CREATE/DELETE 默认只审计，不通知。
- 重要资源 CREATE/UPDATE/DELETE 默认只对 human_admins 组触发通知/审批；其他自动化和控制器账号默认按兜底规则只审计。
- human_admins 默认包含人工用户和 admin-* / *-admin 形式的运维 ServiceAccount。
- 重要资源范围扩展到 ServiceAccount、Role、RoleBinding、ClusterRole、ClusterRoleBinding、PVC、Namespace 等常见关键资源。

### Web UI
- 历史事件默认查询 Limit 调整为 500。
- 历史事件筛选和结果字体、间距下调，提升小窗口可视范围。
- 时区选择补充 UTC±N / Etc/GMT 选项，并保留全球核心 IANA 时区。

### Mongo 冷库权限
冷库不会预先创建空数据库；MongoDB 会在第一次成功写入冷库集合时自动创建。若日志出现 `not authorized on <db>_cold`，请给当前 `MONGO_URI` 使用的用户同时授予热库和冷库 readWrite 权限。

示例 Mongo Shell 命令：

```javascript
use admin
db.grantRolesToUser("<mongo-user>", [
  { role: "readWrite", db: "k8s_delete_interceptor" },
  { role: "readWrite", db: "k8s_delete_interceptor_cold" }
])
```

如果你自定义了 `MONGO_DATABASE` 或站点设置里的 `cold_database`，把上面的库名替换为实际热库和冷库名称。

## v6 patch

- 修复 ActorGroup 匹配漏洞：空 users/groups/service_accounts 不再被视为 `*`，避免只配置了 ServiceAccount 的组误匹配所有用户。
- 通知、审批和回滚类规则必须显式选择 ActorGroup；未命中组的用户或 SA 只做静默审计，不发送 Telegram，不生成回滚入口。
- 规则编辑里选择“全部资源”时自动覆盖其他资源选择；后端 YAML/API 同步规范化为 `api_groups/resources/kinds: ["*"]`。
- Admission Telegram 模板增加事件时间 `{{.time_display}}`，显示为站点默认时区下的真实接收/执行时间。
- Telegram 用户资源新增 username 与审批角色配置；审批无权限时会在按钮反馈里直接提示当前 Telegram numeric ID 和应配置的角色。
- 保持删除审批的安全边界：审批只生成原用户/原 SA 的一次性重试授权，审计程序不会代替原用户执行集群删除。
