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
