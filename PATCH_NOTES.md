# k8s-delete-interceptor v2 latest patch notes

本补丁以 `k8s-delete-interceptor-v2-telegram-route-history-fix` 为主线，参考 `upgraded-rollback-pvc-v6` 的回滚/PVC设计，完成以下实改：

## Build 修复

- 修复 `config.go: undefined: defaultNotificationTemplates`：补齐兼容包装函数，统一回到 v2 的 `defaultTemplates()`。
- 修复 `telegram.go: cannot use kb as map[string]any`：发送路径将 `reply_markup` 按 `any` 处理，同时保留入库结构为 `map[string]any`，避免配置变更键盘覆盖时报静态类型错误。

## 功能强化

- Admission `require_approval` 不再只是永久拒绝：Telegram 事件消息新增“审批放行 / 拒绝”按钮。
- 审批通过后会写入一次性授权票据，原用户在 TTL 内重试同一资源/同一规则/同一操作时才放行，放行后票据立即消费，避免重复使用。
- 授权票据同时支持 MongoDB 和共享 PVC fallback：Mongo 可用时使用 `admission_approval_grants`，本地 PVC 使用 `approvals/pending` 与 `approvals/decided`。
- 拦截/待审批的事件不再显示“执行回滚”按钮，只有真实放行且存在回滚备份的事件才显示回滚按钮，避免未删除却误导回滚。
- Telegram 回调增加基础授权：如果配置了 Telegram 用户，则只有指定审批用户或具备 `superadmin/operator/telegram_approver/config_approver` 等角色的用户可审批/回滚；未配置用户时保留旧版兼容行为。可设置 `TELEGRAM_CALLBACK_REQUIRE_CONFIGURED_USER=true` 强制必须配置用户。
- Telegram 回调在 Mongo 不可用时不再空指针崩溃，而是安全跳过并记录日志。
- DELETE 事件下载 YAML 时，如果 AdmissionReview 的 `object` 为空，会自动回退使用 `old_object`，避免删除事件无法导出 YAML。
- 回滚备份在共享 PVC 中额外生成可执行 YAML 文件：`rollback/manifests/<rollback-id>.yaml`，并在备份记录中保存 `manifest_file` 和 `manifest_sha256`。

## 验证情况

- 已对全部 Go 文件执行 `gofmt`。
- 已用 Go parser 对全部 Go 文件做语法解析检查。
- 当前容器无法访问 `proxy.golang.org`，因此 `go test ./...` 只能推进到依赖下载/`go.sum` 阶段；在有网络的构建环境执行 `go mod tidy && go test ./...` 即可继续完整验证。
