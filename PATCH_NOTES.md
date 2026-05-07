# v7 patch notes

## 重点修复

1. Telegram Admission 通知发送前会按当前运行配置重新校验策略。
   - 如果事件对应规则已经删除、ActorGroup 不再匹配、资源范围不再匹配，队列中的旧通知会被标记为已消费并跳过发送。
   - 这解决了历史队列里旧的通配 UPDATE 规则持续发送 Lease/controller 更新通知的问题。

2. Telegram Admission 通知发送前会按当前模板重新渲染。
   - 旧队列中已经存好的旧文本不再直接发送。
   - 当前模板里的 `时间: {{.time_display}}` 会生效。

3. Policy scope 匹配改为“规则引用范围”模型。
   - 未被任何启用规则引用的 `web_scope_*` 孤儿范围不再参与策略范围判断。
   - 单条规则只检查自己引用的 ResourceScope，避免旧通配 scope 影响新规则。

## 设计结论

这次问题主要不是当前导出的 v56 规则本身错误，而是两个设计漏洞叠加：

- Telegram 队列保存的是渲染后的文本，发送阶段没有基于最新配置重新校验和重新渲染；旧规则生成的通知会在规则删除后继续发送。
- ResourceScope 是全局池，旧版本遗留的孤儿 `web_scope_important_update_notify=*` 会让策略范围判断被污染。v7 已改为只使用启用规则引用的 scope。

## 建议清理

升级后服务会自动跳过不再匹配当前策略的旧 Telegram 通知。如需立即清空旧通知，可在 Mongo 中把旧 pending/sending 的 admission_event 通知置为 sent 或删除。优先建议先升级验证，避免误删仍有效的审批交互。

## 验证

- `gofmt -w *.go`
- Go parser 语法检查通过
- 当前环境缺少 go.sum 且无法联网下载依赖，`go test ./...` 停在依赖校验阶段；有网络环境执行 `go mod tidy && go test ./...`。

## v8 - 自动清理历史无用 Telegram 消息队列

- 新增 Telegram 队列自动清理后台任务，服务启动约 5 秒后执行，之后最多每 1 分钟检查一次。
- Telegram 发送调度器在抢占发送前也会先做一次轻量清理，避免旧策略、旧模板、旧规则产生的历史 pending 队列继续发送。
- 对 `admission_event` 类型 pending/sending 队列做当前策略复核：
  - 当前规则已删除或不再通知：直接删除队列记录。
  - 当前 ActorGroup 不再匹配：直接删除队列记录。
  - 当前 ResourceScope 不再匹配：直接删除队列记录。
  - 队列里的 rule_id 与当前匹配规则不同：直接删除队列记录。
  - Telegram Bot/Chat 已不再属于当前规则目标：直接删除队列记录。
  - 事件本体缺失且队列已超过 5 分钟：直接删除队列记录。
- 新增持久化设置 `telegram_queue_cleanup_ttl`，默认 `24h`。超过该时间仍处于 `pending/sending/failed` 的通知队列会被删除，避免长期堆积。
- Web「站点设置」增加“无用通知队列 TTL”配置项。
- 过期清理只删除未完成队列，不删除已发送交互消息；已发送记录仍按热库/冷库生命周期归档和彻底清理。

## v9 - Build Fix

- Fixed `telegram.go` compile error caused by assigning the two-value result of `DeleteTelegramNotifications` to a single blank identifier.
- Changed stale admission notification cleanup call to discard both return values: `_, _ = ...`.
