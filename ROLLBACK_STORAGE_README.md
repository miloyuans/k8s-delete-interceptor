# Rollback storage upgrade notes

This version adds a storage abstraction for rollback backups and Telegram rollback actions.
It supports MongoDB and a PVC/file backend. The file backend is designed for multi-pod deployments that share a ReadWriteMany PVC.

## New behavior

Rollback buttons now contain two actions:

- `执行回滚`: applies the saved manifest back to Kubernetes.
- `下载 YAML`: sends the executable rollback YAML to the clicking Telegram user by private chat.

Button visibility rules:

- blocked / denied delete notifications do not show rollback buttons, because the delete did not happen.
- approved delete notifications show rollback buttons after the user retries the delete and the admission request is allowed.
- global-whitelist direct allow delete notifications show rollback buttons, because the delete is actually allowed.
- create/update audit notifications still show rollback buttons when a rollback backup is available.

The Telegram group message is updated after clicks and shows:

- rollback execution status: pending / running / applied / failed / expired
- rollback click count
- YAML download click count
- last action
- last clicking Telegram user (clickable mention)
- execution user (clickable mention) and time
- execution error, when failed

If private chat delivery fails, the bot posts a group reminder that mentions the clicking user and asks them to open a private chat with the bot and send `/start`.

## File/PVC backend directory layout

For `storage.data_directory: /var/lib/k8s-delete-interceptor`, rollback data is stored below:

```text
/var/lib/k8s-delete-interceptor/rollback/
├── records/
│   └── ab/cd/<rollback-id>.json
├── manifests/
│   └── ab/cd/<rollback-id>.yaml
├── locks/
│   └── ab/cd/<rollback-id>.lock
├── events/
│   └── YYYY-MM-DD/*.json
├── tmp/
├── .rollback-telegram-offset
└── .rollback-telegram-poll.lock
```

Each rollback record has an independent lock file. Record JSON and manifest YAML writes use temporary files and atomic rename. By default, fsync is enabled for safer PVC writes.

## Multi-pod requirements

For multi-pod mode, the PVC must support ReadWriteMany and the underlying filesystem must provide atomic create-with-O_EXCL and atomic rename semantics across nodes. NFS, EFS, CephFS and similar filesystems are typical choices. Avoid using per-node local volumes for multiple replicas because callbacks and counters will diverge.

Telegram callback consumption remains single-consumer:

- If delete confirmation is enabled, `delete_confirmation.go` polls Telegram and forwards rollback callbacks to `rollbacker.HandleTelegramCallback`.
- If delete confirmation is disabled, `rollback.go` starts its own poller and uses `.rollback-telegram-poll.lock` plus `.rollback-telegram-offset`.

## Status machine

Rollback record statuses:

```text
pending -> running -> applied
pending -> running -> failed
failed  -> running -> applied
pending -> expired
failed  -> expired
```

By default, `applied -> running` is rejected. Set `rollback.allow_reapply: true` only if you explicitly want repeat apply from the same backup.

If a pod crashes after changing a record to `running`, the record can be retried after `rollback.running_timeout_seconds` (default 300 seconds). This prevents a permanent stuck-running state while still blocking accidental double-clicks.

## Migration notes

Existing MongoDB records that contain `manifest_yaml` remain compatible.
File backend records store metadata in JSON and the executable manifest in a separate YAML file. The JSON record contains `manifest_file` and `manifest_sha256`.

## Files changed or added

- Replaced: `rollback.go`
- Added: `rollback_store.go`
- Added: `rollback_store_file.go`
- Added: `rollback_store_mongo.go`
- Updated: `delete_confirmation.go` rollback buttons
- Updated: `main.go` rollback storage logging
- Added: `config.rollback-storage.example.yaml`

## Change notification diff compression

Update audit notifications now flatten Kubernetes arrays that contain a stable `name` field instead of rendering the whole array as JSON. For example, an image-only change is rendered like:

```text
~ spec.template.spec.containers[name=api].image: old-image -> new-image
```

This also applies to nested named arrays such as `env`, `volumeMounts`, and similar Kubernetes object lists. Arrays without a stable `name` field still fall back to compact JSON output. The inline notification limit was reduced so very large diffs are sent as an attachment sooner.

## Telegram user display in rollback status

Rollback status messages now store the Telegram actor metadata from callback payloads:

- Telegram user ID
- username
- display name

The status message renders the last clicking user and execution user as a clickable Telegram mention, preferring `@username` when available and falling back to the user's display name or ID.

## ConfigMap / Secret change detail policy

By default, update notifications for configuration and secret resources do not include detailed field-by-field diffs in Telegram. The full diff is still written to audit storage in the `change_details` field of each audit record, so it is available in both MongoDB audit records and local JSONL audit files.

Default audit-only resources:

```yaml
audit:
  change_detail_audit_only_resources:
    - configmap
    - configmaps
    - cm
    - secret
    - secrets
```

The list supports the same matcher syntax as other resource rules: plain glob patterns and `regex:` patterns. Set the list explicitly if you want to add more resource types, for example `sealedsecret`, `externalsecret`, or custom configuration CRDs.

For these resources, Telegram receives a compact summary like:

```text
触发重要资源更新审计：Secret 'app-secret' in namespace 'prod'。该资源属于配置/密钥类，变更详情已记录到审计存储，不在通知中展示。变更字段数: 3。
```

The complete diff is stored in audit records under:

```json
{
  "notification_reason": "summary sent to Telegram",
  "change_details": "full field-level diff stored for audit"
}
```


## v5 删除拦截策略说明

### 回滚按钮展示策略

拦截成功的 DELETE 通知不再附带回滚或下载 YAML 按钮。原因是 Admission Webhook 已经拒绝了删除请求，资源并未被删除，此时展示回滚按钮容易造成误判。

删除审批消息也只保留“确认删除 / 拒绝”按钮，不再展示“回滚 / 下载 YAML”。回滚按钮只保留在允许执行的 CREATE / UPDATE 审计通知中，用于恢复变更前版本。

### 白名单与默认拦截

当前删除策略顺序为：

1. 自保护资源直接放行，例如 webhook 自己的 namespace 和 ValidatingWebhookConfiguration。
2. `global_whitelist.users` 命中的用户直接放行，不需要审批。
3. 命中 `protected` 规则的 DELETE 默认拦截。
4. 如果用户命中 `delete_confirmation.rules`，则发送 Telegram 交互审批；审批后用户需要重试同一个删除命令，授权只消费一次。
5. 未命中交互审批小名单的用户默认拒绝。
6. 如果开启 `delete_policy.default_block: true`，即使资源没有命中 `protected`，所有 DELETE 也会按上述小名单审批或默认拒绝。

如果要实现“全局大白名单直接放行，小名单交互确认放行，其他默认全拦截删除”，请配置：

```yaml
delete_policy:
  default_block: true

global_whitelist:
  users:
    - system:admin
    - regex:^admin-.*

delete_confirmation:
  enabled: true
  rules:
    - users:
        - system:serviceaccount:prod:deploy-bot
        - alice@example.com
      telegram_ids:
        - "123456789"
```

如果 `delete_policy.default_block` 不配置或为 `false`，则保持旧行为：未命中 `protected` 的删除请求会放行。
