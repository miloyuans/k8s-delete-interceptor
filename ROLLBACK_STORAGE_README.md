# Rollback storage upgrade notes

This version adds a storage abstraction for rollback backups and Telegram rollback actions.
It supports MongoDB and a PVC/file backend. The file backend is designed for multi-pod deployments that share a ReadWriteMany PVC.

## New behavior

Rollback buttons now contain two actions:

- `执行回滚`: applies the saved manifest back to Kubernetes.
- `下载 YAML`: sends the executable rollback YAML to the clicking Telegram user by private chat.

The Telegram group message is updated after clicks and shows:

- rollback execution status: pending / running / applied / failed / expired
- rollback click count
- YAML download click count
- last action
- last clicking Telegram user ID
- execution user and time
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
