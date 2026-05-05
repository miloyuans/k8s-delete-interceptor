# Rollback PVC / File Storage Version

## 1. 解决的问题

这个版本在原有 MongoDB rollback 备份能力基础上，增加了文件/PVC 后端：

- 不配置 MongoDB 时，rollback 不再自动禁用；
- 回滚备份写入 PVC 共享目录；
- 多 Pod 场景通过文件锁避免并发写冲突；
- 每条回滚记录独立 JSON + 独立 YAML；
- Telegram 回滚按钮支持状态展示、点击次数展示；
- 新增“下载 YAML”按钮，点击后从后端读取 YAML 并通过机器人私聊发送给点击用户；
- 私聊失败时，在群聊中 @ 用户提醒先私聊机器人发送 `/start`。

## 2. 必须替换 / 新增的文件

新增：

```text
rollback_store.go
rollback_store_file.go
rollback_store_mongo.go
```

替换：

```text
rollback.go
```

修改：

```text
delete_confirmation.go
```

`delete_confirmation.go` 只需要按 `delete_confirmation_rollback_buttons.patch` 修改 `buildApprovalKeyboard` 中的 rollback 按钮生成逻辑。

## 3. PVC 目录结构

默认根目录：

```text
/var/lib/k8s-delete-interceptor
```

实际 rollback 文件目录：

```text
/var/lib/k8s-delete-interceptor/
└── rollback/
    ├── records/
    │   └── ab/
    │       └── cd/
    │           └── abcd1234ef567890abcd1234.json
    ├── manifests/
    │   └── ab/
    │       └── cd/
    │           └── abcd1234ef567890abcd1234.yaml
    ├── locks/
    │   └── ab/
    │       └── cd/
    │           └── abcd1234ef567890abcd1234.lock
    ├── events/
    │   └── 2026-05-05/
    │       └── 20260505T132011Z-pod-a-uuid.json
    ├── tmp/
    ├── .rollback-telegram-offset
    └── .rollback-telegram-poll.lock
```

## 4. 记录格式

`records/<id>.json` 保存元数据、状态、点击次数和消息引用：

```json
{
  "schema_version": 1,
  "id": "abcd1234ef567890abcd1234",
  "timestamp": "2026-05-05T13:20:11Z",
  "updated_at": "2026-05-05T13:21:33Z",
  "expires_at": "2026-05-06T13:20:11Z",
  "cluster_name": "prod",
  "request_uid": "7d3f...",
  "username": "system:serviceaccount:default:deploy-bot",
  "operation": "DELETE",
  "kind": "Deployment",
  "resource": "deployments",
  "resource_group": "apps",
  "resource_version": "v1",
  "namespace": "default",
  "name": "api-server",
  "resource_display": "Deployment 'api-server' in namespace 'default'",
  "manifest_file": "rollback/manifests/ab/cd/abcd1234ef567890abcd1234.yaml",
  "manifest_sha256": "f0e1...",
  "execution_status": "pending",
  "rollback_click_count": 0,
  "download_click_count": 0,
  "last_clicked_by": "",
  "last_action": "",
  "history": []
}
```

`manifests/<id>.yaml` 保存清洗后的可执行 Kubernetes YAML，用于 server-side apply 和私聊下载。

## 5. 一致性设计

### 5.1 每条记录独立锁

文件锁路径：

```text
rollback/locks/<id[0:2]>/<id[2:4]>/<id>.lock
```

获取锁使用 `O_CREATE|O_EXCL`，同一时刻只有一个 Pod 能成功创建锁文件。

锁文件包含：

```json
{
  "token": "pod-pid-nanotime",
  "pod": "delete-interceptor-xxx",
  "pid": 1,
  "operation": "update",
  "created_at": "2026-05-05T13:22:01Z",
  "expires_at": "2026-05-05T13:23:01Z"
}
```

释放锁时会校验 token，避免误删别的 Pod 新创建的锁。

### 5.2 原子写

所有 JSON/YAML 写入使用：

```text
write tmp -> fsync tmp -> rename tmp final -> fsync parent dir
```

这样可以避免 Pod 被 kill 时产生半截 JSON。

### 5.3 状态机

状态：

```text
pending  -> running -> applied
pending  -> running -> failed
failed   -> running -> applied
pending  -> expired
failed   -> expired
```

默认不允许：

```text
applied -> running
expired -> running
```

如要允许重复回滚：

```yaml
rollback:
  allow_reapply: true
```

## 6. 多 Pod Telegram callback

如果 `delete_confirmation.enabled=true`：

- 只由 delete confirmation 模块轮询 Telegram；
- 它会把非 delete confirmation 的 callback 转给 `rollbacker.HandleTelegramCallback`；
- rollback 模块不启动第二个 poller，避免多个 offset 竞争。

如果 `delete_confirmation.enabled=false`：

- rollback 模块自己轮询 Telegram；
- 使用共享的 `.rollback-telegram-poll.lock` 和 `.rollback-telegram-offset`，避免多 Pod 重复消费 callback。

## 7. Kubernetes / PVC 要求

多 Pod 模式必须使用支持 `ReadWriteMany` 的 PVC，例如 NFS、CephFS、EFS。

底层文件系统必须支持：

- 跨节点原子 `rename`;
- `O_EXCL` 创建语义;
- 多客户端可见的共享目录写入。

如果底层存储无法保证这些语义，建议使用 MongoDB 后端。

## 8. YAML 下载行为

点击“下载 YAML”：

1. 校验 Telegram numeric user id 是否在 `authorized_telegram_ids` 中；
2. 从 MongoDB 或 PVC 文件后端读取对应 rollback manifest；
3. `download_click_count + 1`；
4. 使用 `callback.From.ID` 作为私聊 `chat_id` 发送 YAML 文件；
5. 如果 Telegram 返回 403 或发送失败，在群里通过 `tg://user?id=<id>` @ 点击用户，提示先私聊机器人并发送 `/start`。

## 9. 回滚按钮行为

点击“执行回滚”：

1. `rollback_click_count + 1`；
2. 状态改为 `running`；
3. 编辑原群消息展示“执行中”；
4. 调 Kubernetes API 执行 server-side apply；
5. 成功则状态 `applied`，失败则状态 `failed`；
6. 再次编辑原群消息展示最终状态、点击次数、下载次数、最后点击人、错误信息。

## 10. 编译提示

新增文件后直接正常构建：

```bash
go test ./...
go build -o k8s-delete-interceptor .
```

如果你暂时不想使用 MongoDB，但保留 Mongo driver 依赖不影响编译。
