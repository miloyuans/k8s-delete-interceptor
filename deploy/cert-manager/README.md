# cert-manager deployment

This directory contains a cert-manager based deployment for `k8s-delete-interceptor`.

## Files

- `00-namespace.yaml`: installs the `webhook-system` namespace
- `10-serviceaccount.yaml`: dedicated service account
- `20-cert-manager.yaml`: cert-manager issuers and serving certificate
- `30-service.yaml`: internal HTTPS service
- `35-audit-pvc.yaml`: shared RWX PVC for audit log storage
- `40-deployment.yaml`: webhook deployment
- `50-validatingwebhookconfiguration.yaml`: admission webhook registration
- `config/protected.yaml`: example runtime configuration mounted into the pod

## Prerequisites

- A working Kubernetes cluster
- `cert-manager` already installed
- A container image built from the repo and pushed to a registry your cluster can pull from

## 1. Build and push the image

Update the image in `40-deployment.yaml`, then build and push:

```bash
docker build -t REGISTRY/k8s-delete-interceptor:TAG .
docker push REGISTRY/k8s-delete-interceptor:TAG
```

## 2. Edit the runtime config

Review and update `config/protected.yaml` before installation:

- `cluster_name`
- `telegram.bot_token`
- `telegram.chat_ids`
- `notifications`
- `audit`
- `audit.telegram`
- `audit.create.notify_users`
- `audit.create.notify_resources`
- `audit.update.notify_users`
- `audit.update.notify_resources`
- `delete_confirmation`
- `global_whitelist`
- `lifecycle`
- `lifecycle.telegram`
- `protected`

For example, `global_whitelist.users: ["*milo*", "regex:^system:serviceaccount:.*:breakglass-.*$"]` will match the full Kubernetes username with the same glob or regex semantics used elsewhere in the config.

If you do not want Telegram, leave `bot_token` empty and keep `chat_ids` empty.
The sample Deployment mounts `audit.directory` from a PVC.
With `replicas: 2`, use a RWX-capable storage class.
The sample mounts the same PVC into a pod-specific subdirectory, so each replica keeps its own audit files and does not append into the same log file.
Delete confirmation state is mounted from the same PVC into `/var/lib/k8s-delete-interceptor` using the shared `subPath: shared`, so both replicas can see the same pending approval and one-time authorization pool.

## 3. Apply the namespace

```bash
kubectl apply -f deploy/cert-manager/00-namespace.yaml
```

## 4. Create or update the runtime config ConfigMap

```bash
kubectl -n webhook-system create configmap delete-interceptor-config \
  --from-file=protected.yaml=deploy/cert-manager/config/protected.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
```

## 5. Create the audit PVC

Edit `35-audit-pvc.yaml` first and replace `storageClassName` with a RWX-capable storage class.
For two replicas, do not use a plain RWO disk class.

```bash
kubectl apply -f deploy/cert-manager/35-audit-pvc.yaml
```

## 6. Apply service account, cert-manager resources, and service

```bash
kubectl apply -f deploy/cert-manager/10-serviceaccount.yaml
kubectl apply -f deploy/cert-manager/20-cert-manager.yaml
kubectl apply -f deploy/cert-manager/30-service.yaml
```

## 7. Wait for the serving certificate

```bash
kubectl -n webhook-system wait --for=condition=Ready certificate/delete-interceptor-serving-cert --timeout=180s
```

## 8. Deploy the webhook server

```bash
kubectl apply -f deploy/cert-manager/40-deployment.yaml
kubectl -n webhook-system rollout status deployment/delete-interceptor --timeout=180s
```

This sample Deployment runs with `replicas: 2`.
Each pod writes audit files into its own subdirectory on the shared PVC using `subPathExpr: $(POD_NAME)`, which avoids two pods appending to the same daily log file.

## 9. Register the validating webhook

```bash
kubectl apply -f deploy/cert-manager/50-validatingwebhookconfiguration.yaml
```

This webhook registration includes `CREATE`, `UPDATE`, and `DELETE` so the service can audit create and update requests in addition to delete requests.
The sample configuration is fail-open by default with `failurePolicy: Ignore`.
It also excludes the `webhook-system` namespace, the `webhook-system` Namespace object itself, and several high-noise or self-referential API groups before the request reaches the webhook server.

Audit notifications can either:

- reuse the global `telegram` config with `audit.telegram.use_global: true`
- use their own bot, chats, and template with `audit.telegram.use_global: false`

For `CREATE` and `UPDATE`, Telegram notifications are only sent when both of these match:

- the requesting service account matches `notify_users`
- the resource kind or alias matches `notify_resources`
- if the user first matches `global_whitelist.users`, the request is only audited and the mutation notification rules are skipped

Requests that do not match the notification filters are still written to the local audit file and optional MongoDB sink.
If `notify_resources` is omitted, the webhook falls back to a built-in important-resource list such as Deployment, StatefulSet, Pod, PVC, PV, Service, ConfigMap, Secret, Ingress, Namespace, ServiceAccount, and RBAC resources.
The `webhook-system` namespace is treated as the webhook's own management namespace and is bypassed both in webhook matching rules and in the application self-preservation logic.
Audit files are split by event type in the writable log directory, for example `audit-YYYY-MM-DD-create.jsonl`, `audit-YYYY-MM-DD-update.jsonl`, `audit-YYYY-MM-DD-delete.jsonl`, and `audit-YYYY-MM-DD-lifecycle.jsonl`.
Telegram templates can use `{{title_icon}}` and `{{action_icon}}` so startup, shutdown, intercept, create, update, and allow messages are visually distinct at a glance.
The sample templates also place the title and timestamp on the last line, for example `{{title_icon}} *{{title}}*   \`{{time}}\``.
The notification context also exposes `{{resource_type}}`, `{{resource_name}}`, `{{namespace}}`, and `{{change_details}}`.
For update audit notifications, the webhook compares `oldObject` and `object` from the AdmissionReview and only lists changed fields.
Common Kubernetes metadata noise such as `managedFields`, `resourceVersion`, and `last-applied-configuration` is omitted from the diff.
If the generated update diff is too large for a concise Telegram message, the message states that details were attached and sends the full diff as a text document.

Delete confirmation adds a two-step approval flow for protected resources:

- precedence order is explicit: `global_whitelist.users` > `delete_confirmation.rules.users` > `protected`
- the first matching delete is denied and queued for Telegram approval
- delete confirmation user matching is controlled only by `delete_confirmation.rules.users`
- `global_whitelist.users` uses the same glob and `regex:` matching model as the other user filters
- users matched by `global_whitelist.users` bypass delete confirmation, protected delete interception, and audit Telegram notifications; the request is only recorded in the audit log
- if a user does not match `global_whitelist.users`, the webhook continues into the delete-specific approval list in `delete_confirmation.rules.users`; if that also does not match, the protected delete stays blocked
- `delete_confirmation.chat_ids` can target one or more approval groups; when omitted, the global `telegram.chat_ids` are used
- matching requests are grouped for `aggregate_window_seconds` so batch deletes produce one approval message
- only configured `telegram_ids` can approve or reject the inline buttons
- when one group confirms or rejects, that group's message is updated with the result and the same pending approval messages in other groups are deleted
- after approval, the same Kubernetes user must retry the delete
- every approved resource is allowed once, consumed immediately, and expires after `consume_window_seconds`
- extra resources in a later retry trigger a new approval, while already approved resources are consumed one by one

Notification control applies to Telegram delivery across delete, audit, and lifecycle messages:

- `dedupe_window_seconds` suppresses duplicate alerts within the same time window
- suppressed duplicates are summarized into the next alert for the same signature so repeated events are not silently lost
- failed notifications are persisted locally and retried on the next service startup

Lifecycle notifications can either:

- reuse the global `telegram` config with `lifecycle.telegram.use_global: true`
- use their own bot, chats, and template with `lifecycle.telegram.use_global: false`

When enabled, the service sends:

- a startup notification after the HTTPS listener is successfully bound
- a shutdown notification on graceful termination
- an unexpected-stop notification when the process detects a previous unclean exit in the same pod data directory

The sample Deployment sets `terminationGracePeriodSeconds: 30` so the process has time to send the shutdown notification before the pod is killed.
For hard kills such as `SIGKILL`, OOM, or sudden node loss, the process cannot send a message at the moment it dies. In that case, the webhook will emit an unexpected-stop notification on the next startup if it can still see the previous lifecycle state file in the same mounted pod directory.

## Emergency recovery

If the cluster is already blocked by webhook timeouts, switch the webhook to fail-open first:

```bash
kubectl patch validatingwebhookconfiguration delete-interceptor.k8s.io \
  --type='json' \
  -p='[{"op":"replace","path":"/webhooks/0/failurePolicy","value":"Ignore"}]'
```

Then label the webhook namespace so requests in that namespace are skipped:

```bash
kubectl label namespace webhook-system delete-interceptor.k8s.io/exclude=true --overwrite
```

After that, re-apply the webhook manifest:

```bash
kubectl apply -f deploy/cert-manager/50-validatingwebhookconfiguration.yaml
```

## 10. Verify the webhook

Check that the service has endpoints:

```bash
kubectl -n webhook-system get pods
kubectl -n webhook-system get svc delete-interceptor-svc
kubectl -n webhook-system get endpoints delete-interceptor-svc
```

Check that cert-manager injected the CA bundle:

```bash
kubectl get validatingwebhookconfiguration delete-interceptor.k8s.io -o yaml
```

Look for a populated `caBundle` under `webhooks[].clientConfig`.

Check that the PVC is bound:

```bash
kubectl -n webhook-system get pvc delete-interceptor-audit-pvc
```

Check that each pod got its own audit directory:

```bash
kubectl -n webhook-system get pods -l app.kubernetes.io/name=delete-interceptor
```

## 11. Quick test

Create a protected object name in `config/protected.yaml`, then try to delete it.

Expected behavior:

- normal users still follow the `protected` rules
- `global_whitelist.users` bypass all control logic and only leave audit records
- users matched by `delete_confirmation.rules.users` enter the Telegram approval flow when they delete protected resources

## Uninstall

Delete the webhook registration first so the cluster is not held by a failing admission webhook:

```bash
kubectl delete validatingwebhookconfiguration delete-interceptor.k8s.io
kubectl delete -f deploy/cert-manager/40-deployment.yaml
kubectl delete -f deploy/cert-manager/30-service.yaml
kubectl delete -f deploy/cert-manager/20-cert-manager.yaml
kubectl delete -f deploy/cert-manager/10-serviceaccount.yaml
kubectl delete -f deploy/cert-manager/35-audit-pvc.yaml
kubectl -n webhook-system delete configmap delete-interceptor-config
kubectl delete -f deploy/cert-manager/00-namespace.yaml
```

If you need a different namespace, service name, or image layout, update:

- `40-deployment.yaml`
- `30-service.yaml`
- `20-cert-manager.yaml`
- `50-validatingwebhookconfiguration.yaml`
