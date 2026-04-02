# cert-manager deployment

This directory contains a cert-manager based deployment for `k8s-delete-interceptor`.

## Files

- `00-namespace.yaml`: installs the `webhook-system` namespace
- `10-serviceaccount.yaml`: dedicated service account
- `20-cert-manager.yaml`: cert-manager issuers and serving certificate
- `30-service.yaml`: internal HTTPS service
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
- `audit`
- `user_policies`
- `protected`

If you do not want Telegram, leave `bot_token` empty and keep `chat_ids` empty.
The sample Deployment mounts `audit.directory` with `emptyDir` so the webhook can start with a writable path.
For real persistence, replace that volume with a persistent writable volume.
With more than one replica, use shared RWX storage or per-pod storage such as a StatefulSet. A single RWO PVC is not a safe default for the current Deployment shape.

## 3. Apply the namespace

```bash
kubectl apply -f deploy/cert-manager/00-namespace.yaml
```

## 4. Create or update the runtime config secret

```bash
kubectl -n webhook-system create secret generic delete-interceptor-config \
  --from-file=protected.yaml=deploy/cert-manager/config/protected.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
```

## 5. Apply service account, cert-manager resources, and service

```bash
kubectl apply -f deploy/cert-manager/10-serviceaccount.yaml
kubectl apply -f deploy/cert-manager/20-cert-manager.yaml
kubectl apply -f deploy/cert-manager/30-service.yaml
```

## 6. Wait for the serving certificate

```bash
kubectl -n webhook-system wait --for=condition=Ready certificate/delete-interceptor-serving-cert --timeout=180s
```

## 7. Deploy the webhook server

```bash
kubectl apply -f deploy/cert-manager/40-deployment.yaml
kubectl -n webhook-system rollout status deployment/delete-interceptor --timeout=180s
```

## 8. Register the validating webhook

```bash
kubectl apply -f deploy/cert-manager/50-validatingwebhookconfiguration.yaml
```

This webhook registration includes `CREATE`, `UPDATE`, and `DELETE` so the service can audit create and update requests in addition to delete requests.

## 9. Verify the webhook

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

## 10. Quick test

Create a protected object name in `config/protected.yaml`, then try to delete it.

Expected behavior:

- normal users still follow the `protected` rules
- `allow` users are allowed without notification
- `observe` users are allowed and notified
- `deny` users are blocked and notified

## Uninstall

Delete the webhook registration first so the cluster is not held by a failing admission webhook:

```bash
kubectl delete validatingwebhookconfiguration delete-interceptor.k8s.io
kubectl delete -f deploy/cert-manager/40-deployment.yaml
kubectl delete -f deploy/cert-manager/30-service.yaml
kubectl delete -f deploy/cert-manager/20-cert-manager.yaml
kubectl delete -f deploy/cert-manager/10-serviceaccount.yaml
kubectl -n webhook-system delete secret delete-interceptor-config
kubectl delete -f deploy/cert-manager/00-namespace.yaml
```

If you need a different namespace, service name, or image layout, update:

- `40-deployment.yaml`
- `30-service.yaml`
- `20-cert-manager.yaml`
- `50-validatingwebhookconfiguration.yaml`
