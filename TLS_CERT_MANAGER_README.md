# TLS automation with cert-manager

This project should not manually maintain webhook TLS certificates. The recommended production setup is:

1. cert-manager issues the webhook serving certificate into a Kubernetes Secret.
2. cert-manager cainjector injects the issuing CA into `ValidatingWebhookConfiguration.webhooks[].clientConfig.caBundle`.
3. The pod mounts the TLS Secret at `/etc/certs`.
4. The webhook process dynamically reloads `/etc/certs/tls.crt` and `/etc/certs/tls.key` when cert-manager rotates the Secret.

## Why this fixes `tls: bad certificate`

The Kubernetes API server verifies the webhook server certificate with the `caBundle` embedded in the `ValidatingWebhookConfiguration`.
If the mounted serving certificate and the `caBundle` do not come from the same CA, API server calls fail with errors similar to:

```text
http: TLS handshake error ... remote error: tls: bad certificate
```

The manifest in `deploy/cert-manager/20-cert-manager.yaml` creates:

- a self-signed bootstrap Issuer
- a CA Certificate stored in `delete-interceptor-ca`
- a CA Issuer backed by that CA Secret
- a serving Certificate stored in `delete-interceptor-serving-cert`

The webhook configuration in `deploy/cert-manager/50-validatingwebhookconfiguration.yaml` contains:

```yaml
metadata:
  annotations:
    cert-manager.io/inject-ca-from: webhook-system/delete-interceptor-serving-cert
```

cert-manager cainjector uses this annotation to keep `caBundle` synchronized.

## Dynamic certificate reload

Starting from this version, the Go HTTPS server no longer loads `tls.crt` and `tls.key` only once at startup. It uses `GetCertificate` and reloads the files whenever Kubernetes updates the mounted Secret.

This avoids an important rotation bug:

- cert-manager rotates the Secret
- cainjector updates `caBundle`
- Kubernetes updates the mounted Secret files
- the running pod must serve the new certificate, otherwise API server may verify with the new CA while the pod still serves the old certificate

The webhook logs the loaded certificate subject, expiration time, and DNS SANs each time it reloads the certificate.

## Required DNS names

The serving certificate must include the Service DNS names used by the `ValidatingWebhookConfiguration`:

```yaml
dnsNames:
  - delete-interceptor-svc
  - delete-interceptor-svc.webhook-system
  - delete-interceptor-svc.webhook-system.svc
  - delete-interceptor-svc.webhook-system.svc.cluster.local
```

If you rename the Service or namespace, update these DNS names and the webhook `clientConfig.service` together.

## Verification

Check certificate readiness:

```bash
kubectl -n webhook-system get certificate
kubectl -n webhook-system describe certificate delete-interceptor-serving-cert
```

Check CA injection:

```bash
kubectl get validatingwebhookconfiguration delete-interceptor.k8s.io \
  -o jsonpath='{.webhooks[0].clientConfig.caBundle}' | wc -c
```

Check serving certificate SANs:

```bash
kubectl -n webhook-system get secret delete-interceptor-serving-cert \
  -o jsonpath='{.data.tls\.crt}' | base64 -d | \
  openssl x509 -noout -subject -issuer -dates -ext subjectAltName
```

Check that the mounted Secret and the webhook Service match:

```bash
kubectl -n webhook-system get svc delete-interceptor-svc -o yaml
kubectl get validatingwebhookconfiguration delete-interceptor.k8s.io -o yaml
```

## Do not manually patch caBundle

Manual `caBundle` patching is fragile and will break on certificate rotation. With cert-manager installed, use cainjector annotations instead.
