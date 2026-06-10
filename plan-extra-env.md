# Plan: Add `extraEnv` to the Backend CR

## Context

The `Backend` operator CR currently injects a fixed set of env vars into the
backend container (DB credentials, PORT, SQS_QUEUE_URL). It has no way to pass
AWS authentication credentials or any other arbitrary env var.

The Helm chart solves this with an `awsCredentials.enabled` toggle that injects
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from a named Secret. The operator
has no equivalent, so AWS auth (needed for SQS polling) is simply missing from
operator-managed deployments.

Rather than adding a dedicated `awsCredentials` field (and another field the
next time), we add a single generic `extraEnv: []corev1.EnvVar` field. Since
`corev1.EnvVar` natively supports literal values, `secretKeyRef`, and
`configMapKeyRef`, it covers static credentials, IRSA role ARNs, feature flags,
and anything else without further API changes.

---

## Changes

### 1. `api/v1alpha1/backend_types.go`

Add `ExtraEnv` to `BackendSpec` and import `corev1`:

```go
import corev1 "k8s.io/api/core/v1"

// extraEnv is a list of additional environment variables appended to the
// backend container after the built-in vars.
// +optional
ExtraEnv []corev1.EnvVar `json:"extraEnv,omitempty"`
```

### 2. `api/v1alpha1/zz_generated.deepcopy.go`

Do **not** edit by hand. Regenerate with:

```bash
make generate
```

`controller-gen` will update `DeepCopyInto` for `BackendSpec` to correctly
deep-copy `[]corev1.EnvVar` (which contains pointer fields inside `EnvVarSource`).

### 3. `internal/controller/backend_controller.go`

In `buildDeployment` (around line 506), append extra env after the built-in
vars are assembled, just before the `return`:

```go
envVars = append(envVars, backend.Spec.ExtraEnv...)
```

No other logic is needed. The deployment update path already diffs `Env`
via `equality.Semantic.DeepEqual(existingContainer.Env, desiredContainer.Env)`,
so any change to `extraEnv` in the CR automatically triggers a rolling update.

### 4. `helm-charts/backend-operator/templates/backend-crd.yaml`

Do **not** edit by hand. Regenerate and sync with:

```bash
make sync-helm-crd
```

`make sync-helm-crd` runs `make manifests` (controller-gen produces the full
OpenAPI v3 schema) then copies the result to
`../../helm-charts/backend-operator/templates/backend-crd.yaml`.
The Makefile already has `HELM_CRD` pointing to that path.

---

## Example CR usage

**Static credentials (current prod approach):**
```yaml
spec:
  extraEnv:
    - name: AWS_REGION
      value: eu-west-1
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: taskapp-backend-aws-credentials
          key: AWS_ACCESS_KEY_ID
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: taskapp-backend-aws-credentials
          key: AWS_SECRET_ACCESS_KEY
```

**IRSA (future EKS approach, no static secrets):**
```yaml
spec:
  extraEnv:
    - name: AWS_REGION
      value: eu-west-1
    - name: AWS_ROLE_ARN
      value: arn:aws:iam::425832464758:role/taskapp-prod
    - name: AWS_WEB_IDENTITY_TOKEN_FILE
      value: /var/run/secrets/eks.amazonaws.com/serviceaccount/token
```

---

## Execution order

1. Edit `api/v1alpha1/backend_types.go`
2. `make generate` → updates `zz_generated.deepcopy.go`
3. Edit `internal/controller/backend_controller.go`
4. `make sync-helm-crd` → regenerates CRD + copies to helm chart
5. Build and push new operator image
6. Bump operator image tag in `helm-charts/backend-operator/values.yaml`
7. ArgoCD detects the helm chart change and redeploys the operator

---

## Backward compatibility

`extraEnv` is `omitempty` — existing `Backend` CRs without the field are
completely unaffected. No migration needed.

---

## Verification

1. Apply an updated `Backend` CR with `extraEnv` set to a `secretKeyRef`
2. `kubectl get deployment <name>-backend -o jsonpath='{.spec.template.spec.containers[0].env}'`
   → confirm the extra vars appear at the end of the env list
3. Edit the CR to change a value → confirm the deployment rolls over
4. Apply a CR without `extraEnv` → confirm existing behavior unchanged
