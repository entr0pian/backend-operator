# backend-operator

A Kubernetes operator for the taskapp backend service, built with [Kubebuilder v4](https://book.kubebuilder.io/). It introduces a `Backend` Custom Resource that declaratively manages the backend `Deployment` and `Service` — replacing hand-crafted Helm templates with a reconciliation loop that continuously ensures the cluster matches the desired state.

## How it works

The operator watches `Backend` resources in the `apps.taskapp.io/v1alpha1` API group. On every reconcile it:

1. Creates or updates a `Deployment` with the specified image, tag, and replica count
2. Creates or updates a `ClusterIP` `Service` (port 80 → 8080)
3. Injects the database password from a named `Secret` as `DB_PASSWORD`
4. Updates the `Backend` status with `readyReplicas` and an `Available` condition

Both the Deployment and Service are owned by the Backend CR, so deleting the CR cascades to all child resources.

## Backend CRD

```yaml
apiVersion: apps.taskapp.io/v1alpha1
kind: Backend
metadata:
  name: taskapp-backend
spec:
  image: boicotaz/taskapp-backend   # container image repository
  tag: abc1234                       # image tag (CI writes this)
  replicas: 2                        # optional, default 1
  dbSecret: taskapp-database-secret  # Secret with POSTGRES_PASSWORD key
```

**Status fields:**

| Field | Description |
|---|---|
| `readyReplicas` | Number of pods currently ready |
| `conditions[Available]` | `True` when readyReplicas >= desired |

## Repository structure

```
taskapp-backend-operator/
├── api/v1alpha1/
│   ├── backend_types.go          # CRD schema (Backend spec/status)
│   └── groupversion_info.go      # API group registration
├── internal/controller/
│   └── backend_controller.go     # Reconciliation logic
├── cmd/main.go                   # Manager entry point
├── config/
│   ├── crd/bases/                # Generated CRD YAML (make manifests)
│   ├── rbac/                     # Generated RBAC (make manifests)
│   ├── manager/                  # Manager Deployment template
│   ├── default/                  # Kustomize base
│   └── samples/                  # Example Backend CR
├── test/
│   ├── e2e/                      # End-to-end tests (Kind cluster)
│   └── utils/
├── Dockerfile                    # Multi-stage: golang builder + distroless runtime
└── Makefile                      # Build, test, deploy automation
```

## Prerequisites

- Go 1.25+
- Docker
- kubectl
- A running Kubernetes cluster (local: kind or minikube)

## Development

```bash
# Generate CRDs and RBAC from markers in api/ and internal/controller/
make manifests

# Generate DeepCopy methods
make generate

# Run unit tests (uses envtest — no cluster required)
make test

# Run linter
make lint

# Run manager locally against your current kubeconfig context
make install   # installs CRDs first
make run
```

## Build and deploy

```bash
# Build and push the operator image
make docker-build docker-push IMG=boicotaz/taskapp-backend-operator:<tag>

# Install CRDs into the cluster
make install

# Deploy the manager
make deploy IMG=boicotaz/taskapp-backend-operator:<tag>

# Apply a sample Backend CR
kubectl apply -k config/samples/
```

## Uninstall

```bash
kubectl delete -k config/samples/   # delete Backend CRs
make undeploy                        # remove manager
make uninstall                       # remove CRDs
```

## Distribution via Helm

The operator is packaged as a Helm chart in the companion repo [`taskapp-helmcharts`](https://github.com/boicotaz/taskapp-helmcharts) under `taskapp-operator/`. It is deployed by ArgoCD as part of the standard sync wave order — see [`taskapp-argocd`](https://github.com/boicotaz/taskapp-argocd).

## RBAC

The operator requires the following permissions (auto-generated from markers):

| Resource | Verbs |
|---|---|
| `apps.taskapp.io/backends` | get, list, watch, create, update, patch, delete |
| `apps.taskapp.io/backends/status` | get, update, patch |
| `apps/deployments` | get, list, watch, create, update, patch, delete |
| `core/services` | get, list, watch, create, update, patch, delete |

## License

Copyright 2026. Licensed under the [Apache License 2.0](LICENSE).
