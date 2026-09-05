# Cronos alpha infrastructure

This directory applies only to `yc-gradius/cronos-bot`. Bootstrap creates the namespace;
all other resources are namespaced. It does not install operators, create cluster roles,
modify StorageClasses, or change another workload.

```sh
python3 scripts/bootstrap_infra.py --wait
```

The script validates resource scopes before applying manifests, generates credentials in
memory, sends them to Kubernetes through stdin, and preserves an existing secret. Secret
values are never written to source files, logs, or command-line arguments.

## Services and credentials

| Service | Internal endpoint | Storage | CPU request/limit | RAM request/limit |
| --- | --- | --- | --- | --- |
| PostgreSQL | `postgres:5432` | 5 GiB HDD PVC | 100m / 500m | 128 / 512 MiB |
| RabbitMQ | `rabbitmq:5672` | 1 GiB HDD PVC | 100m / 500m | 128 / 384 MiB |
| Redis | `redis:6379` | ephemeral | 25m / 100m | 32 / 128 MiB |

Namespace CPU/memory limits cannot exceed **4 CPU / 6 GiB**, requests **2 CPU / 4 GiB**,
PVC requests **20 GiB**, ephemeral storage limits **8 GiB**, and there may be at most 12
pods. LoadBalancer and NodePort services are prohibited by quota. Each service has one
replica. Redis has a 64 MiB cache limit and no persistence; PostgreSQL and RabbitMQ are the
persistent data services. The namespace budget reserves room for the application,
worker artifact PVC, migration Job, and verification probes.

Secret `cronos-infra` contains these keys:

- `ADMIN_DATABASE_URL`: migration-only PostgreSQL superuser `cronos_admin`.
- `DATABASE_URL`: non-superuser `cronos_app` with `NOBYPASSRLS`; database `cronos`.
- `RABBITMQ_URL`: dedicated `cronos` user and `cronos` virtual host.
- `REDIS_URL`: password-protected Redis database zero.
- `POSTGRES_PASSWORD`, `POSTGRES_APP_PASSWORD`, `RABBITMQ_PASSWORD`, `REDIS_PASSWORD`:
  service bootstrap credentials.

Only migrations should receive `ADMIN_DATABASE_URL`. Application containers should
select individual secret keys instead of importing the whole infrastructure secret.
The initial database creates the app role, grants public-schema usage, default DML and
sequence grants for objects created by `cronos_admin`, and creates app-owned `langgraph`
schema. Domain migrations must keep tables owned by admin and enable the appropriate
RLS policies. Init scripts run only on an empty PostgreSQL data directory; changing a
Secret does not rotate existing PostgreSQL/RabbitMQ credentials.

App pods need these labels for the network policies:

```yaml
app.kubernetes.io/part-of: cronos
app.kubernetes.io/component: application
```

Use `serviceAccountName: cronos-runtime`, disable token automount, and use a restricted
non-root container security context. The runtime service account has no RoleBinding.
No Kubernetes API permissions are needed by the current applications.

## Isolation boundary

Pod Security Admission enforces the `restricted` profile. Containers have no added
capabilities, use `RuntimeDefault` seccomp and read-only root filesystems. Runtime pods
have no service account token. Separate PVCs, database credentials, broker vhost,
password-protected Redis, ResourceQuota and LimitRange provide enforceable boundaries.

NetworkPolicy manifests deny ingress/egress by default, allow DNS, allow Cronos-labelled
pods to reach its datastores, and allow application HTTPS egress excluding private,
loopback, link-local, multicast and reserved IPv4 ranges. A separate TCP/8000 rule
permits only the configured Telegram proxy IP; its credentials stay in `cronos-app`.
They need a CNI that enforces
Kubernetes NetworkPolicy; the existence of accepted policies is not evidence of enforcement.

**Live check on 2026-09-05:** the cluster has no running Calico/Cilium agents and its YC
metadata does not specify a network-policy provider. A disposable unlabelled in-namespace pod, which had no matching allow rule, connected
to Redis TCP successfully and printed `NETWORK_POLICY_UNENFORCED`. The probe was then
deleted. Default deny is therefore **not enforced** on this cluster. Cluster-wide network changes are
outside this bootstrap's authority. Network policies must not be described as enforced
until a denied-connection probe fails as intended.

Only internal ClusterIP services are created; there is no ingress, public service or
management UI. This does not by itself prevent another cluster workload from reaching
a service when NetworkPolicy is unenforced; service authentication remains mandatory.

## Operations

```sh
kubectl --context yc-gradius --namespace cronos-bot get pods,pvc
kubectl --context yc-gradius --namespace cronos-bot describe resourcequota cronos-budget
kubectl --context yc-gradius --namespace cronos-bot top pods
```

Deleting or recreating pods retains StatefulSet PVCs. Deleting the namespace or its
PVCs is destructive and is not part of the bootstrap. PostgreSQL data and worker files
need backups before a broad rollout; a single replica is not high availability.
