# walshadow Helm chart

Runs `walshadow-stream` for one source PostgreSQL cluster. The chart installs:

- a StatefulSet with exactly one replica. Its one container runs the daemon,
  and the daemon runs the shadow PostgreSQL as a child process
- one volume claim, `data-<release>-0`, mounted at `/var/lib/walshadow`
- a ConfigMap holding the TOML config and tenant fragments
- a ServiceAccount for IRSA or EKS Pod Identity
- a headless Service for the metrics port, and optionally a PodMonitor

One release is one daemon, one replication slot and one shadow. Each database
on the source cluster that you replicate is a [tenant](../../docs/tenants.md)
of that release, not a release of its own.

## Install

The chart is published as an OCI artifact:

```bash
helm install walshadow oci://<registry>/charts/walshadow --version <version> \
  --namespace walshadow -f values.yaml
```

Set at least `image.repository`, `image.tag` and `config`. The image's
PostgreSQL major must equal the source's. A minimal `values.yaml`:

```yaml
image:
  repository: <account>.dkr.ecr.us-east-2.amazonaws.com/walshadow
  tag: sha-<commit>-pg17

config: |
  [source]
  host = "source.internal"
  user = "walshadow_repl"
  dbname = "postgres"
  sslmode = "verify-full"
  slot = "walshadow_cluster"

  [tenants]

tenants:
  acme: |
    [tenant.acme]
    dbname = "acme"
    ...

env:
  - name: WALSHADOW_PG_URL
    valueFrom:
      secretKeyRef: {name: walshadow-source, key: url}

secrets:
  sources:
    - secret:
        name: walshadow-acme-snowflake
        items: [{key: private-key, path: acme.p8}]

serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::<account>:role/<role>

persistence:
  storageClassName: <gp3 class with reclaimPolicy Retain>
```

[ci/snowflake-tenants-values.yaml](ci/snowflake-tenants-values.yaml) is a
complete example with one Snowflake tenant.

## Config

`config` is the root config, written to `/etc/walshadow/walshadow.toml`.
Each entry in `tenants` is written to
`/etc/walshadow/walshadow.d/60-tenant-<id>.toml`, the file `ctl tenant`
manages. `extraFragments` adds other files to `walshadow.d/`. The formats are
the ones in [docs/configuration.md](../../docs/configuration.md) and
[docs/tenants.md](../../docs/tenants.md).

The values are TOML text, not YAML maps, because Helm turns every YAML number
into a float and walshadow rejects `port = 5432.0`.

An init container copies the config into a writable `/etc/walshadow` at every
pod start, so `ctl apply` and `ctl tenant` keep working. Their changes last
until the pod is recreated, so make lasting changes in the values. Any config
change rolls the pod, which drains first.

Keep credentials out of the config:

- Snowflake keys and tokens: mount them with `secrets.sources`, which projects
  them into `/run/secrets/walshadow/`, and point `private_key_file` or
  `token_file` there. Projected volumes pick up rotated secrets without a
  restart.
- The source password: set `WALSHADOW_PG_URL` through `env`. The URL fills in
  what `[source]` leaves unset.
- S3: the ServiceAccount's IAM role. No AWS keys belong in the config.

## Storage

Everything durable is on one volume: the shadow data directory, filtered WAL,
the spill directory and manifests, and the Snowflake state. These are only
consistent as a set, so they share a volume and get snapshotted together. Set
each Snowflake `state.directory` under `/var/lib/walshadow/snowflake/`.

Losing the volume forces a full `BASE_BACKUP` of the source and a full reload
into every destination. In `ack_after = "outbox"` mode it also loses rows
already acknowledged to the source. So:

- use a storage class with `reclaimPolicy: Retain`
- the claim is kept when the StatefulSet is deleted or scaled down
- the default access mode, `ReadWriteOncePod`, stops a second pod mounting it

The volume is pinned to one availability zone, so the pod is too.

The `persistence` values are fixed at install. Kubernetes won't change a
StatefulSet's volume template, so changing them makes `helm upgrade` fail. To
grow the volume, leave `persistence.size` alone and resize the claim, which
works online when the storage class allows expansion:

```bash
kubectl -n walshadow patch pvc data-walshadow-0 \
  -p '{"spec":{"resources":{"requests":{"storage":"500Gi"}}}}'
```

## Operations

```bash
kubectl -n walshadow logs -f walshadow-0
kubectl -n walshadow exec walshadow-0 -- walshadow-stream ctl status
kubectl -n walshadow exec walshadow-0 -- walshadow-stream ctl tenant list
```

On SIGTERM walshadow drains in-flight batches and stops the shadow, which can
take minutes, so `terminationGracePeriodSeconds` defaults to 900. A second
signal exits without draining. On a node that Karpenter manages, set the pod
annotation `karpenter.sh/do-not-disrupt: "true"` so consolidation doesn't
restart it.

## Not done yet

- Health endpoints. The probes are TCP checks on the metrics port until
  `/healthz` exists.
- A NetworkPolicy, a PodDisruptionBudget and `values.schema.json`.
- Live config changes without a restart. Today any change in `config`,
  `tenants` or `extraFragments` rolls the pod.
- Sizing guidance. `resources` and `persistence.size` are placeholders until
  the capacity work measures real numbers.

## Publishing

`.github/workflows/chart.yml` lints and renders the chart on pull requests. On
`main` it pushes the chart to ECR, at
`oci://<CHART_REGISTRY>/charts/walshadow`, using the IAM role in the
`CHART_PUBLISH_ROLE_ARN` repository variable. It publishes each `version` in
`Chart.yaml` once, so bump the version with every chart change.
