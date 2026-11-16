# Core services handoff: event-store, incident-management, correlation-engine

> Ports, env vars, topics and AWS resources are final. Image names and the registry are still to be agreed with Ansh.

For **Ansh** (CI/CD, K8s, GitOps) and **Neeharika** (Kafka/infra). Owner of these services: Rishit.

**Quick facts:**
- Three Node 22 images, all built from the **workspace root** as the build context.
- Every service serves `GET /health` (liveness, always 200 `{status:"ok",service}`), `GET /ready` (readiness, 200 or 503) and `GET /metrics` (Prometheus) on its HTTP port.
- Logs are pino JSON on stdout, one line per entry.
- K8s Service names below assume `event-store` and `incident-management`; the in-cluster URLs depend on them.

| Service | HTTP port (REST + health/ready/metrics) | Other ports | Replicas | HPA |
|---|---|---|---|---|
| event-store | 8081 | none | 1..N (stateless) | yes, CPU 70% |
| incident-management | 8082 | gRPC 50051 | **exactly 1** | **no** |
| correlation-engine | 8083 (health/ready/metrics only) | none | 1..N (stateless) | yes, CPU 70% |

---

## event-store

Archives `raw-events` to S3, stores `normalized-events` and correlation history in DynamoDB, serves a REST read API and is a 3PC participant for incident-management.

**Build** (from the workspace root):
```sh
docker build -f services/event-store/Dockerfile -t event-store:<tag> .
```

**Ports:** `8081` (REST API, `/health`, `/ready`, `/metrics`).

**Kafka:** consumes `raw-events` and `normalized-events` (consumer group `event-store-cg`). Produces nothing.

**REST API** (called in-cluster by correlation-engine and incident-management):
- `GET /events/:eventId`, `GET /events?entityKey=&from=&to=&limit=&cursor=`, `GET /correlations/:correlationId`
- 3PC participant: `POST /tpc/:txId/can-commit | pre-commit | do-commit | abort`. A state conflict returns **409 `{error:"bad_state", state}`**.
- A `cursor` must come from the same query: a non-empty `entityKey` (≤ 2048 UTF-8 bytes) equal to the query's, and a non-empty `sk` (≤ 1024 bytes) inside the query's `from`/`to` range. Anything else returns 400.
- An `entityKey` longer than 1024 UTF-8 bytes, or one that starts with `h:`, is stored under `h:` + its sha256 and looked up the same way, so callers just pass the original key. `h:` is reserved for these hashed keys.

**Env vars:**

| Name | Default | Required in cloud? | Notes |
|---|---|---|---|
| `PORT` | `8081` | no | |
| `KAFKA_BROKERS` | `localhost:29092` | **yes** | Comma-separated bootstrap servers. |
| `KAFKA_SSL` | `false` | **yes** (`true` for Confluent Cloud) | `"true"` enables TLS. |
| `KAFKA_USERNAME` | empty | **yes** (Confluent API key) | SASL is enabled only when set. Secret. |
| `KAFKA_PASSWORD` | empty | **yes** (Confluent API secret) | Secret. |
| `KAFKA_SASL_MECHANISM` | `plain` | no | |
| `KAFKA_GROUP_ID` | `event-store-cg` | no | |
| `RAW_TOPIC` | `raw-events` | no | |
| `NORMALIZED_TOPIC` | `normalized-events` | no | |
| `AWS_REGION` | `ap-south-1` | yes | Same as the Terraform `region`. |
| `RAW_BUCKET` | `cs-raw-events` | **yes** | Terraform output `raw_bucket_name`. |
| `TABLE_PREFIX` | `cs-` | no | Must match Terraform `name_prefix`. |
| `DYNAMODB_ENDPOINT` | unset | **leave unset** | Local only (DynamoDB Local). When set, the service creates its tables itself. |
| `S3_ENDPOINT` | unset | **leave unset** | Local only (S3 mock). When set, path-style addressing is used. |
| `TPC_TIMEOUT_MS` | `10000` | no | Participant timeout. On expiry it first asks the coordinator for its decision; only if the coordinator is unreachable does UNCERTAIN self-abort / PRECOMMITTED self-commit. |
| `COORDINATOR_URL` | `http://localhost:8082` | **yes** | `http://incident-management:8082`. Used for the 3PC decision lookup (`GET /tpc/transactions/:txId`). Empty string disables the lookup (pure timeout rule). |
| `COORDINATOR_TIMEOUT_MS` | `2000` | no | Timeout of that lookup. A timeout counts as "coordinator unreachable" (see Known limits in the design doc). |
| `AWS_REQUEST_TIMEOUT_MS` | `5000` | no | Per-request deadline for DynamoDB and S3, so a stuck call fails well inside the Kafka session timeout. |
| `AWS_MAX_ATTEMPTS` | `3` | no | AWS SDK attempts per call. |
| `TPC_FORCE_VOTE` | unset | **leave unset** | Demo only (`no` forces a "no" vote). |
| `LOG_LEVEL` | `info` | no | |

AWS credentials come from the pod's IAM role (IRSA / EKS Pod Identity). Do **not** set `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in the cloud.

**Readiness:** ready once the Kafka consumers are connected and `DescribeTable` on the events table succeeds.

**Replicas / HPA:** stateless. Suggested HPA: CPU 70%, min 1, max 3. Several replicas are safe for 3PC: participant state changes are conditional DynamoDB writes, so replicas can't overwrite each other's transaction state. Kafka consumption only parallelises up to the partition count of the consumed topics; extra replicas still help the REST API.

---

## incident-management

Creates and updates incidents in Postgres (RDS). **3PC coordinator**: every incident create/update from the correlation-engine commits across its own Postgres participant and the event-store participant, then publishes `incident-updates`. Analyst status changes (`PATCH`) are plain Postgres updates.

**Build** (from the workspace root):
```sh
docker build -f services/incident-management/Dockerfile -t incident-management:<tag> .
```
The image bakes in the AWS RDS CA bundle at `/etc/ssl/rds/global-bundle.pem`.

**Ports:** `8082` (REST API, `/health`, `/ready`, `/metrics`), `50051` (gRPC `incident.v1.IncidentService`, called by correlation-engine; plaintext h2c inside the cluster).

**Kafka:** produces `incident-updates` (key `incidentId`) through a transactional outbox: every message is written in the same Postgres transaction as the change and published in order by a relay. A 3PC change is published only once the coordinator has logged it COMMITTED, and later messages of that incident wait behind it. Delivery is at-least-once, so consumers should dedupe on `incidentId` + `version` + `type`. Consumes nothing.

**REST API:**
- `GET /incidents?status=&severity=&entityKey=&limit=`, `GET /incidents/:id`, `GET /incidents/:id/history`
- `PATCH /incidents/:id` with `{status, version, actor?, note?}`. Returns 409 `{error:"version_conflict", currentVersion}` on a stale version, and 409 `{error:"transaction_pending"}` while a 3PC transaction for that incident is in flight (retry shortly).
- `GET /tpc/transactions?limit=&correlationId=`: the coordinator log, newest first (used in the 3PC demo).
- `GET /tpc/transactions/:txId`: one coordinator decision, or 404. Event Store replicas call this before acting on a 3PC timeout.

**Env vars:**

| Name | Default | Required in cloud? | Notes |
|---|---|---|---|
| `PORT` | `8082` | no | REST + health/ready/metrics. |
| `GRPC_PORT` | `50051` | no | |
| `KAFKA_BROKERS` | `localhost:29092` | **yes** | |
| `KAFKA_SSL` | `false` | **yes** (`true` for Confluent Cloud) | |
| `KAFKA_USERNAME` | empty | **yes** | Secret. |
| `KAFKA_PASSWORD` | empty | **yes** | Secret. |
| `KAFKA_SASL_MECHANISM` | `plain` | no | |
| `INCIDENT_TOPIC` | `incident-updates` | no | |
| `DATABASE_URL` | `postgres://incidents:incidents@localhost:5432/incidents` (only when `PGHOST` is unset) | **leave unset** | Local only. In AWS use the `PG*` vars below; when `PGHOST` is set and `DATABASE_URL` is not, no default is applied. |
| `PGHOST` | unset | **yes** | Terraform output `rds_endpoint`. |
| `PGPORT` | unset | **yes** | Terraform output `rds_port` (5432). |
| `PGDATABASE` | unset | **yes** | Terraform output `rds_database_name` (`incidents`). |
| `PGUSER` | unset | **yes** | `username` key of the RDS master secret. |
| `PGPASSWORD` | unset | **yes** | `password` key of the RDS master secret. Secret. |
| `DATABASE_SSL` | `false` | **yes, `true`** | RDS Postgres 16 forces SSL. |
| `DATABASE_SSL_CA` | `/etc/ssl/rds/global-bundle.pem` | no | Baked into the image. Certificate verification stays on. |
| `EVENT_STORE_URL` | `http://localhost:8081` | **yes** | `http://event-store:8081` |
| `TPC_TIMEOUT_MS` | `3000` | no | Coordinator timeout per phase. |
| `TPC_PARTICIPANT_TIMEOUT_MS` | `10000` | no | Timeout of its local Postgres participant. |
| `TPC_RECOVERY_INTERVAL_MS` | `15000` | no | How often 3PC recovery re-runs after startup. |
| `PROTO_PATH` | `proto/incident/v1/incident.proto` in the image | no | Override for the gRPC contract location. |
| `CORRELATION_WINDOW_MS` | `900000` | no | Must match correlation-engine. |
| `TPC_FAULT` | unset | **leave unset** | Demo only (`crash-after-precommit`). Never set through GitOps. |
| `LOG_LEVEL` | `info` | no | |

**Startup:** opens the HTTP port first (so `/health` answers and `/ready` returns **503**, not "connection refused"), then runs DB migrations, connects the Kafka producer, runs 3PC `recover()` (finishes or aborts in-flight transactions) and starts gRPC. `/ready` turns 200 only after all of that. Recovery then re-runs every 15 s, and a relay publishes the transactional outbox to `incident-updates` every second.

**Held `incident-updates` messages (`outbox_held_rows`):** a 3PC message waits in the outbox until the coordinator logs its transaction COMMITTED. The gauge `outbox_held_rows` counts messages held for more than 5 minutes; it should be 0.
- A held message whose transaction ended **ABORTED** (only possible after a timeout split) is marked `discarded` automatically and logged at error level; later messages of that incident then go out.
- A message held because its transaction is still **PRECOMMITTED** is never expired: recovery (every 15 s) keeps retrying the participant that hasn't acknowledged, usually the event-store. Fix that participant and the message goes out by itself.
- To inspect by hand: `SELECT o.id, o.incident_id, o.tx_id, c.state, o.created_at FROM outbox o LEFT JOIN tpc_coordinator c USING (tx_id) WHERE o.published_at IS NULL AND o.disposition IS NULL ORDER BY o.id;` and `GET /tpc/transactions/<txId>`. Only if a transaction can never finish (e.g. its log entry was lost) drop the message with `UPDATE outbox SET disposition = 'discarded' WHERE id = <id>;`.

**Orphaned 3PC records (`tpc_participant_orphaned`):** a PRECOMMITTED Postgres participant record whose transaction has no coordinator log entry. The coordinator logs every transaction before asking for votes, so this gauge should stay 0; each orphan is also logged at error level. It is never committed or aborted automatically. Check `GET /tpc/transactions/<txId>` and the event-store's correlation history, then decide by hand.

**Replicas:** **exactly 1. No HPA.** It is the 3PC coordinator and runs recovery at startup; two coordinators would race on the same transactions. Use `strategy: Recreate` so a rollout never overlaps two pods. A restart is safe (recovery handles it).

---

## correlation-engine

Consumes `security-alerts`, groups alerts per `entityKey` inside a correlation window (with severity escalation), enriches each group from the event-store over REST and upserts the incident through incident-management's gRPC API.

**Build** (from the workspace root):
```sh
docker build -f services/correlation-engine/Dockerfile -t correlation-engine:<tag> .
```

**Ports:** `8083` (`/health`, `/ready`, `/metrics` only; no business API).

**Kafka:** consumes `security-alerts` (consumer group `correlation-engine-cg`). Produces nothing. Alert validation:
- **Required:** `alertId` and `entityKey` (non-empty strings) and `alertTime` (UTC ISO timestamp).
- **Optional, checked only when present** (absent or `null` is fine): `severity` and `ruleId` (strings; an unknown severity counts as `medium`), `eventCount` (non-negative integer), `relatedEventIds` (array of strings), `entity` (object) and `window` (object whose `start` and `end` are ISO timestamps).

A malformed alert is logged at warn with topic/partition/offset, counted in `alerts_consumed_total{result="invalid"}` and skipped; it never stops the consumer.

**Readiness:** `/ready` is 200 while the consumer is in its consumer group (kafkajs `GROUP_JOIN`) and 503 after a crash, stop or disconnect until it rejoins.

**Env vars:**

| Name | Default | Required in cloud? | Notes |
|---|---|---|---|
| `PORT` | `8083` | no | |
| `KAFKA_BROKERS` | `localhost:29092` | **yes** | |
| `KAFKA_SSL` | `false` | **yes** (`true` for Confluent Cloud) | |
| `KAFKA_USERNAME` | empty | **yes** | Secret. |
| `KAFKA_PASSWORD` | empty | **yes** | Secret. |
| `KAFKA_SASL_MECHANISM` | `plain` | no | |
| `KAFKA_GROUP_ID` | `correlation-engine-cg` | no | |
| `ALERT_TOPIC` | `security-alerts` | no | |
| `INCIDENT_GRPC_ADDR` | `localhost:50051` | **yes** | `incident-management:50051` |
| `EVENT_STORE_URL` | `http://localhost:8081` | **yes** | `http://event-store:8081` |
| `CORRELATION_WINDOW_MS` | `900000` | no | Must match incident-management. |
| `ESCALATE_ALERT_COUNT` | `3` | no | Alerts per group that escalate severity to critical. |
| `ENRICH_TIMEOUT_MS` | `2000` | no | Event-store lookup timeout; on failure the incident is upserted without enrichment. |
| `GRPC_DEADLINE_MS` | `15000` | no | |
| `PROTO_PATH` | `proto/incident/v1/incident.proto` in the image | no | Override for the gRPC contract location. |
| `LOG_LEVEL` | `info` | no | |

**AWS:** none. No IAM role needed.

**Replicas / HPA:** stateless. Suggested HPA: CPU 70%, min 1, max = partition count of `security-alerts` (extra replicas sit idle). Correlation windows are in memory per partition owner; incident-management's merge-by-entity covers rebalances.

---

## Suggested HPAs

Both need CPU `requests` set on the container (suggested starting point: `100m` CPU, `128Mi` memory).

| Deployment | Metric | Target | min / max |
|---|---|---|---|
| correlation-engine | CPU | 70% | 1 / partitions of `security-alerts` |
| event-store | CPU | 70% | 1 / 3 |

## Kafka topics (Neeharika)

Topics are not auto-created; producers use `idempotent: true`, `acks: -1`.

| Topic | Status | Producer → consumers (this repo) | Key | Partitions |
|---|---|---|---|---|
| `raw-events` | exists upstream | log-ingestion → **event-store** | `source.name` | as upstream |
| `normalized-events` | exists upstream | event-normalizer → **event-store** | `entityKey` | as upstream |
| `security-alerts` | exists upstream | detection-rules → **correlation-engine** | `entityKey` | as upstream |
| `incident-updates` | **NEW, please pre-create** | **incident-management** → (none yet) | `incidentId` | 3 (suggested) |

**Confluent ACLs (if used):** read on the topics above plus the groups `event-store-cg` and `correlation-engine-cg`; write on `incident-updates` plus `IdempotentWrite` on the cluster for incident-management.

**`incident-updates` message:**
```json
{
  "schemaVersion": "1.0",
  "type": "incident.created | incident.updated | incident.status_changed",
  "incident": { "incidentId": "…", "correlationId": "…", "entityKey": "…", "severity": "…", "status": "…", "version": 1, "…": "…" },
  "changedAt": "2026-11-16T05:07:00.000Z",
  "producedBy": { "name": "incident-management", "version": "1.0.0" }
}
```

## AWS resources (Terraform in `infra/terraform/`)

Validated only (`terraform validate`); deployment is Ansh's job through the group's IaC/GitOps.
Inputs: `region` (default `ap-south-1`), `name_prefix` (default `cs-`), `vpc_id`, `db_subnet_ids` (2+ private subnets), `allowed_security_group_ids` (EKS node/pod SG), `raw_bucket_name`.

| Resource | Used by |
|---|---|
| DynamoDB `cs-events` (GSI `eventId-index`), `cs-correlations`, `cs-tpc` (on-demand) | event-store |
| S3 raw-events bucket (private, SSE-S3, `raw/` expires after 30 days) | event-store |
| RDS PostgreSQL 16, `db.t4g.micro`, 20 GB gp3, private, master password in Secrets Manager | incident-management |
| SG on RDS allowing 5432 from `allowed_security_group_ids` | incident-management |
| IAM policy `cs-core-services-data-access` | event-store |

| Output | Goes to |
|---|---|
| `events_table_name`, `correlations_table_name`, `tpc_table_name` | reference only (event-store derives them from `TABLE_PREFIX`) |
| `raw_bucket_name` | event-store `RAW_BUCKET` |
| `rds_endpoint` (hostname), `rds_port`, `rds_database_name` | incident-management `PGHOST`, `PGPORT`, `PGDATABASE` |
| `rds_master_secret_arn` | source of incident-management `PGUSER`/`PGPASSWORD` |
| `data_access_policy_arn` | attach to the event-store pod role |

### IAM

- **event-store:** attach `data_access_policy_arn` to its IRSA role (or EKS Pod Identity role).
- **incident-management:** needs **no** DynamoDB or S3 access. It needs network access to RDS (its pods must use a security group listed in `allowed_security_group_ids`) and the RDS master secret. Whatever syncs that secret into K8s (e.g. External Secrets Operator) needs `secretsmanager:GetSecretValue` on `rds_master_secret_arn`; that permission is **not** in this Terraform.
- **correlation-engine:** no AWS access.

### Database connection in AWS

- RDS for PostgreSQL 16 forces SSL, so set `DATABASE_SSL=true`. The CA bundle is already in the image (`DATABASE_SSL_CA`).
- Supply `PGHOST`, `PGPORT`, `PGDATABASE` (from the Terraform outputs) and `PGUSER`, `PGPASSWORD` (the `username`/`password` keys of the RDS-managed master secret, synced into a K8s Secret). The `pg` driver reads these natively.
- **Leave `DATABASE_URL` unset.**
- The schema is created by the service's own migrations at startup; no manual SQL.

## Service-to-service URLs inside K8s

| From | Env var | Value |
|---|---|---|
| incident-management, correlation-engine | `EVENT_STORE_URL` | `http://event-store:8081` |
| correlation-engine | `INCIDENT_GRPC_ADDR` | `incident-management:50051` |
| event-store | `COORDINATOR_URL` | `http://incident-management:8082` |

(Add the namespace, e.g. `event-store.<ns>.svc.cluster.local`, if the services run in different namespaces.)

## Local run (fresh clone)

Prerequisites: Docker with the compose plugin, **Node 22** (`.nvmrc`), `bash`, `curl` and `jq`. The demo runs on Linux and macOS.

```sh
npm ci                                   # install the workspace from the lockfile
docker compose up -d --build             # kafka, topic-init, postgres, dynamodb-local, s3mock + the 3 services
docker compose ps                        # wait until every service shows (healthy); topic-init exits 0
npm test                                 # unit tests in every workspace
INTEGRATION=1 npm test -w event-store -w incident-management   # storage/3PC tests against the compose infra
npm run test:integration                 # end-to-end: events + alerts -> incident, S3, DynamoDB, incident-updates
scripts/demo-3pc.sh                      # the 3PC demo below
```

The local S3 is `adobe/s3mock:5.2.3` (the MinIO image originally planned is no longer published). It is host port 9000 and `http://s3mock:9090` inside compose. In AWS the real S3 bucket from Terraform is used, with the same code.

## Observability (requirement h)

Every service exposes Prometheus metrics on `/metrics` (request rate, errors and latency per route, gRPC calls, 3PC transactions and participant messages, Kafka consumption) plus Node process metrics, and logs pino JSON to stdout. **Scraping, the Grafana dashboards, cluster-health panels and log shipping are Ansh's integration work**; nothing here deploys them.

## Pinned images

`node:22-alpine` (all three Dockerfiles) and `postgres:16-alpine` (compose) are pinned by digest, and the RDS CA bundle is downloaded with `ADD --checksum=sha256:…`, so a changed download fails the build. To refresh:

```sh
docker pull node:22-alpine && docker image inspect node:22-alpine --format '{{index .RepoDigests 0}}'
docker pull postgres:16-alpine && docker image inspect postgres:16-alpine --format '{{index .RepoDigests 0}}'
curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem | sha256sum        # Linux
curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem | shasum -a 256   # macOS
```

Put the new digests in the `FROM` lines / `docker-compose.yml` and the new sha256 in `services/incident-management/Dockerfile`, then re-run the group-repo export.

## Porting to the group repo (P1-7)

The group repo keeps each service self-contained under `services/<name>/` and builds it with that directory as the Docker build context. This workspace shares code through `packages/` and `proto/`, so the services are **exported** rather than copied by hand:

```sh
scripts/export-group-repo.sh [--force] <path-to-group-repo-checkout>
```

This writes `services/event-store/`, `services/incident-management/` and `services/correlation-engine/` into the destination and touches nothing else there (no git commands). An existing non-empty service directory is refused unless `--force` is given, which replaces only those three directories. Each exported directory contains:

- `src/` and `test/` (and `migrations/` for incident-management), copied from `services/<name>/`
- `lib/common/` and, for event-store and incident-management, `lib/tpc/`: vendored from `packages/*/src`, with every `@core/common` / `@core/tpc` import rewritten to a relative path
- `proto/` (incident-management, correlation-engine); the default proto path resolves to `services/<name>/proto/...` (`/app/proto/...` in the image). `PROTO_PATH` still overrides it.
- `package.json` (the service's dependencies plus the vendored packages') and a `package-lock.json` generated with `npm install --package-lock-only`
- `Dockerfile` (same pinned base image, `npm ci --omit=dev`, `USER node`, same ports; incident-management still ships the checksummed RDS CA bundle), `.dockerignore`, `.env.example`, `README.md`

The script fails if anything in the export still references `@core/`, or if any import or `new URL(...)` path would resolve outside the service directory.

Build from the group repo root:

```sh
docker build -t event-store         services/event-store
docker build -t incident-management services/incident-management
docker build -t correlation-engine  services/correlation-engine
```

Tests run per service: `cd services/<name> && npm ci && npm test` (integration tests need `INTEGRATION=1` and the Compose stack). Ports, environment variables and K8s notes are the same as above.

The exported directories are generated: make changes in this repo and re-run the export with `--force`. Nothing is pushed to the group repo automatically. Rishit opens the PR to `Cloud-Lab-Assignment/Cloud-Security-Ingester` after agreeing on it with Ansh.

## 3PC demo

Run on Docker Compose with `scripts/demo-3pc.sh` (Linux or macOS; needs `node`, `curl`, `jq`). Each scenario uses a fresh `198.18.x.y` entity, checked to have no incident first. **Demo the fault cases on Compose, not through GitOps:** `TPC_FORCE_VOTE` and `TPC_FAULT` must never be set in the cluster. The script restores the normal configuration on exit, even after Ctrl-C or a failure.

1. **Normal commit:** an alert creates an incident; the coordinator log shows one COMMITTED transaction, and the Event Store holds the matching correlation version.
2. **Event Store votes no** (`ES_TPC_FORCE_VOTE=no`): every attempt is ABORTED with `vote_no`, the Postgres participant aborts too, and no incident exists. The correlation-engine keeps retrying the alert. Once the Event Store votes normally again, the retry commits.
3. **Coordinator crash after pre-commit** (`IM_TPC_FAULT=crash-after-precommit`): incident-management exits after both participants acknowledged `preCommit`. The log is left PRECOMMITTED, with no incident row. **Non-blocking 3PC:** the Event Store cannot reach the coordinator for a decision, so after its PRECOMMITTED timeout it commits on its own; the script shows that before restarting anything. On restart without the fault, `recover()` replays `preCommit` (the Event Store's COMMITTED counts as an acknowledgement) and commits the Postgres side.

**Demo notes:**
- Upstream has one detection rule (`RULE-AUTH-001`) and suppresses the same entity for 60 s. Live escalation to `critical` (3 alerts by default) therefore needs 3 alerts more than 60 s apart for one entity. The script and the e2e test publish alerts directly to `security-alerts` instead.
- Status changes (`PATCH`) are plain local Postgres updates, not 3PC; only correlation decisions use 3PC.
- When the atomicity guarantee holds, and the two cases where 3PC can split, are explained in `docs/design/core-services.md`.

Output of one run (16 Nov 2026, after the round-2 fixes):

```text
==> Scenario 1: normal commit
    entity 198.18.211.166 has no incident yet
    sent alert RULE-AUTH-001|198.18.211.166|a03cd5c5-7f…
{
  "incidentId": "b3d54474-4f26-4db5-abf6-009d9b62884d",
  "correlationId": "cor-ca41b1351a5bd0607d05f976",
  "status": "open",
  "severity": "high",
  "alertCount": 1,
  "version": 1
}
    coordinator log for cor-ca41b1351a5bd0607d05f976:
{"txId":"2f3b80ba","state":"COMMITTED","reason":null,"kind":"create","version":1,"correlationId":"cor-ca41b1351a5bd0607d05f976"}
    event-store correlation history:
{"version":1,"kind":"create","txId":"2f3b80ba","recordedAt":"2026-11-16T09:52:59.816Z"}

==> Scenario 2: the Event Store votes no
    restarting event-store with TPC_FORCE_VOTE=no
    entity 198.18.175.128 has no incident yet
    sent alert RULE-AUTH-001|198.18.175.128|a72c5f9c-80…
    incidents for 198.18.175.128 (expect none):
[]
    coordinator log for cor-b631d28c757fd5914936860c (expect ABORTED vote_no; the correlation engine keeps retrying):
{"txId":"5ddf306b","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-b631d28c757fd5914936860c"}
{"txId":"b826649a","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-b631d28c757fd5914936860c"}
{"txId":"079f0e68","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-b631d28c757fd5914936860c"}
{"txId":"ffff4a95","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-b631d28c757fd5914936860c"}
{"txId":"a400578f","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-b631d28c757fd5914936860c"}
{"txId":"1c7d3a92","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-b631d28c757fd5914936860c"}
    postgres participant rows for those transactions are ABORTED too:
5ddf306b ABORTED
b826649a ABORTED
079f0e68 ABORTED
ffff4a95 ABORTED
a400578f ABORTED
1c7d3a92 ABORTED
    restoring event-store (votes normally); the engine's retry now commits
{
  "incidentId": "14432c8c-9b70-4968-b0c6-3a9a755ef0a0",
  "correlationId": "cor-b631d28c757fd5914936860c",
  "status": "open",
  "severity": "high",
  "alertCount": 1,
  "version": 1
}
{"txId":"4d3dd7e9","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-b631d28c757fd5914936860c"}
{"txId":"abd94004","state":"COMMITTED","reason":null,"kind":"create","version":1,"correlationId":"cor-b631d28c757fd5914936860c"}

==> Scenario 3: the coordinator crashes after every participant pre-committed
    restarting incident-management with TPC_FAULT=crash-after-precommit
    entity 198.18.210.229 has no incident yet
    sent alert RULE-AUTH-001|198.18.210.229|a754e490-7f…
    incident-management container: exited 1
    "msg":"injected coordinator crash after preCommit"
    coordinator log in postgres (expect PRECOMMITTED) and no incident row yet:
be4d0dce PRECOMMITTED
0 incident row(s) for 198.18.210.229
    waiting for the Event Store to commit on its own (PRECOMMITTED timeout, coordinator unreachable)…
    event-store self-committed after ~11s while the coordinator was down:
{"version":1,"kind":"create","txId":"be4d0dce","recordedAt":"2026-11-16T09:54:07.222Z"}
    restarting incident-management without the fault: recover() finishes the Postgres side
    "recovered":["be4d0dce-ef43-4137-8a26-1f20a8d30e41"]
{
  "incidentId": "f8e1f2ff-56a0-4bf1-b349-4229d22a2358",
  "correlationId": "cor-d7d1b8529a000b1620c08da6",
  "status": "open",
  "severity": "high",
  "alertCount": 1,
  "version": 1
}
{"txId":"be4d0dce","state":"COMMITTED","reason":null,"kind":"create","version":1,"correlationId":"cor-d7d1b8529a000b1620c08da6"}

==> Done. The stack is back to normal (no forced votes, no faults).
    restoring event-store and incident-management to their normal configuration
```
