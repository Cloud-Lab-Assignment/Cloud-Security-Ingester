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
| `TPC_TIMEOUT_MS` | `10000` | no | Participant timeout before UNCERTAIN self-aborts / PRECOMMITTED self-commits. |
| `TPC_FORCE_VOTE` | unset | **leave unset** | Demo only (`no` forces a "no" vote). |
| `LOG_LEVEL` | `info` | no | |

AWS credentials come from the pod's IAM role (IRSA / EKS Pod Identity). Do **not** set `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in the cloud.

**Readiness:** ready once the Kafka consumers are connected and `DescribeTable` on the events table succeeds.

**Replicas / HPA:** stateless. Suggested HPA: CPU 70%, min 1, max 3. Kafka consumption only parallelises up to the partition count of the consumed topics; extra replicas still help the REST API.

---

## incident-management

Creates and updates incidents in Postgres (RDS). **3PC coordinator**: every incident create/update from the correlation-engine commits across its own Postgres participant and the event-store participant, then publishes `incident-updates`. Analyst status changes (`PATCH`) are plain Postgres updates.

**Build** (from the workspace root):
```sh
docker build -f services/incident-management/Dockerfile -t incident-management:<tag> .
```
The image bakes in the AWS RDS CA bundle at `/etc/ssl/rds/global-bundle.pem`.

**Ports:** `8082` (REST API, `/health`, `/ready`, `/metrics`), `50051` (gRPC `incident.v1.IncidentService`, called by correlation-engine; plaintext h2c inside the cluster).

**Kafka:** produces `incident-updates` (key `incidentId`). Consumes nothing.

**REST API:**
- `GET /incidents?status=&severity=&entityKey=&limit=`, `GET /incidents/:id`, `GET /incidents/:id/history`
- `PATCH /incidents/:id` with `{status, version, actor?, note?}`. Returns 409 `{error:"version_conflict", currentVersion}` on a stale version, and 409 `{error:"transaction_pending"}` while a 3PC transaction for that incident is in flight (retry shortly).
- `GET /tpc/transactions?limit=&correlationId=`: the coordinator log, newest first (used in the 3PC demo).

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
| `DATABASE_URL` | `postgres://incidents:incidents@localhost:5432/incidents` | **leave unset** | Local only. In AWS use the `PG*` vars below. |
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
| `CORRELATION_WINDOW_MS` | `900000` | no | Must match correlation-engine. |
| `TPC_FAULT` | unset | **leave unset** | Demo only (`crash-after-precommit`). Never set through GitOps. |
| `LOG_LEVEL` | `info` | no | |

**Startup:** runs DB migrations, then 3PC `recover()` (finishes or aborts in-flight transactions), then starts gRPC and REST. Readiness is 503 until then.

**Replicas:** **exactly 1. No HPA.** It is the 3PC coordinator and runs recovery at startup; two coordinators would race on the same transactions. Use `strategy: Recreate` so a rollout never overlaps two pods. A restart is safe (recovery handles it).

---

## correlation-engine

Consumes `security-alerts`, groups alerts per `entityKey` inside a correlation window (with severity escalation), enriches each group from the event-store over REST and upserts the incident through incident-management's gRPC API.

**Build** (from the workspace root):
```sh
docker build -f services/correlation-engine/Dockerfile -t correlation-engine:<tag> .
```

**Ports:** `8083` (`/health`, `/ready`, `/metrics` only; no business API).

**Kafka:** consumes `security-alerts` (consumer group `correlation-engine-cg`). Produces nothing.

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

(Add the namespace, e.g. `event-store.<ns>.svc.cluster.local`, if the services run in different namespaces.)

## Local run (Docker Compose)

```sh
docker compose up -d --build     # kafka, topic-init, postgres, dynamodb-local, s3mock + the 3 services
npm test                         # unit tests in every workspace
INTEGRATION=1 npm test -w event-store -w incident-management   # storage/3PC tests against the compose infra
npm run test:integration         # end-to-end: raw/normalized events + alerts -> incident, S3, DynamoDB, incident-updates
scripts/demo-3pc.sh              # the 3PC demo below
```

The local S3 is `adobe/s3mock:5.2.3` (the MinIO image originally planned is no longer published). It is host port 9000 and `http://s3mock:9090` inside compose. In AWS the real S3 bucket from Terraform is used, with the same code.

## 3PC demo

Run on Docker Compose with `scripts/demo-3pc.sh`. **Demo the fault cases on Compose, not through GitOps:** `TPC_FORCE_VOTE` and `TPC_FAULT` must never be set in the cluster.

1. **Normal commit:** an alert creates an incident; the coordinator log shows one COMMITTED transaction, and the Event Store holds the matching correlation version.
2. **Event Store votes no** (`ES_TPC_FORCE_VOTE=no`): every attempt is ABORTED with `vote_no`, the Postgres participant aborts too, and no incident exists. The correlation-engine keeps retrying the alert. Once the Event Store votes normally again, the retry commits.
3. **Coordinator crash after pre-commit** (`IM_TPC_FAULT=crash-after-precommit`): incident-management exits after both participants acknowledged `preCommit`. The log is left PRECOMMITTED, with no incident row. On restart without the fault, `recover()` re-sends `preCommit` and commits on both sides. If a participant had already self-committed on its own timeout (the 3PC property 2PC lacks), that counts as an acknowledgement.

**Demo notes:**
- Upstream has one detection rule (`RULE-AUTH-001`) and suppresses the same entity for 60 s. Live escalation to `critical` (3 alerts by default) therefore needs 3 alerts more than 60 s apart for one entity. The script and the e2e test publish alerts directly to `security-alerts` instead.
- Status changes (`PATCH`) are plain local Postgres updates, not 3PC; only correlation decisions use 3PC.

Output of one run (15 Nov 2026, 21:24 UTC):

```text
==> Scenario 1: normal commit
    sent alert RULE-AUTH-001|203.0.113.79|c189ff7d…
{
  "incidentId": "21467dd4-c96e-42b8-b0b4-4ab429c36ae6",
  "correlationId": "cor-727adc0e735144c6e757e148",
  "status": "open",
  "severity": "high",
  "alertCount": 1,
  "version": 1
}
    coordinator log for cor-727adc0e735144c6e757e148:
{"txId":"6ccdf137","state":"COMMITTED","reason":null,"kind":"create","version":1,"correlationId":"cor-727adc0e735144c6e757e148"}
    event-store correlation history:
{"version":1,"kind":"create","txId":"6ccdf137","recordedAt":"2026-11-15T21:24:08.691Z"}

==> Scenario 2: the Event Store votes no
    restarting event-store with TPC_FORCE_VOTE=no
    sent alert RULE-AUTH-001|203.0.113.88|04e46412…
    incidents for 203.0.113.88 (expect none):
[]
    latest coordinator log entries (expect ABORTED vote_no; the correlation engine keeps retrying):
{"txId":"f380df98","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-1f82c6cbe2dbe93bd1f44c73"}
{"txId":"a454144e","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-1f82c6cbe2dbe93bd1f44c73"}
{"txId":"1d12f6cb","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-1f82c6cbe2dbe93bd1f44c73"}
    postgres participant rows for those transactions are ABORTED too:
1d12f6cb|ABORTED
a454144e|ABORTED
f380df98|ABORTED
    restoring event-store (votes normally); the engine's retry now commits
{
  "incidentId": "c5d43336-e1ec-49a0-8d4c-27a1f802bf8a",
  "correlationId": "cor-1f82c6cbe2dbe93bd1f44c73",
  "status": "open",
  "severity": "high",
  "alertCount": 1,
  "version": 1
}
{"txId":"7eb5d50a","state":"ABORTED","reason":"vote_no","kind":"create","version":1,"correlationId":"cor-1f82c6cbe2dbe93bd1f44c73"}
{"txId":"d4096bad","state":"COMMITTED","reason":null,"kind":"create","version":1,"correlationId":"cor-1f82c6cbe2dbe93bd1f44c73"}

==> Scenario 3: the coordinator crashes after every participant pre-committed
    restarting incident-management with TPC_FAULT=crash-after-precommit
    sent alert RULE-AUTH-001|203.0.113.84|9ffbec42…
    incident-management container: exited 1
    "msg":"injected coordinator crash after preCommit"
    coordinator log in postgres (expect PRECOMMITTED) and no incident row yet:
084c8a3f|PRECOMMITTED
0 incident row(s) for 203.0.113.84
    restarting incident-management without the fault: recover() finishes the transaction
    "recovered":["084c8a3f-d763-4415-a945-d295541cfbe1"]
{
  "incidentId": "31fbd49c-d547-4634-bf01-d738d3308eb5",
  "correlationId": "cor-29e5418af3ee039081c73adb",
  "status": "open",
  "severity": "high",
  "alertCount": 1,
  "version": 1
}
{"txId":"084c8a3f","state":"COMMITTED","reason":null,"kind":"create","version":1,"correlationId":"cor-29e5418af3ee039081c73adb"}
    event-store correlation history:
{"version":1,"kind":"create","txId":"084c8a3f"}

==> Done. The stack is back to normal (no forced votes, no faults).
```
