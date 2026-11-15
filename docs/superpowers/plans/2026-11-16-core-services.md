# Core Services (Event Store, Incident Management, Correlation Engine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Rishit's three services for the S25 Cloud Security Monitoring assignment, with 3PC as the scenario concept, ready to hand to Ansh as Docker images, Terraform and a port/env sheet.

**Architecture:**
- **Correlation Engine:** consumes `security-alerts` and groups alerts for each `entityKey` inside a correlation window. It enriches each group from the Event Store over REST and upserts the incident through Incident Management's gRPC API.
- **Incident Management:** the 3PC coordinator. Every correlation decision (incident create or update) commits across two participants, its own Postgres participant and the Event Store's DynamoDB participant. It then publishes `incident-updates`.
- **Event Store:** archives `raw-events` to S3, stores `normalized-events` and correlation history in DynamoDB, and serves a REST read API.

**Tech Stack:**
- Node.js 22 (ESM) with npm workspaces
- kafkajs ^2.2.4, Fastify ^5, pino, prom-client
- @grpc/grpc-js and @grpc/proto-loader
- pg (PostgreSQL 16)
- AWS SDK v3 (DynamoDB, S3)
- `node --test`
- Docker Compose for the local stack, Terraform (AWS) for cloud resources

**Spec:** `docs/project-brief.md` (read it first: assignment rules, chat decisions, upstream message schemas).

**Deadline:** the assignment is due **Wed 18 Nov 2026, 23:59 IST**. Ansh has waited for the port sheet since 4 Nov, so the handoff draft and Terraform (Task 2) come right after the scaffold.

## Working Rules (for the executing agent)

- This repo is Rishit's private repo `rish-kun/cloud-computing-assignment`. Work on `main` and commit after every task. **Push only when Rishit asks.**
- **Never act on the group repo** `Cloud-Lab-Assignment/Cloud-Security-Ingester`. That means no push, no fork, no PR and no comments. Task 10 only prepares a local copy.
- **Missing tools:** the box should already have Node 22, Docker with Compose and Terraform. If any of them is missing, **stop and ask Rishit**. Do not install it.
- **Approved npm packages** for this repo: `kafkajs`, `fastify`, `pino`, `prom-client`, `@grpc/grpc-js`, `@grpc/proto-loader`, `pg`, `@aws-sdk/client-dynamodb`, `@aws-sdk/lib-dynamodb`, `@aws-sdk/client-s3`. Ask before adding anything else.
- **Approved Terraform action:** `terraform init -backend=false`, which downloads the AWS provider. **Never run `terraform plan`, `apply` or `destroy`, create cloud resources, or run `kubectl apply`.** AWS credits belong to the group, and deployment is Ansh's job through GitOps.
- **No secrets in Git.** Only `.env.example` files, holding variable names and dummy local values.

## Global Constraints

- **Runtime and modules:** Node `>=22`, ESM only (`"type": "module"`). Tests use `node --test`. Test globs are always quoted, e.g. `node --test "test/**/*.test.js"`.
- **Workspaces:** the root `package.json` lists the workspaces **explicitly**: `["packages/common","packages/tpc","services/event-store","services/incident-management","services/correlation-engine"]`. `package-lock.json` is committed.
- **Kafka:**
  - Connection env vars match the group repo exactly: `KAFKA_BROKERS` (comma-separated), `KAFKA_SSL` (`"true"` enables TLS), `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_SASL_MECHANISM` (default `plain`).
  - Producers use `idempotent: true` and `acks: -1`, with `allowAutoTopicCreation: false`.
  - Consumers subscribe with `fromBeginning: true`.
- **Topics:**
  - Consumed: `raw-events`, `normalized-events`, `security-alerts`.
  - Produced: `incident-updates`, which is new and must be pre-created by infra.
- **Message conventions:**
  - Every message this repo produces carries `schemaVersion: "1.0"`.
  - Timestamps are UTC `YYYY-MM-DDTHH:mm:ss.sssZ`.
  - The service stamp is `{name, version: "1.0.0"}`.
- **Naming:** camelCase everywhere in JS, JSON, REST and gRPC (proto-loader `keepCase:false`). snake_case only in SQL. Repositories map between the two.
- **Ports:**
  - event-store: REST `8081`
  - incident-management: REST `8082` and gRPC `50051`
  - correlation-engine: HTTP `8083` (health and metrics only)
- **Endpoints and logs:** every service exposes `GET /health` → `{status:"ok",service}`, `GET /ready` (200 or 503) and `GET /metrics` (Prometheus). Logs are pino JSON on stdout.
- **Replicas:** incident-management runs **exactly 1 replica**, because it is the 3PC coordinator and runs recovery at startup. The other two services are stateless and can be scaled by HPA. The correlation-engine's in-memory windows are per partition-owner, and the incident-management fallback merge covers rebalances.
- **Docker images:**
  - Pinned: `node:22-alpine`, `apache/kafka:3.9.0`, `postgres:16-alpine`, `amazon/dynamodb-local:2.5.2`, `minio/minio:RELEASE.2025-04-22T22-12-26Z`.
  - If a pull fails, stop and ask Rishit. Never use `latest`.

## Review Focus

1. **Redelivered and retried alerts:**
   - A redelivered alert (same `alertId` already on the incident) changes nothing.
   - An alert whose upsert **failed** must reach Incident Management on retry.
   - Tests: `correlator ignores duplicate alertId`, `failed upsert forgets alert so retry re-sends` (Task 8) and `upsert with only known alertIds is a no-op` (Task 6).
2. **Event Store down during enrichment:**
   - The Correlation Engine upserts with `enrichment: null` instead of retrying forever.
   - Test: `enrichment failure yields null enrichment` (Task 8).
3. **3PC failure in every phase stays atomic:**
   - A "no" vote or a vote timeout aborts everywhere.
   - A failed `preCommit` aborts.
   - A coordinator crash after all `preCommit` acks is committed by `recover()`.
   - A participant that already self-aborted makes `recover()` abort everyone.
   - UNCERTAIN past its timeout self-aborts. PRECOMMITTED past its timeout self-commits.
   - Tests: Task 3 and Task 6.
4. **Overlapping transactions on one incident:**
   - A second upsert for a `correlationId` with a non-terminal transaction gets a "no" vote.
   - An apply whose `version` is stale fails instead of overwriting.
   - Tests: `concurrent upsert for same correlationId votes no` and `stale apply version rejected` (Task 6).
5. **Unsafe IDs and concurrent PATCH:**
   - IPv6 `entityKey` and `|` in IDs work URL-encoded.
   - A PATCH with a stale `version` returns 409.
   - Tests: Tasks 5 and 7.

---

## Before coding (Rishit, not an agent)

- [ ] Send Ansh `docs/handoff.md` once Task 2 is done. He asked for ports and AWS services on 4 Nov.
- [ ] Ask the group to confirm:
  - (a) your requirements are **f (3PC)** and **g (three storage products)**
  - (b) who owns the **Investigation Query API**
  - (c) that infra will pre-create `incident-updates`
  - (d) that the build context for these images is `services/core` in the group repo (Task 10)

## File Structure

```
package.json, package-lock.json, .nvmrc (22), .dockerignore, .env.example
docker-compose.yml               kafka, topic-init, postgres, dynamodb-local, minio, then the 3 services
proto/incident/v1/incident.proto gRPC contract
packages/common/                 kafka factory, logger, fastify base app (health/ready/metrics), time utils
packages/tpc/                    pure 3PC participant + coordinator state machines (no I/O)
services/event-store/            S3 archive, DynamoDB repo, Kafka consumers, REST API, 3PC participant routes
services/incident-management/    Postgres schema/repo, 3PC coordinator wiring, gRPC + REST, incident-updates producer
services/correlation-engine/     correlator (pure), event-store client, gRPC client, Kafka consumer
infra/terraform/                 DynamoDB, S3, RDS Postgres, IAM policy (fmt/validate only)
docs/handoff.md                  for Ansh/Neeharika: ports, env, topics, AWS resources, build commands, 3PC demo
docs/design/core-services.md     design-doc section for the group design document
test/e2e/pipeline.test.js        end-to-end test against docker compose (INTEGRATION=1)
```

Each service has `src/config.js` (env with defaults), `src/server.js` (entrypoint), `test/`, `Dockerfile` and `.env.example`. Its `package.json` has `"test": "node --test \"test/**/*.test.js\""`.

**Every Dockerfile** builds from the **workspace root** context: `docker build -f services/<name>/Dockerfile .`. It:
1. uses `node:22-alpine`
2. copies `package.json`, `package-lock.json` and **every** workspace's `package.json`
3. runs `npm ci --omit=dev`
4. copies `packages/`, `proto/` and the service's `src/`
5. sets `USER node`, `EXPOSE` and `CMD ["node","services/<name>/src/server.js"]`

`.dockerignore` excludes `node_modules`, `.git`, `docs`, `infra` and `test`.

---

### Task 1: Workspace, shared helpers, local stack

**Files:**
- Create: `package.json`, `.nvmrc`, `.dockerignore`, `.env.example`, `docker-compose.yml`
- Create: `packages/common/package.json` (name `@core/common`), `packages/common/src/{kafka.js,logger.js,app.js,time.js,index.js}`
- Test: `packages/common/test/{time.test.js,app.test.js}`

**Interfaces:**
- Produces (from `@core/common`):
  - `createKafka({ clientId }): Kafka`
  - `createLogger(service): pino.Logger`
  - `buildApp({ service, ready: () => Promise<boolean> }): FastifyInstance`
    - Registers `/health`, `/ready` and `/metrics`.
    - Records `http_requests_total{route,method,status}` and `http_request_duration_seconds{route,method}`.
    - Exposes `app.metrics` (the prom-client registry).
  - `isoTimestamp(ms): string`
  - `SERVICE_VERSION = "1.0.0"`
- Root scripts:
  - `"test": "npm test --workspaces --if-present"`
  - `"test:integration": "INTEGRATION=1 node --test \"test/e2e/**/*.test.js\""`

- [ ] **Step 1: Write failing tests:**
  - `isoTimestamp formats ms`:
    - `isoTimestamp(0) === "1970-01-01T00:00:00.000Z"`
    - `isoTimestamp(Date.UTC(2026,10,16,1,2,3,4)) === "2026-11-16T01:02:03.004Z"`
  - `health ok`: `GET /health` → 200 `{status:"ok",service:"t"}`
  - `ready 503 when not ready` → 503 `{status:"not_ready"}`
  - `metrics exposes request counter`: the body contains `http_requests_total`
- [ ] **Step 2:** Run `npm test -w @core/common`. Expected: FAIL.
- [ ] **Step 3:** Implement `packages/common` and the root files.
- [ ] **Step 4:** Run `npm install && npm test -w @core/common`. Expected: PASS. Commit `package-lock.json`.
- [ ] **Step 5: Write `docker-compose.yml`.** Every container gets a healthcheck.
  - **kafka:** `apache/kafka:3.9.0` in single-node KRaft mode, with this exact environment:
    ```
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: INTERNAL://:9092,EXTERNAL://:29092,CONTROLLER://:9093
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    ```
    Port `29092` is published.
  - **topic-init:** a one-shot `/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --partitions 3 --replication-factor 1 --topic <t>` for each of `raw-events`, `normalized-events`, `security-alerts` and `incident-updates`.
  - **postgres:** `postgres:16-alpine`, db, user and password all `incidents`, port 5432.
  - **dynamodb-local:** command `-jar DynamoDBLocal.jar -sharedDb -inMemory`, port 8000.
  - **minio:** `server /data`, ports 9000 and 9001, user and password `minioadmin`.
  - **No bucket-init container.** The Event Store creates the bucket locally (Task 4).
- [ ] **Step 6:** Run `docker compose up -d kafka topic-init postgres dynamodb-local minio && docker compose ps`. Expected: everything is healthy, and topic-init exited 0. `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list` lists the 4 topics.
- [ ] **Step 7:** Commit `chore: workspace, common helpers, local stack`.

### Task 2: Handoff sheet draft and Terraform (validate only)

This task needs no service code. Finish it early so Rishit can send it to Ansh.

**Files:**
- Create: `docs/handoff.md`, `infra/terraform/{versions.tf,variables.tf,dynamodb.tf,s3.tf,rds.tf,iam.tf,outputs.tf,README.md}`

**Interfaces:**
- **`docs/handoff.md`**, one section per service:
  - the image build command (`docker build -f services/<name>/Dockerfile .`, from the workspace root)
  - ports
  - env vars (name, default, required in cloud?), copied from the Config tables in Tasks 4–8
  - consumed and produced topics
  - health, ready and metrics paths
  - replica rules (incident-management = 1)
  - suggested HPAs: correlation-engine and event-store, CPU 70%
  - topics to pre-create
  - the AWS resources and Terraform outputs
  - how `DATABASE_URL` is built in AWS (see `rds.tf` below)
- **Terraform:**
  - `>= 1.6`, with the AWS provider `~> 5.0`
  - **Variables:** `region` (default `ap-south-1`), `name_prefix` (default `cs-`), `vpc_id`, `db_subnet_ids` (list), `allowed_security_group_ids` (list), `raw_bucket_name`.
  - **`dynamodb.tf`:** three tables, all PAY_PER_REQUEST:
    - `${name_prefix}events`: PK `entityKey` S, SK `sk` S, and GSI `eventId-index` with PK `eventId` and **projection ALL**
    - `${name_prefix}correlations`: PK `correlationId` S, SK `version` N
    - `${name_prefix}tpc`: PK `txId` S
  - **`s3.tf`:**
    - public access blocked, SSE-S3
    - a lifecycle rule expiring `raw/` after 30 days
    - `force_destroy = true`
  - **`rds.tf`:** Postgres 16 with:
    - `db.t4g.micro`, 20 GB gp3
    - `manage_master_user_password = true`
    - `publicly_accessible = false`, `skip_final_snapshot = true`, `deletion_protection = false`
    - a DB subnet group, and an SG allowing 5432 from `allowed_security_group_ids`
    - RDS 16 forces SSL, so services connect with `DATABASE_SSL=true`
    - In AWS, the service is given `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and `PGPASSWORD` from the master secret, which `pg` reads natively.
  - **`iam.tf`:** a policy `${name_prefix}core-services-data-access` allowing:
    - `dynamodb:GetItem`, `PutItem`, `UpdateItem`, `Query`, `Scan` and `DescribeTable` on the 3 tables and `eventId-index`
    - `s3:PutObject` and `GetObject` on `raw/*`, and `s3:ListBucket` on the bucket
  - **Outputs:** the three table names, `raw_bucket_name`, `rds_endpoint`, `rds_master_secret_arn`, `data_access_policy_arn`.

- [ ] **Step 1:** Write the Terraform files.
- [ ] **Step 2:** Run `cd infra/terraform && terraform fmt -check && terraform init -backend=false && terraform validate`. Expected: `Success! The configuration is valid.` No plan or apply.
- [ ] **Step 3:** Write the `docs/handoff.md` draft. Env tables for unbuilt services come from this plan's Config tables. Mark the doc "draft: ports/env are final; image names TBD with Ansh".
- [ ] **Step 4:** Commit `feat(infra): terraform for core services; docs: handoff draft`. Tell Rishit the handoff draft is ready to send.

### Task 3: 3PC state machines (`packages/tpc`)

These are pure logic with no I/O. Storage, transport and clock are injected. This package is what the **f (3PC)** demo stands on.

**Files:**
- Create: `packages/tpc/package.json` (name `@core/tpc`), `packages/tpc/src/{participant.js,coordinator.js,index.js}`
- Test: `packages/tpc/test/{participant.test.js,coordinator.test.js}`

**Interfaces:**
- **Participant states:** `UNCERTAIN` (voted yes), `PRECOMMITTED`, `COMMITTED`, `ABORTED`.
- **Coordinator log states:** `WAITING`, `PRECOMMITTED`, `COMMITTED`, `ABORTED`.
- **`createParticipant(opts)`**, with `opts`:
  - `store`: `{ get(txId), put(record), listPending() }`. `listPending` returns records in UNCERTAIN or PRECOMMITTED.
  - `validate(txId, payload) → Promise<boolean>`
  - `apply(payload) → Promise<void>`
  - `timeoutMs`
  - `now: () => number`
  - `forceVote?: "yes"|"no"`
- A record is `{ txId, state, payload, updatedAt }`.
- The participant returns:
  - `canCommit(txId, payload) → "yes"|"no"`
  - `preCommit(txId)`
  - `doCommit(txId)`
  - `abort(txId)`
  - `sweep() → {committed: string[], aborted: string[]}`
- **Participant message rules:**

  | Message | Allowed from | On repeat | Otherwise |
  |---|---|---|---|
  | `canCommit` | none (new txId) | returns the recorded vote | — |
  | `preCommit` | UNCERTAIN | no-op from PRECOMMITTED | `TpcStateError` |
  | `doCommit` | PRECOMMITTED | no-op from COMMITTED | `TpcStateError` (including from ABORTED or unknown) |
  | `abort` | UNCERTAIN, PRECOMMITTED | no-op from ABORTED | `TpcStateError` from COMMITTED |

  - `doCommit` runs `apply` **exactly once**.
  - **`sweep`:** an UNCERTAIN record older than `timeoutMs` → ABORTED. A PRECOMMITTED record older than `timeoutMs` → `apply` + COMMITTED.
- **`createCoordinator(opts)`**, with `opts`:
  - `log`: `{ put(entry), listUnfinished() }`
  - `participants`: an array of `{name, canCommit, preCommit, doCommit, abort}`
  - `timeoutMs`
  - `now`
  - `fault?: "crash-after-precommit"`
  - `onCommitted?(txId, payloads)`
- An entry is `{ txId, state, payloads, updatedAt }`.
- The coordinator returns:
  - `run(txId, payloads) → {outcome:"committed"|"aborted", reason?}`, where `reason` is `"vote_no"|"vote_timeout"|"precommit_failed"`
  - `recover() → string[]`
- **`run` algorithm:**
  1. Log WAITING.
  2. Send `canCommit` to all, each raced against `timeoutMs`.
  3. If any vote is "no" or times out: log ABORTED, `abort` all, and return.
  4. Log PRECOMMITTED, then send `preCommit` to all.
  5. If any `preCommit` fails: log ABORTED, `abort` all, and return `precommit_failed`.
  6. **Fault hook:** after all `preCommit` acks, if `fault === "crash-after-precommit"`, throw `CoordinatorCrash`.
  7. Send `doCommit` to all, log COMMITTED and call `onCommitted`.
  - Each `abort` and `doCommit` send is **retried up to 5×** with a 200 ms backoff until acked.
- **`recover()`:**
  - A **WAITING** entry → abort all, log ABORTED.
  - A **PRECOMMITTED** entry → re-send `preCommit` to all.
    - If any participant throws `TpcStateError` (it already self-aborted) → abort all, log ABORTED.
    - Otherwise → `doCommit` all, log COMMITTED and call `onCommitted`.
- **Exports:** `TpcStateError`, `CoordinatorCrash`.

- [ ] **Step 1: Write failing participant tests** (fake clock):
  - `yes vote moves to UNCERTAIN`
  - `validate false votes no and records ABORTED`
  - `forceVote no overrides validate`
  - `repeat canCommit returns recorded vote`
  - `doCommit applies once even if repeated`
  - `doCommit on ABORTED throws TpcStateError`
  - `preCommit on unknown tx throws TpcStateError`
  - `sweep aborts UNCERTAIN older than timeoutMs`
  - `sweep commits PRECOMMITTED older than timeoutMs`
  - `sweep leaves fresh records alone`
- [ ] **Step 2: Write failing coordinator tests** (small real timeouts, ≤50 ms):
  - `all yes → committed`: phases in order, log ends COMMITTED, `onCommitted` called once
  - `one no → aborted everywhere`: `reason:"vote_no"`
  - `canCommit timeout → aborted`: `reason:"vote_timeout"`
  - `preCommit failure → aborted`: `reason:"precommit_failed"`
  - `crash after precommit then recover commits`: `run` rejects with `CoordinatorCrash`, and `recover()` returns `[txId]` with all participants COMMITTED
  - `recover aborts when a participant already self-aborted`: a participant whose `preCommit` throws `TpcStateError` → everyone gets `abort`, the log is ABORTED
  - `recover aborts WAITING entries`
  - `abort is retried until acked`: a participant whose `abort` fails twice, then succeeds
- [ ] **Step 3:** Run `npm test -w @core/tpc`. Expected: FAIL.
- [ ] **Step 4:** Implement both state machines.
- [ ] **Step 5:** Run `npm test -w @core/tpc`. Expected: PASS.
- [ ] **Step 6:** Commit `feat(tpc): three-phase commit participant and coordinator`.

### Task 4: Event Store storage and Kafka consumers

**Files:**
- Create: `services/event-store/{package.json,.env.example}`, `services/event-store/src/{config.js,dynamo.js,s3-archive.js,consumers.js,local-setup.js}`
- Test: `services/event-store/test/{dynamo.test.js,s3-archive.test.js,consumers.test.js}`

**Interfaces:**
- **Config:**

  | Env var | Default |
  |---|---|
  | `PORT` | 8081 |
  | `KAFKA_GROUP_ID` | event-store-cg |
  | `RAW_TOPIC` | raw-events |
  | `NORMALIZED_TOPIC` | normalized-events |
  | `AWS_REGION` | ap-south-1 |
  | `DYNAMODB_ENDPOINT` | unset in AWS |
  | `S3_ENDPOINT` | unset in AWS |
  | `RAW_BUCKET` | cs-raw-events |
  | `TABLE_PREFIX` | cs- |
  | `TPC_TIMEOUT_MS` | 10000 |
  | `TPC_FORCE_VOTE` | unset |

  - **Local values** in `.env.example` and compose: `DYNAMODB_ENDPOINT=http://dynamodb-local:8000`, `S3_ENDPOINT=http://minio:9000`, `AWS_ACCESS_KEY_ID=minioadmin`, `AWS_SECRET_ACCESS_KEY=minioadmin`. DynamoDB Local accepts any credentials.
  - When `S3_ENDPOINT` is set, the S3 client uses `forcePathStyle: true`.
- **Tables:** as in Task 2. An events item is `{entityKey, sk, eventId, eventTime, category, outcome, doc}`, where `sk` is `` `${eventTime}#${eventId}` ``.
- **`dynamo.js`:**
  - `putEvent(event) → "stored"|"duplicate"`: conditional on `attribute_not_exists(sk)`
  - `getEvent(eventId) → Event|null`: queries the GSI
  - `queryEvents({entityKey, from, to, limit=100, cursor}) → {items, nextCursor|null}`
    - KeyCondition: `entityKey = :e AND sk BETWEEN :from AND :toMax`, where `:toMax = to + "#￿"`
    - The cursor is base64url JSON of `LastEvaluatedKey`.
  - `putCorrelation(rec)`: conditional on `attribute_not_exists(correlationId)`. A duplicate is treated as already stored.
  - `getCorrelation(correlationId) → rec[]`, ordered by version
  - `tpcStore`: `{get, put, listPending}`. `listPending` is a Scan with a filter on state.
- **`s3-archive.js`:**
  - `rawKey(event) → string`: returns `` `raw/${yyyy}/${mm}/${dd}/${hh}/${eventId}.json` ``, built from `receivedAt` in UTC
  - `archiveRaw(event) → key`
- **`local-setup.js`:** `ensureLocalResources()` creates the 3 tables and the bucket, **only when** `DYNAMODB_ENDPOINT` or `S3_ENDPOINT` is set.
- **`consumers.js`:** `startConsumers({kafka, logger, archiveRaw, putEvent}) → {stop()}`.
  - One consumer group, subscribed to both topics.
  - Invalid JSON is logged and skipped.
  - Storage errors are thrown, so kafkajs retries.

- [ ] **Step 1: Write failing tests.** Integration-gated tests use `{ skip: !process.env.INTEGRATION }`.
  - `putEvent then getEvent round-trips doc`
  - `putEvent same event twice → "duplicate"`
  - `queryEvents includes events exactly at "to"`
  - `queryEvents paginates`: 3 events, `limit:2`, and the second page has 1 item
  - `rawKey layout`: a unit test where `receivedAt "2026-11-16T05:07:00.000Z"` → `raw/2026/11/16/05/<id>.json`
  - `consumer routes by topic`: a unit test with fakes, covering raw → `archiveRaw`, normalized → `putEvent`, and `"not json"` → neither and no throw
- [ ] **Step 2:** Run `npm test -w event-store`, then `INTEGRATION=1 npm test -w event-store` with the compose infra up. Expected: FAIL.
- [ ] **Step 3:** Implement, using `DynamoDBDocumentClient` from `@aws-sdk/lib-dynamodb`.
- [ ] **Step 4:** Run both test commands. Expected: PASS.
- [ ] **Step 5:** Commit `feat(event-store): dynamodb and s3 storage with kafka consumers`.

### Task 5: Event Store REST API and 3PC participant routes

**Files:**
- Create: `services/event-store/src/{routes.js,server.js}`, `services/event-store/Dockerfile`
- Test: `services/event-store/test/routes.test.js` (`app.inject` with fakes)

**Interfaces:**
- **Read routes:**
  - `GET /events/:eventId` → 200, or 404 `{error:"not_found"}`
  - `GET /events?entityKey=&from=&to=&limit=&cursor=` → `{items, nextCursor}`
    - 400 if `entityKey` is missing or `from`/`to` are not ISO timestamps
    - The defaults are `to` = now and `from` = `to` minus 1 h.
  - `GET /correlations/:correlationId` → `{correlationId, versions}`, or 404
- **3PC routes:**
  - `POST /tpc/:txId/can-commit`, body `{payload}` → `{vote}`
  - `POST /tpc/:txId/pre-commit` → `{ack:true}`
  - `POST /tpc/:txId/do-commit` → `{ack:true}`
  - `POST /tpc/:txId/abort` → `{ack:true}`
  - `TpcStateError` → 409 `{error:"bad_state"}`.
- **Payload:** `{correlationId, version, incidentId, alertIds, decision}`.
  - `decision` is `{kind:"create"|"update", severity, newAlertIds, firstSeen, lastSeen}`.
  - `validate` requires these fields, with `version` as a positive integer.
  - `apply` is `putCorrelation({...payload, txId, recordedAt})`.
- **Metric:** `tpc_participant_messages_total{phase,result}`.
- **Server wiring:** `server.js` runs `ensureLocalResources()`, then the consumers and Fastify. It runs `participant.sweep()` every 2 s and stops cleanly on SIGTERM.
- **Readiness:** `/ready` is true once the consumers are connected and `DescribeTable` succeeds on the events table.

- [ ] **Step 1: Write failing tests:**
  - `GET /events/:id 404`
  - `GET /events requires entityKey (400)`
  - `GET /events by IPv6 entityKey`: `2001%3Adb8%3A%3A1` reaches the repo as `"2001:db8::1"`
  - `GET /correlations with encoded id`: `cor-%7Cx` reaches the repo as `cor-|x`
  - `can-commit, pre-commit, do-commit stores correlation once`
  - `pre-commit unknown tx → 409`
  - `do-commit after abort → 409`
  - `TPC_FORCE_VOTE=no → vote no`
- [ ] **Step 2:** Run `npm test -w event-store`. Expected: FAIL.
- [ ] **Step 3:** Implement the routes, the server and the Dockerfile (`EXPOSE 8081`).
- [ ] **Step 4:** Run `npm test -w event-store && docker build -f services/event-store/Dockerfile -t event-store:dev .`. Expected: PASS, and the image builds.
- [ ] **Step 5:** Commit `feat(event-store): rest api, 3pc participant routes, dockerfile`.

### Task 6: Incident Management storage and 3PC coordinator wiring

**Files:**
- Create: `services/incident-management/{package.json,.env.example}`, `services/incident-management/migrations/001_init.sql`
- Create: `services/incident-management/src/{config.js,db.js,migrate.js,incidents.js,tpc-wiring.js,publisher.js}`
- Test: `services/incident-management/test/{merge.test.js,incidents.test.js}`

**Interfaces:**
- **Config:**

  | Env var | Default |
  |---|---|
  | `DATABASE_URL` | `postgres://incidents:incidents@localhost:5432/incidents`. In AWS, use the `PG*` vars instead. |
  | `DATABASE_SSL` | `false` |
  | `PORT` | 8082 |
  | `GRPC_PORT` | 50051 |
  | `EVENT_STORE_URL` | `http://localhost:8081` |
  | `INCIDENT_TOPIC` | incident-updates |
  | `TPC_TIMEOUT_MS` | 3000 (coordinator, per phase) |
  | `TPC_PARTICIPANT_TIMEOUT_MS` | 10000 |
  | `CORRELATION_WINDOW_MS` | 900000 |
  | `TPC_FAULT` | unset, or `crash-after-precommit` |

  Plus the `KAFKA_*` variables.
- **Schema (`001_init.sql`):**
  - `incidents`:
    - `incident_id uuid PK`, `correlation_id text UNIQUE NOT NULL`, `entity_key text`, `title text`
    - `severity text CHECK (severity IN ('low','medium','high','critical'))`
    - `status text NOT NULL DEFAULT 'open' CHECK (status IN ('open','investigating','resolved','false_positive'))`
    - `alert_count int`, `event_count int`, `rule_ids text[]`, `related_event_ids text[]`, `entity jsonb`, `enrichment jsonb`
    - `first_seen timestamptz`, `last_seen timestamptz`, `version int NOT NULL`, `created_at timestamptz`, `updated_at timestamptz`
  - `incident_alerts(incident_id, alert_id)`, with that pair as the PK
  - `incident_history(id bigserial PK, incident_id, tx_id text UNIQUE NULL, change jsonb, actor text, at timestamptz)`
  - `tpc_participant(tx_id text PK, state text, payload jsonb, updated_at timestamptz)`
  - `tpc_coordinator(tx_id text PK, state text, payloads jsonb, updated_at timestamptz)`
  - Index on `incidents(entity_key, status, last_seen)`
- **`incidents.js`:**
  - `mergeCandidate(existing|null, knownAlertIds: Set, candidate) → {kind:"create"|"update"|"noop", incident, newAlertIds}`. A pure function.
    - **create:** gets a UUID v4 and `version` 1.
    - **update:** takes the union of `alertIds` and `ruleIds`, the max severity, the min `firstSeen`, the max `lastSeen`, `eventCount = max(existing, candidate)`, and `relatedEventIds` as the union capped at 200. It sets `version + 1` and never touches `status`.
    - **noop:** every candidate alert ID is already known.
  - `findTarget(candidate) → {existing, knownAlertIds}`. Looks up by `correlationId` first. Otherwise it takes the open or investigating incident with the same `entityKey` whose `lastSeen >= candidate.firstSeen - CORRELATION_WINDOW_MS`.
  - `listIncidents({status, severity, limit=50})`
  - `getIncident(id)`
  - `updateStatus(id, {status, version, actor, note}) → Incident`. Throws `VersionConflict`.
- **`tpc-wiring.js`:** `upsertIncident(candidate) → {incident, outcome:"created"|"updated"|"noop"}`.
  1. Calls `findTarget`, then `mergeCandidate`. A **noop** returns the existing incident without 3PC.
  2. Otherwise it runs `coordinator.run(randomUUID(), payloads)`. The Event Store payload always uses the **incident's stored `correlationId`**.
  3. **Participants:**
     - **`incident-db`:** a local `createParticipant` over `tpc_participant`.
       - `validate` votes **no** if another `tpc_participant` row in UNCERTAIN or PRECOMMITTED has the same `payload->>'correlationId'`.
       - `apply` runs one pg transaction. A create is an `INSERT`. An update runs `UPDATE … WHERE incident_id=$1 AND version=$2-1` and throws if 0 rows are affected. It then inserts alerts with `ON CONFLICT DO NOTHING` and inserts history with `tx_id`.
     - **`event-store`:** an HTTP adapter over Task 5's routes. A 409 maps to `TpcStateError`.
  4. **committed:** `onCommitted` publishes. **aborted:** throws `IncidentAborted(reason)`.
  - `CoordinatorCrash` → log it and `process.exit(1)`. The restart runs `recover()`.
  - `recover()` and its `onCommitted` also publish.
- **`publisher.js`:** `publishIncidentUpdate(type, incident)`.
  - **Message:** `{schemaVersion:"1.0", type:"incident.created"|"incident.updated"|"incident.status_changed", incident, changedAt, producedBy:{name:"incident-management",version:"1.0.0"}}`
  - **Key:** `incidentId`.
- **Metric:** `tpc_transactions_total{outcome}`.

- [ ] **Step 1: Write failing `merge.test.js`:**
  - `create when no existing`
  - `update unions alerts and bumps version`
  - `update never changes status`
  - `severity takes max`
  - `upsert with only known alertIds is a no-op`
- [ ] **Step 2: Write failing `incidents.test.js`** (INTEGRATION, using compose Postgres and a fake event-store participant):
  - `migrate is idempotent`
  - `upsertIncident commits to postgres and event-store and publishes incident.created`
  - `event-store votes no → no row, IncidentAborted("vote_no")`
  - `TPC_FAULT crash then recover() commits and publishes`
  - `concurrent upsert for same correlationId votes no`: hold tx A in PRECOMMITTED, then upsert B is aborted with `vote_no`
  - `stale apply version rejected`
  - `findTarget falls back to open incident for same entity inside window`
  - `updateStatus stale version throws VersionConflict`
- [ ] **Step 3:** Run `npm test -w incident-management`, then `INTEGRATION=1 npm test -w incident-management`. Expected: FAIL.
- [ ] **Step 4: Implement.** `migrate.js` applies `migrations/*.sql` in order, records them in `schema_migrations`, and is idempotent. When `DATABASE_SSL=true`, `db.js` passes `ssl: { ca: fs.readFileSync(DATABASE_SSL_CA) }` and keeps certificate verification on. **Never set `rejectUnauthorized: false`.** `DATABASE_SSL_CA` defaults to `/etc/ssl/rds/global-bundle.pem`. The incident-management Dockerfile puts the AWS RDS CA bundle there with `ADD https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /etc/ssl/rds/global-bundle.pem`. Add `DATABASE_SSL_CA` to the Config table and to the handoff.
- [ ] **Step 5:** Run both test commands. Expected: PASS.
- [ ] **Step 6:** Commit `feat(incident-management): postgres store, 3pc coordinator, incident-updates`.

### Task 7: Incident Management gRPC and REST APIs

**Files:**
- Create: `proto/incident/v1/incident.proto`, `services/incident-management/src/{grpc.js,routes.js,server.js}`, `services/incident-management/Dockerfile`
- Test: `services/incident-management/test/{grpc.test.js,routes.test.js}`

**Interfaces:**
- **`incident.proto`:**
  - package `incident.v1`
  - `service IncidentService { rpc UpsertIncident(UpsertIncidentRequest) returns (Incident); rpc GetIncident(GetIncidentRequest) returns (Incident); }`
  - `UpsertIncidentRequest { string correlation_id=1; string entity_key=2; string title=3; string severity=4; repeated string alert_ids=5; repeated string rule_ids=6; string first_seen=7; string last_seen=8; int32 event_count=9; repeated string related_event_ids=10; string entity_json=11; string enrichment_json=12; }`
  - `GetIncidentRequest { string incident_id=1; }`
  - `Incident { string incident_id=1; string correlation_id=2; string entity_key=3; string title=4; string severity=5; string status=6; int32 alert_count=7; int32 event_count=8; string first_seen=9; string last_seen=10; int32 version=11; string created_at=12; string updated_at=13; string outcome=14; }`
  - `outcome` is set only by UpsertIncident.
  - Load with proto-loader using `{keepCase:false, longs:String, defaults:true}`.
- **`grpc.js`:** `startGrpc({port, upsertIncident, getIncident}) → grpc.Server`. Errors map as follows:
  - `IncidentAborted` → `ABORTED`
  - validation errors (missing `correlationId`, bad severity) → `INVALID_ARGUMENT`
  - not found → `NOT_FOUND`
  - anything else → `INTERNAL`
- **REST routes** (camelCase JSON):
  - `GET /incidents?status=&severity=&limit=` → `{items}`
  - `GET /incidents/:id` → 200, or 404
  - `PATCH /incidents/:id`, body `{status, version, actor?, note?}` → 200 (and publishes `incident.status_changed`), 409 `{error:"version_conflict"}` or 400
  - `GET /tpc/transactions?limit=20&correlationId=` → the latest `tpc_coordinator` rows
  - Status changes are plain Postgres updates. Only correlation decisions use 3PC, and the design doc says so.
- **Server wiring:** `server.js` runs migrations, then `coordinator.recover()`, then starts gRPC, REST and a `participant.sweep()` every 2 s.

- [ ] **Step 1: Write failing tests:**
  - `grpc UpsertIncident round-trip`: port 0, a fake `upsertIncident`, and camelCase fields including `outcome`
  - `grpc maps IncidentAborted to ABORTED`
  - `grpc rejects missing correlationId with INVALID_ARGUMENT`
  - `PATCH with stale version returns 409`
  - `PATCH invalid status returns 400`
  - `GET /incidents filters by status`
- [ ] **Step 2:** Run `npm test -w incident-management`. Expected: FAIL.
- [ ] **Step 3:** Implement the APIs and the Dockerfile (`EXPOSE 8082 50051`).
- [ ] **Step 4:** Run `npm test -w incident-management && docker build -f services/incident-management/Dockerfile -t incident-management:dev .`. Expected: PASS, and the image builds.
- [ ] **Step 5:** Commit `feat(incident-management): grpc and rest apis, dockerfile`.

### Task 8: Correlation Engine

**Files:**
- Create: `services/correlation-engine/{package.json,.env.example,Dockerfile}`, `services/correlation-engine/src/{config.js,correlator.js,event-store-client.js,incident-client.js,consumer.js,server.js}`
- Test: `services/correlation-engine/test/{correlator.test.js,consumer.test.js}`

**Interfaces:**
- **Config:**

  | Env var | Default |
  |---|---|
  | `PORT` | 8083 |
  | `KAFKA_GROUP_ID` | correlation-engine-cg |
  | `ALERT_TOPIC` | security-alerts |
  | `INCIDENT_GRPC_ADDR` | localhost:50051 |
  | `EVENT_STORE_URL` | `http://localhost:8081` |
  | `CORRELATION_WINDOW_MS` | 900000 |
  | `ESCALATE_ALERT_COUNT` | 3 |
  | `ENRICH_TIMEOUT_MS` | 2000 |
  | `GRPC_DEADLINE_MS` | 15000 |

  Plus the `KAFKA_*` variables.
- **`correlator.js`:** `createCorrelator({windowMs, escalateCount}) → { ingest(alert) → Candidate|null, forget(alertId) }`.
  - State is per `entityKey`: `{correlationId, alerts: Map, firstSeen, lastSeen, windowStart, windowEnd}`.
  - **New group:** an alert whose `alertTime` is more than `windowMs` after `lastSeen` starts a new group. Out-of-order alerts inside the window join the group and may lower `firstSeen`.
  - **Correlation ID:** `"cor-" + sha256(entityKey + "|" + firstAlertId).hex.slice(0,24)`
  - **Duplicate `alertId`:** returns `null`.
  - **`forget(alertId)`:** removes the alert, and removes the group if it becomes empty.
  - **Severity:** the max of the alert severities (low < medium < high < critical), raised to `critical` at `escalateCount` or more alerts, or with 2 or more distinct `ruleId`s.
  - **Title:** `` `${alertCount} correlated alert(s) for ${entityKey}` ``.
  - No idle eviction. This is out of scope for the demo.
- **`Candidate`:**
  - `{correlationId, entityKey, title, severity, alertIds, ruleIds, firstSeen, lastSeen, windowStart, windowEnd, eventCount, relatedEventIds, entity}`
  - `windowStart` and `windowEnd` are the min of `alert.window.start` and the max of `alert.window.end`.
  - `eventCount` is the sum of the alerts' `eventCount`.
  - `relatedEventIds` is the union, capped at 200.
- **`event-store-client.js`:** `fetchEnrichment(entityKey, windowStart, windowEnd) → {storedEventCount, distinctUsers, failures}|null`.
  - Pages `GET /events` with URL-encoded params, up to 500 items.
  - Returns `null` on timeout or error.
- **`incident-client.js`:** `upsertIncident(candidate, enrichment) → Incident` (with `outcome`). Uses a `GRPC_DEADLINE_MS` deadline.
- **`consumer.js`:** `handleAlert(message, deps)`.
  1. Parses the message. Invalid JSON is logged and skipped.
  2. Calls `ingest`. A `null` result is skipped.
  3. Calls `fetchEnrichment`, then `upsertIncident`.
  4. **On any gRPC error except `INVALID_ARGUMENT`:** call `correlator.forget(alert.alertId)`, then **rethrow**, so kafkajs retries and the retry re-ingests the alert. `INVALID_ARGUMENT` is logged and skipped.
  - **Metrics:** `alerts_consumed_total`, `incidents_upserted_total{outcome}`.

- [ ] **Step 1: Write failing `correlator.test.js`.** Fixtures use the `security-alerts` shape from the brief.
  - `two alerts same entity inside window share correlationId`
  - `alert after window starts new correlationId`
  - `correlator ignores duplicate alertId`
  - `forget lets the same alert be ingested again`
  - `out-of-order alert inside window lowers firstSeen`
  - `window spans alert.window start/end`
  - `escalates to critical at escalateCount alerts`
  - `escalates to critical with two distinct ruleIds`
  - `different entities never share a group`
- [ ] **Step 2: Write failing `consumer.test.js`** with fakes:
  - `enrichment failure yields null enrichment`
  - `failed upsert forgets alert so retry re-sends`: the first call throws `UNAVAILABLE`, and handling the same message again calls `upsertIncident` again
  - `INVALID_ARGUMENT is skipped without throw`
  - `invalid json skipped without throw`
- [ ] **Step 3:** Run `npm test -w correlation-engine`. Expected: FAIL.
- [ ] **Step 4:** Implement the engine and the Dockerfile (`EXPOSE 8083`).
- [ ] **Step 5:** Run `npm test -w correlation-engine && docker build -f services/correlation-engine/Dockerfile -t correlation-engine:dev .`. Expected: PASS, and the image builds.
- [ ] **Step 6:** Commit `feat(correlation-engine): alert correlation, enrichment, grpc upsert`.

### Task 9: End-to-end run in Docker Compose, final handoff and 3PC demo

**Files:**
- Modify: `docker-compose.yml` (add the 3 services built from their Dockerfiles, with compose hostnames in env and `depends_on: condition: service_healthy`)
- Create: `test/e2e/pipeline.test.js`, `scripts/demo-3pc.sh`
- Modify: `docs/handoff.md`

- [ ] **Step 1: Write the e2e test** (`INTEGRATION=1`, broker `localhost:29092`).
  - **Setup:** each run uses a **random** `entityKey` (`198.51.100.<rand>`) and fresh UUIDs.
  - **Inputs:** one `raw-events` message, two `normalized-events` messages, and two `security-alerts` messages for the entity, with different `alertId`s and `alertTime`s 30 s apart.
  - **Assertions** (poll up to 30 s):
    - (a) `GET :8082/incidents?status=open` has exactly 1 incident with that `entityKey` and `alertCount: 2`
    - (b) `GET :8081/correlations/<correlationId>` has 2 versions
    - (c) the MinIO object for the raw `eventId` exists
    - (d) `incident-updates` shows `incident.created`, then `incident.updated`, for that `incidentId`
    - (e) `GET :8082/tpc/transactions?correlationId=<id>` shows 2 `COMMITTED`
  - **Second case, `redelivered alert is a no-op`:** re-send alert 1, wait 5 s, and expect `version` unchanged.
- [ ] **Step 2:** Run `docker compose up -d --build && npm run test:integration`. Expected: PASS.
- [ ] **Step 3: Write `scripts/demo-3pc.sh`.** It walks through the three scenarios on Compose, printing `GET /tpc/transactions` after each:
  1. normal commit
  2. restart event-store with `TPC_FORCE_VOTE=no` and send an alert → aborted, and no incident
  3. restart incident-management with `TPC_FAULT=crash-after-precommit` and send an alert → the container exits; restart it without the fault, and `recover()` commits
  - Run it once and paste the output into `docs/handoff.md`.
- [ ] **Step 4:** Finalize `docs/handoff.md` (remove "draft"). Add these demo notes:
  - Upstream has one rule (`RULE-AUTH-001`) and suppresses the same entity for 60 s, so live escalation needs 3 alerts more than 60 s apart.
  - Demo the 3PC fault cases on Compose, not through GitOps.
- [ ] **Step 5:** Commit `test: end-to-end pipeline; docs: final handoff and 3pc demo`.

### Task 10: Design-doc section, task division and the group-repo copy (local only)

**Files:**
- Create: `docs/design/core-services.md`, `docs/task_division_rishit.txt`

- [ ] **Step 1: Write `docs/design/core-services.md`.**
  - **Responsibilities** of each service.
  - **A Mermaid diagram:** security-alerts → correlation-engine → gRPC → incident-management ⇄ 3PC ⇄ event-store, plus the Kafka topics and S3/DynamoDB/RDS.
  - **Interconnections:** Kafka, REST and gRPC, and why each one fits where it is used.
  - **Storage:** why there are three storage products (requirement g).
  - **3PC:**
    - states, timeouts and recovery
    - why 3PC instead of 2PC: a PRECOMMITTED participant can commit on its own, so a coordinator crash doesn't block
    - known limits: a network partition can split the decision; status PATCHes are plain local updates; the coordinator runs as a single replica
  - **Verify:** the Mermaid code renders on GitHub.
- [ ] **Step 2:** Write `docs/task_division_rishit.txt` with the line `<ID no.> | Rishit | Services: Correlation Engine, Incident Management, Event Store | Requirements: f (3PC), g (3 storage products)`. Leave the ID number for Rishit to fill in.
- [ ] **Step 3: Prepare the group-repo copy, local only.**
  1. Run `git clone --depth 1 https://github.com/Cloud-Lab-Assignment/Cloud-Security-Ingester /tmp/csi` and create the branch `rishit/core-services`.
  2. Copy the workspace into `/tmp/csi/services/core/`: `package.json`, `package-lock.json`, `.dockerignore`, `.env.example`, `packages/`, `proto/`, `services/`, `infra/terraform/` (as `infra/`), `docs/handoff.md` and `docs/design/`. **Do not copy** `docs/project-brief.md` or `docs/superpowers/`.
  3. In the copy, update the build commands in `handoff.md` to use the context `services/core`.
  4. Run `cd /tmp/csi/services/core && npm ci && npm test` and the three `docker build` commands.
  5. Commit on the local branch.
  - **Never `gh repo fork`, push or open a PR.** Tell Rishit the branch is ready, so he can push it and open the PR against the original repo's `main`.
- [ ] **Step 4:** Commit `docs: design section and task division for core services` in this repo.

### Task 11 (skip unless Rishit confirms ownership): Investigation Query API

This is a read-only Fastify service on `8084`:
- `GET /investigations/:incidentId` joins the incident (from Incident Management REST), its correlation versions and its related events (both from the Event Store), over the incident's window.
- Tests: `investigation joins incident, correlations and events` (fakes) and `missing incident → 404`.
- It follows the same Dockerfile and handoff pattern.
