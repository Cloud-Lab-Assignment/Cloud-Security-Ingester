# Review fixes (16 Nov 2026)

Three independent reviews of `main@d5f34c9` (GPT-6.1 Sol: 3PC correctness, data path, deployability). The main claims were spot-checked against the code. Work through this list in order. The deadline is **Wed 18 Nov 2026, 23:59 IST**, so P0 and P1 come first; P2 can stay as documented limits if time runs out.

## Working rules (same as the plan)

- Work on `main`. Make one commit per item, written as `fix(<scope>): …` and referencing the item ID (e.g. `P0-2`).
- **Do not install, upgrade or uninstall software without asking Rishit first.** This includes `npm install <new-pkg>`, apt packages and new Docker images. `npm ci` with the existing lockfile is fine. No new npm dependencies are needed for anything below.
- **Never act on the group repo** `Cloud-Lab-Assignment/Cloud-Security-Ingester`: no clone with push, no fork, no PR.
- Terraform: `fmt` and `validate` only. Never `plan`, `apply` or `destroy`.
- **TDD:**
  - For every behaviour fix, first add a test that fails on the current code, then fix the code.
  - Run `npm test` after each item.
  - After P0 and P1, also run `INTEGRATION=1 npm test -w event-store -w incident-management`, `npm run test:integration`, and `bash scripts/demo-3pc.sh` on the Compose stack.
- When all P0 and P1 items pass, update `docs/handoff.md` (the recorded demo output too) and `docs/design/core-services.md`. Then **push to `origin main`** and append a short "Done" log at the bottom of this file.

---

## P0: demo and viva critical

### P0-1 `scripts/demo-3pc.sh` only runs on Linux
- **Problem:** line 25 uses GNU `date -d @…` and line 43 reads `/proc/sys/kernel/random/uuid`. Both fail on macOS, which is where Rishit may demo.
- **Fix:** generate UUIDs and ISO timestamps with `node -e` (`crypto.randomUUID()`, `new Date(s*1000).toISOString()`). Node is already required.
- **Also fix these in the same script:**
  - **Reused entities.** It picks a random entity from 254 values, so a re-run can hit an entity that already has an incident, and "expect none" or `wait_incident` gives wrong results. Generate a unique entity per run (e.g. a random `198.18.x.y`), then assert that `GET /incidents?entityKey=` is empty before sending.
  - **No cleanup trap.** Add a `trap … EXIT` that clears `ES_TPC_FORCE_VOTE` and `IM_TPC_FAULT` and recreates event-store and incident-management normally, so a mid-script failure never leaves the stack faulted.
  - **Scenario 3 proves less than it claims.** Before restarting incident-management, poll the Event Store's `GET /correlations/:id` (or its tpc record) until its PRECOMMITTED timeout (10 s) has self-committed. Print that state. This shows the non-blocking 3PC property, not just coordinator replay.

### P0-2 PATCH races a 3PC update and makes Postgres reject a decided commit
- **Where:** `services/incident-management/src/incidents.js:187` (`updateStatus`) and `src/tpc-wiring.js:~195` (`validateDecision`).
- **Sequence:**
  1. `canCommit` reads incident version N with no lock.
  2. PATCH runs: no pending participant row exists yet, so it bumps the version to N+1 and commits.
  3. The participant records UNCERTAIN, both participants pre-commit, and the Event Store commits.
  4. The Postgres apply throws `StaleVersion` forever.
- **Fix:** serialise per incident. Take `pg_advisory_xact_lock(hashtext(correlationId))` inside one Postgres transaction in both places:
  - **PATCH:** take the lock, then check pending rows and update.
  - **incident-db participant canCommit:** take the lock, validate, and insert the UNCERTAIN `tpc_participant` row in the same transaction.

  This needs the Postgres participant store's `canCommit` path to run validate and record together, through a `validateAndReserve` hook or by doing it in `createIncidentDbParticipant`.
- **Test:** an integration test that runs PATCH between validate and record. It must get 409 `transaction_pending`, or the vote must be no. A decided transaction must never fail its apply.

### P0-3 Event Store 3PC state writes are unconditional, so replicas race
- **Where:** `services/event-store/src/dynamo.js:175` (`tpcStore.put` is a plain `PutCommand`).
- **Problem:** with two or more replicas, one replica's sweeper can overwrite a newer `PRECOMMITTED` written by another replica with `ABORTED`. The handoff allows the event-store to scale to 1..3 pods.
- **Fix:**
  - Give `put` an expected previous state: `ConditionExpression: attribute_not_exists(txId) OR #s = :expected`.
  - Make `packages/tpc/src/participant.js` pass the state it read and treat a `ConditionalCheckFailed` as "re-read and re-evaluate".
  - Add a test using the in-memory store, which honours the same contract.
- **Fallback if short on time:** set the event-store to **exactly 1 replica, no HPA** in `docs/handoff.md`. Then requirement c's HPA rests on correlation-engine plus one other stateless service, so tell Ansh.

### P0-4 Docs over-claim 3PC atomicity
- **Where:** `docs/design/core-services.md`, the "Timeouts and recovery" and "Known limits" sections.
- **Fix:** state precisely when the guarantee holds.
  - It holds for clean crash failures with failure detection.
  - It can split in these cases:
    - **(a) Participant crash mid-preCommit.** A participant persists PRECOMMITTED and crashes before acking. The coordinator aborts the other side. The crashed participant restarts and self-commits on its timeout.
    - **(b) Network partition**, which is already listed.
  - Name the textbook remedy: a termination protocol where participants consult each other before acting on a timeout, or a consensus-based commit.
- **Mitigation to implement (cheap):** on startup, before the sweeper runs, a participant should first ask the coordinator for its decision. Add `GET /tpc/transactions/:txId` on incident-management and have the event-store query it for PRECOMMITTED and UNCERTAIN records older than the timeout. Only if the coordinator is unreachable does it fall back to its timeout rule. This closes case (a) whenever the coordinator is alive.

### P0-5 Fill in `docs/task_division_rishit.txt`
- **Problem:** it still says `<ID no.>`.
- **Fix:** leave a clear `TODO(Rishit): ID number` and don't guess. Rishit fills it in.

---

## P1: correctness before integration

### P1-1 Coordinator marks COMMITTED and publishes even if doCommit was never acknowledged
- **Where:** `packages/tpc/src/coordinator.js:69` (`commitAll` ignores `sendWithRetry`'s unacked list).
- **Fix:** if any participant is unacked after the retries, leave the log entry as `PRECOMMITTED`. The decision is still commit, and recovery finishes it. Don't call `onCommitted` yet. Return `{outcome:'committed', pending:[…]}` so the gRPC caller still gets success.

### P1-2 Recovery runs only once, at startup
- **Where:** `services/incident-management/src/server.js:71`.
- **Problem:** a transaction deferred because a participant was unreachable is never retried until the next restart.
- **Fix:** run `recover()` on an interval (e.g. every 15 s) after startup. Skip any transaction that `run()` is currently driving: track in-flight txIds in a Set. Add a test.

### P1-3 incident-updates can be lost (3PC commits and PATCH)
- **Where:** `tpc-wiring.js:249` (`onCommitted`) and `routes.js:83` (PATCH).
- **Problem:** both publish after the DB commit and only log failures. A crash or Kafka error loses the message forever.
- **Fix:** a transactional outbox.
  - Add an `outbox` table (`id bigserial`, `type`, `incident_id`, `payload jsonb`, `created_at`, `published_at null`).
  - Insert into it inside the same Postgres transaction as the 3PC apply (`applyDecision`) and as `updateStatus`. Store the snapshot at that moment.
  - A loop publishes unpublished rows in id order, every 1 s, and marks them published.
  - Delete the direct publish calls.
- **Migration:** `002_outbox.sql`.

### P1-4 One malformed alert stops the correlation-engine consumer
- **Where:** `services/correlation-engine/src/consumer.js:52` and `correlator.js:49`.
- **Problem:** a wrong-typed field throws outside the try, kafkajs crashes the consumer, and the HTTP server stays up while consumption is dead.
- **Fix:**
  - Validate the alert shape (types of `alertId`, `entityKey`, `severity`, `alertTime`, `window`, `eventCount`, `relatedEventIds`) before ingesting.
  - On failure, log it, count it in `alerts_consumed_total{result="invalid"}`, and skip, resolving the offset.
  - Also catch any unexpected error from `ingest()` the same way.

### P1-5 correlation-engine `/ready` stays 503 after kafkajs auto-recovers
- **Where:** `consumer.js:106`.
- **Fix:** track `GROUP_JOIN` (sets ready), and `CRASH`, `STOP` and `DISCONNECT` (clear it), the way the event-store already does.

### P1-6 Fallback merge crosses unrelated windows
- **Where:** `services/incident-management/src/incidents.js:137` (`findTarget`).
- **Fix:** also require `first_seen <= candidate.lastSeen + window`, not only `last_seen >= candidate.firstSeen - window`. Add a test with an alert from yesterday.

### P1-7 Group-repo port won't build
- **Problem:** this is the integration blocker. The Dockerfiles need the workspace root (root `package.json`, the lockfile, `packages/`, `proto/`). The group repo's services are each self-contained under `services/<name>/`, built with that directory as the context.
- **Fix:** add `scripts/export-group-repo.sh <dest>`. For each of the three services, it writes a **self-contained** `<dest>/services/<name>/` containing:
  - the service `src/` and `test/`
  - `lib/common/` and (where used) `lib/tpc/`, vendored from `packages/`, with imports rewritten from `@core/common` and `@core/tpc` to relative paths
  - `proto/` (incident-management and correlation-engine)
  - a merged `package.json` (the service's dependencies plus those of the vendored packages)
  - a Dockerfile whose build context is the service directory
  - the `.env.example`

  Generate each `package-lock.json` with `npm install --package-lock-only` inside the exported directory; **ask Rishit before running it**, because it downloads registry metadata. Verify with `docker build` on each exported directory and run its tests. **Do not push anything to the group repo.**
- **Docs:** describe the result in `docs/handoff.md` ("Porting to the group repo"). Rishit opens the PR after agreeing on it with Ansh.

### P1-8 Handoff and how-to-run accuracy
- **Where:** `docs/handoff.md`.
- **Fixes:**
  - **Fresh-clone steps:** Node 22, then `npm ci`, then `docker compose up -d --build`, then wait for `docker compose ps` to show healthy, then the tests, then the demo. Prerequisites are `bash`, `curl` and `jq`.
  - **Startup:** incident-management doesn't *listen* until recovery finishes, so probes see connection refused, not 503. Either start the HTTP server first with `/ready` returning 503 until recovery is done (preferred, and matches the doc), or fix the doc.
  - **Undocumented:** add `PROTO_PATH`, and note that `DATABASE_URL` is used only when `PGHOST` is unset.
  - **Requirement h:** state that services emit metrics and logs, and that dashboards, scraping and log shipping are Ansh's integration work.

### P1-9 Add the deployment diagram to the design doc
- **Where:** `docs/design/core-services.md`.
- **Fix:** add a Mermaid deployment diagram covering:
  - the EKS boundary, Argo CD and the GitHub repo
  - the three Deployments with replica and HPA notes
  - IRSA for the event-store to DynamoDB and S3
  - private RDS with SSL and the Secrets Manager secret synced into a K8s Secret
  - Confluent Kafka over SASL_SSL
  - Prometheus and Grafana scraping `/metrics`

---

## P2: if time allows (otherwise list under "Known limits" in the design doc)

- **P2-1 Duplicate incident after restart + resolve + redelivery** (`incidents.js:131`, `001_init.sql:28`). Make `alert_id` globally unique (`incident_alerts` unique on `alert_id`). In `findTarget`, look up an existing owner by alertId first; if every alert is already owned, return noop.
- **P2-2 Escalation and event totals are lost after a correlation-engine rebalance** (`correlator.js:124`, `incidents.js:77`).
  - Store per-alert `severity`, `rule_id` and `event_count` in `incident_alerts`.
  - In `mergeCandidate`, derive `alertCount`, `eventCount` and severity escalation (≥3 alerts or ≥2 distinct rules gives critical) from the merged set.
- **P2-3 Oversized `entityKey`** (upstream allows a 3000-byte `source.name`). DynamoDB rejects it, and `consumers.js:74` drops the event silently.
  - Store long keys as `h:` + sha256 and use the same mapping in `GET /events?entityKey=`.
  - Never drop silently: log at error level with the topic, partition and offset, and count it.
- **P2-4 Storage calls without deadlines** (`consumers.js:52`). Set an AWS SDK `requestTimeout` and `maxAttempts` below the Kafka session timeout. Call `heartbeat()` between retries.
- **P2-5 Correlator memory grows without bound** (`correlator.js:68`). Evict groups whose window closed more than 2 windows ago, plus their alertIds. P2-1 makes durable dedupe the safety net.
- **P2-6 Invalid cursor returns 500** (`dynamo.js:42`). Require exactly the string fields `{entityKey, sk}`, and require the `entityKey` to equal the query's. Otherwise return 400.
- **P2-7 Pin the base images** (`node:22-alpine`, `postgres:16-alpine`) by digest. Verify the RDS CA bundle with a pinned sha256 and document how to refresh it.
- **P2-8 e2e fixture** uses `userType:"user"`, which the upstream schema doesn't allow. Use a valid enum value.

## Done log

<!-- The box agent appends: item ID, commit SHA, test evidence. -->
