# Review fixes, round 2 (16 Nov 2026)

Three independent re-reviews (GPT-6.1 Sol) of the round-1 fixes in `e2443bb..ea2e27d`. The blocker R2-1 was found by two reviewers independently, and one of them reproduced it with a Node test. R2-1, R2-3 and R2-4 were spot-checked against the code.

**The working rules from `docs/review-fixes-2026-11-16.md` still apply:**
- one commit per item, with the item ID in the message
- TDD: a failing test first
- no installs without asking Rishit
- never touch the group repo
- Terraform `fmt` / `validate` only

When R2-1 to R2-5 pass, run the full suite and the demo, update the docs, push to `origin main`, and append a Done log here. Deadline: **Wed 18 Nov 2026, 23:59 IST**.

Round-1 verdicts (static review):
- **Fixed:** P0-1, P0-2, P0-3, P0-5, P1-1, P1-4, P1-5, P1-6, P1-7, P1-9, P2-4, P2-7, P2-8.
- **Partially fixed:** P0-4, P1-2, P1-3, P1-8, P2-1, P2-3, P2-5, P2-6. These are covered below.

---

## Blocker

### R2-1 Recovery acts on a stale snapshot and can reverse a commit
- **Where:** `packages/tpc/src/coordinator.js:155` (`recoverOnce`), called periodically by `services/incident-management/src/recovery.js`.
- **Sequence:**
  1. `listUnfinished()` returns tx B as `WAITING`.
  2. While recovery works on an earlier entry, `run()` takes B to a commit decision. One participant commits, but the other never acknowledges doCommit, so `run()` returns `{outcome:'committed', pending:[…]}` and leaves `inFlight`.
  3. Recovery reaches B, trusts the stale `WAITING`, logs `ABORTED` and aborts the peer.

  The result is an atomicity split, and the decision endpoint then reports ABORTED.
- **Fix:**
  - Serialise `run()` and recovery **per transaction**: an in-process per-txId mutex, held by `run()` for its whole duration and by recovery while it handles that txId.
  - Inside the mutex, **re-read the entry from the durable log** (add `log.get(txId)` to the log contract, plus the Postgres implementation) and act on its *current* state.
  - Skip terminal states (COMMITTED, ABORTED). Never write ABORTED over a log entry that is PRECOMMITTED or later.
- **Test:** recovery snapshots B as WAITING, then B completes through `run()` (committed with pending) before recovery reaches it. B must not be aborted; the next recovery finishes the commit.

## Major

### R2-2 Participant apply and its COMMITTED state aren't atomic
- **Where:** `packages/tpc/src/participant.js:74`.
- **Sequence:**
  1. Replica A reads `PRECOMMITTED` and applies the business write.
  2. Before A records `COMMITTED`, replica B (or a timeout path) records `ABORTED`.
  3. A's conditional write fails and it throws.

  The business write stays even though the participant is ABORTED.
- **Fix:** give the store contract an atomic `commit(txId, expectedState, payload)` that does the business write and the state transition together:
  - **Event Store:** DynamoDB `TransactWriteItems`, combining the correlation-history `Put` (with `attribute_not_exists`) and the tpc `Update` with `ConditionExpression #s = :expected`.
  - **incident-db:** run `applyDecision` and the `tpc_participant` state update in **one** Postgres transaction, with `UPDATE … WHERE state = $expected`.

  The in-memory test store implements the same contract.
- **Test:** the round-2 reviewer's race: one side applies while the other aborts. Afterwards there must be no business write without COMMITTED, and no COMMITTED without a business write.

### R2-3 "Coordinator unreachable" fallback is too broad
- **Where:** `services/event-store/src/coordinator-client.js:27` and `packages/tpc/src/participant.js:133`. A 404, a 5xx, malformed JSON, an unknown state and a 2 s timeout all return `null`, which triggers the unilateral timeout rule.
- **Fix:** return a tagged result:
  - **Transport failure** (connection refused, DNS, socket error) → `unreachable`: use the timeout rule.
  - **404:** the coordinator logs WAITING before any canCommit, so a 404 means it never started the transaction.
    - For an `UNCERTAIN` participant → abort.
    - For a `PRECOMMITTED` participant → keep pending and log at error level. It can't happen with an intact log.
  - **5xx, malformed body or unknown state** → `pending`: keep the state and retry on the next sweep.
  - **Lookup timeout:** keep the current behaviour (fall back), but **document** that a slow-but-alive coordinator can still cause a split. Name it as the remaining 3PC limit; a termination protocol or consensus is needed to remove it.
- **Test:** cover each response class.

### R2-4 Migration 003 can't run on a database that already has duplicates
- **Where:** `services/incident-management/migrations/003_alert_unique.sql`.
- **Problem:** `CREATE UNIQUE INDEX` fails if any alert is already linked to two incidents, so the service crash-loops on startup.
- **Fix:** before creating the index, in the same migration, keep the earliest association per `alert_id` and delete the rest. Order by the incident's `created_at`, then `incident_id`. Log the count with `RAISE NOTICE`.
- **Test:** an upgrade test that applies 001 and 002, inserts duplicate associations, runs 003, and checks that the index exists and one owner per alert remains.

### R2-5 Outbox relay ordering and 3PC gating
- **Where:** `services/incident-management/src/outbox.js:27` and `tpc-wiring.js:213`.
- **Problems:**
  - **(a)** Two overlapping relays (e.g. during a rollout) can publish version 2 before version 1.
  - **(b)** A 3PC outbox row is published while the coordinator is still PRECOMMITTED (the event-store hasn't acked doCommit). This undoes P1-1's intent.
- **Fix:**
  - **(a)** Take `pg_advisory_xact_lock(<relay constant>)` at the start of each relay batch transaction, before selecting rows.
  - **(b)** Add a nullable `tx_id` column to `outbox` (migration 004). 3PC rows carry their txId. The relay publishes a 3PC row only once `tpc_coordinator.state = 'COMMITTED'` for that txId. PATCH rows (tx_id null) publish immediately.
- **Tests:** two relays with a paused first send keep the order; a 3PC row with an unacked event-store waits until recovery commits.

---

## Minor (do if time allows; otherwise list under Known limits)

- **R2-6 Hash namespace collision** (`services/event-store/src/dynamo.js:68`). A literal entity key that already looks like `h:<64 hex>` shares a partition with the hashed long key. **Fix:** in `storageKey`, also hash any literal key that starts with `h:`. Callers still only see originals, so no read-path change is needed. Add a test.
- **R2-7 Late alerts skip eviction** (`services/correlation-engine/src/correlator.js:104`). Eviction only runs when the watermark advances, so replayed old alerts for many entities accumulate. **Fix:** run the eviction sweep after every ingest, cheaply: keep groups in a min-heap or sorted structure by window end, or sweep at most every N alerts. Add a test.
- **R2-8 Cursor edge cases still return 500** (`services/event-store/src/dynamo.js:54`). **Fix:** require a non-empty `sk` and a non-empty `entityKey`, within DynamoDB's byte limits (sort key ≤ 1024 bytes, partition key ≤ 2048 bytes). Otherwise return 400.
- **R2-9 Docs:**
  - **`docs/handoff.md:139`:** separate the required alert fields from the optional ones, which are checked only when present.
  - **`docs/handoff.md:274`:** give `shasum -a 256` for macOS alongside `sha256sum`.
  - **`docs/design/core-services.md`:**
    - Say "effects are idempotent" instead of "applied exactly once". After a crash, the apply can run twice.
    - Qualify "a coordinator crash never blocks": repeated `PENDING` lookups hold participants.
    - Add the `UNCERTAIN → COMMITTED` transition (taken when the coordinator reports COMMITTED) to the state diagram.
    - Add R2-3's slow-coordinator split to Known limits, plus anything left unfixed from this list.

## Done log

<!-- The box agent appends: item ID, commit SHA, test evidence. -->

### 16 Nov 2026 (box agent)

All nine items are done (R2-1 to R2-9). Each behaviour fix had a test that failed on the old code first; one commit per item, or one per half where a fix spans the shared library and a service.

| Item | Commit(s) | Test evidence |
|---|---|---|
| R2-1 | `0673524` | `recovery does not abort a tx that run() committed (with pending) after the snapshot` (the review's sequence: recovery aborted B on the old code), `recovery skips an entry that became terminal after the snapshot`, `run() waits for a recovery that holds the same txId`. Per-txId lock held by `run()` and by recovery; recovery re-reads with `log.get` (the Postgres log already had it). |
| R2-2 | `c2f35a5` (tpc + event-store `TransactWriteItems`), `15cb5f4` (Postgres store) | tpc races (doCommit vs abort, sweep commit vs abort, abort vs commit); DynamoDB Local `concurrent doCommit and abort on two replicas never split` (15 txs); `postgres store commit is atomic with the business write (R2-2)` (incl. 5 concurrent commit-vs-abort races): no business write without COMMITTED and no COMMITTED without one. |
| R2-3 | `70adeba` (tpc + event-store client), `6ac6a8b` (incident-db `decisionOf`) | One client test per response class (200, 404 → UNKNOWN, 5xx/other/malformed/unknown state → PENDING, transport/connection refused/stalled → unreachable); `incident-db sweep: no coordinator entry means the tx never started (R2-3)`. Lookup timeout kept as "unreachable" and documented as the remaining limit. |
| R2-4 | `e9ce9df` | `003 upgrades a database that already links one alert to two incidents (R2-4)`: failed with `could not create unique index` before; keeps the earliest owner, index exists. |
| R2-5 | `9debe3a` | `two overlapping relays keep per-incident order (R2-5a)` (published [2, 1] before), `a 3PC outbox row waits until the coordinator logs COMMITTED (R2-5b)`, plus `a status change queued behind a held-back 3PC row keeps its place`: later rows of the same incident wait behind a held-back 3PC row so order holds. Migration `004_outbox_tx.sql`. |
| R2-6 | `0cd6b3e` | Unit + DynamoDB Local test: a literal `h:<sha>` key and the long key it hashes stay in separate partitions. |
| R2-7 | `60d6707` | `replayed old alerts for many entities are evicted without the watermark advancing (R2-7)`: 301 groups before, ≤ 51 after (sweep at least every `evictEvery` = 256 ingests by default). |
| R2-8 | `ddaaea4` | Cursor edge cases (empty, oversized keys incl. multi-byte, `sk` outside `from`/`to`, lone surrogates) return 400; unit, route and integration tests. |
| R2-9 | `ed4e860`, `93f3abe` | Docs: required vs optional alert fields, `shasum -a 256`, idempotent effects, qualified non-blocking claim, `UNCERTAIN → COMMITTED`, slow-coordinator split in Known limits; demo output re-recorded. |

Final run on `93f3abe`: `npm test` 203/203 (common 10, tpc 68, event-store 68, incident-management 24, correlation-engine 33); `INTEGRATION=1 npm test -w event-store -w incident-management` 88/88 and 54/54; after `docker compose up -d --build` (migrations 001–004 applied), `npm run test:integration` 2/2 twice and `scripts/demo-3pc.sh` exit 0; `scripts/export-group-repo.sh` re-run: exported services pass 68 / 24 / 33 and build.

Still open (documented, not bugs in this round): P2-2 from round 1 (escalation after a rebalance), the slow-but-alive-coordinator split (needs a termination protocol or consensus), and a PRECOMMITTED record the coordinator has no entry for, which stays pending until an operator resolves it.
