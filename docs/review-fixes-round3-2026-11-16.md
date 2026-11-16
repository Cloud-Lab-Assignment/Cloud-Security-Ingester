# Review fixes, round 3 (16 Nov 2026)

Re-reviews (GPT-6.1 Sol) of the round-2 fixes in `e0faa02..c7033db`. **No blockers.** The findings below are edge cases; R3-1, R3-2 and R3-3 were spot-checked against the code.

**The working rules from `docs/review-fixes-2026-11-16.md` still apply:**
- one commit per item, with the item ID in the message
- TDD: a failing test first
- no installs without asking Rishit
- never touch the group repo
- Terraform `fmt` / `validate` only

When done, run the full suite and the demo, push to `origin main`, and append a Done log here. Deadline: **Wed 18 Nov 2026, 23:59 IST**.

**This should be the last code round.** After it, freeze the code and spend the remaining time on the demo, the viva and the group-repo PR. If an item turns out larger than about an hour, document it under Known limits instead.

Round-2 verdicts:
- **Fixed:** R2-3 (event-store side).
- **Partially fixed:** R2-1, R2-2, R2-3 (incident-db logging), R2-4, R2-5. These are covered below.
- **R2-6 to R2-9:** see the section near the end.

---

## Major

### R3-1 Event Store accepts an existing history row without checking it matches
- **Where:** `services/event-store/src/dynamo.js:306`, the `TransactionCanceledException` fallback in `tpcStore.commit`.
- **Sequence:**
  1. The history already holds transaction A's `(correlationId, version)`.
  2. Transaction B commits the same key with a different incident or decision. The transaction is cancelled with `[ConditionalCheckFailed, None]`.
  3. The fallback writes B's COMMITTED state alone and acknowledges success, but the stored business data is A's.
- **Fix:** take the fallback only when the existing row is this transaction's own write.
  - Read the existing correlation item with `ConsistentRead` and compare `txId`, plus the business fields if a legacy row has no txId.
  - Only then do the conditional COMMITTED put.
  - Otherwise throw a conflict. Do not acknowledge.
- **Test:** an existing row with a different txId → no ack, and the state is not COMMITTED. Keep the matching re-commit test.

### R3-2 Recovery can hold a transaction lock forever
- **Where:** `packages/tpc/src/coordinator.js:45` (`sendWithRetry` awaits `p[phase](txId)` with no deadline) and `:93` (the per-txId lock).
- **Sequence:** recovery takes T's lock, and an `abort`, `doCommit` or log call never settles. `run(T)` waits behind it forever, and every later `recover()` joins the hung pass.
- **Fix:**
  - Race each participant call in `sendWithRetry` against `timeoutMs`, as the vote and preCommit phases already do. A timeout counts as a failed attempt.
  - Make sure the event-store HTTP adapter in incident-management has an `AbortController` deadline at or below that timeout.
  - Bound how long a queued `run()` waits for the lock: reject after, say, `2 × timeoutMs × retries`, and the gRPC caller gets ABORTED/UNAVAILABLE and retries.
  - Don't release a lock while an operation can still be changing state.
- **Test:** a participant whose `abort` never resolves. Recovery finishes, leaving the entry unfinished for the next pass, and a queued `run()` rejects in bounded time.

### R3-3 Outbox rows from before migration 004 skip the 3PC gate
- **Where:** `services/incident-management/migrations/004_outbox_tx.sql:5`.
- **Problem:** unpublished correlation rows written before 004 get `tx_id = NULL`, so they publish immediately even if their transaction is still PRECOMMITTED.
- **Fix:** in 004 (or a new 005), backfill `tx_id` for unpublished correlation rows from `incident_history`, matching `incident_id` and version. Leave status-change rows NULL.
- **Test:** an upgrade test: an unpublished correlation row from before 004 plus a PRECOMMITTED coordinator entry. After the migration it waits for COMMITTED.

### R3-4 A held-back outbox row can block an incident forever
- **Where:** `services/incident-management/src/outbox.js:40`.
- **Problem:** if a 3PC row's transaction never reaches COMMITTED (e.g. it ends ABORTED after the documented timeout split), that row and every later row for the same incident never publish. Nothing resolves them.
- **Fix:**
  - Add a `disposition` column (`NULL` | `discarded`) and exclude discarded rows from the blocking predicate.
  - The relay marks a row `discarded` automatically, and logs at error level, when its coordinator entry is **ABORTED**: Postgres can't have a committed apply for an ABORTED tx unless the split happened, and either way the coordinator's decision wins.
  - **Do not** auto-expire PRECOMMITTED rows.
  - Expose the count of held rows older than 5 minutes as the metric `outbox_held_rows`, and document the manual resolution in `docs/handoff.md`.
- **Test:** a 3PC row whose tx ends ABORTED is discarded, and the next row for that incident publishes.

---

## Minor

- **R3-5 Migration 003 leaves stale counts** (`003_alert_unique.sql:22`). After deleting duplicate associations, recompute `alert_count` (and the generated title, if derived from it) for the affected incidents, in a new migration 005 so 003 stays unchanged for databases that already ran it. Fix the misleading comment. Add a test.
- **R3-6 Orphaned PRECOMMITTED records are silent** (`tpc-wiring.js:277`). Pass the service logger through `createIncidentDbParticipant` into `createParticipant`, so the UNKNOWN-while-PRECOMMITTED error is logged. Count such records in a metric (e.g. `tpc_participant_orphaned`).
- **R3-7 3PC docs** (`docs/design/core-services.md:105` and `:128`).
  - Qualify "never writes ABORTED over PRECOMMITTED". Recovery does exactly that when a participant reports ABORTED, and that is correct.
  - Add the unknown-transaction `abort → ABORTED` tombstone transition and the timeout guards to the state diagram.
  - Mention R3-2's deadlines and R3-4's discard rule.

## R2-6 to R2-9 findings

Third reviewer verdicts:
- **Fixed:** R2-6, R2-7, R2-8.
- **Partially fixed:** R2-9. Its "never writes ABORTED over PRECOMMITTED" finding is already covered by R3-7.

### R3-8 Demo can move on before phase 3 finishes
- **Where:** `scripts/demo-3pc.sh:118` waits only for the Postgres incident row.
- **Problem:** the row can show up while the Event Store's concurrent commit is still running. The script then prints `PRECOMMITTED` or hits a correlation-history 404 and exits.
- **Fix:** add a helper that polls until the coordinator log shows `COMMITTED` for the tx **and** `GET /correlations/:id` returns the matching version. Use it in every scenario that ends in a commit, including after recovery in scenario 3. Re-record the output in `docs/handoff.md`.

### R3-9 README has no clear "run it" path
- **Where:** `README.md:14`.
- **Fix:** add a short "Run it locally" section at the top that links to the "Local run" section of `docs/handoff.md` and to `scripts/demo-3pc.sh`. Under "Start here", keep the brief, then mark the plan and review rounds 1 to 3 as history, with "done" against each.

### R3-10 Cursor docs describe the wrong key
- **Where:** `docs/handoff.md:37`.
- **Fix:** describe the cursor as opaque. Its decoded `entityKey` is `storageKey(entityKey)`, which is the hashed form for long keys and for literal `h:` keys. Clients must pass the `nextCursor` back unchanged.

**For Rishit, not the box:** fill in the ID number in `docs/task_division_rishit.txt`.

## Done log

<!-- The box agent appends: item ID, commit SHA, test evidence. -->

### 16 Nov 2026 (box agent)

All ten items are done (R3-1 to R3-10); none needed moving to Known limits. Each behaviour fix had a test that failed on the old code first.

| Item | Commit(s) | Test evidence |
|---|---|---|
| R3-1 | `cb9abfe` | Fake-client and DynamoDB Local tests: another tx's history row for the same `(correlationId, version)` → `HistoryConflict`, doCommit not acked, record stays PRECOMMITTED, row unchanged; a legacy row without `txId` only moves the state when `incidentId`, `alertIds` and `decision` match; the matching re-commit test is kept. The participant sweep now logs and skips a record whose commit fails instead of stopping the pass. |
| R3-2 | `971b2b2` (tpc), `cd90153` (gRPC mapping) | `recover() with an abort that never resolves finishes in bounded time`, `… a doCommit that never resolves leaves the entry PRECOMMITTED for the next pass`, `a queued run() gives up waiting for the txId lock with CoordinatorBusy and writes nothing`, `recover() rejects in bounded time when listUnfinished hangs`, `grpc maps CoordinatorBusy to UNAVAILABLE`. The event-store adapter's deadline already equals `TPC_TIMEOUT_MS`. |
| R3-3 | `53de431` | `005 backfills tx_id on pre-004 correlation outbox rows so they wait for COMMITTED (R3-3)`: the PRECOMMITTED update and the status change behind it stay held. |
| R3-4 | `cf13c60` | `a held 3PC row whose transaction ended ABORTED is discarded and unblocks the incident`, `outbox_held_rows counts 3PC rows held for more than 5 minutes; PRECOMMITTED is never discarded`. Migration 006; manual resolution in `docs/handoff.md`. |
| R3-5 | `5defc00` | `007 recomputes alert_count and the generated title after the 003 dedupe` (custom titles untouched). 003's comment corrected; its SQL unchanged. |
| R3-6 | `3cd73f0` | `orphaned PRECOMMITTED records are logged and counted`; gauge `tpc_participant_orphaned`. |
| R3-7 | `f96bc9f` | Docs: diagram guards and tombstone, qualified recovery wording, deadlines, discard rule, new Known limits (hung Postgres calls, history conflicts). |
| R3-8 | `6b1c814`, `da4d5a9` | `wait_committed` (coordinator COMMITTED and the Event Store's matching version) after every committing scenario; demo exit 0, output re-recorded. |
| R3-9 | `5c8e278` | README: "Run it locally" at the top; plan and rounds 1–3 listed as history, marked done. |
| R3-10 | `bb8acdb` | Handoff describes the cursor as opaque (`storageKey(entityKey)` inside). |

Final run on `5c8e278`: `npm test` 217/217 (common 10, tpc 77, event-store 72, incident-management 25, correlation-engine 33); `INTEGRATION=1 npm test -w event-store -w incident-management` 94/94 and 60/60; after `docker compose up -d --build` (migrations 001–007 applied) `npm run test:integration` 2/2 twice and `scripts/demo-3pc.sh` exit 0; `/metrics` shows `outbox_held_rows 0` and `tpc_participant_orphaned 0`; `scripts/export-group-repo.sh` re-run: exported services pass 72 / 25 / 33 and build.

Code frozen. Remaining for Rishit: the ID number in `docs/task_division_rishit.txt`, regenerating the group-repo copy with `scripts/export-group-repo.sh` and opening the PR after agreeing with Ansh, and the group confirmations in the plan's "Before coding" list.
