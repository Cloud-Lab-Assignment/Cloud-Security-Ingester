#!/usr/bin/env bash
# 3PC demo on the Docker Compose stack (requirement f). Runs on Linux and macOS.
#
#   docker compose up -d --build
#   scripts/demo-3pc.sh
#
# Walks through three scenarios and prints the coordinator log
# (GET /tpc/transactions) after each one:
#   1. normal commit
#   2. the Event Store votes no                 -> aborted everywhere, no incident
#   3. the coordinator crashes after preCommit  -> the Event Store commits on its
#      own (non-blocking 3PC), then recover() finishes the rest on restart
#
# Needs: docker compose, node, curl, jq. Whatever happens, the stack is put back
# to its normal state on exit (no forced votes, no faults).
set -euo pipefail

cd "$(dirname "$0")/.."

IM=${IM_URL:-http://localhost:8082}
ES=${ES_URL:-http://localhost:8081}
TX_FIELDS='{txId: .txId[0:8], state, reason, kind, version, correlationId}'

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
note() { printf '    %s\n' "$*"; }
fail() { printf '\033[1;31mFAILED: %s\033[0m\n' "$*" >&2; exit 1; }

# Portable helpers (no GNU date, no /proc): node is already required by the project.
uuid() { node -e 'console.log(require("crypto").randomUUID())'; }
iso_ago() { node -e 'console.log(new Date(Date.now() - Number(process.argv[1]) * 1000).toISOString())' "${1:-0}"; }
uri() { jq -rn --arg v "$1" '$v|@uri'; }
# The correlation engine's correlationId for a group opened by this alert.
correlation_id() {
  node -e 'const [e, a] = process.argv.slice(1); console.log("cor-" + require("crypto").createHash("sha256").update(`${e}|${a}`).digest("hex").slice(0, 24))' "$1" "$2"
}

# Put the stack back to normal whatever happens (P0-1).
restore() {
  local code=$?
  unset ES_TPC_FORCE_VOTE IM_TPC_FAULT
  note "restoring event-store and incident-management to their normal configuration"
  docker compose up -d event-store incident-management >/dev/null 2>&1 || true
  exit "$code"
}
trap restore EXIT

wait_healthy() {
  local svc=$1 i id
  for i in $(seq 1 90); do
    id=$(docker compose ps -q "$svc")
    if [ -n "$id" ] && [ "$(docker inspect -f '{{.State.Health.Status}}' "$id" 2>/dev/null)" = healthy ]; then
      return 0
    fi
    sleep 1
  done
  fail "service $svc did not become healthy"
}

incidents_for() { curl -fsS "$IM/incidents?entityKey=$(uri "$1")&limit=10"; }

# A fresh entity per scenario, checked to have no incident yet (P0-1).
fresh_entity() {
  local e i
  for i in $(seq 1 20); do
    e=$(node -e 'const c = require("crypto"); console.log(`198.18.${c.randomInt(256)}.${c.randomInt(1, 255)}`)')
    if [ "$(incidents_for "$e" | jq '.items | length')" = 0 ]; then
      echo "$e"
      return 0
    fi
  done
  fail "no unused 198.18.x.y entity found"
}

# Publish one RULE-AUTH-001 alert for an entity, keyed by entityKey. Prints the alertId.
send_alert() {
  local entity=$1 trigger alert_id alert
  trigger=$(uuid)
  alert_id="RULE-AUTH-001|$entity|$trigger"
  alert=$(jq -cn --arg e "$entity" --arg t "$trigger" --arg id "$alert_id" --arg at "$(iso_ago 0)" --arg ws "$(iso_ago 60)" '{
    schemaVersion: "1.0", alertId: $id, alertTime: $at,
    ruleId: "RULE-AUTH-001", ruleName: "Brute-force login attempts", ruleVersion: "1.0.0",
    severity: "high", alertType: "brute_force", title: "Possible brute force from \($e)",
    description: "3PC demo alert", entityKey: $e, entity: {srcIp: $e, userId: null, host: null},
    triggerEventId: $t, relatedEventIds: [$t], eventCount: 6,
    window: {start: $ws, end: $at, durationSeconds: 60},
    evidence: {threshold: 5, observedFailures: 6, usernamesTried: ["root", "admin"]},
    status: "open", raisedBy: {name: "detection-rules-service", version: "1.0.0"}}')
  printf '%s\t%s\n' "$entity" "$alert" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic security-alerts \
    --property parse.key=true --property "key.separator=$(printf '\t')" >/dev/null
  echo "$alert_id"
}

show_incident() { incidents_for "$1" | jq '.items[] | {incidentId, correlationId, status, severity, alertCount, version}'; }
show_tx() { curl -fsS "$IM/tpc/transactions?limit=10&correlationId=$(uri "$1")" | jq -c ".items | reverse | .[] | $TX_FIELDS"; }
show_latest_tx() { curl -fsS "$IM/tpc/transactions?limit=${1:-5}" | jq -c ".items | reverse | .[] | $TX_FIELDS"; }
show_correlation() {
  curl -fsS "$ES/correlations/$(uri "$1")" | jq -c '.versions[] | {version, kind: .decision.kind, txId: .txId[0:8], recordedAt}'
}

wait_incident() {
  local entity=$1 i
  for i in $(seq 1 60); do
    [ "$(incidents_for "$entity" | jq '.items | length')" -gt 0 ] && return 0
    sleep 1
  done
  fail "no incident for $entity"
}

for s in event-store incident-management correlation-engine; do wait_healthy "$s"; done

# ---------------------------------------------------------------------------
say "Scenario 1: normal commit"
E1=$(fresh_entity)
note "entity $E1 has no incident yet"
A1=$(send_alert "$E1")
note "sent alert ${A1:0:40}…"
wait_incident "$E1"
show_incident "$E1"
CORR1=$(incidents_for "$E1" | jq -r '.items[0].correlationId')
note "coordinator log for $CORR1:"
show_tx "$CORR1"
note "event-store correlation history:"
show_correlation "$CORR1"

# ---------------------------------------------------------------------------
say "Scenario 2: the Event Store votes no"
note "restarting event-store with TPC_FORCE_VOTE=no"
ES_TPC_FORCE_VOTE=no docker compose up -d event-store >/dev/null 2>&1
wait_healthy event-store
E2=$(fresh_entity)
note "entity $E2 has no incident yet"
A2=$(send_alert "$E2")
note "sent alert ${A2:0:40}…"
sleep 8
note "incidents for $E2 (expect none):"
COUNT2=$(incidents_for "$E2" | jq '.items | length')
incidents_for "$E2" | jq -c '.items'
[ "$COUNT2" = 0 ] || fail "an incident was created although the Event Store voted no"
CORR2=$(correlation_id "$E2" "$A2")
note "coordinator log for $CORR2 (expect ABORTED vote_no; the correlation engine keeps retrying):"
show_tx "$CORR2"
note "postgres participant rows for those transactions are ABORTED too:"
docker compose exec -T postgres psql -U incidents -Atc \
  "select left(tx_id,8) || ' ' || state from tpc_participant where payload->>'correlationId' = '$CORR2' order by updated_at"

note "restoring event-store (votes normally); the engine's retry now commits"
docker compose up -d event-store >/dev/null 2>&1
wait_healthy event-store
wait_incident "$E2"
show_incident "$E2"
show_tx "$CORR2" | tail -2

# ---------------------------------------------------------------------------
say "Scenario 3: the coordinator crashes after every participant pre-committed"
note "restarting incident-management with TPC_FAULT=crash-after-precommit"
IM_TPC_FAULT=crash-after-precommit docker compose up -d incident-management >/dev/null 2>&1
wait_healthy incident-management
E3=$(fresh_entity)
note "entity $E3 has no incident yet"
A3=$(send_alert "$E3")
note "sent alert ${A3:0:40}…"
CORR3=$(correlation_id "$E3" "$A3")
state=""
for i in $(seq 1 30); do
  state=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$(docker compose ps -aq incident-management)")
  [ "${state%% *}" = exited ] && break
  sleep 1
done
[ "${state%% *}" = exited ] || fail "incident-management did not crash"
note "incident-management container: $state"
docker compose logs --no-color --tail 50 incident-management 2>/dev/null | grep -o '"msg":"injected coordinator crash[^"]*"' | tail -1 | sed 's/^/    /'
note "coordinator log in postgres (expect PRECOMMITTED) and no incident row yet:"
docker compose exec -T postgres psql -U incidents -Atc \
  "select left(tx_id,8) || ' ' || state from tpc_coordinator where payloads->'event-store'->>'correlationId' = '$CORR3'"
docker compose exec -T postgres psql -U incidents -Atc \
  "select count(*) || ' incident row(s) for $E3' from incidents where entity_key = '$E3'"

# Non-blocking 3PC: with the coordinator gone, the PRECOMMITTED Event Store
# cannot reach it for a decision and commits on its own timeout (10 s).
note "waiting for the Event Store to commit on its own (PRECOMMITTED timeout, coordinator unreachable)…"
committed=""
for i in $(seq 1 40); do
  if curl -fsS "$ES/correlations/$(uri "$CORR3")" >/dev/null 2>&1; then committed=yes; break; fi
  sleep 1
done
[ -n "$committed" ] || fail "the Event Store did not self-commit"
note "event-store self-committed after ~${i}s while the coordinator was down:"
show_correlation "$CORR3"

note "restarting incident-management without the fault: recover() finishes the Postgres side"
docker compose up -d incident-management >/dev/null 2>&1
wait_healthy incident-management
docker compose logs --no-color --tail 80 incident-management 2>/dev/null | grep -o '"recovered":\[[^]]*\]' | tail -1 | sed 's/^/    /'
wait_incident "$E3"
show_incident "$E3"
show_tx "$CORR3"

say "Done. The stack is back to normal (no forced votes, no faults)."
