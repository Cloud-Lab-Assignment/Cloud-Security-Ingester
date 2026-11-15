#!/usr/bin/env bash
# 3PC demo on the Docker Compose stack (requirement f).
#
#   docker compose up -d --build
#   scripts/demo-3pc.sh
#
# Walks through three scenarios and prints the coordinator log
# (GET /tpc/transactions) after each one:
#   1. normal commit
#   2. the Event Store votes no            -> aborted everywhere, no incident
#   3. the coordinator crashes after preCommit -> recover() commits on restart
#
# Needs: docker compose, curl, jq. Leaves the stack running in its normal state.
set -euo pipefail

cd "$(dirname "$0")/.."

IM=${IM_URL:-http://localhost:8082}
ES=${ES_URL:-http://localhost:8081}
TX_FIELDS='{txId: .txId[0:8], state, reason, kind, version, correlationId}'

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
note() { printf '    %s\n' "$*"; }

iso() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%S.000Z; }

# Wait until a compose service reports healthy.
wait_healthy() {
  local svc=$1 i
  for i in $(seq 1 60); do
    if [ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q "$svc")" 2>/dev/null)" = healthy ]; then
      return 0
    fi
    sleep 1
  done
  echo "service $svc did not become healthy" >&2
  exit 1
}

# Publish one RULE-AUTH-001 alert for an entity, keyed by entityKey.
send_alert() {
  local entity=$1 trigger now
  trigger=$(cat /proc/sys/kernel/random/uuid)
  now=$(date -u +%s)
  local alert
  alert=$(jq -cn --arg e "$entity" --arg t "$trigger" --arg at "$(iso "$now")" --arg ws "$(iso $((now - 60)))" '{
    schemaVersion: "1.0", alertId: "RULE-AUTH-001|\($e)|\($t)", alertTime: $at,
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
  note "sent alert RULE-AUTH-001|$entity|${trigger:0:8}…"
}

incidents_for() {
  curl -fsS "$IM/incidents?entityKey=$(jq -rn --arg e "$1" '$e|@uri')&limit=10"
}

show_incident() {
  incidents_for "$1" | jq '.items[] | {incidentId, correlationId, status, severity, alertCount, version}'
}

show_tx() {
  local corr=$1
  curl -fsS "$IM/tpc/transactions?limit=10&correlationId=$(jq -rn --arg c "$corr" '$c|@uri')" | jq -c ".items | reverse | .[] | $TX_FIELDS"
}

show_latest_tx() {
  curl -fsS "$IM/tpc/transactions?limit=${1:-5}" | jq -c ".items | reverse | .[] | $TX_FIELDS"
}

wait_incident() {
  local entity=$1 i
  for i in $(seq 1 60); do
    if [ "$(incidents_for "$entity" | jq '.items | length')" -gt 0 ]; then return 0; fi
    sleep 1
  done
  echo "no incident for $entity" >&2
  return 1
}

rand_entity() { echo "203.0.113.$((RANDOM % 254 + 1))"; }

for s in event-store incident-management correlation-engine; do wait_healthy "$s"; done

# ---------------------------------------------------------------------------
say "Scenario 1: normal commit"
E1=$(rand_entity)
send_alert "$E1"
wait_incident "$E1"
show_incident "$E1"
CORR1=$(incidents_for "$E1" | jq -r '.items[0].correlationId')
note "coordinator log for $CORR1:"
show_tx "$CORR1"
note "event-store correlation history:"
curl -fsS "$ES/correlations/$(jq -rn --arg c "$CORR1" '$c|@uri')" | jq -c '.versions[] | {version, kind: .decision.kind, txId: .txId[0:8], recordedAt}'

# ---------------------------------------------------------------------------
say "Scenario 2: the Event Store votes no"
note "restarting event-store with TPC_FORCE_VOTE=no"
ES_TPC_FORCE_VOTE=no docker compose up -d event-store >/dev/null 2>&1
wait_healthy event-store
E2=$(rand_entity)
send_alert "$E2"
sleep 8
note "incidents for $E2 (expect none):"
incidents_for "$E2" | jq -c '.items'
note "latest coordinator log entries (expect ABORTED vote_no; the correlation engine keeps retrying):"
show_latest_tx 3
note "postgres participant rows for those transactions are ABORTED too:"
docker compose exec -T postgres psql -U incidents -Atc \
  "select left(tx_id,8), state from tpc_participant order by updated_at desc limit 3"

note "restoring event-store (votes normally); the engine's retry now commits"
docker compose up -d event-store >/dev/null 2>&1
wait_healthy event-store
wait_incident "$E2"
show_incident "$E2"
show_latest_tx 2

# ---------------------------------------------------------------------------
say "Scenario 3: the coordinator crashes after every participant pre-committed"
note "restarting incident-management with TPC_FAULT=crash-after-precommit"
IM_TPC_FAULT=crash-after-precommit docker compose up -d incident-management >/dev/null 2>&1
wait_healthy incident-management
E3=$(rand_entity)
send_alert "$E3"
for i in $(seq 1 30); do
  state=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$(docker compose ps -aq incident-management)")
  [ "${state%% *}" = exited ] && break
  sleep 1
done
note "incident-management container: $state"
docker compose logs --no-color --tail 50 incident-management 2>/dev/null | grep -o '"msg":"injected coordinator crash[^"]*"' | tail -1 | sed 's/^/    /'
note "coordinator log in postgres (expect PRECOMMITTED) and no incident row yet:"
docker compose exec -T postgres psql -U incidents -Atc \
  "select left(tx_id,8), state from tpc_coordinator order by created_at desc limit 1"
docker compose exec -T postgres psql -U incidents -Atc \
  "select count(*) || ' incident row(s) for $E3' from incidents where entity_key = '$E3'"

note "restarting incident-management without the fault: recover() finishes the transaction"
docker compose up -d incident-management >/dev/null 2>&1
wait_healthy incident-management
docker compose logs --no-color --tail 50 incident-management 2>/dev/null | grep -o '"recovered":\[[^]]*\]' | tail -1 | sed 's/^/    /'
wait_incident "$E3"
show_incident "$E3"
CORR3=$(incidents_for "$E3" | jq -r '.items[0].correlationId')
show_tx "$CORR3"
note "event-store correlation history:"
curl -fsS "$ES/correlations/$(jq -rn --arg c "$CORR3" '$c|@uri')" | jq -c '.versions[] | {version, kind: .decision.kind, txId: .txId[0:8]}'

say "Done. The stack is back to normal (no forced votes, no faults)."
