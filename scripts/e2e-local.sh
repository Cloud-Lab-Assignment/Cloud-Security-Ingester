#!/usr/bin/env bash
# End-to-end smoke test of the full local stack (docker-compose.yml + docker-compose.full.yml).
# Sends 6 failed SSH logins from one IP through the public ingestion API and waits
# for the resulting incident to appear in the Investigation Query API.
set -euo pipefail

INGEST="${INGEST_URL:-http://localhost:3000}"
QUERY="${QUERY_URL:-http://localhost:8084}"
IP="198.18.$((RANDOM % 250 + 1)).$((RANDOM % 250 + 1))"

echo "==> sending 6 failed logins from $IP to $INGEST/events"
for i in 1 2 3 4 5 6; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INGEST/events" \
    -H 'content-type: application/json' \
    -d "{\"source\":{\"type\":\"auth_log\",\"name\":\"bastion-ssh\"},\"format\":\"text\",\"payload\":\"Nov 16 10:00:0$i bastion sshd[123$i]: Failed password for root from $IP port 4242$i ssh2\"}")
  [ "$code" = "202" ] || { echo "ingest returned $code"; exit 1; }
done
echo "    all accepted (202)"

echo "==> waiting for an incident for $IP in $QUERY/incidents"
for attempt in $(seq 1 45); do
  body=$(curl -s "$QUERY/incidents?limit=200" || true)
  id=$(printf '%s' "$body" | node -e '
    let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{
      try{const m=(JSON.parse(s).items||[]).find(i=>String(i.entityKey).includes(process.argv[1]));
      if(m)console.log(m.incidentId)}catch{}})' "$IP")
  if [ -n "$id" ]; then
    echo "    incident $id found after ~$((attempt * 2))s"
    echo "==> incident detail (incident + history + correlation history + events)"
    curl -s "$QUERY/incidents/$id" | node -e '
      let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const b=JSON.parse(s);
      console.log(JSON.stringify({incidentId:b.incident.incidentId,entityKey:b.incident.entityKey,
        severity:b.incident.severity,status:b.incident.status,version:b.incident.version,
        history:(b.history||[]).length,correlationHistory:b.correlationHistory?"present":null,
        events:(b.events||[]).length},null,2))})'
    echo "==> PASS"
    exit 0
  fi
  sleep 2
done
echo "==> FAIL: no incident for $IP after 90s"
exit 1
