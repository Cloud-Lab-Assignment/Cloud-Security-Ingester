# Testing the deployed system, service by service

Run these after `terraform apply` has finished and Argo CD shows the apps. Every check uses a browser, `curl`, or **Cloud Shell** (the terminal button in the Google Cloud console, which has `gcloud` and `kubectl` preinstalled). Nothing here deploys anything, so it does not break the GitOps rule.

Set these once in Cloud Shell (or your own terminal):

```bash
PROJECT=project-01b2c261-3004-404e-9a0
REGION=asia-south1
ZONE=asia-south1-a
gcloud config set project $PROJECT
gcloud container clusters get-credentials cloud-sec --zone $ZONE     # lets kubectl read the cluster
```

## 0. Are the pods actually running?

```bash
kubectl -n cloud-sec get pods
kubectl -n cloud-sec get svc                                        # EXTERNAL-IP of log-ingestion and investigation-query-api
```

All six Deployments should be `Running` and `READY 1/1` (log-ingestion has 2 pods, investigation-query-api 2). The same is visible in the Argo CD UI.

If pods show `ImagePullBackOff`, the images have not been built yet (`gitops/` already points at the project's Artifact Registry). In GitHub:

1. Settings -> Secrets and variables -> Actions: secret `GCP_SA_KEY` (`terraform output -raw ci_service_account_key | base64 -d`, from the Cloud Shell that holds the Terraform state) and variable `GCP_PROJECT_ID` = `project-01b2c261-3004-404e-9a0`.
2. Actions -> ci -> **Run workflow** on `main`. CI builds the six images, commits the new tag, and Argo CD rolls them out a minute or two later.

Then save the two public addresses:

```bash
INGEST=http://$(kubectl -n cloud-sec get svc log-ingestion -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
QUERY=http://$(kubectl -n cloud-sec get svc investigation-query-api -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo $INGEST $QUERY
```

## 1. Whole pipeline in one command

Before testing services one by one, the end-to-end script tells you whether everything is connected:

```bash
INGEST_URL=$INGEST QUERY_URL=$QUERY scripts/e2e-local.sh
```

`PASS` with an incident means log-ingestion, the function, detection-rules, correlation-engine, incident-management (gRPC + 3PC), event-store and the query API all work. Despite its name the script works against any URL. If it fails, the sections below tell you which service broke.

To make the internal services reachable from Cloud Shell, start port-forwards once (they run in the background):

```bash
kubectl -n cloud-sec port-forward svc/event-store 8081:8081 >/dev/null &
kubectl -n cloud-sec port-forward svc/incident-management 8082:8082 >/dev/null &
kubectl -n cloud-sec port-forward svc/correlation-engine 8083:8083 >/dev/null &
```

## 2. Service by service

### Log Ingestion (public REST, GKE)

```bash
curl -s $INGEST/health        # {"status":"ok",...}
curl -s $INGEST/ready         # {"status":"ready","kafka":"connected"}  -> Kafka login to Confluent works
curl -s -X POST $INGEST/events -H 'content-type: application/json' -d '{
  "source":{"type":"auth_log","name":"bastion-ssh"},"format":"text",
  "payload":"Nov 18 10:00:00 bastion sshd[1]: Failed password for root from 203.0.113.50 port 4242 ssh2"}'
# -> 202 {"eventId":"...","status":"published"}
curl -s -X POST $INGEST/events -H 'content-type: application/json' -d '{"bad":true}'   # -> 400
```

Then in **Confluent Cloud -> cluster -> Topics -> raw-events -> Messages**, the event appears.

### Event Normalizer (Cloud Run function)

It is triggered by the Confluent connector, so the event you just sent should already have gone through it.

- **Confluent Cloud -> Connectors -> cloud-sec-normalizer-trigger**: status *Running*, "messages processed" going up.
- **Google Cloud -> Cloud Run functions -> cloud-sec-event-normalizer -> Logs**: lines `batch normalized` with `processed` / `failed` counts.
- **Confluent -> Topics -> normalized-events -> Messages**: the event in the common schema (`category: authentication`, `outcome: failure`, `entityKey: 203.0.113.50`).

To test the function on its own, bypassing the connector:

```bash
URL=$(gcloud functions describe cloud-sec-event-normalizer --region $REGION --format='value(serviceConfig.uri)')
NOW=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
curl -s -X POST "$URL" -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H 'content-type: application/json' -d "[{\"schemaVersion\":\"1.0\",\"eventId\":\"$(cat /proc/sys/kernel/random/uuid)\",\"receivedAt\":\"$NOW\",\"eventTime\":null,\"source\":{\"type\":\"auth_log\",\"name\":\"manual\"},\"format\":\"text\",\"payload\":\"Nov 18 10:00:00 h sshd[1]: Failed password for root from 203.0.113.51 port 1 ssh2\",\"ingestedBy\":{\"name\":\"manual\",\"version\":\"1\"}},{\"bad\":true}]"
# -> {"processed":1,"failed":1}   (the bad record lands in raw-events-dlq)
```

A call without the `Authorization` header must be refused (401 or 403); that proves the function is not open to the internet.

### Detection Rules (GKE, Kafka only)

It has no HTTP port. Send **6 failed logins from one IP within a minute** (section 1's script does this), then:

- **Confluent -> Topics -> security-alerts -> Messages**: an alert with `ruleId: RULE-AUTH-001` and that IP as `entityKey`.
- Logs: `kubectl -n cloud-sec logs deploy/detection-rules --tail=50`, or in Grafana -> Explore -> Loki: `{app="detection-rules"}`.

The rule suppresses repeat alerts for the same IP for 60 seconds, so use a new IP for each test.

### Correlation Engine (GKE, Kafka in, gRPC out)

```bash
curl -s localhost:8083/ready                                   # 200 = it has joined its consumer group
curl -s localhost:8083/metrics | grep alerts_consumed_total
```

To test it in isolation, produce an alert straight into **Confluent -> Topics -> security-alerts -> Messages -> Produce a new message** (value):

```json
{"alertId":"manual-1","entityKey":"198.51.100.7","alertTime":"2026-11-18T10:00:00.000Z","severity":"high","ruleId":"RULE-AUTH-001"}
```

Within a few seconds an incident for `198.51.100.7` appears in `$QUERY/incidents`. That proves Kafka -> correlation-engine -> **gRPC** -> incident-management. Three alerts for the same entity escalate it to `critical`.

### Incident Management (GKE, gRPC + REST, Cloud SQL, 3PC coordinator)

```bash
curl -s localhost:8082/ready                                   # 200 = migrations ran on Cloud SQL, Kafka connected, recovery done
curl -s "localhost:8082/incidents?limit=5" | jq
curl -s "localhost:8082/tpc/transactions?limit=5" | jq          # coordinator log: state COMMITTED
ID=<an incidentId>; V=<its version>
curl -s -X PATCH localhost:8082/incidents/$ID -H 'content-type: application/json' \
  -d "{\"status\":\"investigating\",\"version\":$V,\"actor\":\"analyst\"}"   # -> incident with version+1
```

**Confluent -> Topics -> incident-updates** shows `incident.created` and `incident.status_changed` messages.

### Event Store (GKE, REST, Cloud Storage + DynamoDB, 3PC participant)

```bash
curl -s localhost:8081/ready                                   # 200 = Kafka connected and DynamoDB reachable from GKE
curl -s "localhost:8081/events?entityKey=203.0.113.50&limit=10" | jq '.items | length'
curl -s localhost:8081/correlations/<correlationId> | jq        # the incident's 3PC-written history
```

- **AWS console -> DynamoDB -> Tables**: `cs-events` (one item per normalized event), `cs-correlations` (one item per incident version), `cs-tpc` (transaction state `COMMITTED`). This proves the cross-cloud call from GKE to AWS works.
- **Google Cloud -> Cloud Storage -> cloud-sec-raw-... -> raw/YYYY/MM/DD/HH/**: one JSON object per raw event. This proves the S3-compatible write with the HMAC key works.

### Investigation Query API (public REST, GKE)

```bash
curl -s $QUERY/ready                                           # 200 only if event-store AND incident-management answer
curl -s "$QUERY/incidents?limit=5" | jq
curl -s $QUERY/incidents/<incidentId> | jq                      # incident + history + correlationHistory + events
curl -s "$QUERY/entities/203.0.113.50/timeline" | jq '{events: (.events|length), incidents: (.incidents|length)}'
curl -s "$QUERY/events" -o /dev/null -w '%{http_code}\n'        # 400: entityKey is required
```

### Security Analytics (Flink on Dataproc)

1. Check the job runs: **Dataproc -> Jobs** shows it *Running*. If there is no job, build, upload and submit it from Cloud Shell (it has Java and Maven):
   ```bash
   git clone https://github.com/Cloud-Lab-Assignment/Cloud-Security-Ingester.git && cd Cloud-Security-Ingester/services/security-analytics
   mvn -q package -DskipTests
   gcloud storage cp target/security-analytics-1.0.0.jar gs://cloud-sec-artifacts-5b296c/artifacts/security-analytics.jar
   gcloud dataproc jobs submit flink --cluster=cloud-sec-flink --region=asia-south1      --jar=gs://cloud-sec-artifacts-5b296c/artifacts/security-analytics.jar --async
   ```
2. Flink UI: **Dataproc -> Clusters -> cloud-sec-flink -> Web Interfaces -> Flink** (or YARN ResourceManager -> the application -> ApplicationMaster). The job graph should show records flowing.
3. Send a burst of events (the e2e script or k6), **wait about 2-3 minutes**, then send a few more. Windows are 60 s and 300 s of event time, and a window closes only when later events (or 60 s of idleness) move the watermark past it.
4. **Confluent -> Topics -> results -> Messages**: records like `{"metric":"events_per_source_ip","dimension":{"name":"srcIp","value":"203.0.113.50"},"window":{...},"aggregates":{"eventCount":6,"failureCount":6,...}}`.

Malformed input goes to `normalized-events-dlq`.

## 3. Platform checks

| What | How | Expected |
|---|---|---|
| GitOps | Argo CD UI | All apps *Synced* + *Healthy*. Delete a pod or change a replica count by hand: Argo CD puts it back |
| Metrics | Grafana -> "Cloud Security Monitoring" | RPS, error rate, latency per service, HPA replicas, CPU and memory |
| Logs | Grafana -> Explore -> Loki `{namespace="cloud-sec"}` | JSON log lines from all six services |
| HPA | `k6 run -e BASE_URL=$INGEST loadtest/ingest-load.js` (or the Docker form in `loadtest/README.md`), then `kubectl -n cloud-sec get hpa -w` | log-ingestion scales from 2 toward 8 pods, then back after the load stops |

The cluster is capped at 3 nodes for the free-trial CPU quota. If HPA pods stay *Pending* under load, the nodes are full; that still demonstrates the HPA deciding to scale, but lower `PEAK_RPS` if you want every pod scheduled.

## 4. If something fails

| Symptom | Likely cause | Where to look |
|---|---|---|
| `/ready` 503 on log-ingestion, event-store, correlation-engine | Kafka login | `kubectl -n cloud-sec get secret kafka-credentials`; pod logs for SASL errors |
| incident-management `/ready` stays 503 | Cloud SQL unreachable or wrong password | `kubectl -n cloud-sec logs deploy/incident-management` |
| event-store 503 or DynamoDB errors | AWS key / region | `event-store-cloud` secret; pod logs mention `DescribeTable` |
| Events in `raw-events` but none in `normalized-events` | connector or function | connector status and errors in Confluent; function logs in Google Cloud |
| Nothing in `results` | Flink job not running, or watermark not advanced yet | Dataproc job logs; send more events and wait |
