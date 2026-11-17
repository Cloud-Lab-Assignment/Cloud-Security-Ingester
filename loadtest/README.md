# Load test (HPA demo)

Prereqs: [k6](https://k6.io/docs/get-started/installation/), cluster deployed via ArgoCD.

```bash
# 1. find the public URL of log-ingestion
kubectl -n cloud-sec get svc log-ingestion

# 2. in a second terminal, watch the autoscaler
kubectl -n cloud-sec get hpa -w
kubectl -n cloud-sec top pods

# 3. smoke test (30s, 20 RPS)
k6 run -e PROFILE=smoke -e BASE_URL=http://<hostname> loadtest/ingest-load.js

# 4. full test (~12 min): 20 RPS -> 300 RPS, 5 min hold, back to 20 RPS
k6 run -e BASE_URL=http://<hostname> loadtest/ingest-load.js
```

Expected: CPU on `log-ingestion` exceeds the 50% target during the ramp, replicas rise from 2 toward 8,
then return to 2 about 2 minutes after the load drops. `detection-rules` (consuming `normalized-events`)
and `correlation-engine` scale from Kafka-driven CPU load. Capture screenshots of the Grafana
"HPA replicas" panel for the report.

If replicas don't rise, lower the target with `PEAK_RPS` higher (e.g. 600), or check `kubectl top pods` works (metrics-server).
Failed-login events reuse a pool of 20 IPs so the brute-force detection rule also fires during the run.
