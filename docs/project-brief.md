# Project brief: core services for Cloud Security Monitoring

This is the spec for the work in this repo. It condenses the assignment PDF, the group chat and the group repo as of 16 Nov 2026. The chat and the PDF are kept off Git on purpose, so this file is the source of truth.

## Assignment

- **Course:** CS/SS G527 Cloud Computing, BITS Pilani, Sem I 2025-26. 60 marks (15%), of which 12 are for individual contribution.
- **Deadline:** Nalanda, **Wed 18 Nov 2026, 23:59 IST**. A demonstration follows, where each member demos and answers questions on their own requirements and services.
- **Scenario S25:** "Cloud Security Monitoring: correlating events across simulated cloud services". The scenario's concept is **3PC** (three-phase commit).
- **The group's eight services:**
  1. Log Ingestion
  2. Event Normalizer
  3. Correlation Engine
  4. Event Store
  5. Detection Rules
  6. Incident Management
  7. Investigation Query API
  8. Security Analytics

### Requirements a–h

The PDF splits these two per member.

- **a.** All infrastructure (networks, clusters, databases, storage, IAM) is provisioned only through IaC, such as Terraform.
- **b.** One service is serverless and does an asynchronous, event-driven task. The group must use REST, gRPC **and** message queues, each at least once.
- **c.** Stateless services run on managed Kubernetes (EKS/GKE/AKS). At least two critical services get CPU/memory-based HPAs, and a load test shows them scaling out.
- **d.** Every Kubernetes deployment goes through a GitOps controller (ArgoCD/Flux). Direct `kubectl apply` is forbidden.
- **e.** A Flink job runs on a managed cluster, consumes Kafka, does a stateful windowed aggregation and writes to a separate `results` topic.
- **f.** The scenario's concept (3PC) is applied to the domain and shown in the demo.
- **g.** At least three distinct storage products: object storage (S3/GCS), managed SQL (RDS/Cloud SQL) and managed NoSQL (DynamoDB/Firestore).
- **h.** Prometheus and Grafana dashboards showing RPS, error rate, latency and cluster health, plus centralized logs from every service.

**Deliverables:**
- a design document covering the system overview, deployment architecture, microservice diagrams, responsibilities, interconnections and the rationale for each choice
- the code in GitHub, with all IaC, Kubernetes manifests and GitOps config
- `task_division.txt` listing each member's ID number, name and assigned tasks

## Who does what (from the group chat)

| Member | Owns |
|---|---|
| Ansh | CI/CD (GitHub Actions), GitOps, observability, the K8s cluster and final integration. Wants a **Dockerfile + Terraform** for each service and the **ports and AWS services** each one needs. |
| Aaditya | Log Ingestion, Event Normalizer (serverless), Detection Rules, Security Analytics (Flink). All are merged into the group repo. |
| Neeharika (joined 12 Nov) | Terraform and hosting for Kafka (Confluent Cloud), the Flink job and the deployment of Aaditya's services on AWS/GCP |
| **Rishit (this repo)** | **Correlation Engine, Incident Management Service, Event Store**, plus "the concept" (3PC), from the "core pipeline and the concept" bundle |

**Not yet decided:**
- **Investigation Query API:** no owner.
- **Rishit's two a–h requirements:** not named in the chat. The natural fit is **f (3PC)** and **g (three storage products)**, since all three storage products live in these services. Confirm with the group.

**Ground rules from Ansh:**
- Build each service as a Docker image.
- Use DynamoDB if a shared database is needed.
- Don't run a cluster yourself. Ansh deploys, and credits are tight.
- Push code to the group repo `Cloud-Lab-Assignment/Cloud-Security-Ingester` under `services/<name>/`, through a PR to the original repo's `main`, not a fork.

## Upstream contracts (group repo `main` @ `6a58033`)

The Node services use ESM, kafkajs 2.2.4, Fastify 5 and `node --test`. Kafka connection env vars are `KAFKA_BROKERS` (comma-separated), `KAFKA_SSL`, `KAFKA_USERNAME`, `KAFKA_PASSWORD` and `KAFKA_SASL_MECHANISM` (plain). Producers use `idempotent: true` and `acks: -1`. Topics are not auto-created, so infra must pre-create every topic.

| Topic | Producer → consumers | Key |
|---|---|---|
| `raw-events` | log-ingestion → event-normalizer, **event-store** | `source.name` |
| `normalized-events` | event-normalizer → detection-rules, security-analytics, **event-store** | `entityKey` |
| `security-alerts` | detection-rules → **correlation-engine** | `entityKey` |
| `results` | security-analytics → (none yet) | `metric\|value` |

The full schemas are in the group repo:
- `services/event-normalizer/src/schemas/*.json` (raw and normalized events)
- `services/detection-rules/src/services/detection-engine.js` (alerts)

**Field conventions:**
- `schemaVersion: "1.0"`
- UTC timestamps in the form `YYYY-MM-DDTHH:mm:ss.sssZ`
- a `{name, version}` service stamp
- `eventId` is a lowercase UUID v4 and stays the same across redelivery

**raw-events:**
```
schemaVersion, eventId, receivedAt, eventTime|null,
source:{type,name,host?,region?,accountId?}, format:"json"|"text",
payload, ingestedBy
```

**normalized-events:**
```
schemaVersion, eventId, eventTime, receivedAt, normalizedAt, entityKey,
category, action, outcome:"success"|"failure"|"unknown",
actor:{userId,userName,userType}, network:{srcIp,srcPort,dstIp,dstPort,protocol},
target:{resourceType,resourceId}, source, message, attributes,
origin:{topic,partition,offset}, normalizedBy
```
`entityKey` is `network.srcIp ?? actor.userId ?? source.name`.

**security-alerts:**
```
schemaVersion:"1.0", alertId:"RULE-AUTH-001|<entityKey>|<triggerEventId>",
alertTime, ruleId, ruleName, ruleVersion, severity:"high", alertType:"brute_force",
title, description, entityKey, entity:{srcIp,userId|null,host|null}, triggerEventId,
relatedEventIds (last 20 only), eventCount, window:{start,end,durationSeconds},
evidence:{threshold,observedFailures,usernamesTried[]}, status:"open",
raisedBy:{name:"detection-rules-service",version:"1.0.0"}
```

**Alert caveats:**
- `alertId` is deterministic, but alerts **can be redelivered**.
- `alertId` contains `|` and raw IPs, so it is not URL-safe.
- `relatedEventIds` is capped at 20.

## What these services must do

- **Event Store:** keeps every raw and normalized event and the history of correlation decisions.
  - Raw events are archived to **S3**.
  - Normalized events and correlation history go into **DynamoDB**.
  - A **REST** read API serves events by ID and by entity and time range.
  - It is a **3PC participant** for correlation-history writes.
- **Correlation Engine:** consumes `security-alerts` and groups related alerts for the same entity into candidate incidents, using a correlation window and severity escalation.
  - It enriches each candidate from the Event Store over REST.
  - It sends candidates to Incident Management over **gRPC**.
- **Incident Management:** creates and updates incidents in **PostgreSQL (RDS)**.
  - It is the **3PC coordinator**. Each correlation decision (an incident create or update) commits atomically across its own Postgres participant and the Event Store participant. Analyst status changes are plain local updates.
  - It exposes **gRPC** (for the Correlation Engine) and **REST** (list, get, status changes).
  - It publishes changes to the Kafka topic `incident-updates`.
- **All three:**
  - `/health`, `/ready` and Prometheus `/metrics`
  - pino JSON logs on stdout
  - a Dockerfile
  - Terraform for the AWS resources they need
