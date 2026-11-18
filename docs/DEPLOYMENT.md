# Deployment guide (Google Cloud + Confluent Cloud + AWS DynamoDB)

Everything is provisioned by Terraform and deployed by Argo CD from Git. You never run `kubectl apply`.

- **Google Cloud** (`asia-south1`): GKE, Dataproc (Flink), the Cloud Run function, Cloud SQL, Cloud Storage, Artifact Registry.
- **Confluent Cloud**: Kafka (Basic cluster hosted on Google Cloud, same region) and the managed connector that triggers the function.
- **AWS** (`ap-south-1`): only the Event Store's three DynamoDB tables (on-demand, costs cents).

Rough running cost: about US$6-10/day on Google Cloud (GKE nodes, Dataproc, Cloud SQL, NAT) plus Confluent Basic usage. **Destroy when done (section 9).**

## 0. What gets built

| # | Service | Runs on | Talks via |
|---|---|---|---|
| 1 | log-ingestion | GKE (public LoadBalancer) | REST in, Kafka `raw-events` out |
| 2 | event-normalizer | **Cloud Run function** (triggered by Confluent's Cloud Functions Gen 2 Sink connector) | Kafka `raw-events` -> `normalized-events` / DLQ |
| 3 | correlation-engine | GKE | Kafka `security-alerts` in, **gRPC** to incident-management, REST to event-store |
| 4 | event-store | GKE | Kafka in, REST API, 3PC participant; Cloud Storage + DynamoDB |
| 5 | detection-rules | GKE | Kafka `normalized-events` -> `security-alerts` |
| 6 | incident-management | GKE (1 pod, no HPA) | gRPC + REST, 3PC coordinator; Cloud SQL PostgreSQL; Kafka `incident-updates` |
| 7 | investigation-query-api | GKE (public LoadBalancer) | REST to event-store and incident-management |
| 8 | security-analytics | **Flink on Dataproc** | Kafka `normalized-events` -> windowed aggregation -> `results` |

## 1. Prerequisites

1. **Google Cloud:** a project with billing (your credits) enabled. Install the [gcloud CLI](https://cloud.google.com/sdk/docs/install), then:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   gcloud config set project <your-project-id>
   ```
2. **Confluent Cloud:** sign up at confluent.cloud (new accounts get free credits). Create a **Cloud resource management** API key: *Administration -> API keys -> Add key -> My account (or a service account with OrganizationAdmin) -> Cloud resource management*. Keep the key and secret.
3. **AWS:** an account and `aws configure` with a user that can create DynamoDB tables and IAM users.
4. Tools: Terraform >= 1.6, Docker, Node 22, git. Optional: k6 (or run it through Docker), Maven + JDK 11/17 (or build the jar through Docker as shown below).
5. A **public** GitHub repo you can push to (Argo CD reads it anonymously).

You do not need `kubectl`. If you ever want a cluster shell, use **Cloud Shell** in the Google Cloud console and run the `gke_get_credentials` command that Terraform prints.

## 1b. Test locally first (no cloud, about 5 minutes)

```bash
npm ci && npm test                                   # unit tests (use Node 22)
docker compose -f docker-compose.yml -f docker-compose.full.yml up -d --build
scripts/e2e-local.sh                                 # 6 failed logins -> incident via the query API
docker run --rm -v "$PWD/loadtest:/lt:ro" grafana/k6 run -e PROFILE=smoke \
  -e BASE_URL=http://host.docker.internal:3000 /lt/ingest-load.js
docker compose -f docker-compose.yml -f docker-compose.full.yml down -v
```

Runs every service except Flink; the normalizer handler is driven by a local Kafka consumer. Verified 17 Nov 2026: e2e PASS, k6 smoke 0% errors, Cloud Run function entry point PASS.

## 2. Provision the infrastructure

Create `infra/terraform/terraform.tfvars` (it is git-ignored, never commit it):

```hcl
gcp_project_id             = "<your-gcp-project-id>"
confluent_cloud_api_key    = "<confluent cloud api key>"
confluent_cloud_api_secret = "<confluent cloud api secret>"
gitops_repo_url            = "https://github.com/<you>/<repo>.git"
argocd_admin_password      = "<pick one>"
grafana_admin_password     = "<pick one>"
```

Then:

```bash
cd infra/terraform
terraform init

# Step 1: network, cluster, Kafka (the Helm/Kubernetes providers need the cluster first)
terraform apply -target=google_container_node_pool.main -target=confluent_kafka_topic.topics

# Step 2: everything else
terraform apply
```

Takes ~25-35 minutes (Cloud SQL, GKE, Dataproc and the function build are the slow parts). If the first run fails on an API that was "just enabled", wait a minute and run `terraform apply` again. If an IP in `load_balancer_ips` says "pending", run `terraform apply` once more.

What it creates: VPC + Cloud NAT, GKE (autoscaling 2-3 nodes: the free trial allows 24 vCPUs and Dataproc uses part of them), Artifact Registry, Confluent environment/cluster/topics/API key/connector, the Cloud Run function, Dataproc with Flink, Cloud SQL PostgreSQL 16 (private IP), two Cloud Storage buckets, DynamoDB tables (AWS), Argo CD + root app, Prometheus/Grafana, Loki/Promtail, the `cloud-sec` namespace, the `infra-endpoints` ConfigMap and three Secrets (`kafka-credentials`, `incident-db`, `event-store-cloud`).

## 3. Point GitOps at your registry and push

```bash
scripts/set-registry.sh <your-gcp-project-id>        # rewrites PROJECT_ID in gitops/
git add gitops && git commit -m "gitops: set registry" && git push origin main
```

Argo CD (already watching `gitops/argocd` on `main`) creates all service Applications. Pods sit in `ImagePullBackOff` until images exist (next step).

## 4. Build images

**Option A, CI (preferred).** In GitHub -> Settings -> Secrets and variables -> Actions (the workflow already requests `contents: write` for the jobs that commit, so the repository's default "Workflow permissions" setting does not need changing):
- secret `GCP_SA_KEY` = output of `terraform -chdir=infra/terraform output -raw ci_service_account_key | base64 -d`
- variable `GCP_PROJECT_ID` = your project id (and `GCP_REGION` if not `asia-south1`)

Push any change to `main`, or open **Actions -> ci -> Run workflow**. `.github/workflows/ci.yml` tests, builds the six images, pushes them to Artifact Registry tagged with the commit SHA, then commits that tag into `gitops/services/*/kustomization.yaml`. Argo CD syncs the commit. That commit is the only way a deployment changes.

**Option B, by hand** (from the repo root):

```bash
REG=asia-south1-docker.pkg.dev/<your-gcp-project-id>/cloud-sec
gcloud auth configure-docker asia-south1-docker.pkg.dev
TAG=v1
docker build -t $REG/log-ingestion:$TAG   services/log-ingestion
docker build -t $REG/detection-rules:$TAG services/detection-rules
for s in event-store incident-management correlation-engine investigation-query-api; do
  docker build -f services/$s/Dockerfile -t $REG/$s:$TAG .
done
for s in log-ingestion detection-rules event-store incident-management correlation-engine investigation-query-api; do
  docker push $REG/$s:$TAG
done
sed -i "s/newTag: .*/newTag: $TAG/" gitops/services/*/kustomization.yaml
git commit -am "gitops: deploy $TAG" && git push origin main
```

## 5. Deploy the Flink job on Dataproc

```bash
# build (Maven via Docker, no local Java needed)
docker run --rm -v "$PWD/services/security-analytics:/w" -w /w maven:3.9-eclipse-temurin-17 mvn -q -B package
BUCKET=$(terraform -chdir=infra/terraform output -raw artifacts_bucket)
gcloud storage cp services/security-analytics/target/security-analytics-1.0.0.jar gs://$BUCKET/artifacts/security-analytics.jar

# submit (the exact command is also printed as the flink_submit_command output)
$(terraform -chdir=infra/terraform output -raw flink_submit_command)
```

Kafka address and credentials reach the job through the cluster's `flink:containerized.*.env.*` properties set by Terraform, so the submit command needs no arguments. Check it in the console: **Dataproc -> Clusters -> cloud-sec-flink -> Web Interfaces -> Flink** (job RUNNING) and **Jobs**.

The job is built for Flink 1.17 on Java 11 to match the Dataproc 2.2 image.

## 6. Verify

All from a browser or `curl`; no kubectl.

1. **Argo CD:** `http://<load_balancer_ips.argocd>`, user `admin`, your password. Every Application `Synced` + `Healthy`. Click `log-ingestion` and `investigation-query-api`; the Service node shows each external IP.
2. **Send events:**
   ```bash
   for i in 1 2 3 4 5 6; do
     curl -s -X POST http://<log-ingestion-ip>/events -H 'content-type: application/json' -d \
       "{\"source\":{\"type\":\"auth_log\",\"name\":\"bastion-ssh\"},\"format\":\"text\",\"payload\":\"Nov 16 10:00:0$i bastion sshd[1]: Failed password for root from 203.0.113.9 port 4242$i ssh2\"}"
   done
   curl -s http://<query-api-ip>/incidents | jq
   curl -s http://<query-api-ip>/incidents/<incidentId> | jq    # incident + history + correlation history + events
   ```
3. **Pipeline evidence:**
   - Confluent Cloud console: topics with messages; **Connectors -> cloud-sec-normalizer-trigger** Running, records processed.
   - Google Cloud console: **Cloud Run functions -> cloud-sec-event-normalizer** (invocations, logs); **Cloud Storage** raw bucket objects under `raw/`; **Cloud SQL**; **Dataproc** Flink job.
   - AWS console: DynamoDB `cs-events`, `cs-correlations`, `cs-tpc` items.
4. **Grafana:** `http://<load_balancer_ips.grafana>`, user `admin`. Dashboard "Cloud Security Monitoring" and Explore -> Loki `{namespace="cloud-sec"}`. Function logs are in Cloud Logging; Flink logs in the Dataproc job page.

## 7. Load test (HPA scaling)

```bash
docker run --rm -v "$PWD/loadtest:/lt:ro" grafana/k6 run -e BASE_URL=http://<log-ingestion-ip> /lt/ingest-load.js
```

Watch the Grafana panel "HPA replicas (current vs desired)" and Argo CD's resource tree: log-ingestion goes 2 -> up to 8 pods, and the node pool grows if needed, then both shrink after the load stops. Screenshot for the report.

## 8. Demonstrating 3PC (requirement f)

Fault switches (`TPC_FORCE_VOTE`, `TPC_FAULT`) must never be set through GitOps. Demo the failure scenarios locally: `docker compose up -d --build` then `scripts/demo-3pc.sh` (see `docs/handoff.md`, "3PC demo"). In the cloud show the happy path: the incident in the query API, its committed transaction in DynamoDB `cs-tpc`, the matching version in `cs-correlations`, and incident-management's logs in Grafana/Loki (`{app="incident-management"}`).

## 9. Teardown

```bash
cd infra/terraform
terraform destroy -target=helm_release.argocd_root   # removes app LoadBalancers first
terraform destroy
```

If a destroy stalls on the VPC, delete leftover forwarding rules / load balancers in the console and re-run.

## 10. Known limits and caveats

- `terraform validate` passes, but **nothing has been applied** yet; expect the first real apply to surface account-side issues (quotas, an API needing a minute after being enabled, Confluent region availability).
- Confluent's connector sends **record values only**. The function gives each normalized event a synthetic Kafka origin (`raw-events`, partition 0, offset = receive time); the real topic/partition/offset are not known to it.
- The function's ingress is open to the internet (Confluent calls it from outside your VPC) but every request needs a Google identity token of the invoker service account.
- Secrets (Kafka key, DB password, AWS access key, HMAC key) pass through Terraform state and are visible in the Dataproc cluster properties. Keep the state private; production would use Secret Manager + External Secrets.
- Cloud SQL is reached over its private IP without TLS (traffic stays inside the VPC).
- One `@core/tpc` test fails on Node 24 / Windows and passes 77/77 on Node 22 / Linux (what the images and CI use).
- `services/*/node_modules` are committed upstream; consider removing them from Git.
