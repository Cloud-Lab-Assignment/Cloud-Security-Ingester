# Deployment guide (AWS, end to end)

Everything is provisioned by Terraform and deployed by ArgoCD from Git. You never run `kubectl apply`.
Region defaults to `ap-south-1`. Estimated cost while running: roughly US$8-12/day (EKS control plane, 3 x t3.large, 3 x MSK kafka.t3.small, 3 x EMR m5.xlarge, NAT gateway, RDS). **Destroy when done (section 9).**

## 0. What gets built

| # | Service | Owner | Runs on | Talks via |
|---|---|---|---|---|
| 1 | log-ingestion | Aaditya | EKS (public LoadBalancer) | REST in, Kafka `raw-events` out |
| 2 | event-normalizer | Aaditya | **Lambda** (MSK trigger) | Kafka `raw-events` -> `normalized-events` / DLQ |
| 3 | correlation-engine | Rishit | EKS | Kafka `security-alerts` in, **gRPC** to incident-management, REST to event-store |
| 4 | event-store | Rishit | EKS | Kafka in, REST API, 3PC participant; S3 + DynamoDB |
| 5 | detection-rules | Aaditya | EKS | Kafka `normalized-events` -> `security-alerts` |
| 6 | incident-management | Rishit | EKS (1 pod, no HPA) | gRPC + REST, 3PC coordinator; RDS PostgreSQL; Kafka `incident-updates` |
| 7 | investigation-query-api | Ansh | EKS (public LoadBalancer) | REST to event-store and incident-management |
| 8 | security-analytics | Aaditya | **Flink on EMR** | Kafka `normalized-events` -> windowed aggregation -> `results` |

Requirement map: REST + gRPC + Kafka are all used (a); one Lambda (b); HPAs on log-ingestion, detection-rules, correlation-engine, event-store (c); ArgoCD (d); Flink on EMR (e); 3PC between incident-management and event-store (f); S3 + DynamoDB + RDS (g); Prometheus, Grafana, Loki (h).

## 1. Prerequisites (your laptop)

- AWS account with admin rights and an access key (`aws configure`). Install: AWS CLI v2, Terraform >= 1.6, Node 22, Maven + JDK 17, Docker, k6, git.
- Fork/clone of the GitHub repo you will push to. The repo URL is a Terraform variable (`gitops_repo_url`); ArgoCD must be able to read it, so use a **public** repo (or add repo credentials to ArgoCD).
- You do not need `kubectl` locally. Where a cluster command is unavoidable use **AWS CloudShell** (browser terminal with kubectl) after `aws eks update-kubeconfig ...` (printed by Terraform).

## 2. Provision the infrastructure

```bash
cd infra/terraform
terraform init

# Two-step first apply: Helm/Kubernetes providers need the cluster to exist.
terraform apply -target=module.vpc -target=module.eks \
  -var gitops_repo_url=https://github.com/<you>/<repo>.git
terraform apply \
  -var gitops_repo_url=https://github.com/<you>/<repo>.git \
  -var 'argocd_admin_password=<choose>' -var 'grafana_admin_password=<choose>'
```

Takes ~25-35 min (EKS, MSK, EMR, RDS are slow). Before the full apply, install the Lambda's dependencies: `cd services/event-normalizer && npm ci --omit=dev` (they are already committed in this repo, so usually nothing to do).

`terraform output` then shows the ECR registry, RDS/MSK endpoints, and the ArgoCD and Grafana URLs. If a load-balancer hostname says "pending", run `terraform apply` once more.

What Terraform created (module `modules/core-data` is Rishit's data layer: DynamoDB `cs-events`, `cs-correlations`, `cs-tpc`, the raw-events S3 bucket, RDS PostgreSQL 16 and the event-store IAM policy):
VPC + NAT, EKS (+ metrics-server), ECR repos, MSK, Lambda + MSK trigger, EMR (Flink), ArgoCD + root app, kube-prometheus-stack, Loki/Promtail, the `cloud-sec` namespace, the `event-store` service account (IRSA), the `infra-endpoints` ConfigMap and the `incident-db` Secret.

## 3. Point GitOps at your registry

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
scripts/set-account-id.sh "$ACCOUNT_ID"          # rewrites ACCOUNT_ID in gitops/
git add gitops && git commit -m "gitops: set ECR registry" && git push origin main
```

ArgoCD (already watching `gitops/argocd` on `main`) now creates all service Applications. Pods stay `ImagePullBackOff` until images exist (next step).

## 4. Build images

Option A, CI (preferred): add repo secrets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (and variable `AWS_REGION` if not `ap-south-1`) in GitHub, then push any change to `main`. `.github/workflows/ci.yml` runs the tests, builds the six images, pushes them to ECR tagged with the commit SHA, then **commits the new tag into `gitops/services/*/kustomization.yaml`**. ArgoCD sees that commit and rolls out. That commit is the only way a deployment changes.

Option B, by hand (first bootstrap, or if CI is not set up): from the repo root

```bash
REG=$ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin $REG
TAG=v1
docker build -t $REG/cloud-sec/log-ingestion:$TAG        services/log-ingestion
docker build -t $REG/cloud-sec/detection-rules:$TAG      services/detection-rules
for s in event-store incident-management correlation-engine investigation-query-api; do
  docker build -f services/$s/Dockerfile -t $REG/cloud-sec/$s:$TAG .
done
for s in log-ingestion detection-rules event-store incident-management correlation-engine investigation-query-api; do
  docker push $REG/cloud-sec/$s:$TAG
done
# then set newTag: v1 in each gitops/services/*/kustomization.yaml, commit and push
sed -i "s/newTag: .*/newTag: $TAG/" gitops/services/*/kustomization.yaml
git commit -am "gitops: deploy $TAG" && git push origin main
```

## 5. Deploy the Flink job (security-analytics)

```bash
cd services/security-analytics && mvn -q package -DskipTests
BUCKET=$(terraform -chdir=../../infra/terraform output -raw artifacts_bucket)
aws s3 cp target/*.jar s3://$BUCKET/artifacts/security-analytics.jar   # use the shaded (non -original) jar
```

The cluster submits the job as a step at creation. If the jar was not uploaded yet that step failed (harmless): re-submit from the **EMR console -> your cluster -> Steps -> Add step** (type Custom JAR, JAR `command-runner.jar`, arguments = the same `flink run -m yarn-cluster -d ...` line shown in `infra/terraform/emr.tf`), or `aws emr add-steps`. Confirm in the EMR console (Application user interfaces -> Flink) that the job is RUNNING.

## 6. Verify

All checks work from a browser or `curl`; no kubectl required.

1. **ArgoCD** (hostname in the `load_balancer_hostnames` Terraform output, user `admin`, the password you set): every Application `Synced` + `Healthy`. Click `log-ingestion` -> the Service node shows its public LoadBalancer hostname.
2. **Send an event** (replace the host):
   ```bash
   curl -s -X POST http://<log-ingestion-host>/events -H 'content-type: application/json' -d '{
     "source":{"type":"auth_log","name":"bastion-ssh"},"format":"text",
     "payload":"Nov 16 10:00:00 bastion sshd[1234]: Failed password for root from 203.0.113.9 port 4242 ssh2"}'
   # -> 202 {"eventId":"...","status":"published"}
   ```
   Send 5+ of these from the same IP inside a minute to trigger `RULE-AUTH-001`; after a few seconds an incident exists:
   ```bash
   curl -s http://<investigation-query-api-host>/incidents | jq
   curl -s http://<investigation-query-api-host>/incidents/<incidentId> | jq     # incident + history + correlation history + events
   ```
3. **Pipeline evidence in the AWS console:** Lambda `...-event-normalizer` (Monitor -> invocations/logs), MSK topics, DynamoDB `cs-events` / `cs-tpc` items, S3 raw bucket objects under `raw/`, RDS tables via the query API, EMR Flink job.
4. **Grafana** (hostname in `load_balancer_hostnames`, user `admin`): dashboard "Cloud Security Monitoring" (RPS, 5xx rate, p50/p95/p99, HPA replicas, CPU/memory, node and pod health) and Explore -> Loki `{namespace="cloud-sec"}` for logs of every service.

## 7. Load test (HPA scaling)

```bash
k6 run -e BASE_URL=http://<log-ingestion-host> loadtest/ingest-load.js
```

Watch the Grafana panel "HPA replicas (current vs desired)" and ArgoCD's resource tree (pods appear as the Deployment scales): log-ingestion goes 2 -> up to 8 pods, then back after the load stops. Screenshot both for the report. See `loadtest/README.md`.

## 8. Demonstrating 3PC (requirement f)

The fault-injection switches (`TPC_FORCE_VOTE`, `TPC_FAULT`) must **never** be set through GitOps. Demo the failure scenarios locally on Docker Compose: `docker compose up -d --build` then `scripts/demo-3pc.sh` (see `docs/handoff.md`, "3PC demo"). In the cloud show the happy path: the incident in the query API, the COMMITTED transaction in the `cs-tpc` DynamoDB table (event-store participant) and the matching version in `cs-correlations`, plus the incident-management logs in Grafana/Loki (`{app="incident-management"}`).

## 9. Teardown

```bash
# 1. remove LoadBalancer Services first, otherwise their ELBs block VPC deletion
#    (delete the 'root' app in the ArgoCD UI with cascade, or:)
terraform -chdir=infra/terraform destroy -target=helm_release.argocd_root
# 2. everything else
terraform -chdir=infra/terraform destroy
```
If the destroy stalls on the VPC, delete leftover load balancers / ENIs in the EC2 console and re-run.

## 10. Known limits and caveats

- **Terraform was not validated or applied** in this workspace (no Terraform installed). Run `terraform fmt && terraform validate` first and expect to fix small provider-version issues.
- Kafka uses MSK's VPC-only plaintext listener (no SASL) because upstream services and the Flink job connect without credentials. Rishit's services support SASL/TLS through `KAFKA_*` env vars if you later switch to Confluent Cloud.
- The RDS password is copied into a Kubernetes Secret by Terraform (so it is in Terraform state). Keep state private. Production alternative: External Secrets Operator.
- `investigation-query-api` and `log-ingestion` are public and unauthenticated (demo). Add auth/WAF for anything real.
- `services/*/node_modules` are committed upstream (1,300+ files). Remove them from Git (`git rm -r --cached`) once the Lambda packaging runs `npm ci` in CI.
- `incident-management` test "recovery does not abort a tx that run() committed" is timing-sensitive and failed once in a full-suite run on Windows (passes alone).
- Only `RULE-AUTH-001` exists in detection-rules; the escalation path needs 3 alerts for one entity more than 60 s apart.
