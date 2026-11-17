# Terraform

Three providers: **Google Cloud** (most of the system), **Confluent Cloud** (Kafka) and **AWS** (only DynamoDB).

| File | What |
|---|---|
| `network.tf` | APIs, VPC + subnet (with GKE pod/service ranges), firewall, Cloud NAT, private services access for Cloud SQL |
| `gke.tf` | Artifact Registry, GKE cluster + autoscaling node pool, `cloud-sec` namespace, `infra-endpoints` ConfigMap, Secrets (`kafka-credentials`, `incident-db`, `event-store-cloud`), CI service account |
| `confluent.tf` | Confluent environment, Basic Kafka cluster on GCP, service account + API key, 7 topics, the managed connector that triggers the function |
| `function.tf` | event-normalizer Cloud Run function (2nd gen), Secret Manager secret, invoker identity for the connector |
| `dataproc.tf` | Dataproc cluster with the Flink component, Kafka settings passed to the job's containers |
| `storage.tf` | Cloud Storage buckets (raw archive, artifacts), HMAC key for the Event Store, Cloud SQL PostgreSQL 16 |
| `dynamodb.tf` | calls `modules/core-data` (DynamoDB tables), IAM user + access key for the Event Store |
| `helm.tf` | Argo CD (+ root app-of-apps), kube-prometheus-stack, Loki |
| `outputs.tf` | endpoints, IPs, the Flink submit command, the CI key |

Apply order and everything else: [docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md).
`terraform validate` passes (Terraform 1.9, google 6.50, confluent 2.95, aws 5.100); nothing has been applied yet.
