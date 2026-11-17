# Terraform

Root module = platform; `modules/core-data` = data layer for the core services (Rishit).

| File | What |
|---|---|
| `network.tf` | VPC, NAT, S3/DynamoDB gateway endpoints, MSK + Lambda + EMR security groups |
| `eks.tf` | EKS, event-store IRSA, namespace, `infra-endpoints` ConfigMap, `incident-db` Secret |
| `storage.tf` | calls `modules/core-data` (DynamoDB x3, S3 raw bucket, RDS PostgreSQL) + artifacts bucket |
| `msk.tf` | Managed Kafka |
| `lambda.tf` | event-normalizer Lambda + MSK event-source mapping |
| `emr.tf` | EMR cluster with Flink, job submission step |
| `ecr.tf` | one image repository per Kubernetes service |
| `helm.tf` | ArgoCD (+ root app-of-apps), kube-prometheus-stack, Loki |
| `outputs.tf` | endpoints and hostnames |

Apply order and everything else: [docs/DEPLOYMENT.md](../../docs/DEPLOYMENT.md). The module's own notes: [modules/core-data/README.md](modules/core-data/README.md).
Run `terraform fmt && terraform validate` before the first apply (not yet run in this workspace).
