# Core services: Terraform (AWS)

AWS resources for Rishit's three services (event-store, incident-management, correlation-engine).
The correlation-engine needs no AWS resources of its own.

## What it creates

| File | Resources |
|---|---|
| `dynamodb.tf` | `${name_prefix}events` (PK `entityKey`, SK `sk`, GSI `eventId-index`, projection ALL), `${name_prefix}correlations` (PK `correlationId`, SK `version` N), `${name_prefix}tpc` (PK `txId`). All PAY_PER_REQUEST. |
| `s3.tf` | Raw-events archive bucket: public access blocked, bucket-owner-enforced, SSE-S3 (AES256), `raw/` expires after 30 days, `force_destroy = true`. |
| `rds.tf` | PostgreSQL 16 on `db.t4g.micro`, 20 GB gp3, encrypted, not public, master password managed by RDS in Secrets Manager, no final snapshot, no deletion protection. A DB subnet group and a security group allowing 5432 from `allowed_security_group_ids`. |
| `iam.tf` | Policy `${name_prefix}core-services-data-access`: DynamoDB `GetItem/PutItem/UpdateItem/Query/Scan/DescribeTable` on the 3 tables and the GSI, `s3:PutObject/GetObject` on `raw/*`, `s3:ListBucket` on the bucket. |
| `outputs.tf` | `events_table_name`, `correlations_table_name`, `tpc_table_name`, `raw_bucket_name`, `rds_endpoint` (hostname), `rds_port`, `rds_database_name`, `rds_master_secret_arn`, `data_access_policy_arn`. |

The provider tags everything with `Project`, `Component`, `Owner` and `ManagedBy` via `default_tags`.

## Variables

| Name | Default | Notes |
|---|---|---|
| `region` | `ap-south-1` | |
| `name_prefix` | `cs-` | Must match the event-store's `TABLE_PREFIX`. |
| `vpc_id` | required | The EKS cluster's VPC. |
| `db_subnet_ids` | required | At least two private subnets in different AZs. |
| `allowed_security_group_ids` | required | SGs allowed to reach Postgres (EKS node/pod SG). |
| `raw_bucket_name` | required | Globally unique; set the event-store's `RAW_BUCKET` to the same value. |

There is no backend block. The group's IaC decides where state lives (e.g. an S3 backend added at integration time).

## Validate only (in this repo)

Only these commands are run here. Nothing in this repo touches AWS:

```sh
cd infra/terraform
terraform fmt -check
terraform init -backend=false
terraform validate   # -> Success! The configuration is valid.
```

**Never run `terraform plan`, `apply` or `destroy` from this repo.** AWS credits belong to the group.
`.terraform/` is gitignored; `.terraform.lock.hcl` is committed to pin the provider.

## Deployment

Deployment is Ansh's job, through the group's IaC/GitOps pipeline. See `docs/handoff.md` for how the
outputs map to the services' environment variables and IAM roles.
