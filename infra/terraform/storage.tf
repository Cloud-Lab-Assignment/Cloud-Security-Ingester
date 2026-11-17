# Storage layout (requirement g: three distinct products):
#   S3        - raw-events archive              (module core-data, Event Store)
#   DynamoDB  - events / correlations / 3PC log (module core-data, Event Store)
#   RDS PG    - incidents + 3PC coordinator log (module core-data, Incident Management)
# The data layer is Rishit's Terraform, consumed here as a module. This file also
# creates a separate bucket for platform artifacts (Flink jar, checkpoints, EMR logs).

resource "random_id" "suffix" {
  byte_length = 3
}

module "core_data" {
  source = "./modules/core-data"

  name_prefix                = "cs-" # must equal TABLE_PREFIX in the event-store
  vpc_id                     = module.vpc.vpc_id
  db_subnet_ids              = module.vpc.private_subnets
  allowed_security_group_ids = [module.eks.node_security_group_id]
  raw_bucket_name            = "${local.name}-raw-${random_id.suffix.hex}"
}

# ---------------------------------------------------------------- artifacts bucket
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${local.name}-artifacts-${random_id.suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "expire-checkpoints-and-logs"
    status = "Enabled"
    filter {
      prefix = "flink-checkpoints/"
    }
    expiration {
      days = 7
    }
  }
}
