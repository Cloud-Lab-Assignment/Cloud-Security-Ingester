module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = local.name
  cluster_version = var.eks_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true

  cluster_addons = {
    coredns                = {}
    kube-proxy             = {}
    vpc-cni                = {}
    eks-pod-identity-agent = {}
    # metrics-server is what the HPAs (CPU / memory) read from.
    metrics-server = {}
  }

  eks_managed_node_groups = {
    default = {
      instance_types = var.eks_node_instance_types
      min_size       = var.eks_node_min
      max_size       = var.eks_node_max
      desired_size   = var.eks_node_desired
      disk_size      = 30
    }
  }
}

# ---------------------------------------------------------------- IRSA
# Only the event-store touches AWS APIs (DynamoDB + S3), so it gets the data-access
# policy from the core-data module. correlation-engine / incident-management /
# investigation-query-api need no AWS permissions.
module "event_store_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.44"

  role_name = "${local.name}-event-store"

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["cloud-sec:event-store"]
    }
  }

  role_policy_arns = {
    data = module.core_data.data_access_policy_arn
  }
}

resource "kubernetes_namespace" "app" {
  metadata {
    name = "cloud-sec"
  }
  depends_on = [module.eks]
}

# ServiceAccount referenced by gitops/services/event-store/deployment.yaml.
resource "kubernetes_service_account" "event_store" {
  metadata {
    name      = "event-store"
    namespace = kubernetes_namespace.app.metadata[0].name
    annotations = {
      "eks.amazonaws.com/role-arn" = module.event_store_irsa.iam_role_arn
    }
  }
}

# ---------------------------------------------------------------- runtime config
# Non-secret endpoints, consumed through envFrom by every Deployment.
# Variable names follow docs/handoff.md.
resource "kubernetes_config_map" "infra" {
  metadata {
    name      = "infra-endpoints"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    KAFKA_BROKERS           = aws_msk_cluster.main.bootstrap_brokers
    KAFKA_BOOTSTRAP_SERVERS = aws_msk_cluster.main.bootstrap_brokers
    KAFKA_SSL               = "false" # MSK plaintext listener, VPC-only (see msk.tf)
    AWS_REGION              = var.region
    RAW_BUCKET              = module.core_data.raw_bucket_name
    TABLE_PREFIX            = "cs-"
    PGHOST                  = module.core_data.rds_endpoint
    PGPORT                  = tostring(module.core_data.rds_port)
    PGDATABASE              = module.core_data.rds_database_name
    DATABASE_SSL            = "true"
    EVENT_STORE_URL         = "http://event-store:8081"
    COORDINATOR_URL         = "http://incident-management:8082"
    INCIDENT_GRPC_ADDR      = "incident-management:50051"
    INCIDENT_MANAGEMENT_URL = "http://incident-management:8082"
  }
}

# RDS master credentials -> K8s Secret (PGUSER / PGPASSWORD for incident-management).
# Note: the value passes through Terraform state - keep the state bucket private/encrypted.
# For production, replace with External Secrets Operator reading rds_master_secret_arn.
data "aws_secretsmanager_secret_version" "rds" {
  secret_id = module.core_data.rds_master_secret_arn
}

resource "kubernetes_secret" "incident_db" {
  metadata {
    name      = "incident-db"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    PGUSER     = jsondecode(data.aws_secretsmanager_secret_version.rds.secret_string)["username"]
    PGPASSWORD = jsondecode(data.aws_secretsmanager_secret_version.rds.secret_string)["password"]
  }
}
