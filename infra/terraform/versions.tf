terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.10"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.10"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.31"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.14"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Optional remote state (create the bucket first):
  # backend "gcs" {
  #   bucket = "<your-tfstate-bucket>"
  #   prefix = "cloud-security-ingester"
  # }
}

# --- Google Cloud: GKE, Dataproc, Cloud Run function, Cloud SQL, Cloud Storage, Artifact Registry
# Credentials: `gcloud auth application-default login`
provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# --- Confluent Cloud: Kafka cluster, topics, the connector that triggers the function
# Credentials: a Cloud API key (not a cluster key) from Confluent Cloud > API keys
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# --- AWS: only the Event Store's DynamoDB tables
# Credentials: `aws configure` or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "google_client_config" "current" {}

provider "kubernetes" {
  host                   = "https://${google_container_cluster.main.endpoint}"
  token                  = data.google_client_config.current.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.main.master_auth[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = "https://${google_container_cluster.main.endpoint}"
    token                  = data.google_client_config.current.access_token
    cluster_ca_certificate = base64decode(google_container_cluster.main.master_auth[0].cluster_ca_certificate)
  }
}
