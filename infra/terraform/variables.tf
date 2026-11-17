variable "project" {
  description = "Name prefix for all resources"
  type        = string
  default     = "cloud-sec"
}

# ---------- Google Cloud ----------
variable "gcp_project_id" {
  description = "Google Cloud project ID (billing must be enabled)"
  type        = string
}

variable "gcp_region" {
  description = "Region for GKE, Dataproc, Cloud SQL, the function and the Confluent cluster"
  type        = string
  default     = "asia-south1"
}

variable "gcp_zone" {
  description = "Zone for the (zonal) GKE cluster and Dataproc"
  type        = string
  default     = "asia-south1-a"
}

variable "gke_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "gke_node_min" {
  type    = number
  default = 2
}

variable "gke_node_max" {
  description = "Max nodes; must leave room for HPA scale-out during the load test"
  type        = number
  default     = 5
}

variable "cloudsql_tier" {
  type    = string
  default = "db-custom-1-3840"
}

variable "dataproc_image_version" {
  description = "Dataproc image; 2.2 ships Flink 1.17.0 on Java 11 (matches services/security-analytics/pom.xml)"
  type        = string
  default     = "2.2-debian12"
}

variable "dataproc_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "dataproc_workers" {
  type    = number
  default = 2
}

# ---------- Confluent Cloud ----------
variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud resource management scope)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  type      = string
  sensitive = true
}

# ---------- AWS (DynamoDB only) ----------
variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

# ---------- GitOps / add-ons ----------
variable "gitops_repo_url" {
  type    = string
  default = "https://github.com/ashkew2202/Cloud-Security-Ingester.git"
}

variable "gitops_revision" {
  type    = string
  default = "main"
}

variable "gitops_path" {
  description = "Path in the repo holding the ArgoCD root (app-of-apps) manifests"
  type        = string
  default     = "gitops/argocd"
}

variable "argocd_admin_password" {
  description = "ArgoCD UI password for user admin"
  type        = string
  sensitive   = true
  default     = "change-me-argocd"
}

variable "grafana_admin_password" {
  type      = string
  sensitive = true
  default   = "change-me-please"
}

locals {
  name = var.project

  # Microservices that run as containers on GKE (the other two: event-normalizer =
  # Cloud Run function, security-analytics = Flink on Dataproc).
  k8s_services = [
    "log-ingestion",
    "detection-rules",
    "correlation-engine",
    "event-store",
    "incident-management",
    "investigation-query-api",
  ]

  kafka_topics = [
    "raw-events",
    "raw-events-dlq",
    "normalized-events",
    "normalized-events-dlq",
    "security-alerts",
    "incident-updates",
    "results",
  ]

  # "SASL_SSL://pkc-xxxxx.asia-south1.gcp.confluent.cloud:9092" -> "pkc-...:9092"
  kafka_bootstrap = replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")

  registry = "${var.gcp_region}-docker.pkg.dev/${var.gcp_project_id}/${google_artifact_registry_repository.images.repository_id}"
}

resource "random_id" "suffix" {
  byte_length = 3
}
