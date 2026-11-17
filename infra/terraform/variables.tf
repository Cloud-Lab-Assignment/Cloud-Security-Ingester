variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-south-1"
}

variable "project" {
  description = "Name prefix for all resources"
  type        = string
  default     = "cloud-sec"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "az_count" {
  description = "Number of AZs (MSK needs >= 2; 3 recommended)"
  type        = number
  default     = 3
}

# ---------- EKS ----------
variable "eks_version" {
  type    = string
  default = "1.30"
}

variable "eks_node_instance_types" {
  type    = list(string)
  default = ["t3.large"]
}

variable "eks_node_min" {
  type    = number
  default = 2
}

variable "eks_node_max" {
  description = "Max nodes; must leave room for HPA scale-out during the load test"
  type        = number
  default = 6
}

variable "eks_node_desired" {
  type    = number
  default = 3
}

# ---------- MSK ----------
variable "msk_kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "msk_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

# ---------- EMR / Flink ----------
variable "emr_release_label" {
  description = "EMR release that ships Flink 1.20 (matches services/security-analytics/pom.xml)"
  type        = string
  default     = "emr-7.5.0"
}

variable "emr_instance_type" {
  type    = string
  default = "m5.xlarge"
}

variable "emr_core_count" {
  type    = number
  default = 2
}

# ---------- GitOps ----------
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
  name = "${var.project}-${var.environment}"

  azs = slice(data.aws_availability_zones.available.names, 0, var.az_count)

  # Microservices that run as containers on EKS (the other two: event-normalizer = Lambda,
  # security-analytics = Flink on EMR).
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

  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}
