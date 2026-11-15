terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Credentials come from the standard AWS provider chain (env, profile, IRSA, ...).
# Nothing secret is configured here.
provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "cloud-security-monitoring"
      Component = "core-services"
      Owner     = "rishit"
      ManagedBy = "terraform"
    }
  }
}
