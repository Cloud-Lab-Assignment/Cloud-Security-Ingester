terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# No provider block: this module is called from infra/terraform (the root),
# which configures the AWS provider and default_tags.
