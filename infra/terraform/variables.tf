variable "region" {
  description = "AWS region for all core-services resources."
  type        = string
  default     = "ap-south-1"
}

variable "name_prefix" {
  description = "Prefix for resource names. Must match the services' TABLE_PREFIX env var."
  type        = string
  default     = "cs-"
}

variable "vpc_id" {
  description = "VPC that hosts the EKS cluster; the RDS instance and its security group live here."
  type        = string
}

variable "db_subnet_ids" {
  description = "Private subnet IDs for the RDS subnet group (at least two, in different AZs)."
  type        = list(string)

  validation {
    condition     = length(var.db_subnet_ids) >= 2
    error_message = "RDS needs at least two subnets in different availability zones."
  }
}

variable "allowed_security_group_ids" {
  description = "Security groups allowed to reach Postgres on 5432 (e.g. the EKS node or pod security group)."
  type        = list(string)
}

variable "raw_bucket_name" {
  description = "Globally unique S3 bucket name for the raw-events archive. Services read it from RAW_BUCKET."
  type        = string
}
