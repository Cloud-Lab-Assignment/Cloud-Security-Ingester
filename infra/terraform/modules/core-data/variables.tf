variable "name_prefix" {
  description = "Prefix for resource names. Must match the services' TABLE_PREFIX env var."
  type        = string
  default     = "cs-"
}
