output "events_table_name" {
  description = "DynamoDB table for normalized events (GSI eventId-index)."
  value       = aws_dynamodb_table.events.name
}

output "correlations_table_name" {
  description = "DynamoDB table for correlation-decision history."
  value       = aws_dynamodb_table.correlations.name
}

output "tpc_table_name" {
  description = "DynamoDB table for the Event Store's 3PC participant log."
  value       = aws_dynamodb_table.tpc.name
}

output "raw_bucket_name" {
  description = "S3 bucket for the raw-events archive (event-store RAW_BUCKET)."
  value       = aws_s3_bucket.raw.bucket
}

output "rds_endpoint" {
  description = "RDS hostname (incident-management PGHOST)."
  value       = aws_db_instance.incidents.address
}

output "rds_port" {
  description = "RDS port (incident-management PGPORT)."
  value       = aws_db_instance.incidents.port
}

output "rds_database_name" {
  description = "Database name (incident-management PGDATABASE)."
  value       = aws_db_instance.incidents.db_name
}

output "rds_master_secret_arn" {
  description = "Secrets Manager ARN of the RDS-managed master credentials (JSON keys: username, password)."
  value       = aws_db_instance.incidents.master_user_secret[0].secret_arn
}

output "data_access_policy_arn" {
  description = "IAM policy to attach to the event-store pod role (IRSA)."
  value       = aws_iam_policy.data_access.arn
}
