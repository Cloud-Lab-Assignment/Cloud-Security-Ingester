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

output "data_access_policy_arn" {
  description = "IAM policy granting the event-store access to its tables."
  value       = aws_iam_policy.data_access.arn
}
