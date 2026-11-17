output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "ecr_repositories" {
  description = "Image repositories, one per Kubernetes service"
  value       = { for k, r in aws_ecr_repository.svc : k => r.repository_url }
}

output "ecr_registry" {
  description = "ECR registry host; replace ACCOUNT_ID in gitops/ with the account part"
  value       = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com"
}

output "msk_bootstrap_brokers" {
  value = aws_msk_cluster.main.bootstrap_brokers
}

output "kafka_topics_expected" {
  value = local.kafka_topics
}

output "artifacts_bucket" {
  description = "Upload the Flink jar to s3://<this>/artifacts/security-analytics.jar"
  value       = aws_s3_bucket.artifacts.id
}

output "raw_bucket_name" {
  value = module.core_data.raw_bucket_name
}

output "dynamodb_tables" {
  value = {
    events       = module.core_data.events_table_name
    correlations = module.core_data.correlations_table_name
    tpc          = module.core_data.tpc_table_name
  }
}

output "rds_endpoint" {
  value = module.core_data.rds_endpoint
}

output "rds_secret_arn" {
  value = module.core_data.rds_master_secret_arn
}

output "normalizer_lambda" {
  value = aws_lambda_function.normalizer.function_name
}

output "emr_cluster_id" {
  value = aws_emr_cluster.flink.id
}

output "argocd_admin_password_command" {
  description = "Run in AWS CloudShell (kubectl preinstalled) or any shell with kubectl"
  value       = "kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d"
}

output "load_balancer_hostnames" {
  description = "Public hostnames of the argocd / grafana / log-ingestion Services"
  value = {
    argocd  = try(data.kubernetes_service.argocd.status[0].load_balancer[0].ingress[0].hostname, "pending - re-run terraform apply")
    grafana = try(data.kubernetes_service.grafana.status[0].load_balancer[0].ingress[0].hostname, "pending - re-run terraform apply")
  }
}
