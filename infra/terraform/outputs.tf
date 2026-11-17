output "gke_get_credentials" {
  description = "Only needed for optional kubectl access (e.g. from Cloud Shell)"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.main.name} --zone ${var.gcp_zone} --project ${var.gcp_project_id}"
}

output "image_registry" {
  description = "Artifact Registry path; scripts/set-registry.sh writes it into gitops/"
  value       = local.registry
}

output "kafka_bootstrap" {
  value = local.kafka_bootstrap
}

output "confluent_cluster_id" {
  value = confluent_kafka_cluster.main.id
}

output "artifacts_bucket" {
  description = "Upload the Flink jar to gs://<this>/artifacts/security-analytics.jar"
  value       = google_storage_bucket.artifacts.name
}

output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "dynamodb_tables" {
  value = {
    events       = module.core_data.events_table_name
    correlations = module.core_data.correlations_table_name
    tpc          = module.core_data.tpc_table_name
  }
}

output "cloudsql_private_ip" {
  value = google_sql_database_instance.incidents.private_ip_address
}

output "normalizer_function" {
  value = google_cloudfunctions2_function.normalizer.name
}

output "dataproc_cluster" {
  value = google_dataproc_cluster.flink.name
}

output "flink_submit_command" {
  value = "gcloud dataproc jobs submit flink --cluster=${google_dataproc_cluster.flink.name} --region=${var.gcp_region} --jar=gs://${google_storage_bucket.artifacts.name}/artifacts/security-analytics.jar"
}

output "load_balancer_ips" {
  description = "Public IPs of the argocd / grafana Services (log-ingestion and the query API appear in the ArgoCD UI)"
  value = {
    argocd  = try(data.kubernetes_service.argocd.status[0].load_balancer[0].ingress[0].ip, "pending - re-run terraform apply")
    grafana = try(data.kubernetes_service.grafana.status[0].load_balancer[0].ingress[0].ip, "pending - re-run terraform apply")
  }
}

output "ci_service_account_key" {
  description = "Paste into the GitHub secret GCP_SA_KEY: terraform output -raw ci_service_account_key | base64 -d"
  value       = google_service_account_key.ci.private_key
  sensitive   = true
}
