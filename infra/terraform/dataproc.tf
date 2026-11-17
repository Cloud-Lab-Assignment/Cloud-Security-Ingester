# Security Analytics Service: Flink job on Dataproc (Flink optional component, YARN).
# Reads normalized-events from Confluent Cloud, does stateful event-time windowed
# aggregation, writes the results topic. Checkpoints go to Cloud Storage.
#
# Build + upload the jar, then submit (docs/DEPLOYMENT.md, "Deploy the Flink job"):
#   cd services/security-analytics && mvn -q package
#   gsutil cp target/security-analytics-1.0.0.jar gs://<artifacts_bucket>/artifacts/security-analytics.jar
#   gcloud dataproc jobs submit flink --cluster=<cluster> --region=<region> \
#     --jar=gs://<artifacts_bucket>/artifacts/security-analytics.jar

resource "google_service_account" "dataproc" {
  account_id   = "${local.name}-dataproc"
  display_name = "Dataproc Flink cluster"
}

resource "google_project_iam_member" "dataproc_worker" {
  project = var.gcp_project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}

resource "google_storage_bucket_iam_member" "dataproc_artifacts" {
  bucket = google_storage_bucket.artifacts.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc.email}"
}

resource "google_dataproc_cluster" "flink" {
  name   = "${local.name}-flink"
  region = var.gcp_region

  graceful_decommission_timeout = "120s"

  cluster_config {
    staging_bucket = google_storage_bucket.artifacts.name

    gce_cluster_config {
      zone             = var.gcp_zone
      subnetwork       = google_compute_subnetwork.main.id
      internal_ip_only = true # outbound to Confluent goes through Cloud NAT
      service_account  = google_service_account.dataproc.email
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
      ]
    }

    master_config {
      num_instances = 1 # Flink HA mode is not supported on 3-master clusters
      machine_type  = var.dataproc_machine_type
      disk_config {
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = var.dataproc_workers
      machine_type  = var.dataproc_machine_type
      disk_config {
        boot_disk_size_gb = 50
      }
    }

    software_config {
      image_version       = var.dataproc_image_version
      optional_components = ["FLINK"]

      # The job reads its settings from environment variables (AnalyticsConfig.java);
      # on YARN they are handed to the JobManager and TaskManager containers.
      override_properties = merge(
        {
          "flink:taskmanager.numberOfTaskSlots" = "2"
          "flink:state.backend"                 = "rocksdb"
          "flink:state.checkpoints.dir"         = "gs://${google_storage_bucket.artifacts.name}/flink-checkpoints"
        },
        merge([
          for role in ["master", "taskmanager"] : {
            "flink:containerized.${role}.env.KAFKA_BOOTSTRAP_SERVERS" = local.kafka_bootstrap
            "flink:containerized.${role}.env.KAFKA_USERNAME"          = confluent_api_key.app.id
            "flink:containerized.${role}.env.KAFKA_PASSWORD"          = confluent_api_key.app.secret
            "flink:containerized.${role}.env.INPUT_TOPIC"             = "normalized-events"
            "flink:containerized.${role}.env.OUTPUT_TOPIC"            = "results"
            "flink:containerized.${role}.env.CHECKPOINT_URI"          = "gs://${google_storage_bucket.artifacts.name}/flink-checkpoints"
          }
        ]...)
      )
    }

    endpoint_config {
      enable_http_port_access = true # Component Gateway: Flink UI from the console
    }
  }

  depends_on = [
    google_project_iam_member.dataproc_worker,
    google_storage_bucket_iam_member.dataproc_artifacts,
    google_compute_router_nat.main,
  ]
}
