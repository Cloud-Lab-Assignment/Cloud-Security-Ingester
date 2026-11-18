# Serverless service: Event Normalizer, as a Cloud Run function (2nd gen).
# Event-driven & asynchronous: Confluent's managed "Google Cloud Functions Gen 2
# Sink" connector (confluent.tf) invokes it with batches of raw-events records;
# it publishes to normalized-events (or raw-events-dlq). Entry point: src/gcf.js.

data "archive_file" "normalizer" {
  type        = "zip"
  source_dir  = "${path.module}/../../services/event-normalizer"
  output_path = "${path.module}/.build/event-normalizer.zip"
  # Cloud Build installs dependencies from package.json/package-lock.json.
  excludes = ["node_modules", "test", "scripts", ".env", ".env.example"]
}

resource "google_storage_bucket_object" "normalizer_source" {
  name   = "functions/event-normalizer-${data.archive_file.normalizer.output_md5}.zip"
  bucket = google_storage_bucket.artifacts.name
  source = data.archive_file.normalizer.output_path
}

resource "google_service_account" "normalizer" {
  account_id   = "${local.name}-normalizer"
  display_name = "event-normalizer function runtime"
}

resource "google_cloudfunctions2_function" "normalizer" {
  name     = "${local.name}-event-normalizer"
  location = var.gcp_region

  build_config {
    runtime     = "nodejs22"
    entry_point = "normalize"
    source {
      storage_source {
        bucket = google_storage_bucket.artifacts.name
        object = google_storage_bucket_object.normalizer_source.name
      }
    }
  }

  service_config {
    available_memory      = "256Mi"
    timeout_seconds       = 60
    max_instance_count    = 10
    service_account_email = google_service_account.normalizer.email
    # Confluent Cloud calls from the internet, so ingress is open, but every
    # request must carry a token for an account with the invoker role.
    ingress_settings               = "ALLOW_ALL"
    all_traffic_on_latest_revision = true

    environment_variables = {
      KAFKA_BROKERS        = local.kafka_bootstrap
      KAFKA_SSL            = "true"
      KAFKA_SASL_MECHANISM = "plain"
      KAFKA_USERNAME       = confluent_api_key.app.id
      RAW_TOPIC            = "raw-events"
    }

    secret_environment_variables {
      key        = "KAFKA_PASSWORD"
      project_id = var.gcp_project_id
      secret     = google_secret_manager_secret.kafka_password.secret_id
      version    = "latest"
    }
  }

  depends_on = [
    google_project_service.apis,
    google_secret_manager_secret_version.kafka_password,
    google_secret_manager_secret_iam_member.normalizer,
  ]
}

# Kafka secret for the function, kept out of plain environment variables.
resource "google_project_service" "secretmanager" {
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_secret_manager_secret" "kafka_password" {
  secret_id = "${local.name}-kafka-password"
  replication {
    auto {}
  }
  depends_on = [google_project_service.secretmanager]
}

resource "google_secret_manager_secret_version" "kafka_password" {
  secret      = google_secret_manager_secret.kafka_password.id
  secret_data = confluent_api_key.app.secret
}

resource "google_secret_manager_secret_iam_member" "normalizer" {
  secret_id = google_secret_manager_secret.kafka_password.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.normalizer.email}"
}

# Identity the Confluent connector uses to invoke the function.
resource "google_service_account" "connector" {
  account_id   = "${local.name}-confluent-invoker"
  display_name = "Confluent connector -> event-normalizer"
}

resource "google_service_account_key" "connector" {
  service_account_id = google_service_account.connector.name
}

resource "google_cloudfunctions2_function_iam_member" "connector" {
  project        = var.gcp_project_id
  location       = var.gcp_region
  cloud_function = google_cloudfunctions2_function.normalizer.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.connector.email}"
}

# A 2nd-gen function is served by Cloud Run; invocation checks run.invoker.
resource "google_cloud_run_service_iam_member" "connector" {
  project  = var.gcp_project_id
  location = var.gcp_region
  service  = google_cloudfunctions2_function.normalizer.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.connector.email}"
}
