# Storage layout (requirement g: three distinct products):
#   Cloud Storage  - raw-events archive (Event Store), Flink jar/checkpoints, function source
#   Cloud SQL PG   - incidents + 3PC coordinator log (Incident Management)
#   DynamoDB (AWS) - events / correlations / 3PC participant log (Event Store), see dynamodb.tf

# ---------------------------------------------------------------- Cloud Storage
resource "google_storage_bucket" "raw" {
  name                        = "${local.name}-raw-${random_id.suffix.hex}"
  location                    = var.gcp_region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["raw/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "artifacts" {
  name                        = "${local.name}-artifacts-${random_id.suffix.hex}"
  location                    = var.gcp_region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["flink-checkpoints/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

# The Event Store writes the archive through the S3-compatible XML API of Cloud
# Storage, with an HMAC key owned by this service account.
resource "google_service_account" "event_store_gcs" {
  account_id   = "${local.name}-event-store-gcs"
  display_name = "Event Store raw archive writer"
}

resource "google_storage_bucket_iam_member" "event_store_raw" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.event_store_gcs.email}"
}

resource "google_storage_hmac_key" "event_store" {
  service_account_email = google_service_account.event_store_gcs.email
}

# ---------------------------------------------------------------- Cloud SQL (PostgreSQL 16)
resource "random_password" "db" {
  length  = 24
  special = false
}

resource "google_sql_database_instance" "incidents" {
  name             = "${local.name}-incidents-${random_id.suffix.hex}"
  database_version = "POSTGRES_16"
  region           = var.gcp_region

  deletion_protection = false

  settings {
    tier              = var.cloudsql_tier
    edition           = "ENTERPRISE"
    availability_type = "ZONAL"
    disk_size         = 20
    disk_type         = "PD_SSD"

    ip_configuration {
      # Private IP only: reachable from GKE inside the VPC, never from the internet.
      ipv4_enabled    = false
      private_network = google_compute_network.main.id
    }

    backup_configuration {
      enabled = true
    }
  }

  depends_on = [google_service_networking_connection.private_services]
}

resource "google_sql_database" "incidents" {
  name     = "incidents"
  instance = google_sql_database_instance.incidents.name
}

resource "google_sql_user" "incidents" {
  name     = "incidents"
  instance = google_sql_database_instance.incidents.name
  password = random_password.db.result
}
