# ---------------------------------------------------------------- image registry
resource "google_artifact_registry_repository" "images" {
  repository_id = local.name
  location      = var.gcp_region
  format        = "DOCKER"
  description   = "Container images for the Kubernetes services"

  depends_on = [google_project_service.apis]
}

# ---------------------------------------------------------------- GKE
# Least-privilege node identity: logs, metrics and image pulls only.
resource "google_service_account" "gke_nodes" {
  account_id   = "${local.name}-gke-nodes"
  display_name = "GKE nodes"
}

resource "google_project_iam_member" "gke_nodes" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/artifactregistry.reader",
  ])

  project = var.gcp_project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

# Zonal cluster (one control plane, free tier eligible) with its own node pool.
# HPAs read CPU/memory from the metrics server GKE installs by default.
resource "google_container_cluster" "main" {
  name     = local.name
  location = var.gcp_zone

  network    = google_compute_network.main.id
  subnetwork = google_compute_subnetwork.main.id

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  release_channel {
    channel = "REGULAR"
  }

  depends_on = [google_project_service.apis]
}

resource "google_container_node_pool" "main" {
  name     = "default"
  cluster  = google_container_cluster.main.name
  location = var.gcp_zone

  initial_node_count = var.gke_node_min

  autoscaling {
    min_node_count = var.gke_node_min
    max_node_count = var.gke_node_max
  }

  node_config {
    machine_type    = var.gke_machine_type
    disk_size_gb    = 50
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  lifecycle {
    ignore_changes = [initial_node_count]
  }
}

# ---------------------------------------------------------------- runtime config
resource "kubernetes_namespace" "app" {
  metadata {
    name = "cloud-sec"
  }
  depends_on = [google_container_node_pool.main]
}

# Non-secret endpoints, consumed through envFrom by every Deployment.
# Variable names follow docs/handoff.md.
resource "kubernetes_config_map" "infra" {
  metadata {
    name      = "infra-endpoints"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    # Kafka (Confluent Cloud, SASL PLAIN over TLS)
    KAFKA_BROKERS           = local.kafka_bootstrap
    KAFKA_BOOTSTRAP_SERVERS = local.kafka_bootstrap
    KAFKA_SSL               = "true"
    KAFKA_SASL_MECHANISM    = "plain"

    # Event Store: DynamoDB on AWS, raw archive on Cloud Storage (S3-compatible API)
    AWS_REGION       = var.aws_region
    TABLE_PREFIX     = "cs-"
    RAW_BUCKET       = google_storage_bucket.raw.name
    S3_ENDPOINT      = "https://storage.googleapis.com"
    S3_REGION        = "auto"
    S3_CREATE_BUCKET = "false" # Terraform owns the bucket

    # Incident Management: Cloud SQL over its private IP (traffic stays inside the VPC)
    PGHOST       = google_sql_database_instance.incidents.private_ip_address
    PGPORT       = "5432"
    PGDATABASE   = google_sql_database.incidents.name
    DATABASE_SSL = "false"

    # In-cluster service URLs
    EVENT_STORE_URL         = "http://event-store:8081"
    COORDINATOR_URL         = "http://incident-management:8082"
    INCIDENT_GRPC_ADDR      = "incident-management:50051"
    INCIDENT_MANAGEMENT_URL = "http://incident-management:8082"
  }
}

# Secrets. Their values pass through Terraform state: keep the state private.
resource "kubernetes_secret" "kafka" {
  metadata {
    name      = "kafka-credentials"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    KAFKA_USERNAME = confluent_api_key.app.id
    KAFKA_PASSWORD = confluent_api_key.app.secret
  }
}

resource "kubernetes_secret" "incident_db" {
  metadata {
    name      = "incident-db"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    PGUSER     = google_sql_user.incidents.name
    PGPASSWORD = random_password.db.result
  }
}

resource "kubernetes_secret" "event_store_cloud" {
  metadata {
    name      = "event-store-cloud"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    # DynamoDB (AWS IAM user, see dynamodb.tf)
    AWS_ACCESS_KEY_ID     = aws_iam_access_key.event_store.id
    AWS_SECRET_ACCESS_KEY = aws_iam_access_key.event_store.secret
    # Cloud Storage HMAC key (see storage.tf)
    S3_ACCESS_KEY_ID     = google_storage_hmac_key.event_store.access_id
    S3_SECRET_ACCESS_KEY = google_storage_hmac_key.event_store.secret
  }
}

# ---------------------------------------------------------------- CI identity
# GitHub Actions pushes images with this account's key (GitHub secret GCP_SA_KEY).
# It can only write to the image registry; it has no access to the cluster.
resource "google_service_account" "ci" {
  account_id   = "${local.name}-ci"
  display_name = "GitHub Actions image publisher"
}

resource "google_artifact_registry_repository_iam_member" "ci" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_service_account_key" "ci" {
  service_account_id = google_service_account.ci.name
}
