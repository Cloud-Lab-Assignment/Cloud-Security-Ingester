# Kafka on Confluent Cloud, hosted on Google Cloud in the same region as
# everything else (the function connector requires the same region).

resource "confluent_environment" "main" {
  display_name = local.name
}

resource "confluent_kafka_cluster" "main" {
  display_name = local.name
  availability = "SINGLE_ZONE"
  cloud        = "GCP"
  region       = var.gcp_region

  basic {}

  environment {
    id = confluent_environment.main.id
  }
}

# One service account for the platform: services, the Flink job, the connector
# and Terraform's own topic management. CloudClusterAdmin keeps the demo simple;
# per-service accounts with ACLs are the production setup (see docs/handoff.md).
resource "confluent_service_account" "app" {
  display_name = "${local.name}-app"
  description  = "Kafka clients of the cloud-sec services"
}

resource "confluent_role_binding" "app" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

resource "confluent_api_key" "app" {
  display_name = "${local.name}-app"
  description  = "Kafka API key for the cloud-sec services"

  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.app]
}

resource "confluent_kafka_topic" "topics" {
  for_each = toset(local.kafka_topics)

  topic_name       = each.key
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  credentials {
    key    = confluent_api_key.app.id
    secret = confluent_api_key.app.secret
  }
}

# raw-events -> Cloud Run function (event-normalizer). Fully managed connector;
# it authenticates to Google with the invoker service account key from function.tf.
resource "confluent_connector" "normalizer" {
  environment {
    id = confluent_environment.main.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  config_sensitive = {
    "kafka.api.key"        = confluent_api_key.app.id
    "kafka.api.secret"     = confluent_api_key.app.secret
    "gcp.credentials.json" = base64decode(google_service_account_key.connector.private_key)
  }

  config_nonsensitive = {
    "connector.class"       = "GoogleCloudFunctionsGen2Sink"
    "name"                  = "${local.name}-normalizer-trigger"
    "kafka.auth.mode"       = "KAFKA_API_KEY"
    "topics"                = "raw-events"
    "input.data.format"     = "JSON"
    "tasks.max"             = "1"
    "gcf.auth.type"         = "Google Cloud Service Account"
    "gcf.name"              = google_cloudfunctions2_function.normalizer.name
    "gcf.region.name"       = var.gcp_region
    "gcf.project.id"        = var.gcp_project_id
    "max.batch.size"        = "50"
    "batch.json.as.array"   = "true"
    "behavior.on.error"     = "IGNORE" # failed batches go to the connector's DLQ topic
    "max.retries"           = "5"
    "retry.on.status.codes" = "429,500-"
  }

  depends_on = [
    confluent_kafka_topic.topics,
    google_cloudfunctions2_function_iam_member.connector,
    google_cloud_run_service_iam_member.connector,
  ]
}
