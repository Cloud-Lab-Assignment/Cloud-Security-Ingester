# Managed Kafka (message-queue communication path).
#
# Listener choice: the services in /services use kafkajs / Flink without IAM auth,
# so brokers expose TLS + plaintext (TLS_PLAINTEXT) with unauthenticated access,
# restricted to the VPC by the security group. Topics: see local.kafka_topics.

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.name}"
  retention_in_days = 7
}

resource "aws_msk_configuration" "main" {
  name           = "${local.name}-config"
  kafka_versions = [var.msk_kafka_version]

  server_properties = <<-EOT
    auto.create.topics.enable=true
    num.partitions=3
    default.replication.factor=${min(var.az_count, 3)}
    min.insync.replicas=2
    delete.topic.enable=true
    log.retention.hours=72
  EOT
}

resource "aws_msk_cluster" "main" {
  cluster_name           = local.name
  kafka_version          = var.msk_kafka_version
  number_of_broker_nodes = var.az_count

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  client_authentication {
    unauthenticated = true
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}
