# Security Analytics Service: Flink job on EMR (YARN).
# Reads `normalized-events`, does stateful time-windowed aggregation, writes `results`.
#
# Build + upload the jar first:
#   cd services/security-analytics && mvn -q package
#   aws s3 cp target/security-analytics-*.jar s3://<bucket>/artifacts/security-analytics.jar
# (the bucket name is the `artifacts_bucket` output)

# EMR's managed policy requires these tags on the VPC and subnets.
resource "aws_ec2_tag" "emr_vpc" {
  resource_id = module.vpc.vpc_id
  key         = "for-use-with-amazon-emr-managed-policies"
  value       = "true"
}

resource "aws_ec2_tag" "emr_subnet" {
  count       = length(module.vpc.private_subnets)
  resource_id = module.vpc.private_subnets[count.index]
  key         = "for-use-with-amazon-emr-managed-policies"
  value       = "true"
}

resource "aws_iam_role" "emr_service" {
  name = "${local.name}-emr-service"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

resource "aws_iam_role" "emr_ec2" {
  name = "${local.name}-emr-ec2"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "emr_ec2" {
  name = "flink-s3-and-logs"
  role = aws_iam_role.emr_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2_ssm" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${local.name}-emr-ec2"
  role = aws_iam_role.emr_ec2.name
}

resource "aws_emr_cluster" "flink" {
  name          = "${local.name}-flink"
  release_label = var.emr_release_label
  applications  = ["Flink", "Hadoop"]

  service_role = aws_iam_role.emr_service.arn
  log_uri      = "s3://${aws_s3_bucket.artifacts.id}/emr-logs/"

  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false

  ec2_attributes {
    subnet_id                         = module.vpc.private_subnets[0]
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
    additional_master_security_groups = aws_security_group.emr_extra.id
    additional_slave_security_groups  = aws_security_group.emr_extra.id
  }

  master_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = var.emr_core_count
  }

  configurations_json = jsonencode([
    {
      Classification = "flink-conf"
      Properties = {
        "taskmanager.numberOfTaskSlots" = "2"
        "state.backend"                 = "rocksdb"
        "state.checkpoints.dir"         = "s3://${aws_s3_bucket.artifacts.id}/flink-checkpoints"
        "execution.checkpointing.mode"  = "EXACTLY_ONCE"
      }
    }
  ])

  # Submit the Flink job on cluster start. Env vars match AnalyticsConfig.java.
  step {
    name              = "security-analytics-flink-job"
    action_on_failure = "CONTINUE"

    hadoop_jar_step {
      jar = "command-runner.jar"
      args = [
        "flink", "run", "-m", "yarn-cluster", "-d",
        "-yD", "containerized.master.env.KAFKA_BOOTSTRAP_SERVERS=${aws_msk_cluster.main.bootstrap_brokers}",
        "-yD", "containerized.taskmanager.env.KAFKA_BOOTSTRAP_SERVERS=${aws_msk_cluster.main.bootstrap_brokers}",
        "-yD", "containerized.master.env.CHECKPOINT_URI=s3://${aws_s3_bucket.artifacts.id}/flink-checkpoints",
        "-yD", "containerized.taskmanager.env.CHECKPOINT_URI=s3://${aws_s3_bucket.artifacts.id}/flink-checkpoints",
        "-yD", "containerized.master.env.INPUT_TOPIC=normalized-events",
        "-yD", "containerized.taskmanager.env.INPUT_TOPIC=normalized-events",
        "-yD", "containerized.master.env.OUTPUT_TOPIC=results",
        "-yD", "containerized.taskmanager.env.OUTPUT_TOPIC=results",
        "s3://${aws_s3_bucket.artifacts.id}/artifacts/security-analytics.jar",
      ]
    }
  }

  lifecycle {
    ignore_changes = [step]
  }

  depends_on = [
    aws_iam_role_policy_attachment.emr_service,
    aws_ec2_tag.emr_vpc,
    aws_ec2_tag.emr_subnet,
  ]
}
