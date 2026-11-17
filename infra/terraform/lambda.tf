# Serverless service: Event Normalizer.
# Event-driven & asynchronous: Lambda is invoked by the MSK event source mapping
# whenever records land on the `raw-events` topic, then publishes to `normalized-events`
# (or `raw-events-dlq` for invalid events).
#
# Build prerequisite:  cd services/event-normalizer && npm ci --omit=dev

data "archive_file" "normalizer" {
  type        = "zip"
  source_dir  = "${path.module}/../../services/event-normalizer"
  output_path = "${path.module}/.build/event-normalizer.zip"
  excludes    = ["test", "scripts", ".env", ".env.example", "package-lock.json"]
}

resource "aws_cloudwatch_log_group" "normalizer" {
  name              = "/aws/lambda/${local.name}-event-normalizer"
  retention_in_days = 7
}

resource "aws_iam_role" "normalizer" {
  name = "${local.name}-event-normalizer"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# Includes VPC ENI access, CloudWatch Logs, and the MSK read permissions.
resource "aws_iam_role_policy_attachment" "normalizer_msk" {
  role       = aws_iam_role.normalizer.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaMSKExecutionRole"
}

resource "aws_iam_role_policy_attachment" "normalizer_vpc" {
  role       = aws_iam_role.normalizer.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_lambda_function" "normalizer" {
  function_name = "${local.name}-event-normalizer"
  role          = aws_iam_role.normalizer.arn
  runtime       = "nodejs20.x"
  handler       = "src/handler.handler"
  timeout       = 60
  memory_size   = 256

  filename         = data.archive_file.normalizer.output_path
  source_code_hash = data.archive_file.normalizer.output_base64sha256

  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      KAFKA_BROKERS = aws_msk_cluster.main.bootstrap_brokers
      KAFKA_SSL     = "false"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.normalizer,
    aws_iam_role_policy_attachment.normalizer_msk,
    aws_iam_role_policy_attachment.normalizer_vpc,
  ]
}

resource "aws_lambda_event_source_mapping" "normalizer" {
  event_source_arn  = aws_msk_cluster.main.arn
  function_name     = aws_lambda_function.normalizer.arn
  topics            = ["raw-events"]
  starting_position = "LATEST"
  batch_size        = 100

  maximum_batching_window_in_seconds = 5
}
