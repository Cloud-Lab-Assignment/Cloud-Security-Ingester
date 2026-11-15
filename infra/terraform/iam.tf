# Data access for the event-store pods. Attach to the event-store IRSA / pod role.
# incident-management needs no DynamoDB or S3 access (it reaches the Event Store over REST).

data "aws_iam_policy_document" "data_access" {
  statement {
    sid = "DynamoDBTables"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:DescribeTable",
    ]
    resources = [
      aws_dynamodb_table.events.arn,
      "${aws_dynamodb_table.events.arn}/index/eventId-index",
      aws_dynamodb_table.correlations.arn,
      aws_dynamodb_table.tpc.arn,
    ]
  }

  statement {
    sid       = "RawArchiveObjects"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/*"]
  }

  statement {
    sid       = "RawArchiveBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]
  }
}

resource "aws_iam_policy" "data_access" {
  name        = "${var.name_prefix}core-services-data-access"
  description = "Event Store access to its DynamoDB tables and the raw-events S3 archive"
  policy      = data.aws_iam_policy_document.data_access.json
}
