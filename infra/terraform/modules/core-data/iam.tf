# Data access for the event-store. The pods run on GKE, so the root module
# attaches this policy to an IAM user whose access key is stored in a K8s Secret.
# The raw-events archive lives in Google Cloud Storage and is not covered here.

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
}

resource "aws_iam_policy" "data_access" {
  name        = "${var.name_prefix}core-services-data-access"
  description = "Event Store access to its DynamoDB tables"
  policy      = data.aws_iam_policy_document.data_access.json
}
