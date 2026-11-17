# The one AWS piece: the Event Store's DynamoDB tables (Rishit's module).
# The Event Store's 3PC participant relies on DynamoDB conditional writes.

module "core_data" {
  source = "./modules/core-data"

  name_prefix = "cs-" # must equal TABLE_PREFIX in the event-store
}

# GKE pods cannot use AWS IAM roles, so the event-store authenticates with an
# access key of a dedicated IAM user that has only the table policy.
resource "aws_iam_user" "event_store" {
  name = "${local.name}-event-store"
}

resource "aws_iam_user_policy_attachment" "event_store" {
  user       = aws_iam_user.event_store.name
  policy_arn = module.core_data.data_access_policy_arn
}

resource "aws_iam_access_key" "event_store" {
  user = aws_iam_user.event_store.name
}
