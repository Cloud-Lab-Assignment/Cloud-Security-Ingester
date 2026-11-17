# Event Store: normalized events, queried by entity and time range, or by eventId via the GSI.
resource "aws_dynamodb_table" "events" {
  name         = "${var.name_prefix}events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "entityKey"
  range_key    = "sk" # "<eventTime>#<eventId>"

  attribute {
    name = "entityKey"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "eventId"
    type = "S"
  }

  global_secondary_index {
    name            = "eventId-index"
    hash_key        = "eventId"
    projection_type = "ALL"
  }
}

# Event Store: correlation-decision history, one item per incident version (3PC participant data).
resource "aws_dynamodb_table" "correlations" {
  name         = "${var.name_prefix}correlations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "correlationId"
  range_key    = "version"

  attribute {
    name = "correlationId"
    type = "S"
  }

  attribute {
    name = "version"
    type = "N"
  }
}

# Event Store: 3PC participant log (transaction state per txId).
resource "aws_dynamodb_table" "tpc" {
  name         = "${var.name_prefix}tpc"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "txId"

  attribute {
    name = "txId"
    type = "S"
  }
}
