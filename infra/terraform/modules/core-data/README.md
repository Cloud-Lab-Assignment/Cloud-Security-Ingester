# core-data module (AWS DynamoDB)

The Event Store's three DynamoDB tables and the IAM policy that grants access to them.
This is the only part of the system that stays on AWS: the Event Store's 3PC participant
is written against DynamoDB conditional writes.

| Resource | Purpose |
|---|---|
| `${name_prefix}events` (PK `entityKey`, SK `sk`, GSI `eventId-index`) | normalized events |
| `${name_prefix}correlations` (PK `correlationId`, SK `version`) | correlation-decision history (3PC participant data) |
| `${name_prefix}tpc` (PK `txId`) | 3PC participant log |
| IAM policy `${name_prefix}core-services-data-access` | attached by the root to the event-store IAM user |

All tables are on-demand (PAY_PER_REQUEST). Originally this module also created the raw S3 bucket and
RDS PostgreSQL; those moved to Google Cloud Storage and Cloud SQL in the root module (see Git history).
