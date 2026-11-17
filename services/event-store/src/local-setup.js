import { CreateTableCommand, waitUntilTableExists } from '@aws-sdk/client-dynamodb';
import { CreateBucketCommand } from '@aws-sdk/client-s3';
import { tableNames, EVENT_ID_INDEX } from './dynamo.js';

// Same shapes as infra/terraform/dynamodb.tf.
export function tableDefinitions(prefix) {
  const t = tableNames(prefix);
  return [
    {
      TableName: t.events,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'entityKey', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
        { AttributeName: 'eventId', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'entityKey', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: EVENT_ID_INDEX,
          KeySchema: [{ AttributeName: 'eventId', KeyType: 'HASH' }],
          Projection: { ProjectionType: 'ALL' },
        },
      ],
    },
    {
      TableName: t.correlations,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'correlationId', AttributeType: 'S' },
        { AttributeName: 'version', AttributeType: 'N' },
      ],
      KeySchema: [
        { AttributeName: 'correlationId', KeyType: 'HASH' },
        { AttributeName: 'version', KeyType: 'RANGE' },
      ],
    },
    {
      TableName: t.tpc,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [{ AttributeName: 'txId', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'txId', KeyType: 'HASH' }],
    },
  ];
}

async function ensureTables({ ddb, tablePrefix, logger }) {
  for (const def of tableDefinitions(tablePrefix)) {
    try {
      await ddb.send(new CreateTableCommand(def));
      logger.info({ table: def.TableName }, 'created dynamodb table');
    } catch (err) {
      if (err?.name !== 'ResourceInUseException') throw err;
    }
    await waitUntilTableExists({ client: ddb, maxWaitTime: 30 }, { TableName: def.TableName });
  }
}

async function ensureBucket({ s3, bucket, logger }) {
  try {
    await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    logger.info({ bucket }, 'created s3 bucket');
  } catch (err) {
    if (err?.name === 'BucketAlreadyOwnedByYou' || err?.name === 'BucketAlreadyExists') return;
    // Keep serving: an S3 outage shows up as raw-events consumer errors (and retries).
    logger.warn({ err, bucket }, 'could not create s3 bucket; raw archiving will fail until it exists');
  }
}

// Local development only: create the tables and the bucket on the emulators.
// In AWS both endpoints are unset and Terraform owns these resources.
export async function ensureLocalResources({ config, ddb, s3, logger }) {
  if (config.dynamodbEndpoint && ddb) await ensureTables({ ddb, tablePrefix: config.tablePrefix, logger });
  if (config.s3Endpoint && config.s3CreateBucket !== false && s3) {
    await ensureBucket({ s3, bucket: config.rawBucket, logger });
  }
}
