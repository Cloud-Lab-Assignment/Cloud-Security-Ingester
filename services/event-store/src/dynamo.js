import { DynamoDBClient, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';

export const EVENT_ID_INDEX = 'eventId-index';

export function tableNames(prefix) {
  return {
    events: `${prefix}events`,
    correlations: `${prefix}correlations`,
    tpc: `${prefix}tpc`,
  };
}

export function createDynamoClients({ region, endpoint }) {
  const ddb = new DynamoDBClient({ region, ...(endpoint ? { endpoint } : {}) });
  const doc = DynamoDBDocumentClient.from(ddb, {
    marshallOptions: { removeUndefinedValues: true },
  });
  return { ddb, doc };
}

export class InvalidCursorError extends Error {
  constructor(message = 'invalid cursor') {
    super(message);
    this.name = 'InvalidCursorError';
  }
}

// Cursor = base64url(JSON(LastEvaluatedKey)).
export function encodeCursor(lastEvaluatedKey) {
  return Buffer.from(JSON.stringify(lastEvaluatedKey)).toString('base64url');
}

export function decodeCursor(cursor) {
  if (typeof cursor !== 'string' || !/^[A-Za-z0-9_-]+$/.test(cursor)) throw new InvalidCursorError();
  let key;
  try {
    key = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    throw new InvalidCursorError();
  }
  if (key === null || typeof key !== 'object' || Array.isArray(key)) throw new InvalidCursorError();
  for (const v of Object.values(key)) {
    if (typeof v !== 'string' && typeof v !== 'number') throw new InvalidCursorError();
  }
  return key;
}

const isConditionFailure = (err) => err?.name === 'ConditionalCheckFailedException';

// Canonical millisecond ISO form so sort keys compare correctly as strings.
const canonicalTime = (t) => new Date(t).toISOString();

// The normalized event is kept verbatim as a JSON string in `doc`: it round-trips
// exactly (large integers, deep nesting, empty values) without DynamoDB type limits.
// entityKey/sk/eventId/eventTime/category/outcome are top-level for keys and filtering.
function toEventItem(event) {
  const eventTime = canonicalTime(event.eventTime);
  return {
    entityKey: event.entityKey,
    sk: `${eventTime}#${event.eventId}`,
    eventId: event.eventId,
    eventTime,
    category: event.category ?? null,
    outcome: event.outcome ?? null,
    doc: JSON.stringify(event),
  };
}

const fromEventItem = (item) => JSON.parse(item.doc);

export function createDynamoRepo({ doc, tablePrefix }) {
  const tables = tableNames(tablePrefix);

  async function ping() {
    const out = await doc.send(new DescribeTableCommand({ TableName: tables.events }));
    return Boolean(out.Table);
  }

  async function putEvent(event) {
    try {
      await doc.send(
        new PutCommand({
          TableName: tables.events,
          Item: toEventItem(event),
          ConditionExpression: 'attribute_not_exists(sk)',
        }),
      );
      return 'stored';
    } catch (err) {
      if (isConditionFailure(err)) return 'duplicate';
      throw err;
    }
  }

  async function getEvent(eventId) {
    const out = await doc.send(
      new QueryCommand({
        TableName: tables.events,
        IndexName: EVENT_ID_INDEX,
        KeyConditionExpression: 'eventId = :id',
        ExpressionAttributeValues: { ':id': eventId },
        Limit: 1,
      }),
    );
    const item = out.Items?.[0];
    return item ? fromEventItem(item) : null;
  }

  // Events for one entity with from <= eventTime <= to, oldest first.
  async function queryEvents({ entityKey, from, to, limit = 100, cursor }) {
    const out = await doc.send(
      new QueryCommand({
        TableName: tables.events,
        KeyConditionExpression: 'entityKey = :e AND sk BETWEEN :from AND :toMax',
        ExpressionAttributeValues: {
          ':e': entityKey,
          ':from': canonicalTime(from),
          ':toMax': `${canonicalTime(to)}#￿`,
        },
        Limit: limit,
        ScanIndexForward: true,
        ...(cursor ? { ExclusiveStartKey: decodeCursor(cursor) } : {}),
      }),
    );
    return {
      items: (out.Items ?? []).map(fromEventItem),
      nextCursor: out.LastEvaluatedKey ? encodeCursor(out.LastEvaluatedKey) : null,
    };
  }

  // One item per (correlationId, version). A redelivered commit is "duplicate", not an error.
  async function putCorrelation(rec) {
    try {
      await doc.send(
        new PutCommand({
          TableName: tables.correlations,
          Item: rec,
          ConditionExpression: 'attribute_not_exists(correlationId)',
        }),
      );
      return 'stored';
    } catch (err) {
      if (isConditionFailure(err)) return 'duplicate';
      throw err;
    }
  }

  async function getCorrelation(correlationId) {
    const items = [];
    let ExclusiveStartKey;
    do {
      const out = await doc.send(
        new QueryCommand({
          TableName: tables.correlations,
          KeyConditionExpression: 'correlationId = :c',
          ExpressionAttributeValues: { ':c': correlationId },
          ScanIndexForward: true,
          ConsistentRead: true,
          ExclusiveStartKey,
        }),
      );
      items.push(...(out.Items ?? []));
      ExclusiveStartKey = out.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return items;
  }

  // Participant state for @core/tpc: records are {txId, state, payload, updatedAt}.
  const tpcStore = {
    async get(txId) {
      const out = await doc.send(new GetCommand({ TableName: tables.tpc, Key: { txId }, ConsistentRead: true }));
      return out.Item ?? null;
    },
    async put(record) {
      await doc.send(new PutCommand({ TableName: tables.tpc, Item: record }));
    },
    async listPending() {
      const items = [];
      let ExclusiveStartKey;
      do {
        const out = await doc.send(
          new ScanCommand({
            TableName: tables.tpc,
            FilterExpression: '#s IN (:u, :p)',
            ExpressionAttributeNames: { '#s': 'state' },
            ExpressionAttributeValues: { ':u': 'UNCERTAIN', ':p': 'PRECOMMITTED' },
            ConsistentRead: true,
            ExclusiveStartKey,
          }),
        );
        items.push(...(out.Items ?? []));
        ExclusiveStartKey = out.LastEvaluatedKey;
      } while (ExclusiveStartKey);
      return items;
    },
  };

  return { tables, ping, putEvent, getEvent, queryEvents, putCorrelation, getCorrelation, tpcStore };
}
