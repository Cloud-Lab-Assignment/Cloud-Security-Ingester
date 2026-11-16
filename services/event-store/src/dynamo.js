import { createHash } from 'node:crypto';
import { DynamoDBClient, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';
import { StoreConflict } from '@core/tpc';
import { awsClientOptions } from './aws-options.js';

export const EVENT_ID_INDEX = 'eventId-index';

export function tableNames(prefix) {
  return {
    events: `${prefix}events`,
    correlations: `${prefix}correlations`,
    tpc: `${prefix}tpc`,
  };
}

export function createDynamoClients({ region, endpoint, requestTimeoutMs, maxAttempts }) {
  const ddb = new DynamoDBClient({
    region,
    ...awsClientOptions({ requestTimeoutMs, maxAttempts }),
    ...(endpoint ? { endpoint } : {}),
  });
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

// DynamoDB key size limits, in bytes of UTF-8.
export const MAX_PARTITION_KEY_BYTES = 2048;
export const MAX_SORT_KEY_BYTES = 1024;

// Canonical millisecond ISO form so sort keys compare correctly as strings.
const canonicalTime = (t) => new Date(t).toISOString();

// The sort-key bounds of an events query for from <= eventTime <= to.
export function sortKeyRange({ from, to }) {
  return { min: canonicalTime(from), max: `${canonicalTime(to)}#\uffff` };
}

// A DynamoDB key string: non-empty, valid UTF-8 (no lone surrogates), within `maxBytes`.
const validKeyString = (v, maxBytes) =>
  typeof v === 'string' && v.length > 0 && v.isWellFormed() && Buffer.byteLength(v, 'utf8') <= maxBytes;

// Cursor = base64url(JSON(LastEvaluatedKey)) of the events table, i.e. exactly
// {entityKey, sk} as strings that DynamoDB accepts as a key (R2-8). When
// `entityKey` is given, the cursor must belong to that entity (a cursor from
// another query is rejected, not followed). When `range` ({from, to}) is
// given, sk must lie inside that query's sort-key range: DynamoDB rejects a
// start key outside the key condition with a ValidationException.
export function encodeCursor(lastEvaluatedKey) {
  return Buffer.from(JSON.stringify(lastEvaluatedKey)).toString('base64url');
}

export function decodeCursor(cursor, entityKey, range) {
  if (typeof cursor !== 'string' || !/^[A-Za-z0-9_-]+$/.test(cursor)) throw new InvalidCursorError();
  let key;
  try {
    key = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    throw new InvalidCursorError();
  }
  if (key === null || typeof key !== 'object' || Array.isArray(key)) throw new InvalidCursorError();
  const fields = Object.keys(key).sort();
  if (fields.length !== 2 || fields[0] !== 'entityKey' || fields[1] !== 'sk') throw new InvalidCursorError();
  if (!validKeyString(key.entityKey, MAX_PARTITION_KEY_BYTES) || !validKeyString(key.sk, MAX_SORT_KEY_BYTES)) {
    throw new InvalidCursorError();
  }
  if (entityKey !== undefined && key.entityKey !== entityKey) {
    throw new InvalidCursorError('cursor belongs to another entityKey');
  }
  if (range !== undefined) {
    const { min, max } = sortKeyRange(range);
    if (key.sk < min || key.sk > max) throw new InvalidCursorError('cursor is outside the queried time range');
  }
  return { entityKey: key.entityKey, sk: key.sk };
}

// DynamoDB limits a partition key to 2048 bytes (and a sort key to 1024).
// Upstream allows longer entity keys (e.g. a 3000-byte source.name), so keys
// over MAX_ENTITY_KEY_BYTES of UTF-8 are stored as "h:" + sha256 hex. The
// original stays in the item (`doc`, and `entityKeyOriginal`), and queries
// apply the same mapping, so callers only ever see the original key.
// "h:" is reserved for hashed keys: a literal key starting with it is hashed
// too (R2-6), so it can never share a partition with a long key's hash.
export const MAX_ENTITY_KEY_BYTES = 1024;
const HASHED_PREFIX = 'h:';

export function storageKey(entityKey) {
  if (Buffer.byteLength(entityKey, 'utf8') <= MAX_ENTITY_KEY_BYTES && !entityKey.startsWith(HASHED_PREFIX)) {
    return entityKey;
  }
  return `${HASHED_PREFIX}${createHash('sha256').update(entityKey, 'utf8').digest('hex')}`;
}

const isConditionFailure = (err) => err?.name === 'ConditionalCheckFailedException';

// The correlation-history row for a committed 3PC record (R2-2). The txId was
// folded into the payload at canCommit; recordedAt is the commit time.
export function correlationItem({ txId, payload, updatedAt }) {
  const { txId: _folded, ...rest } = payload;
  return { ...rest, txId, recordedAt: new Date(updatedAt).toISOString() };
}

// The normalized event is kept verbatim as a JSON string in `doc`: it round-trips
// exactly (large integers, deep nesting, empty values) without DynamoDB type limits.
// entityKey/sk/eventId/eventTime/category/outcome are top-level for keys and filtering.
function toEventItem(event) {
  const eventTime = canonicalTime(event.eventTime);
  const key = storageKey(event.entityKey);
  return {
    entityKey: key,
    ...(key === event.entityKey ? {} : { entityKeyOriginal: event.entityKey }),
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
    const key = storageKey(entityKey);
    const { min, max } = sortKeyRange({ from, to });
    const out = await doc.send(
      new QueryCommand({
        TableName: tables.events,
        KeyConditionExpression: 'entityKey = :e AND sk BETWEEN :from AND :toMax',
        ExpressionAttributeValues: { ':e': key, ':from': min, ':toMax': max },
        Limit: limit,
        ScanIndexForward: true,
        ...(cursor ? { ExclusiveStartKey: decodeCursor(cursor, key, { from, to }) } : {}),
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

  // `state` is a reserved word. A missing item fails this check too.
  const stateIs = (state) => ({
    ConditionExpression: '#s = :expected',
    ExpressionAttributeNames: { '#s': 'state' },
    ExpressionAttributeValues: { ':expected': state },
  });

  // Participant state for @core/tpc: records are {txId, state, payload, updatedAt}.
  // put(record, expected) is a conditional write when `expected` is given:
  // {state: null} = the record must not exist, {state: S} = its state must be S.
  // A failed condition (another replica wrote first) becomes StoreConflict.
  // commit(record, expected) is the atomic "apply + COMMITTED" (R2-2).
  const tpcStore = {
    async get(txId) {
      const out = await doc.send(new GetCommand({ TableName: tables.tpc, Key: { txId }, ConsistentRead: true }));
      return out.Item ?? null;
    },
    async put(record, expected) {
      const condition =
        expected === undefined
          ? {}
          : expected.state === null
            ? { ConditionExpression: 'attribute_not_exists(txId)' }
            : stateIs(expected.state);
      try {
        await doc.send(new PutCommand({ TableName: tables.tpc, Item: record, ...condition }));
      } catch (err) {
        if (expected === undefined) throw err;
        // A put racing another replica's commit transaction on the same item
        // is rejected with TransactionConflictException: re-read as well.
        if (isConditionFailure(err) || err?.name === 'TransactionConflictException') {
          throw new StoreConflict(record.txId, expected.state);
        }
        throw err;
      }
    },
    // One TransactWriteItems: the correlation-history row (unique per
    // correlationId + version) and the move to COMMITTED, conditional on the
    // state the participant read. Either both happen or neither does.
    // Cancellation reasons, in TransactItems order [history, tpc]:
    //  - tpc ConditionalCheckFailed / TransactionConflict: the record changed
    //    (e.g. another replica aborted or committed it) -> StoreConflict, the
    //    participant re-reads. This also covers a retry after a crash where
    //    the transaction had gone through: the re-read finds COMMITTED.
    //  - only the history condition failed: the row already exists although
    //    the record is still in the expected state. Atomic commits cannot
    //    leave that behind, so it is a row written by the pre-R2-2 apply-then-
    //    put path (or by another tx for the same correlationId + version, which
    //    the old apply also treated as "duplicate"). Keep that row and make
    //    the state move alone, still conditionally.
    async commit(record, expected) {
      try {
        await doc.send(
          new TransactWriteCommand({
            TransactItems: [
              {
                Put: {
                  TableName: tables.correlations,
                  Item: correlationItem(record),
                  ConditionExpression: 'attribute_not_exists(correlationId)',
                },
              },
              { Put: { TableName: tables.tpc, Item: record, ...stateIs(expected.state) } },
            ],
          }),
        );
      } catch (err) {
        if (err?.name !== 'TransactionCanceledException') throw err;
        const [history, tpc] = (err.CancellationReasons ?? []).map((r) => r?.Code ?? 'None');
        if (tpc === 'ConditionalCheckFailed' || tpc === 'TransactionConflict') {
          throw new StoreConflict(record.txId, expected.state);
        }
        if (history === 'ConditionalCheckFailed' && tpc === 'None') {
          await tpcStore.put(record, expected);
          return;
        }
        throw err;
      }
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
