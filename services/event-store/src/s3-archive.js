import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { isIsoTimestamp } from '@core/common';

const SAFE_ID = /^[A-Za-z0-9._-]+$/;

// A custom endpoint (MinIO or another emulator) needs path-style addressing.
export function createS3Client({ region, endpoint }) {
  return new S3Client({ region, ...(endpoint ? { endpoint, forcePathStyle: true } : {}) });
}

// raw/yyyy/mm/dd/hh/<eventId>.json, partitioned by receivedAt in UTC.
export function rawKey(event) {
  const { eventId, receivedAt } = event ?? {};
  if (typeof eventId !== 'string' || !SAFE_ID.test(eventId) || eventId.startsWith('.')) {
    throw new Error(`raw event has an invalid eventId: ${JSON.stringify(eventId)}`);
  }
  if (!isIsoTimestamp(receivedAt)) {
    throw new Error(`raw event ${eventId} has an invalid receivedAt: ${JSON.stringify(receivedAt)}`);
  }
  const d = new Date(receivedAt);
  const pad = (n) => String(n).padStart(2, '0');
  return `raw/${d.getUTCFullYear()}/${pad(d.getUTCMonth() + 1)}/${pad(d.getUTCDate())}/${pad(d.getUTCHours())}/${eventId}.json`;
}

export function createS3Archive({ s3, bucket }) {
  // Idempotent: a redelivered event overwrites the same key with the same body.
  async function archiveRaw(event) {
    const key = rawKey(event);
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: JSON.stringify(event),
        ContentType: 'application/json',
      }),
    );
    return key;
  }
  return { archiveRaw };
}
