// Google Cloud Run function (2nd gen, HTTP) entry point.
//
// Confluent Cloud's "Google Cloud Functions Gen 2 Sink" connector calls this
// function with batches of records from the raw-events topic. It reuses the
// Lambda handler unchanged by translating the request into the Kafka event
// shape that handler expects.
//
// The connector posts record VALUES only (batch.json.as.array=true gives a JSON
// array): topic, partition and offset are not included. The normalized schema
// requires an origin, so one is synthesised: the configured topic, partition 0
// and the receive time in epoch seconds as the offset. A body element shaped
// like {topic, partition, offset, value} is also accepted and used as-is.
import { handler } from './handler.js';

const RAW_TOPIC = process.env.RAW_TOPIC ?? 'raw-events';

function isRecordEnvelope(item) {
  return (
    item !== null &&
    typeof item === 'object' &&
    'value' in item &&
    Number.isInteger(item.partition) &&
    Number.isInteger(item.offset)
  );
}

// -> { "<topic>-<partition>": [{ offset, value: <base64 of the raw event JSON> }] }
export function toKafkaEvent(body, nowSeconds = Math.floor(Date.now() / 1000)) {
  const items = Array.isArray(body) ? body : [body];
  const records = {};

  items.forEach((item, index) => {
    let topic = RAW_TOPIC;
    let partition = 0;
    let offset = nowSeconds + index;
    let value = item;

    if (isRecordEnvelope(item)) {
      topic = typeof item.topic === 'string' ? item.topic : RAW_TOPIC;
      partition = item.partition;
      offset = item.offset;
      value = item.value;
    }

    const text = typeof value === 'string' ? value : JSON.stringify(value);
    const key = `${topic}-${partition}`;
    (records[key] ??= []).push({
      offset,
      value: Buffer.from(text ?? '', 'utf8').toString('base64')
    });
  });

  return { eventSource: 'confluent:gcf-sink', records };
}

export function createHttpFunction(invoke = handler) {
  return async function normalize(req, res) {
    if (req.method !== 'POST') {
      res.status(405).json({ error: 'method_not_allowed' });
      return;
    }

    const body = req.body;
    if (body === undefined || body === null || (Array.isArray(body) && body.length === 0)) {
      res.status(200).json({ processed: 0, failed: 0 });
      return;
    }

    try {
      // Invalid events are routed to the DLQ inside the handler and still count
      // as handled; only an infrastructure failure (Kafka down) is a 500, which
      // makes the connector retry the batch.
      const result = await invoke(toKafkaEvent(body));
      console.log(JSON.stringify({ severity: 'INFO', message: 'batch normalized', ...result }));
      res.status(200).json(result);
    } catch (error) {
      console.error(JSON.stringify({ severity: 'ERROR', message: 'normalization failed', error: error.message }));
      res.status(500).json({ error: 'normalization_failed' });
    }
  };
}

// Entry point name configured in Terraform (entry_point = "normalize").
export const normalize = createHttpFunction();
