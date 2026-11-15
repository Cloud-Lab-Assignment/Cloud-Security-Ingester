import {
  validateRawEvent,
  validateNormalizedEvent
} from './validator.js';

const SERVICE = {
  name: 'event-normalizer',
  version: '1.0.0'
};

function isValidTimestamp(value) {
  if (typeof value !== 'string') return false;

  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) {
    return false;
  }

  const date = new Date(value);
  return !Number.isNaN(date.getTime()) && date.toISOString() === value;
}

function getPayloadTimestamp(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }

  const keys = ['timestamp', 'eventTime', 'time'];

  for (const key of keys) {
    if (isValidTimestamp(payload[key])) {
      return payload[key];
    }
  }

  return null;
}

function getEventTime(rawEvent) {
  return (
    getPayloadTimestamp(rawEvent.payload) ??
    rawEvent.eventTime ??
    rawEvent.receivedAt
  );
}

function parseAuthLog(payload) {
  const line = String(payload);

  const match = line.match(
    /sshd\[\d+\]: Failed password for (invalid user )?(\S+) from ([\d.]+) port (\d+) ssh2/
  );

  if (!match) return null;

  const invalidUser = Boolean(match[1]);
  const userName = match[2];
  const srcIp = match[3];

  return {
    category: 'authentication',
    action: 'login',
    outcome: 'failure',
    actor: {
      userId: null,
      userName,
      userType: 'unknown'
    },
    network: {
      srcIp,
      srcPort: Number(match[4]),
      dstIp: null,
      dstPort: null,
      protocol: null
    },
    target: {
      resourceType: 'host',
      resourceId: null
    },
    message: `Failed SSH password login for ${invalidUser ? 'invalid user ' : ''}${userName} from ${srcIp}`,
    attributes: {
      authMethod: 'password',
      invalidUser,
      program: 'sshd'
    }
  };
}

function getFallbackDetails(payload) {
  let action = 'unknown';

  if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
    const value = payload.eventName ?? payload.action ?? payload.operation;

    if (typeof value === 'string' && value.trim()) {
      action = value
        .trim()
        .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
        .replace(/[^a-zA-Z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '')
        .toLowerCase();

      if (!/^[a-z][a-z0-9_]*$/.test(action)) {
        action = 'unknown';
      }
    }
  }

  return {
    category: 'other',
    action,
    outcome: 'unknown',
    actor: {
      userId: null,
      userName: null,
      userType: 'unknown'
    },
    network: {
      srcIp: null,
      srcPort: null,
      dstIp: null,
      dstPort: null,
      protocol: null
    },
    target: {
      resourceType: null,
      resourceId: null
    },
    message: 'Unrecognized event',
    attributes: {}
  };
}

export function normalizeEvent(rawEvent, origin) {
  const rawResult = validateRawEvent(rawEvent);

  if (!rawResult.valid) {
    throw new Error(
      `Raw event validation failed: ${JSON.stringify(rawResult.errors)}`
    );
  }

  if (
    !origin ||
    typeof origin.topic !== 'string' ||
    !Number.isInteger(origin.partition) ||
    !Number.isInteger(origin.offset)
  ) {
    throw new Error('Invalid Kafka origin information');
  }

  const parsed = rawEvent.source.type === 'auth_log'
    ? parseAuthLog(rawEvent.payload)
    : null;

  const details = parsed ?? getFallbackDetails(rawEvent.payload);

  if (details.target.resourceId === null) {
    details.target.resourceId = rawEvent.source.name;
  }

  const source = {
    type: rawEvent.source.type,
    name: rawEvent.source.name,
    host: rawEvent.source.host ?? null,
    region: rawEvent.source.region ?? null,
    accountId: rawEvent.source.accountId ?? null
  };

  const event = {
    schemaVersion: rawEvent.schemaVersion,
    eventId: rawEvent.eventId,
    eventTime: getEventTime(rawEvent),
    receivedAt: rawEvent.receivedAt,
    normalizedAt: new Date().toISOString(),
    entityKey:
      details.network.srcIp ??
      details.actor.userId ??
      source.name,
    ...details,
    source,
    origin,
    normalizedBy: SERVICE
  };

  const result = validateNormalizedEvent(event);

  if (!result.valid) {
    throw new Error(
      `Normalized event validation failed: ${JSON.stringify(result.errors)}`
    );
  }

  return event;
}
