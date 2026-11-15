import { AUTH_RULE, isFailedAuthentication } from '../rules/auth-brute-force.js';

const SERVICE = { name: 'detection-rules-service', version: '1.0.0' };
const MAX_EVENTS_PER_ENTITY = 1000;

export function createDetectionEngine() {
  const entities = new Map();

  function reset() {
    entities.clear();
  }

  function evaluate(event, { warmup = false } = {}) {
    if (!event?.eventId || !event.entityKey || !event.eventTime) {
      throw new Error('Normalized event is missing required detection fields');
    }

    const eventTime = Date.parse(event.eventTime);

    if (!Number.isFinite(eventTime)) {
      throw new Error('Invalid eventTime');
    }

    if (!isFailedAuthentication(event) || !event.network?.srcIp) {
      return [];
    }

    const entityKey = event.entityKey;
    const ip = event.network.srcIp;

    let state = entities.get(entityKey);

    if (!state) {
      state = { events: new Map(), suppressedUntil: 0 };
      entities.set(entityKey, state);
    }

    if (state.events.has(event.eventId)) {
      return [];
    }

    for (const [id, item] of state.events) {
      if (eventTime - item.time > AUTH_RULE.windowMs) {
        state.events.delete(id);
      }
    }

    state.events.set(event.eventId, {
      time: eventTime,
      userName: event.actor?.userName ?? null
    });

    if (state.events.size > MAX_EVENTS_PER_ENTITY) {
      const oldest = [...state.events.entries()]
        .sort((a, b) => a[1].time - b[1].time);

      while (state.events.size > MAX_EVENTS_PER_ENTITY) {
        state.events.delete(oldest.shift()[0]);
      }
    }

    const current = [...state.events.entries()]
      .filter(([, item]) =>
        item.time >= eventTime - AUTH_RULE.windowMs &&
        item.time <= eventTime
      )
      .sort((a, b) => a[1].time - b[1].time);

    if (current.length < AUTH_RULE.threshold) {
      return [];
    }

    if (eventTime < state.suppressedUntil) {
      return [];
    }

    state.suppressedUntil = eventTime + AUTH_RULE.windowMs;

    if (warmup) {
      return [];
    }

    const events = current.map(([id, item]) => ({ id, ...item }));
    const usernames = [
      ...new Set(events.map(item => item.userName).filter(Boolean))
    ];

    const alert = {
      schemaVersion: '1.0',
      alertId: `${AUTH_RULE.ruleId}|${entityKey}|${event.eventId}`,
      alertTime: new Date(eventTime).toISOString(),
      ruleId: AUTH_RULE.ruleId,
      ruleName: AUTH_RULE.ruleName,
      ruleVersion: AUTH_RULE.ruleVersion,
      severity: 'high',
      alertType: 'brute_force',
      title: `${events.length} failed logins from ${ip} within 60 seconds`,
      description: `Source IP ${ip} produced ${events.length} failed authentication events within 60 seconds, reaching the threshold of 5.`,
      entityKey,
      entity: {
        srcIp: ip,
        userId: event.actor?.userId ?? null,
        host: event.target?.resourceId ?? event.source?.host ?? null
      },
      triggerEventId: event.eventId,
      relatedEventIds: events.slice(-20).map(item => item.id),
      eventCount: events.length,
      window: {
        start: new Date(events[0].time).toISOString(),
        end: new Date(events[events.length - 1].time).toISOString(),
        durationSeconds: 60
      },
      evidence: {
        threshold: AUTH_RULE.threshold,
        observedFailures: events.length,
        usernamesTried: usernames
      },
      status: 'open',
      raisedBy: SERVICE
    };

    return [alert];
  }

  return { evaluate, reset };
}
