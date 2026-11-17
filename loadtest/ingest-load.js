// k6 load test for the public Log Ingestion Service (POST /events).
//
// Goal: generate sustained traffic so the log-ingestion HPA (CPU 50%, 2 -> 8 pods)
// visibly scales out, then back in. Watch it with:
//   kubectl -n cloud-sec get hpa log-ingestion -w
// and the "HPA replicas" panel in the Grafana "Cloud Security Monitoring" dashboard.
//
// Run:
//   k6 run -e BASE_URL=http://<log-ingestion-LB-hostname> loadtest/ingest-load.js
//   (get the hostname: kubectl -n cloud-sec get svc log-ingestion)
// Optional: -e PEAK_RPS=400  -e HOLD=6m   (quick smoke: -e PROFILE=smoke)

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
const PEAK_RPS = Number(__ENV.PEAK_RPS || 300);
const HOLD = __ENV.HOLD || '5m';

const smoke = __ENV.PROFILE === 'smoke';

export const options = {
  scenarios: {
    ingest: {
      // Open model: arrival rate is fixed regardless of latency, so the
      // service really is pushed (closed-loop VUs would back off when slow).
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 1000,
      stages: smoke
        ? [
            { target: 20, duration: '10s' },
            { target: 20, duration: '20s' },
          ]
        : [
            { target: 20, duration: '1m' },            // warm-up at baseline
            { target: PEAK_RPS, duration: '2m' },      // ramp -> HPA scales out
            { target: PEAK_RPS, duration: HOLD },      // sustained peak
            { target: 20, duration: '2m' },            // drop -> HPA scales in
            { target: 20, duration: '2m' },
          ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],                 // <2% errors
    http_req_duration: ['p(95)<800', 'p(99)<2000'], // ms
    checks: ['rate>0.98'],
  },
};

const accepted = new Counter('events_accepted');
const rejected = new Counter('events_rejected');

const REGIONS = ['ap-south-1', 'us-east-1', 'eu-west-1'];
const USERS = ['root', 'admin', 'ubuntu', 'deploy', 'alice', 'bob'];
const IAM_ACTIONS = ['CreateAccessKey', 'AttachUserPolicy', 'DeleteTrail', 'PutBucketPolicy'];

function rnd(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function ip() {
  // Small pool of attacker IPs so the "5 failed logins / 60s from one IP"
  // detection rule actually fires during the test.
  return `203.0.113.${1 + Math.floor(Math.random() * 20)}`;
}

function authLogEvent() {
  const day = new Date().toUTCString().slice(5, 11);
  return {
    source: { type: 'auth_log', name: 'bastion-ssh', host: `bastion-${1 + Math.floor(Math.random() * 3)}`, region: rnd(REGIONS), accountId: '123456789012' },
    format: 'text',
    payload: `${day} ${new Date().toISOString().slice(11, 19)} bastion sshd[${1000 + Math.floor(Math.random() * 8000)}]: Failed password for ${Math.random() < 0.3 ? 'invalid user ' : ''}${rnd(USERS)} from ${ip()} port ${20000 + Math.floor(Math.random() * 40000)} ssh2`,
    eventTime: new Date().toISOString(),
  };
}

function cloudTrailEvent() {
  return {
    source: { type: 'cloudtrail', name: 'org-trail', region: rnd(REGIONS), accountId: '123456789012' },
    format: 'json',
    payload: {
      eventName: rnd(IAM_ACTIONS),
      userIdentity: { userName: rnd(USERS), type: 'IAMUser' },
      sourceIPAddress: ip(),
      awsRegion: rnd(REGIONS),
      requestId: uuidv4(),
    },
    eventTime: new Date().toISOString(),
  };
}

function buildEvent() {
  return Math.random() < 0.6 ? authLogEvent() : cloudTrailEvent();
}

const HEADERS = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const res = http.post(`${BASE_URL}/events`, JSON.stringify(buildEvent()), HEADERS);

  const ok = check(res, {
    'status is 202': (r) => r.status === 202,
    'has eventId': (r) => r.status !== 202 || !!r.json('eventId'),
  });

  (ok ? accepted : rejected).add(1);
}

// Sanity gate before the load starts.
export function setup() {
  const r = http.get(`${BASE_URL}/health`);
  if (r.status !== 200) {
    throw new Error(`log-ingestion not healthy at ${BASE_URL} (status ${r.status})`);
  }
}
