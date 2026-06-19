// k6 load test: hits POST /publish with batches of events, ~30%+ duplicate
// event_ids, to satisfy the >=20,000 event / >=30% duplicate requirement.
//
// Install k6: https://github.com/grafana/k6 (see their README for OS-specific install)
// Run:        k6 run --vus 20 --duration 60s scripts/k6/load_test.js
//
// After the run, check GET /stats on the aggregator and paste the
// numbers (received / unique_processed / duplicate_dropped / throughput)
// into report.md Bab 9.

import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080/publish';
const BATCH_SIZE = 50;
const TOPICS = ['auth', 'payments', 'orders', 'system'];

// a shared pool of "already seen" ids per VU iteration to force re-sends
let seenIds = [];

export const options = {
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      rate: 200,            // batches per second target (~10k events/sec at batch=50)
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 20,
      maxVUs: 50,
    },
  },
};

function randomTopic() {
  return TOPICS[Math.floor(Math.random() * TOPICS.length)];
}

function buildBatch() {
  const batch = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    const isDuplicate = seenIds.length > 20 && Math.random() < 0.30;
    const eventId = isDuplicate
      ? seenIds[Math.floor(Math.random() * seenIds.length)]
      : uuidv4();

    if (!isDuplicate) {
      seenIds.push(eventId);
      if (seenIds.length > 2000) seenIds.shift(); // bound memory
    }

    batch.push({
      topic: randomTopic(),
      event_id: eventId,
      timestamp: new Date().toISOString(),
      source: 'k6-load-test',
      payload: { seq: i },
    });
  }
  return batch;
}

export default function () {
  const batch = buildBatch();
  const res = http.post(TARGET_URL, JSON.stringify(batch), {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, {
    'status is 202': (r) => r.status === 202,
  });
}
