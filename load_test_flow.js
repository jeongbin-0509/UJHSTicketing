import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:5000';
const MAX_WAIT_SECONDS = Number(__ENV.MAX_WAIT_SECONDS || 240);

const reserveSuccess = new Counter('reserve_success');
const admittedRate = new Rate('admitted_rate');
const journeyFailure = new Rate('journey_failure');
const queueWait = new Trend('queue_wait_duration', true);

export const options = {
  stages: [
    { duration: __ENV.STAGE_100 || '30s', target: 100 },
    { duration: __ENV.STAGE_300 || '45s', target: 300 },
    { duration: __ENV.STAGE_1000 || '60s', target: 1000 },
    { duration: __ENV.STAGE_HOLD || '60s', target: 1000 },
    { duration: '20s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    journey_failure: ['rate<0.01'],
    admitted_rate: ['rate>0.95'],
    queue_wait_duration: ['p(95)<240000'],
  },
};

function failJourney(message, response) {
  journeyFailure.add(true);
  console.error(`${message}: status=${response && response.status} url=${response && response.url}`);
}

export default function () {
  const journeyStarted = Date.now();
  let admitted = false;
  let failed = false;

  const enter = http.get(`${BASE_URL}/enter`);
  if (!check(enter, { 'enter reached queue/apply': (r) => r.status === 200 && (r.url.endsWith('/waiting') || r.url.endsWith('/apply')) })) {
    failJourney('enter failed', enter);
    return;
  }
  admitted = enter.url.endsWith('/apply');

  const deadline = Date.now() + MAX_WAIT_SECONDS * 1000;
  while (!admitted && Date.now() < deadline) {
    const status = http.get(`${BASE_URL}/queue/status`, { redirects: 0 });
    let body = null;
    try { body = status.json(); } catch (_) { /* recorded below */ }

    if (status.status !== 200 || !body || body.valid !== true) {
      failJourney('queue became invalid', status);
      failed = true;
      break;
    }
    admitted = body.can_enter === true;
    if (!admitted) sleep(0.5 + Math.random() * 0.5);
  }

  admittedRate.add(admitted);
  if (!admitted) {
    if (!failed) {
      journeyFailure.add(true);
      console.error('queue wait timed out');
    }
    return;
  }
  queueWait.add(Date.now() - journeyStarted);

  // active TTL을 실제 브라우저처럼 갱신한다.
  const heartbeat = http.post(`${BASE_URL}/heartbeat`, null, { redirects: 0 });
  if (heartbeat.status !== 200) {
    failJourney('heartbeat failed', heartbeat);
    return;
  }

  const unique = String((exec.scenario.iterationInTest + 1) % 100000000).padStart(8, '0');
  const phone = `010-${unique.slice(0, 4)}-${unique.slice(4)}`;
  const reserve = http.post(`${BASE_URL}/reserve`, {
    student_name: 'LoadTester',
    student_phone: phone,
    parent_name: 'LoadParent',
    parent_phone: phone,
    people_count: '1',
  });

  const reserved = reserve.status === 200 && reserve.url.endsWith('/success');
  if (reserved) reserveSuccess.add(1);
  journeyFailure.add(!reserved);
  check(reserve, { 'reservation completed': () => reserved });
}
