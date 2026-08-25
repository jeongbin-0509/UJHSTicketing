import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:5000';
const MAX_WAIT_SECONDS = Number(__ENV.MAX_WAIT_SECONDS || 240);
const EXACT_QUEUE_THRESHOLD = Number(__ENV.EXACT_QUEUE_THRESHOLD || 100);

http.setResponseCallback(http.expectedStatuses(200, 302, 409));

const reserveSuccess = new Counter('reserve_success');
const seatsReserved = new Counter('seats_reserved');
const soldOut = new Counter('sold_out');
const capacityRejected = new Counter('capacity_rejected');
const admittedRate = new Rate('admitted_rate');
const handledRate = new Rate('handled_rate');
const journeyFailure = new Rate('journey_failure');
const queueWait = new Trend('queue_wait_duration', true);
const queueExactPolls = new Counter('queue_exact_polls');
const queueSharedPolls = new Counter('queue_shared_polls');

export const options = {
  scenarios: {
    ticket_journey: {
      executor: 'per-vu-iterations',
      vus: Number(__ENV.VUS || 100),
      iterations: 1,
      maxDuration: __ENV.MAX_DURATION || '5m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    journey_failure: ['rate<0.01'],
    handled_rate: ['rate>0.99'],
    queue_wait_duration: ['p(95)<240000'],
  },
};

function failJourney(message, response) {
  journeyFailure.add(true);
  console.error(`${message}: status=${response && response.status} url=${response && response.url}`);
}

function queuePollDelay(waitingCount) {
  if (waitingCount <= 3) return 0.35;
  if (waitingCount <= 10) return 0.7;
  if (waitingCount <= 30) return 1.2;
  if (waitingCount <= 100) return 2.5;
  if (waitingCount <= 300) return 5;
  return 8;
}

export default function () {
  const journeyStarted = Date.now();
  let admitted = false;
  let failed = false;
  let queueNo = null;
  let useSharedProgress = false;

  const enter = http.get(`${BASE_URL}/enter`, { redirects: 0 });
  const location = enter.headers.Location || '';
  if (enter.status === 302 && location === '/' && enter.headers['X-Ticket-Result'] === 'sold-out') {
    soldOut.add(1);
    handledRate.add(true);
    journeyFailure.add(false);
    return;
  }
  if (!check(enter, { 'enter joined queue': (r) => r.status === 302 && (location === '/waiting' || location === '/apply') })) {
    failJourney('enter failed', enter);
    handledRate.add(false);
    return;
  }
  admitted = location === '/apply';
  const entryPage = http.get(`${BASE_URL}${location}`, { redirects: 0 });
  if (entryPage.status !== 200) {
    failJourney('entry page failed', entryPage);
    handledRate.add(false);
    return;
  }

  const deadline = Date.now() + MAX_WAIT_SECONDS * 1000;
  while (!admitted && Date.now() < deadline) {
    if (useSharedProgress && queueNo !== null) {
      queueSharedPolls.add(1);
      const progressResponse = http.get(`${BASE_URL}/queue/progress`);
      let progress = null;
      try { progress = progressResponse.json(); } catch (_) { /* exact fallback below */ }
      if (progressResponse.status === 200 && progress && progress.open && !progress.sold_out) {
        const front = Number(progress.front_queue_no || queueNo);
        const estimatedCount = Math.max(queueNo - front, 0);
        if (estimatedCount > Number(progress.exact_threshold || EXACT_QUEUE_THRESHOLD)) {
          sleep(queuePollDelay(estimatedCount) + Math.random() * 0.25);
          continue;
        }
      }
      useSharedProgress = false;
    }

    queueExactPolls.add(1);
    const status = http.get(`${BASE_URL}/queue/status`, { redirects: 0 });
    let body = null;
    try { body = status.json(); } catch (_) { /* recorded below */ }

    if (status.status === 200 && body && body.sold_out === true) {
      soldOut.add(1);
      handledRate.add(true);
      journeyFailure.add(false);
      return;
    }
    if (status.status !== 200 || !body || body.valid !== true) {
      failJourney('queue became invalid', status);
      handledRate.add(false);
      failed = true;
      break;
    }
    admitted = body.can_enter === true;
    queueNo = Number(body.queue_no || queueNo);
    if (!admitted) {
      const waitingCount = Number(body.waiting_count || 0);
      useSharedProgress = waitingCount > EXACT_QUEUE_THRESHOLD;
      sleep(queuePollDelay(waitingCount) + Math.random() * 0.25);
    }
  }

  admittedRate.add(admitted);
  if (!admitted) {
    if (!failed) {
      journeyFailure.add(true);
      handledRate.add(false);
      console.error('queue wait timed out');
    }
    return;
  }
  queueWait.add(Date.now() - journeyStarted);

  if (!location.endsWith('/apply')) {
    const applyPage = http.get(`${BASE_URL}/apply`, { redirects: 0 });
    if (applyPage.status === 302 && applyPage.headers['X-Ticket-Result'] === 'sold-out') {
      soldOut.add(1);
      handledRate.add(true);
      journeyFailure.add(false);
      return;
    }
    if (applyPage.status !== 200) {
      failJourney('apply page failed', applyPage);
      handledRate.add(false);
      return;
    }
  }

  // active TTL을 실제 브라우저처럼 갱신한다.
  const heartbeat = http.post(`${BASE_URL}/heartbeat`, null, { redirects: 0 });
  if (heartbeat.status !== 200) {
    failJourney('heartbeat failed', heartbeat);
    handledRate.add(false);
    return;
  }

  const unique = String((exec.scenario.iterationInTest + 1) % 100000000).padStart(8, '0');
  const phone = `010-${unique.slice(0, 4)}-${unique.slice(4)}`;
  const peopleCount = Math.random() < 0.5 ? 1 : 2;
  const reserve = http.post(`${BASE_URL}/reserve`, {
    student_name: 'LoadTester',
    student_phone: phone,
    parent_name: 'LoadParent',
    parent_phone: phone,
    people_count: String(peopleCount),
  }, { redirects: 0 });

  const reserved = reserve.status === 302 && reserve.headers.Location === '/success';
  const capacityResult = reserve.headers['X-Ticket-Result'] || '';
  const expectedRejection = reserve.status === 409 && (
    capacityResult === 'sold-out' || capacityResult === 'insufficient-seats'
  );
  if (reserved) {
    reserveSuccess.add(1);
    seatsReserved.add(peopleCount);
  } else if (capacityResult === 'sold-out') {
    soldOut.add(1);
  } else if (capacityResult === 'insufficient-seats') {
    capacityRejected.add(1);
  }
  handledRate.add(reserved || expectedRejection);
  journeyFailure.add(!reserved && !expectedRejection);
  check(reserve, { 'reservation handled': () => reserved || expectedRejection });
}
