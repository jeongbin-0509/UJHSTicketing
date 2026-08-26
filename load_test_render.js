import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'https://xn--zo5b8y.kr';
const PATH = __ENV.TEST_PATH || '/healthz';
const METHOD = (__ENV.METHOD || 'GET').toUpperCase();
const SPREAD_SECONDS = Number(__ENV.SPREAD_SECONDS || 0);
const MAX_ATTEMPTS = Math.max(1, Number(__ENV.MAX_ATTEMPTS || 1));
const endpointOk = new Rate('endpoint_ok');
const networkStatus0 = new Counter('network_status_0');
const retryAttempts = new Counter('retry_attempts');

export const options = {
  scenarios: {
    render_layer: {
      executor: 'per-vu-iterations',
      vus: Number(__ENV.VUS || 4000),
      iterations: 1,
      maxDuration: __ENV.MAX_DURATION || '2m',
    },
  },
  thresholds: {
    endpoint_ok: ['rate>0.99'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  if (SPREAD_SECONDS > 0) sleep(Math.random() * SPREAD_SECONDS);
  const entryToken = `${Math.random().toString(16).slice(2, 10)}-${Math.random().toString(16).slice(2, 6)}-4${Math.random().toString(16).slice(2, 5)}-a${Math.random().toString(16).slice(2, 5)}-${Math.random().toString(16).slice(2, 14)}`;
  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
    if (attempt > 0) retryAttempts.add(1);
    const params = {
      redirects: 0,
      timeout: '60s',
    };
    const response = METHOD === 'POST'
      ? http.post(`${BASE_URL}${PATH}`, { entry_token: entryToken, response: 'json' }, params)
      : http.get(`${BASE_URL}${PATH}`, params);
    const passed = response.status >= 200 && response.status < 400;
    if (passed) {
      endpointOk.add(true);
      return;
    }
    if (response.status === 0) networkStatus0.add(1);
    if (attempt + 1 < MAX_ATTEMPTS) {
      sleep(0.25 * (2 ** attempt) + Math.random() * 0.25);
    }
  }
  endpointOk.add(false);
}
