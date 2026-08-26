# 운정고등학교 입시설명회 신청을 위한 티켓팅 시스템입니다.

동시에 많은 사용자가 접속하는 상황을 고려하여 **Redis 기반 대기열**, **PostgreSQL 트랜잭션**, **Connection Pool**, **캐싱**을 적용해 서버 부하와 좌석 중복 문제를 줄이도록 설계했습니다.

---

## 1. Tech Stack

### Backend
- Python
- Flask
- Gunicorn

### Database
- Supabase PostgreSQL
- Upstash Redis

### Deployment
- Render

### Load Testing
- k6

## 부하 테스트

홈 화면 캐시만 확인하는 `load_test_home.js`와 별도로, 실제 사용자 경로를 검증하는
`load_test_flow.js`가 있습니다. 테스트 전 별도의 테스트 환경에서 판매 시간을 열고,
좌석 수를 충분히 설정한 뒤 실행하세요. 운영 데이터에는 실행하지 마세요.

```bash
BASE_URL=https://test.example.com VUS=100 k6 run load_test_flow.js
```

스크립트는 `/enter` → `/queue/status` 폴링 → `/heartbeat` → `/reserve`를 수행하고,
HTTP 코드뿐 아니라 실제 입장률(`admitted_rate`), 완료 수(`reserve_success`), 전체 과정
실패율(`journey_failure`), 대기시간(`queue_wait_duration`)을 기록합니다.
각 가상 사용자는 전체 신청 과정을 정확히 한 번만 수행합니다. `VUS=100`, `300`,
`1000`으로 각각 실행해 단계별 결과를 비교할 수 있습니다.

대기열 자동 테스트는 `pip install -r requirements-dev.txt` 후 `pytest -q`로 실행합니다.
최초 입장에는 응답 지연을 견디기 위한 `ACTIVE_INITIAL_TTL`(기본 60초)을 적용하고,
첫 `/heartbeat` 이후에는 `ACTIVE_HEARTBEAT_TTL`(기본 30초)로 전환됩니다. 현재
측정된 1,000명 테스트의 HTTP p95(17.73초)보다 길게 잡아 정상 요청의 오탐 만료를 막습니다.

Redis 명령 사용량을 줄이기 위해 일반 대기자는 `ZRANK` 한 번으로 상태를 확인하고,
앞 3명 또는 청소 트리거에만 원자 입장 스크립트를 실행합니다. 원거리 대기자는
`/queue/progress`의 공유 상태를 사용하며 기본 캐시는 3초, 개인 순위 전환 기준은
앞 50명입니다. 신청실 하트비트는 10초 주기이고 presence TTL은 30초입니다.

Render 연결 수용 구간만 분리해서 측정할 때는 `load_test_render.js`를 사용합니다.
기본값은 `/healthz`에 4,000명 순간 요청이며 `SPREAD_SECONDS=30`으로 유입을
분산하거나 `MAX_ATTEMPTS=5`로 연결 재시도 회복률을 비교할 수 있습니다.

---

## 2. System Architecture

```text
사용자 브라우저
      ↓
    Render
      ↓
Flask + Gunicorn
   ├───────────────┐
   ↓               ↓
Upstash Redis   Supabase PostgreSQL
   ↓               ↓
대기열 관리       신청 정보 / 좌석 정보 저장
