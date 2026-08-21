# UJHS Admission Ticketing - Optimized

운정고 입시설명회 티켓팅용 성능 개선 버전입니다.

## 입력 항목

기존 입시설명회 신청 형식으로 복구했습니다.

- 학생 이름
- 학생 전화번호
- 보호자 이름
- 보호자 전화번호
- 신청 인원: 1명 / 2명
- 학생 전화번호 기준 중복 신청 방지
- 인원 수만큼 연속 좌석 자동 배정

## 성능 개선

- PostgreSQL `ThreadedConnectionPool` 재사용
- 메인 표시 데이터: local cache -> Redis -> Supabase
- `/queue/status` 정상 경로에서 Supabase 직접 조회 없음
- 대기열 앞쪽 사용자만 admission lock 시도
- adaptive polling + jitter
- 최종 좌석 배정은 PostgreSQL `FOR UPDATE` transaction 유지
- `/healthz` 제공

## 환경변수

기존 `.env` 값에 아래를 추가하는 것을 권장합니다.

```env
DB_POOL_MIN=1
DB_POOL_MAX=8
DB_POOL_WAIT_SECONDS=5
LOCAL_CACHE_SECONDS=1
REDIS_KEY_PREFIX=ticket:v3
```

비밀키/DB 비밀번호/Redis 토큰은 저장소에 올리지 마세요.

## Supabase

SQL Editor에서 `supabase_setup.sql`을 실행합니다.
기존 `test_reservations` 테이블은 새 코드에서 사용하지 않습니다. 실제 신청은 `reservations` 테이블에 저장됩니다.

## Render Start Command

```bash
gunicorn app:app --workers 2 --threads 8 --timeout 60 --keep-alive 5 --max-requests 2000 --max-requests-jitter 200
```

`--preload`는 사용하지 마세요.

Health Check Path:

```text
/healthz
```

## k6 테스트

```powershell
$env:BASE_URL="https://실제주소"
k6 run .\load_test_home.js
```

10 VU부터 시작해서 50 -> 100 -> 200 순으로 올리세요.
