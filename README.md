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
