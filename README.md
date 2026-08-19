# UJHS Student Ticketing Test

학생 대상 티켓팅 부하/대기열 테스트 버전입니다.

## 변경점
- 메인 화면은 관리자 설정 오픈 시간 전에는 잠김
- 오픈 시간 도달 시 메인 버튼 자동 활성화
- 서버에서도 오픈 시간을 검사하므로 URL 직접 접근 차단
- 신청 정보는 학번 5자리만 입력
- `test_reservations` 테이블에 학번/좌석/시간 저장
- 관리자 페이지에서 오픈/마감 시간, 총 좌석 수 변경 가능
- 테스트 전체 초기화 및 엑셀 다운로드 가능

## Supabase
`supabase_setup_test.sql`을 SQL Editor에서 한 번 실행하세요.

## 실행
```bash
pip install -r requirements.txt
python app.py
```

관리자: `/admin`

## Render
Start Command:
```bash
gunicorn app:app --workers 2 --threads 4 --timeout 60
```

환경변수는 `.env.example` 항목을 Render Environment에 등록하세요.
