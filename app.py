import os
import time
import uuid
from io import BytesIO

import pandas as pd
import psycopg2
import psycopg2.extras

from dotenv import load_dotenv
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    session,
    jsonify,
    send_file,
)

from upstash_redis import Redis


# =========================================================
# 기본 설정
# =========================================================

load_dotenv()

app = Flask(__name__)

app.secret_key = os.getenv(
    "SECRET_KEY",
    "change-this-secret-key"
)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

TOTAL_SEATS = int(
    os.getenv("TOTAL_SEATS", 300)
)

MAX_ACTIVE = int(
    os.getenv("MAX_ACTIVE", 30)
)

APPLY_TIME_LIMIT = int(
    os.getenv("APPLY_TIME_LIMIT", 180)
)

HEARTBEAT_TIMEOUT = 30


# =========================================================
# Redis 연결
# =========================================================

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN"),
)


# =========================================================
# DB 연결
# =========================================================

def get_db():

    return psycopg2.connect(
        SUPABASE_DB_URL,
        cursor_factory=psycopg2.extras.RealDictCursor
    )


# =========================================================
# 남은 좌석
# =========================================================

def get_remaining_seats():

    conn = get_db()
    cur = conn.cursor()

    try:

        cur.execute("""
            SELECT
                next_seat,
                total_seats
            FROM seat_counter
            WHERE id = 1
        """)

        data = cur.fetchone()

        if not data:
            return 0

        return max(
            data["total_seats"]
            - data["next_seat"]
            + 1,
            0
        )

    finally:

        cur.close()
        conn.close()


# =========================================================
# Redis 유틸
# =========================================================

def get_active_tokens():

    tokens = redis.smembers("queue:active")

    if not tokens:
        return []

    return list(tokens)


def cleanup_expired_users():

    """
    heartbeat가 끊기거나
    입장 제한 시간이 지난 사용자 제거
    """

    active_tokens = get_active_tokens()

    now = int(time.time())

    for token in active_tokens:

        heartbeat = redis.get(
            f"heartbeat:{token}"
        )

        entered_at = redis.get(
            f"entry_time:{token}"
        )

        remove_user = False

        # heartbeat 사라짐
        if heartbeat is None:
            remove_user = True

        # 입장 시간 초과
        if entered_at:

            entered_at = int(entered_at)

            if now - entered_at > APPLY_TIME_LIMIT:
                remove_user = True

        if remove_user:

            redis.srem(
                "queue:active",
                token
            )

            redis.delete(
                f"heartbeat:{token}"
            )

            redis.delete(
                f"entry_time:{token}"
            )

            redis.delete(
                f"entry:{token}"
            )


def try_admit_users():

    """
    빈 슬롯만큼
    대기열 앞사람 입장
    """

    cleanup_expired_users()

    active_count = len(
        get_active_tokens()
    )

    available_slots = (
        MAX_ACTIVE - active_count
    )

    if available_slots <= 0:
        return

    for _ in range(available_slots):

        token = redis.lpop(
            "queue:waiting"
        )

        if not token:
            break

        now = int(time.time())

        redis.sadd(
            "queue:active",
            token
        )

        redis.set(
            f"entry:{token}",
            "allowed",
            ex=APPLY_TIME_LIMIT
        )

        redis.set(
            f"entry_time:{token}",
            now,
            ex=APPLY_TIME_LIMIT
        )

        redis.set(
            f"heartbeat:{token}",
            now,
            ex=HEARTBEAT_TIMEOUT
        )


def remove_active_user(token):

    if not token:
        return

    redis.srem(
        "queue:active",
        token
    )

    redis.delete(
        f"heartbeat:{token}"
    )

    redis.delete(
        f"entry:{token}"
    )

    redis.delete(
        f"entry_time:{token}"
    )

    try_admit_users()


# =========================================================
# 첫 접속
# =========================================================

@app.route("/")
def index():

    # 이미 예약 완료한 경우
    if session.get("completed"):
        return redirect("/success")

    # 이미 토큰 있는 사용자
    token = session.get("queue_token")

    if token:

        # 이미 입장 가능한 상태
        if redis.get(f"entry:{token}"):

            return redirect("/apply")

        # 대기열에 아직 있다면
        waiting = redis.lrange(
            "queue:waiting",
            0,
            -1
        )

        if token in waiting:
            return redirect("/waiting")

    # 신규 사용자
    token = str(uuid.uuid4())

    queue_no = redis.incr(
        "queue:last_number"
    )

    session["queue_token"] = token
    session["queue_no"] = queue_no

    redis.set(
        f"queue:number:{token}",
        queue_no,
        ex=3600
    )

    redis.rpush(
        "queue:waiting",
        token
    )

    try_admit_users()

    if redis.get(f"entry:{token}"):

        return redirect("/apply")

    return redirect("/waiting")


# =========================================================
# 대기 페이지
# =========================================================

@app.route("/waiting")
def waiting():

    token = session.get(
        "queue_token"
    )

    if not token:
        return redirect("/")

    if redis.get(f"entry:{token}"):

        return redirect("/apply")

    return render_template(
        "waiting.html"
    )


# =========================================================
# 대기 상태 API
# =========================================================

@app.route("/queue/status")
def queue_status():

    token = session.get(
        "queue_token"
    )

    if not token:

        return jsonify({
            "error": True
        }), 401

    try_admit_users()

    # 이미 입장 가능
    if redis.get(f"entry:{token}"):

        return jsonify({
            "can_enter": True,
            "waiting_count": 0,
            "queue_no": session.get(
                "queue_no"
            )
        })

    waiting_list = redis.lrange(
        "queue:waiting",
        0,
        -1
    )

    try:

        position = waiting_list.index(
            token
        )

        waiting_count = position

    except ValueError:

        waiting_count = 0

    return jsonify({
        "can_enter": False,
        "waiting_count": waiting_count,
        "queue_no": session.get(
            "queue_no"
        )
    })


# =========================================================
# 신청 페이지
# =========================================================

@app.route("/apply")
def apply():

    token = session.get(
        "queue_token"
    )

    if not token:
        return redirect("/")

    if not redis.get(
        f"entry:{token}"
    ):
        return redirect("/waiting")

    # heartbeat 갱신
    redis.set(
        f"heartbeat:{token}",
        int(time.time()),
        ex=HEARTBEAT_TIMEOUT
    )

    remaining = get_remaining_seats()

    if remaining <= 0:

        remove_active_user(token)

        return "모든 좌석이 마감되었습니다."

    entered_at = redis.get(
        f"entry_time:{token}"
    )

    remaining_time = APPLY_TIME_LIMIT

    if entered_at:

        elapsed = (
            int(time.time())
            - int(entered_at)
        )

        remaining_time = max(
            APPLY_TIME_LIMIT - elapsed,
            0
        )

    return render_template(
        "apply.html",
        remaining=remaining,
        remaining_time=remaining_time
    )


# =========================================================
# heartbeat
# =========================================================

@app.route("/heartbeat", methods=["POST"])
def heartbeat():

    token = session.get(
        "queue_token"
    )

    if not token:

        return jsonify({
            "ok": False
        }), 401

    if not redis.get(
        f"entry:{token}"
    ):

        return jsonify({
            "ok": False,
            "expired": True
        })

    redis.set(
        f"heartbeat:{token}",
        int(time.time()),
        ex=HEARTBEAT_TIMEOUT
    )

    return jsonify({
        "ok": True
    })


# =========================================================
# 예약
# =========================================================

@app.route("/reserve", methods=["POST"])
def reserve():

    token = session.get(
        "queue_token"
    )

    if not token:

        return redirect("/")

    if not redis.get(
        f"entry:{token}"
    ):

        return redirect("/waiting")

    student_name = request.form[
        "student_name"
    ].strip()

    student_phone = request.form[
        "student_phone"
    ].strip()

    parent_name = request.form[
        "parent_name"
    ].strip()

    parent_phone = request.form[
        "parent_phone"
    ].strip()

    try:

        people_count = int(
            request.form["people_count"]
        )

    except ValueError:

        return "잘못된 신청 인원입니다."

    if people_count not in [1, 2]:

        return "신청 인원이 올바르지 않습니다."

    conn = get_db()
    cur = conn.cursor()

    try:

        conn.autocommit = False

        # 좌석 카운터 락
        cur.execute("""
            SELECT
                next_seat,
                total_seats
            FROM seat_counter
            WHERE id = 1
            FOR UPDATE
        """)

        seat_data = cur.fetchone()

        if not seat_data:

            conn.rollback()

            return "좌석 정보가 없습니다."

        next_seat = seat_data[
            "next_seat"
        ]

        total_seats = seat_data[
            "total_seats"
        ]

        if (
            next_seat
            + people_count
            - 1
            > total_seats
        ):

            conn.rollback()

            return "남은 좌석이 부족합니다."

        seat_list = list(
            range(
                next_seat,
                next_seat
                + people_count
            )
        )

        seat_numbers = ", ".join(
            map(str, seat_list)
        )

        # 예약 저장
        cur.execute("""
            INSERT INTO reservations (
                student_name,
                student_phone,
                parent_name,
                parent_phone,
                people_count,
                seat_numbers
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """, (
            student_name,
            student_phone,
            parent_name,
            parent_phone,
            people_count,
            seat_numbers
        ))

        # 좌석 카운터 증가
        cur.execute("""
            UPDATE seat_counter
            SET next_seat =
                next_seat + %s
            WHERE id = 1
        """, (
            people_count,
        ))

        conn.commit()

        session["completed"] = True
        session["student_name"] = (
            student_name
        )
        session["people_count"] = (
            people_count
        )
        session["seat_numbers"] = (
            seat_numbers
        )

        # 슬롯 반환
        remove_active_user(
            token
        )

        return redirect(
            "/success"
        )

    except psycopg2.errors.UniqueViolation:

        conn.rollback()

        return (
            "이미 신청된 "
            "학생 전화번호입니다."
        )

    except Exception as e:

        conn.rollback()

        return (
            f"신청 중 오류 발생: {e}"
        )

    finally:

        cur.close()
        conn.close()


# =========================================================
# 신청 완료
# =========================================================

@app.route("/success")
def success():

    if not session.get(
        "completed"
    ):

        return redirect("/")

    return render_template(
        "success.html",
        student_name=session.get(
            "student_name"
        ),
        people_count=session.get(
            "people_count"
        ),
        seat_numbers=session.get(
            "seat_numbers"
        )
    )


# =========================================================
# 관리자
# =========================================================

@app.route("/admin")
def admin():

    conn = get_db()
    cur = conn.cursor()

    try:

        cur.execute("""
            SELECT *
            FROM reservations
            ORDER BY id ASC
        """)

        reservations = (
            cur.fetchall()
        )

        cur.execute("""
            SELECT
                next_seat,
                total_seats
            FROM seat_counter
            WHERE id = 1
        """)

        seat_data = (
            cur.fetchone()
        )

    finally:

        cur.close()
        conn.close()

    used_seats = (
        seat_data["next_seat"]
        - 1
    )

    remaining = (
        seat_data["total_seats"]
        - used_seats
    )

    active_count = len(
        get_active_tokens()
    )

    waiting_count = redis.llen(
        "queue:waiting"
    )

    return render_template(
        "admin.html",
        reservations=reservations,
        used_seats=used_seats,
        remaining=remaining,
        total_seats=seat_data[
            "total_seats"
        ],
        active_count=active_count,
        waiting_count=waiting_count
    )


# =========================================================
# 엑셀 다운로드
# =========================================================

@app.route("/admin/excel")
def download_excel():

    conn = get_db()

    df = pd.read_sql_query("""
        SELECT
            id AS 번호,
            student_name AS 학생이름,
            student_phone AS 학생전화번호,
            parent_name AS 보호자이름,
            parent_phone AS 보호자전화번호,
            people_count AS 신청인원,
            seat_numbers AS 배정좌석,
            created_at AS 신청시간
        FROM reservations
        ORDER BY id ASC
    """, conn)

    conn.close()

    output = BytesIO()

    with pd.ExcelWriter(
        output,
        engine="openpyxl"
    ) as writer:

        df.to_excel(
            writer,
            index=False,
            sheet_name="신청자목록"
        )

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=(
            "입시설명회_신청자목록.xlsx"
        ),
        mimetype=(
            "application/"
            "vnd.openxmlformats-"
            "officedocument."
            "spreadsheetml.sheet"
        )
    )


# =========================================================
# 대기열 초기화
# =========================================================

@app.route("/admin/reset-queue")
def reset_queue():

    keys = redis.keys("queue:*")

    for key in keys:
        redis.delete(key)

    entry_keys = redis.keys(
        "entry:*"
    )

    for key in entry_keys:
        redis.delete(key)

    heartbeat_keys = redis.keys(
        "heartbeat:*"
    )

    for key in heartbeat_keys:
        redis.delete(key)

    entry_time_keys = redis.keys(
        "entry_time:*"
    )

    for key in entry_time_keys:
        redis.delete(key)

    return "대기열 초기화 완료"


# =========================================================
# 현재 대기열 상태
# =========================================================

@app.route("/admin/queue-status")
def admin_queue_status():

    cleanup_expired_users()

    active_count = len(
        get_active_tokens()
    )

    waiting_count = redis.llen(
        "queue:waiting"
    )

    return jsonify({
        "max_active": MAX_ACTIVE,
        "active_count": active_count,
        "waiting_count": waiting_count,
        "available_slots":
            max(
                MAX_ACTIVE
                - active_count,
                0
            )
    })


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    app.run(
        debug=True
    )