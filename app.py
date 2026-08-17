import os
import re
import time
import uuid
from functools import wraps
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
    Response,
)

from upstash_redis import Redis


# =========================================================
# 환경변수
# =========================================================

load_dotenv()

app = Flask(__name__)

app.secret_key = os.getenv(
    "SECRET_KEY",
    "change-this-secret-key"
)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

MAX_ACTIVE = int(
    os.getenv("MAX_ACTIVE", 30)
)

APPLY_TIME_LIMIT = int(
    os.getenv("APPLY_TIME_LIMIT", 180)
)

HEARTBEAT_TIMEOUT = int(
    os.getenv("HEARTBEAT_TIMEOUT", 30)
)

ADMIN_USERNAME = os.getenv(
    "ADMIN_USERNAME",
    "admin"
)

ADMIN_PASSWORD = os.getenv(
    "ADMIN_PASSWORD",
    "admin1234"
)


# =========================================================
# Redis
# =========================================================

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)


# =========================================================
# PostgreSQL
# =========================================================

def get_db():

    if not SUPABASE_DB_URL:
        raise RuntimeError(
            "SUPABASE_DB_URL이 설정되어 있지 않습니다."
        )

    return psycopg2.connect(
        SUPABASE_DB_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
        connect_timeout=10
    )


# =========================================================
# 관리자 인증
# =========================================================

def check_admin(username, password):

    return (
        username == ADMIN_USERNAME
        and password == ADMIN_PASSWORD
    )


def admin_required(function):

    @wraps(function)
    def decorated(*args, **kwargs):

        auth = request.authorization

        if (
            not auth
            or not check_admin(
                auth.username,
                auth.password
            )
        ):

            return Response(
                "관리자 인증이 필요합니다.",
                401,
                {
                    "WWW-Authenticate":
                    'Basic realm="Admin"'
                }
            )

        return function(*args, **kwargs)

    return decorated


# =========================================================
# 좌석
# =========================================================

def get_seat_data():

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

        return cur.fetchone()

    finally:

        cur.close()
        conn.close()


def get_remaining_seats():

    data = get_seat_data()

    if not data:
        return 0

    return max(
        data["total_seats"]
        - data["next_seat"]
        + 1,
        0
    )


# =========================================================
# Redis Queue 유틸
# =========================================================

def acquire_queue_lock():

    """
    여러 요청이 동시에 대기자를 입장시키는 것을
    최대한 방지하기 위한 Redis lock.
    """

    lock_id = str(uuid.uuid4())

    success = redis.set(
        "queue:admit_lock",
        lock_id,
        nx=True,
        ex=5
    )

    if success:
        return lock_id

    return None


def release_queue_lock(lock_id):

    if not lock_id:
        return

    current = redis.get(
        "queue:admit_lock"
    )

    if current == lock_id:
        redis.delete(
            "queue:admit_lock"
        )


def get_active_count():

    count = redis.scard(
        "queue:active"
    )

    return int(count or 0)


def cleanup_active_users():

    """
    신청 페이지에서 heartbeat가 끊겼거나
    제한 시간이 지난 사용자를 제거한다.
    """

    tokens = redis.smembers(
        "queue:active"
    ) or []

    now = int(time.time())

    for token in tokens:

        entry_allowed = redis.get(
            f"entry:{token}"
        )

        heartbeat = redis.get(
            f"heartbeat:{token}"
        )

        entered_at = redis.get(
            f"entered_at:{token}"
        )

        expired = False

        if entry_allowed is None:
            expired = True

        elif heartbeat is None:
            expired = True

        elif entered_at:

            elapsed = (
                now - int(entered_at)
            )

            if elapsed >= APPLY_TIME_LIMIT:
                expired = True

        if expired:

            redis.srem(
                "queue:active",
                token
            )

            redis.delete(
                f"entry:{token}"
            )

            redis.delete(
                f"heartbeat:{token}"
            )

            redis.delete(
                f"entered_at:{token}"
            )


def try_admit_users():

    """
    신청 페이지의 빈 슬롯만큼
    대기열 앞사람을 입장시킨다.
    """

    lock_id = acquire_queue_lock()

    if not lock_id:
        return

    try:

        cleanup_active_users()

        while True:

            active_count = get_active_count()

            if active_count >= MAX_ACTIVE:
                break

            token = redis.lpop(
                "queue:waiting"
            )

            if not token:
                break

            # 대기 페이지에서 이미 사라진 사용자면 건너뜀
            waiting_alive = redis.get(
                f"waiting_alive:{token}"
            )

            if waiting_alive is None:
                continue

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
                f"entered_at:{token}",
                now,
                ex=APPLY_TIME_LIMIT
            )

            redis.set(
                f"heartbeat:{token}",
                now,
                ex=HEARTBEAT_TIMEOUT
            )

            redis.delete(
                f"waiting_alive:{token}"
            )

    finally:

        release_queue_lock(
            lock_id
        )


def remove_active_user(token):

    if not token:
        return

    redis.srem(
        "queue:active",
        token
    )

    redis.delete(
        f"entry:{token}"
    )

    redis.delete(
        f"heartbeat:{token}"
    )

    redis.delete(
        f"entered_at:{token}"
    )

    redis.delete(
        f"waiting_alive:{token}"
    )

    try_admit_users()


def remove_waiting_user(token):

    if not token:
        return

    redis.lrem(
        "queue:waiting",
        0,
        token
    )

    redis.delete(
        f"waiting_alive:{token}"
    )


def clear_ticket_session():

    session.pop(
        "queue_token",
        None
    )

    session.pop(
        "queue_no",
        None
    )

    session.pop(
        "completed",
        None
    )

    session.pop(
        "student_name",
        None
    )

    session.pop(
        "people_count",
        None
    )

    session.pop(
        "seat_numbers",
        None
    )


# =========================================================
# 입력 검증
# =========================================================

def valid_name(name):

    return bool(
        re.fullmatch(
            r"[가-힣a-zA-Z\s]{2,20}",
            name
        )
    )


def valid_phone(phone):

    return bool(
        re.fullmatch(
            r"01[0-9]-\d{3,4}-\d{4}",
            phone
        )
    )


# =========================================================
# 메인 페이지
# =========================================================

@app.route("/")
def index():

    # 메인으로 들어오면 기존 예매 세션 제거
    old_token = session.get(
        "queue_token"
    )

    if old_token:

        remove_waiting_user(
            old_token
        )

        if redis.sismember(
            "queue:active",
            old_token
        ):
            remove_active_user(
                old_token
            )

    clear_ticket_session()

    remaining = get_remaining_seats()

    return render_template(
        "index.html",
        remaining=remaining
    )


# =========================================================
# 예매하기 버튼
# =========================================================

@app.route("/enter")
def enter():

    remaining = get_remaining_seats()

    if remaining <= 0:
        return redirect("/")

    old_token = session.get(
        "queue_token"
    )

    # 이미 신청 페이지에 입장한 사용자
    if old_token:

        if redis.get(
            f"entry:{old_token}"
        ):

            return redirect(
                "/apply"
            )

        waiting_list = redis.lrange(
            "queue:waiting",
            0,
            -1
        )

        if old_token in waiting_list:

            redis.set(
                f"waiting_alive:{old_token}",
                "1",
                ex=60
            )

            return redirect(
                "/waiting"
            )

    # 새로운 대기자
    token = str(
        uuid.uuid4()
    )

    queue_no = redis.incr(
        "queue:last_number"
    )

    session[
        "queue_token"
    ] = token

    session[
        "queue_no"
    ] = int(queue_no)

    redis.set(
        f"queue_number:{token}",
        int(queue_no),
        ex=3600
    )

    redis.set(
        f"waiting_alive:{token}",
        "1",
        ex=60
    )

    redis.rpush(
        "queue:waiting",
        token
    )

    try_admit_users()

    if redis.get(
        f"entry:{token}"
    ):

        return redirect(
            "/apply"
        )

    return redirect(
        "/waiting"
    )


# =========================================================
# 대기 화면
# =========================================================

@app.route("/waiting")
def waiting():

    token = session.get(
        "queue_token"
    )

    if not token:

        return redirect("/")

    if redis.get(
        f"entry:{token}"
    ):

        return redirect(
            "/apply"
        )

    return render_template(
        "waiting.html"
    )


# =========================================================
# 대기 상태
# =========================================================

@app.route("/queue/status")
def queue_status():

    token = session.get(
        "queue_token"
    )

    queue_no = session.get(
        "queue_no"
    )

    if not token:

        return jsonify({
            "valid": False
        }), 401

    # 대기 중인 사용자 생존 확인
    redis.set(
        f"waiting_alive:{token}",
        "1",
        ex=60
    )

    try_admit_users()

    if redis.get(
        f"entry:{token}"
    ):

        return jsonify({
            "valid": True,
            "can_enter": True,
            "queue_no": queue_no,
            "waiting_count": 0,
            "active_count":
                get_active_count()
        })

    waiting_list = redis.lrange(
        "queue:waiting",
        0,
        -1
    )

    try:

        position = (
            waiting_list.index(token)
            + 1
        )

    except ValueError:

        position = 0

    return jsonify({
        "valid": True,
        "can_enter": False,
        "queue_no": queue_no,
        "waiting_count":
            max(position - 1, 0),
        "active_count":
            get_active_count()
    })


# =========================================================
# 신청 화면
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

        return redirect(
            "/waiting"
        )

    entered_at = redis.get(
        f"entered_at:{token}"
    )

    if not entered_at:

        remove_active_user(
            token
        )

        clear_ticket_session()

        return redirect("/")

    elapsed = (
        int(time.time())
        - int(entered_at)
    )

    remaining_time = max(
        APPLY_TIME_LIMIT
        - elapsed,
        0
    )

    if remaining_time <= 0:

        remove_active_user(
            token
        )

        clear_ticket_session()

        return redirect("/")

    redis.set(
        f"heartbeat:{token}",
        int(time.time()),
        ex=HEARTBEAT_TIMEOUT
    )

    remaining = get_remaining_seats()

    if remaining <= 0:

        remove_active_user(
            token
        )

        clear_ticket_session()

        return redirect("/")

    return render_template(
        "apply.html",
        remaining=remaining,
        remaining_time=remaining_time
    )


# =========================================================
# Heartbeat
# =========================================================

@app.route(
    "/heartbeat",
    methods=["POST"]
)
def heartbeat():

    token = session.get(
        "queue_token"
    )

    if not token:

        return jsonify({
            "ok": False,
            "expired": True
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
# 시간 초과 / 나가기
# =========================================================

@app.route(
    "/leave",
    methods=["POST"]
)
def leave():

    token = session.get(
        "queue_token"
    )

    if token:

        remove_waiting_user(
            token
        )

        remove_active_user(
            token
        )

    clear_ticket_session()

    return jsonify({
        "ok": True
    })


# =========================================================
# 신청
# =========================================================

@app.route(
    "/reserve",
    methods=["POST"]
)
def reserve():

    token = session.get(
        "queue_token"
    )

    if not token:
        return redirect("/")

    if not redis.get(
        f"entry:{token}"
    ):

        clear_ticket_session()

        return redirect("/")

    student_name = request.form.get(
        "student_name",
        ""
    ).strip()

    student_phone = request.form.get(
        "student_phone",
        ""
    ).strip()

    parent_name = request.form.get(
        "parent_name",
        ""
    ).strip()

    parent_phone = request.form.get(
        "parent_phone",
        ""
    ).strip()

    try:

        people_count = int(
            request.form.get(
                "people_count",
                "0"
            )
        )

    except ValueError:

        people_count = 0

    # 서버 검증

    if not valid_name(
        student_name
    ):

        return (
            "학생 이름을 "
            "정확히 입력해주세요.",
            400
        )

    if not valid_name(
        parent_name
    ):

        return (
            "보호자 이름을 "
            "정확히 입력해주세요.",
            400
        )

    if not valid_phone(
        student_phone
    ):

        return (
            "학생 전화번호를 "
            "정확히 입력해주세요.",
            400
        )

    if not valid_phone(
        parent_phone
    ):

        return (
            "보호자 전화번호를 "
            "정확히 입력해주세요.",
            400
        )

    if people_count not in (
        1,
        2
    ):

        return (
            "신청 인원이 "
            "올바르지 않습니다.",
            400
        )

    conn = get_db()
    cur = conn.cursor()

    try:

        conn.autocommit = False

        # 좌석 카운터를 잠가
        # 동시 예약 시 중복 배정 방지
        cur.execute("""
            SELECT
                next_seat,
                total_seats
            FROM seat_counter
            WHERE id = 1
            FOR UPDATE
        """)

        seat_data = (
            cur.fetchone()
        )

        if not seat_data:

            conn.rollback()

            return (
                "좌석 정보가 없습니다.",
                500
            )

        next_seat = (
            seat_data[
                "next_seat"
            ]
        )

        total_seats = (
            seat_data[
                "total_seats"
            ]
        )

        last_seat = (
            next_seat
            + people_count
            - 1
        )

        if (
            last_seat
            > total_seats
        ):

            conn.rollback()

            return (
                "남은 좌석이 부족합니다.",
                409
            )

        seat_list = list(
            range(
                next_seat,
                next_seat
                + people_count
            )
        )

        seat_numbers = ", ".join(
            map(
                str,
                seat_list
            )
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

        # 좌석 증가
        cur.execute("""
            UPDATE seat_counter
            SET next_seat =
                next_seat + %s
            WHERE id = 1
        """, (
            people_count,
        ))

        conn.commit()

        # 성공 화면용
        session[
            "completed"
        ] = True

        session[
            "student_name"
        ] = student_name

        session[
            "people_count"
        ] = people_count

        session[
            "seat_numbers"
        ] = seat_numbers

        # 신청 페이지 슬롯 반환
        remove_active_user(
            token
        )

        session.pop(
            "queue_token",
            None
        )

        session.pop(
            "queue_no",
            None
        )

        return redirect(
            "/success"
        )

    except psycopg2.errors.UniqueViolation:

        conn.rollback()

        return (
            "이미 신청된 "
            "학생 전화번호입니다.",
            409
        )

    except Exception as e:

        conn.rollback()

        print(
            "예약 오류:",
            e
        )

        return (
            "신청 처리 중 오류가 "
            "발생했습니다.",
            500
        )

    finally:

        cur.close()
        conn.close()


# =========================================================
# 성공 페이지
# =========================================================

@app.route("/success")
def success():

    if not session.get(
        "completed"
    ):

        return redirect("/")

    student_name = session.get(
        "student_name"
    )

    people_count = session.get(
        "people_count"
    )

    seat_numbers = session.get(
        "seat_numbers"
    )

    # 성공 화면 최초 1회 표시 후
    # 완료 세션 제거
    session.pop(
        "completed",
        None
    )

    session.pop(
        "student_name",
        None
    )

    session.pop(
        "people_count",
        None
    )

    session.pop(
        "seat_numbers",
        None
    )

    return render_template(
        "success.html",
        student_name=student_name,
        people_count=people_count,
        seat_numbers=seat_numbers
    )


# =========================================================
# 관리자
# =========================================================

@app.route("/admin")
@admin_required
def admin():

    cleanup_active_users()

    try_admit_users()

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

    remaining = max(
        seat_data["total_seats"]
        - used_seats,
        0
    )

    active_count = (
        get_active_count()
    )

    waiting_count = int(
        redis.llen(
            "queue:waiting"
        ) or 0
    )

    return render_template(
        "admin.html",
        reservations=reservations,
        total_seats=
            seat_data["total_seats"],
        used_seats=used_seats,
        remaining=remaining,
        active_count=active_count,
        waiting_count=waiting_count,
        max_active=MAX_ACTIVE
    )


# =========================================================
# 엑셀
# =========================================================

@app.route("/admin/excel")
@admin_required
def download_excel():

    conn = get_db()

    try:

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

    finally:

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
        download_name=
            "입시설명회_신청자목록.xlsx",
        mimetype=
            "application/vnd.openxmlformats-"
            "officedocument.spreadsheetml.sheet"
    )


# =========================================================
# Redis 대기열 초기화
# =========================================================

@app.route(
    "/admin/reset-queue",
    methods=["POST"]
)
@admin_required
def reset_queue():

    patterns = [
        "queue:*",
        "entry:*",
        "heartbeat:*",
        "entered_at:*",
        "waiting_alive:*",
        "queue_number:*",
    ]

    for pattern in patterns:

        keys = redis.keys(
            pattern
        ) or []

        for key in keys:

            redis.delete(
                key
            )

    return redirect(
        "/admin"
    )


# =========================================================
# 서버 실행
# =========================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )