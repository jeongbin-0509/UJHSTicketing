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
# CONFIG
# =========================================================

load_dotenv()

app = Flask(__name__)

SECRET_KEY = os.getenv("SECRET_KEY")

if not SECRET_KEY:
    raise RuntimeError("SECRET_KEY가 없습니다.")

app.secret_key = SECRET_KEY

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

MAX_ACTIVE = int(os.getenv("MAX_ACTIVE", 30))

# 신청창을 잡고 있을 수 있는 시간
APPLY_TIME_LIMIT = int(
    os.getenv("APPLY_TIME_LIMIT", 180)
)

ADMIN_USERNAME = os.getenv(
    "ADMIN_USERNAME",
    "admin"
)

ADMIN_PASSWORD = os.getenv(
    "ADMIN_PASSWORD",
    "change-me"
)


# =========================================================
# REDIS
# =========================================================

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN"),
)


WAITING_KEY = "ticket:waiting"
ACTIVE_KEY = "ticket:active"
LAST_NUMBER_KEY = "ticket:last_number"
LOCK_KEY = "ticket:admit_lock"


# =========================================================
# DATABASE
# =========================================================

def get_db():

    if not SUPABASE_DB_URL:
        raise RuntimeError(
            "SUPABASE_DB_URL이 없습니다."
        )

    return psycopg2.connect(
        SUPABASE_DB_URL,
        cursor_factory=
            psycopg2.extras.RealDictCursor,
        connect_timeout=10,
    )


# =========================================================
# ADMIN
# =========================================================

def admin_required(func):

    @wraps(func)
    def wrapper(*args, **kwargs):

        auth = request.authorization

        if (
            not auth
            or auth.username != ADMIN_USERNAME
            or auth.password != ADMIN_PASSWORD
        ):
            return Response(
                "관리자 인증이 필요합니다.",
                401,
                {
                    "WWW-Authenticate":
                    'Basic realm="Admin"'
                },
            )

        return func(*args, **kwargs)

    return wrapper


# =========================================================
# SEATS
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
        0,
    )


# =========================================================
# QUEUE
# =========================================================

def now():
    return int(time.time())


def cleanup_expired_active():

    """
    신청 제한시간이 끝난 사용자들을
    한 번에 active에서 제거한다.
    """

    redis.zremrangebyscore(
        ACTIVE_KEY,
        0,
        now(),
    )


def active_count():

    cleanup_expired_active()

    return int(
        redis.zcard(ACTIVE_KEY) or 0
    )


def waiting_count():

    return int(
        redis.zcard(WAITING_KEY) or 0
    )


def acquire_lock():

    lock_id = str(uuid.uuid4())

    success = redis.set(
        LOCK_KEY,
        lock_id,
        nx=True,
        ex=3,
    )

    if success:
        return lock_id

    return None


def release_lock(lock_id):

    if not lock_id:
        return

    current = redis.get(
        LOCK_KEY
    )

    if current == lock_id:
        redis.delete(
            LOCK_KEY
        )


def admit_users():

    """
    빈 신청 슬롯만큼
    대기열 앞사람을 입장시킨다.
    """

    lock_id = acquire_lock()

    if not lock_id:
        return

    try:

        cleanup_expired_active()

        count = int(
            redis.zcard(
                ACTIVE_KEY
            ) or 0
        )

        slots = MAX_ACTIVE - count

        if slots <= 0:
            return

        # 대기열 앞에서 slots명
        users = redis.zrange(
            WAITING_KEY,
            0,
            slots - 1,
        ) or []

        if not users:
            return

        expire_at = (
            now()
            + APPLY_TIME_LIMIT
        )

        for token in users:

            redis.zrem(
                WAITING_KEY,
                token,
            )

            redis.zadd(
                ACTIVE_KEY,
                {
                    token:
                        expire_at
                },
            )

    finally:

        release_lock(
            lock_id
        )


def user_is_active(token):

    if not token:
        return False

    cleanup_expired_active()

    score = redis.zscore(
        ACTIVE_KEY,
        token,
    )

    return score is not None


def get_user_remaining_time(token):

    score = redis.zscore(
        ACTIVE_KEY,
        token,
    )

    if score is None:
        return 0

    return max(
        int(float(score))
        - now(),
        0,
    )


def user_is_waiting(token):

    if not token:
        return False

    return (
        redis.zscore(
            WAITING_KEY,
            token,
        )
        is not None
    )


def remove_user(token):

    if not token:
        return

    redis.zrem(
        WAITING_KEY,
        token,
    )

    redis.zrem(
        ACTIVE_KEY,
        token,
    )


def get_waiting_position(token):

    rank = redis.zrank(
        WAITING_KEY,
        token,
    )

    if rank is None:
        return None

    return int(rank) + 1


def clear_session():

    keys = [
        "queue_token",
        "queue_no",
        "completed",
        "student_name",
        "people_count",
        "seat_numbers",
    ]

    for key in keys:
        session.pop(
            key,
            None
        )


# =========================================================
# VALIDATION
# =========================================================

def valid_name(value):

    return bool(
        re.fullmatch(
            r"[가-힣A-Za-z\s]{2,20}",
            value,
        )
    )


def valid_phone(value):

    return bool(
        re.fullmatch(
            r"01\d-\d{3,4}-\d{4}",
            value,
        )
    )


# =========================================================
# HOME
# =========================================================

@app.route("/")
def index():

    old_token = session.get(
        "queue_token"
    )

    if old_token:

        remove_user(
            old_token
        )

        # 슬롯이 생겼을 수 있음
        admit_users()

    clear_session()

    remaining = get_remaining_seats()

    return render_template(
        "index.html",
        remaining=remaining,
    )


# =========================================================
# ENTER QUEUE
# =========================================================

@app.route("/enter")
def enter():

    if get_remaining_seats() <= 0:
        return redirect("/")

    token = session.get(
        "queue_token"
    )

    # 이미 신청창 입장 상태
    if (
        token
        and user_is_active(token)
    ):
        return redirect(
            "/apply"
        )

    # 이미 대기 중
    if (
        token
        and user_is_waiting(token)
    ):
        return redirect(
            "/waiting"
        )

    # 신규 사용자
    token = str(
        uuid.uuid4()
    )

    queue_no = int(
        redis.incr(
            LAST_NUMBER_KEY
        )
    )

    session["queue_token"] = token
    session["queue_no"] = queue_no

    # queue_no 자체를 score로 사용
    redis.zadd(
        WAITING_KEY,
        {
            token: queue_no
        },
    )

    admit_users()

    if user_is_active(token):

        return redirect(
            "/apply"
        )

    return redirect(
        "/waiting"
    )


# =========================================================
# WAITING
# =========================================================

@app.route("/waiting")
def waiting():

    token = session.get(
        "queue_token"
    )

    if not token:
        return redirect("/")

    if user_is_active(token):
        return redirect(
            "/apply"
        )

    if not user_is_waiting(token):
        return redirect("/")

    return render_template(
        "waiting.html"
    )


# =========================================================
# QUEUE STATUS
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

    # 만료 슬롯 정리 + 다음 사람 입장
    admit_users()

    if user_is_active(token):

        return jsonify({
            "valid": True,
            "can_enter": True,
            "queue_no": queue_no,
        })

    position = get_waiting_position(
        token
    )

    if position is None:

        return jsonify({
            "valid": False
        })

    return jsonify({
        "valid": True,
        "can_enter": False,
        "queue_no": queue_no,

        # 내 앞 사람 수
        "waiting_count":
            max(
                position - 1,
                0
            ),
    })


# =========================================================
# APPLY
# =========================================================

@app.route("/apply")
def apply():

    token = session.get(
        "queue_token"
    )

    if not token:
        return redirect("/")

    if not user_is_active(token):

        if user_is_waiting(token):
            return redirect(
                "/waiting"
            )

        clear_session()

        return redirect("/")

    remaining_time = (
        get_user_remaining_time(
            token
        )
    )

    if remaining_time <= 0:

        remove_user(token)
        clear_session()
        admit_users()

        return redirect("/")

    remaining = get_remaining_seats()

    if remaining <= 0:

        remove_user(token)
        clear_session()
        admit_users()

        return redirect("/")

    return render_template(
        "apply.html",
        remaining=remaining,
        remaining_time=
            remaining_time,
    )


# =========================================================
# LEAVE
# =========================================================

@app.route(
    "/leave",
    methods=["POST"],
)
def leave():

    token = session.get(
        "queue_token"
    )

    if token:
        remove_user(token)

    clear_session()

    # 다음 대기자 즉시 입장
    admit_users()

    return jsonify({
        "ok": True
    })


# =========================================================
# RESERVE
# =========================================================

@app.route(
    "/reserve",
    methods=["POST"],
)
def reserve():

    token = session.get(
        "queue_token"
    )

    if (
        not token
        or not user_is_active(token)
    ):
        clear_session()

        return redirect("/")

    student_name = (
        request.form.get(
            "student_name",
            ""
        ).strip()
    )

    student_phone = (
        request.form.get(
            "student_phone",
            ""
        ).strip()
    )

    parent_name = (
        request.form.get(
            "parent_name",
            ""
        ).strip()
    )

    parent_phone = (
        request.form.get(
            "parent_phone",
            ""
        ).strip()
    )

    try:

        people_count = int(
            request.form.get(
                "people_count",
                0,
            )
        )

    except (ValueError, TypeError):

        people_count = 0


    if not valid_name(
        student_name
    ):
        return (
            "학생 이름을 확인해주세요.",
            400,
        )

    if not valid_name(
        parent_name
    ):
        return (
            "보호자 이름을 확인해주세요.",
            400,
        )

    if not valid_phone(
        student_phone
    ):
        return (
            "학생 전화번호를 확인해주세요.",
            400,
        )

    if not valid_phone(
        parent_phone
    ):
        return (
            "보호자 전화번호를 확인해주세요.",
            400,
        )

    if people_count not in (
        1,
        2,
    ):
        return (
            "신청 인원을 확인해주세요.",
            400,
        )


    conn = get_db()
    cur = conn.cursor()

    try:

        conn.autocommit = False

        # -----------------------------------------
        # 좌석 row lock
        # -----------------------------------------

        cur.execute("""
            SELECT
                next_seat,
                total_seats
            FROM seat_counter
            WHERE id = 1
            FOR UPDATE
        """)

        seat = cur.fetchone()

        if not seat:

            conn.rollback()

            return (
                "좌석 데이터가 없습니다.",
                500,
            )


        next_seat = int(
            seat["next_seat"]
        )

        total_seats = int(
            seat["total_seats"]
        )


        if (
            next_seat
            + people_count
            - 1
            > total_seats
        ):

            conn.rollback()

            return (
                "남은 좌석이 부족합니다.",
                409,
            )


        seats = list(
            range(
                next_seat,
                next_seat
                + people_count,
            )
        )

        seat_numbers = ", ".join(
            map(
                str,
                seats
            )
        )


        # -----------------------------------------
        # 예약 저장
        # -----------------------------------------

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
            seat_numbers,
        ))


        # -----------------------------------------
        # 다음 좌석
        # -----------------------------------------

        cur.execute("""
            UPDATE seat_counter
            SET next_seat =
                next_seat + %s
            WHERE id = 1
        """, (
            people_count,
        ))


        conn.commit()


    except psycopg2.errors.UniqueViolation:

        conn.rollback()

        return (
            "이미 신청된 학생 전화번호입니다.",
            409,
        )


    except Exception as error:

        conn.rollback()

        print(
            "RESERVE ERROR:",
            repr(error)
        )

        return (
            "신청 처리 중 오류가 발생했습니다.",
            500,
        )


    finally:

        cur.close()
        conn.close()


    # ---------------------------------------------
    # 성공
    # ---------------------------------------------

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


    remove_user(token)

    session.pop(
        "queue_token",
        None
    )

    session.pop(
        "queue_no",
        None
    )

    # 슬롯 반환 후 다음 사람 입장
    admit_users()

    return redirect(
        "/success"
    )


# =========================================================
# SUCCESS
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

    # 한 번만 볼 수 있도록 제거
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
        seat_numbers=seat_numbers,
    )


# =========================================================
# ADMIN
# =========================================================

@app.route("/admin")
@admin_required
def admin():

    admit_users()

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

        seat = cur.fetchone()

    finally:

        cur.close()
        conn.close()


    if not seat:

        return (
            "seat_counter가 없습니다.",
            500,
        )


    total = int(
        seat["total_seats"]
    )

    used = (
        int(seat["next_seat"])
        - 1
    )

    remaining = max(
        total - used,
        0,
    )


    return render_template(
        "admin.html",

        reservations=
            reservations,

        total_seats=
            total,

        used_seats=
            used,

        remaining=
            remaining,

        active_count=
            active_count(),

        waiting_count=
            waiting_count(),

        max_active=
            MAX_ACTIVE,
    )


# =========================================================
# EXCEL
# =========================================================

@app.route("/admin/excel")
@admin_required
def excel():

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
            ORDER BY id
        """, conn)

    finally:

        conn.close()


    output = BytesIO()

    with pd.ExcelWriter(
        output,
        engine="openpyxl",
    ) as writer:

        df.to_excel(
            writer,
            index=False,
            sheet_name="신청자목록",
        )


    output.seek(0)


    return send_file(
        output,
        as_attachment=True,
        download_name=
            "입시설명회_신청자목록.xlsx",
        mimetype=(
            "application/vnd.openxmlformats-"
            "officedocument.spreadsheetml.sheet"
        ),
    )


# =========================================================
# RESET QUEUE
# =========================================================

@app.route(
    "/admin/reset-queue",
    methods=["POST"],
)
@admin_required
def reset_queue():

    redis.delete(
        WAITING_KEY
    )

    redis.delete(
        ACTIVE_KEY
    )

    redis.delete(
        LAST_NUMBER_KEY
    )

    redis.delete(
        LOCK_KEY
    )

    return redirect(
        "/admin"
    )


# =========================================================
# DEBUG
# =========================================================

@app.route("/admin/debug")
@admin_required
def debug():

    cleanup_expired_active()

    return jsonify({

        "MAX_ACTIVE":
            MAX_ACTIVE,

        "active":
            redis.zcard(
                ACTIVE_KEY
            ) or 0,

        "waiting":
            redis.zcard(
                WAITING_KEY
            ) or 0,

        "active_users":
            redis.zrange(
                ACTIVE_KEY,
                0,
                -1,
            ) or [],

        "waiting_users":
            redis.zrange(
                WAITING_KEY,
                0,
                -1,
            ) or [],
    })


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    print()
    print("==============================")
    print(" UJHS TICKETING")
    print("------------------------------")
    print("MAX_ACTIVE:", MAX_ACTIVE)
    print(
        "APPLY_TIME_LIMIT:",
        APPLY_TIME_LIMIT
    )
    print("==============================")
    print()

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True,
    )