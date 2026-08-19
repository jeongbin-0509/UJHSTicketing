import os
import re
import time
import uuid
from datetime import datetime, timezone
from functools import wraps
from io import BytesIO
from zoneinfo import ZoneInfo

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
# ENV
# =========================================================

load_dotenv()

app = Flask(__name__)

SECRET_KEY = os.getenv("SECRET_KEY")

if not SECRET_KEY:
    raise RuntimeError(
        "SECRET_KEY가 없습니다."
    )

app.secret_key = SECRET_KEY


SUPABASE_DB_URL = os.getenv(
    "SUPABASE_DB_URL"
)

MAX_ACTIVE = int(
    os.getenv(
        "MAX_ACTIVE",
        30,
    )
)

APPLY_TIME_LIMIT = int(
    os.getenv(
        "APPLY_TIME_LIMIT",
        180,
    )
)

ADMIN_USERNAME = os.getenv(
    "ADMIN_USERNAME",
    "admin",
)

ADMIN_PASSWORD = os.getenv(
    "ADMIN_PASSWORD",
    "change-me",
)

KST = ZoneInfo(
    "Asia/Seoul"
)


# =========================================================
# REDIS
# =========================================================

redis = Redis(
    url=os.getenv(
        "UPSTASH_REDIS_REST_URL"
    ),
    token=os.getenv(
        "UPSTASH_REDIS_REST_TOKEN"
    ),
)


WAITING_KEY = "ticket:waiting"
ACTIVE_KEY = "ticket:active"
LAST_NUMBER_KEY = "ticket:last_number"
LOCK_KEY = "ticket:admit_lock"


# =========================================================
# SALE CACHE
# =========================================================
#
# ticket_settings를 요청마다 Supabase에서
# 가져오지 않도록 2초 동안 메모리 캐시
#
# Gunicorn worker별 캐시지만
# 최대 2초 차이만 생기므로 테스트용으로 충분
# =========================================================

SALE_CACHE_SECONDS = 2

_sale_cache = {
    "time": 0,
    "settings": None,
}


def clear_sale_cache():
    _sale_cache["time"] = 0
    _sale_cache["settings"] = None


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
# ADMIN AUTH
# =========================================================

def admin_required(func):

    @wraps(func)
    def wrapper(
        *args,
        **kwargs,
    ):

        auth = (
            request.authorization
        )

        if (
            not auth
            or auth.username
            != ADMIN_USERNAME
            or auth.password
            != ADMIN_PASSWORD
        ):

            return Response(
                "관리자 인증이 필요합니다.",
                401,
                {
                    "WWW-Authenticate":
                    'Basic realm="Admin"'
                },
            )

        return func(
            *args,
            **kwargs,
        )

    return wrapper


# =========================================================
# DB / 좌석
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
        int(
            data["total_seats"]
        )
        - int(
            data["next_seat"]
        )
        + 1,
        0,
    )


# =========================================================
# 판매시간
# =========================================================

def get_ticket_settings(
    force=False,
):

    current = time.time()

    if (
        not force
        and _sale_cache[
            "settings"
        ] is not None
        and current
        - _sale_cache["time"]
        < SALE_CACHE_SECONDS
    ):

        return _sale_cache[
            "settings"
        ]

    conn = get_db()
    cur = conn.cursor()

    try:

        cur.execute("""
            SELECT
                open_at,
                close_at
            FROM ticket_settings
            WHERE id = 1
        """)

        settings = (
            cur.fetchone()
        )

        _sale_cache[
            "settings"
        ] = settings

        _sale_cache[
            "time"
        ] = current

        return settings

    finally:

        cur.close()
        conn.close()


def get_sale_state(
    force=False,
):

    settings = (
        get_ticket_settings(
            force=force
        )
    )

    now_utc = datetime.now(
        timezone.utc
    )

    if (
        not settings
        or not settings[
            "open_at"
        ]
    ):

        return {
            "state":
                "not_configured",

            "is_open":
                False,

            "open_at":
                None,

            "close_at":
                None,

            "now":
                now_utc,
        }

    open_at = settings[
        "open_at"
    ]

    close_at = settings[
        "close_at"
    ]

    if now_utc < open_at:

        state = "before"
        is_open = False

    elif (
        close_at
        and now_utc
        >= close_at
    ):

        state = "closed"
        is_open = False

    else:

        state = "open"
        is_open = True

    return {
        "state":
            state,

        "is_open":
            is_open,

        "open_at":
            open_at,

        "close_at":
            close_at,

        "now":
            now_utc,
    }


def dt_local_value(dt):

    if not dt:
        return ""

    return (
        dt.astimezone(KST)
        .strftime(
            "%Y-%m-%dT%H:%M"
        )
    )


def parse_kst_datetime(
    value,
):

    if not value:
        return None

    naive = datetime.strptime(
        value,
        "%Y-%m-%dT%H:%M",
    )

    return (
        naive
        .replace(
            tzinfo=KST
        )
        .astimezone(
            timezone.utc
        )
    )


# =========================================================
# REDIS QUEUE
# =========================================================

def now_ts():

    return int(
        time.time()
    )


def cleanup_expired_active():

    redis.zremrangebyscore(
        ACTIVE_KEY,
        0,
        now_ts(),
    )


def active_count():

    cleanup_expired_active()

    return int(
        redis.zcard(
            ACTIVE_KEY
        )
        or 0
    )


def waiting_count():

    return int(
        redis.zcard(
            WAITING_KEY
        )
        or 0
    )


def acquire_lock():

    lock_id = str(
        uuid.uuid4()
    )

    success = redis.set(
        LOCK_KEY,
        lock_id,
        nx=True,
        ex=3,
    )

    if success:
        return lock_id

    return None


def release_lock(
    lock_id,
):

    if not lock_id:
        return

    current = redis.get(
        LOCK_KEY
    )

    if current == lock_id:

        redis.delete(
            LOCK_KEY
        )


# =========================================================
# 빈 슬롯만큼 대기자 입장
# =========================================================
#
# 중요:
# 여기서는 Supabase 조회를 하지 않음.
#
# 판매시간 검사는 /enter /waiting
# /queue/status /apply /reserve에서 처리
# =========================================================

def admit_users():

    lock_id = acquire_lock()

    if not lock_id:
        return

    try:

        cleanup_expired_active()

        count = int(
            redis.zcard(
                ACTIVE_KEY
            )
            or 0
        )

        slots = (
            MAX_ACTIVE
            - count
        )

        if slots <= 0:
            return

        users = redis.zrange(
            WAITING_KEY,
            0,
            slots - 1,
        ) or []

        if not users:
            return

        expire_at = (
            now_ts()
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


def user_is_active(
    token,
):

    if not token:
        return False

    cleanup_expired_active()

    return (
        redis.zscore(
            ACTIVE_KEY,
            token,
        )
        is not None
    )


def user_is_waiting(
    token,
):

    if not token:
        return False

    return (
        redis.zscore(
            WAITING_KEY,
            token,
        )
        is not None
    )


def get_user_remaining_time(
    token,
):

    score = redis.zscore(
        ACTIVE_KEY,
        token,
    )

    if score is None:
        return 0

    return max(
        int(float(score))
        - now_ts(),
        0,
    )


def get_waiting_position(
    token,
):

    rank = redis.zrank(
        WAITING_KEY,
        token,
    )

    if rank is None:
        return None

    return (
        int(rank)
        + 1
    )


def remove_user(
    token,
):

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


def reset_queue_data():

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


def clear_session():

    for key in [
        "queue_token",
        "queue_no",
        "completed",
        "student_number",
        "seat_number",
    ]:

        session.pop(
            key,
            None,
        )


# =========================================================
# VALIDATION
# =========================================================

def valid_student_number(
    value,
):

    return bool(
        re.fullmatch(
            r"\d{5}",
            value,
        )
    )


# =========================================================
# MAIN
# =========================================================

@app.route("/")
def index():

    old_token = (
        session.get(
            "queue_token"
        )
    )

    if old_token:

        remove_user(
            old_token
        )

        admit_users()

    clear_session()

    # 메인 진입 시에는
    # 좌석 + 판매시간 조회
    remaining = (
        get_remaining_seats()
    )

    sale = get_sale_state()

    return render_template(
        "index.html",

        remaining=
            remaining,

        sale_state=
            sale["state"],

        open_at_text=(
            f"{sale['open_at'].astimezone(KST).month}월 "
            f"{sale['open_at'].astimezone(KST).day}일 "
            f"{sale['open_at'].astimezone(KST).hour}시 "
            f"{sale['open_at'].astimezone(KST).minute:02d}분 "
            f"오픈 예정"
            if sale["open_at"]
            else None
        ),

        close_at_text=(
            sale[
                "close_at"
            ]
            .astimezone(KST)
            .strftime(
                "%Y-%m-%d %H:%M"
            )
            if sale["close_at"]
            else None
        ),

        open_at_ms=(
            int(
                sale[
                    "open_at"
                ].timestamp()
                * 1000
            )
            if sale["open_at"]
            else None
        ),

        close_at_ms=(
            int(
                sale[
                    "close_at"
                ].timestamp()
                * 1000
            )
            if sale["close_at"]
            else None
        ),

        server_now_ms=int(
            sale[
                "now"
            ].timestamp()
            * 1000
        ),
    )


# =========================================================
# ENTER
# =========================================================

@app.route("/enter")
def enter():

    token = session.get("queue_token")

    # 이미 신청창에 들어가 있는 경우
    if token and user_is_active(token):
        return redirect("/apply")

    # 이미 대기 중인 경우
    if token and user_is_waiting(token):
        return redirect("/waiting")

    # 신규 사용자
    token = str(uuid.uuid4())

    queue_no = int(
        redis.incr(
            LAST_NUMBER_KEY
        )
    )

    session["queue_token"] = token
    session["queue_no"] = queue_no

    # 대기열 등록
    redis.zadd(
        WAITING_KEY,
        {
            token: queue_no
        }
    )

    # 빈 슬롯 있으면 즉시 입장
    admit_users()

    if user_is_active(token):
        return redirect("/apply")

    return redirect("/waiting")


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

    sale = get_sale_state()

    if not sale["is_open"]:

        remove_user(
            token
        )

        clear_session()

        return redirect("/")

    if user_is_active(
        token
    ):

        return redirect(
            "/apply"
        )

    if not user_is_waiting(
        token
    ):

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
            "valid": False,
        }), 401

    # 캐시된 판매시간 사용
    sale = get_sale_state()

    if not sale[
        "is_open"
    ]:

        remove_user(
            token
        )

        clear_session()

        return jsonify({
            "valid":
                False,

            "closed":
                True,
        })

    admit_users()

    if user_is_active(
        token
    ):

        return jsonify({
            "valid":
                True,

            "can_enter":
                True,

            "queue_no":
                queue_no,
        })

    position = (
        get_waiting_position(
            token
        )
    )

    if position is None:

        return jsonify({
            "valid": False,
        })

    return jsonify({
        "valid":
            True,

        "can_enter":
            False,

        "queue_no":
            queue_no,

        "waiting_count":
            max(
                position - 1,
                0,
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

    sale = get_sale_state()

    if not sale["is_open"]:

        remove_user(
            token
        )

        clear_session()

        return redirect("/")

    if not user_is_active(
        token
    ):

        if user_is_waiting(
            token
        ):

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

        remove_user(
            token
        )

        clear_session()

        admit_users()

        return redirect("/")

    remaining = (
        get_remaining_seats()
    )

    if remaining <= 0:

        remove_user(
            token
        )

        clear_session()

        admit_users()

        return redirect("/")

    return render_template(
        "apply.html",

        remaining=
            remaining,

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

        remove_user(
            token
        )

    clear_session()

    admit_users()

    return jsonify({
        "ok": True,
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
        or not user_is_active(
            token
        )
    ):

        clear_session()

        return redirect("/")

    sale = get_sale_state()

    if not sale["is_open"]:

        remove_user(
            token
        )

        clear_session()

        return redirect("/")

    student_number = (
        request.form.get(
            "student_number",
            "",
        )
        .strip()
    )

    if not valid_student_number(
        student_number
    ):

        return (
            "학번은 숫자 5자리로 입력해주세요.",
            400,
        )

    conn = get_db()
    cur = conn.cursor()

    try:

        conn.autocommit = False

        # 좌석 row lock
        cur.execute("""
            SELECT
                next_seat,
                total_seats
            FROM seat_counter
            WHERE id = 1
            FOR UPDATE
        """)

        seat = (
            cur.fetchone()
        )

        if not seat:

            conn.rollback()

            return (
                "좌석 데이터가 없습니다.",
                500,
            )

        next_seat = int(
            seat[
                "next_seat"
            ]
        )

        total_seats = int(
            seat[
                "total_seats"
            ]
        )

        if (
            next_seat
            > total_seats
        ):

            conn.rollback()

            return (
                "남은 좌석이 없습니다.",
                409,
            )

        # 신청 저장
        cur.execute("""
            INSERT INTO
                test_reservations
            (
                student_number,
                seat_number
            )
            VALUES (
                %s,
                %s
            )
        """, (
            student_number,
            next_seat,
        ))

        # 다음 좌석
        cur.execute("""
            UPDATE seat_counter
            SET next_seat =
                next_seat + 1
            WHERE id = 1
        """)

        conn.commit()

        seat_number = (
            next_seat
        )

    except (
        psycopg2.errors
        .UniqueViolation
    ):

        conn.rollback()

        return (
            "이미 신청된 학번입니다.",
            409,
        )

    except Exception as error:

        conn.rollback()

        print(
            "RESERVE ERROR:",
            repr(error),
        )

        return (
            "신청 처리 중 오류가 발생했습니다.",
            500,
        )

    finally:

        cur.close()
        conn.close()

    session[
        "completed"
    ] = True

    session[
        "student_number"
    ] = student_number

    session[
        "seat_number"
    ] = seat_number

    # 슬롯 반환
    remove_user(
        token
    )

    session.pop(
        "queue_token",
        None,
    )

    session.pop(
        "queue_no",
        None,
    )

    # 다음 사람 즉시 입장
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

    student_number = (
        session.get(
            "student_number"
        )
    )

    seat_number = (
        session.get(
            "seat_number"
        )
    )

    # 성공 페이지 최초 1회만 표시
    session.pop(
        "completed",
        None,
    )

    session.pop(
        "student_number",
        None,
    )

    session.pop(
        "seat_number",
        None,
    )

    return render_template(
        "success.html",

        student_number=
            student_number,

        seat_number=
            seat_number,
    )


# =========================================================
# ADMIN
# =========================================================

@app.route("/admin")
@admin_required
def admin():

    # 빈 슬롯 있으면
    # 관리자 접속 시에도 정리
    admit_users()

    conn = get_db()
    cur = conn.cursor()

    try:

        cur.execute("""
            SELECT
                id,
                student_number,
                seat_number,
                created_at
            FROM test_reservations
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

        seat = (
            cur.fetchone()
        )

        cur.execute("""
            SELECT
                open_at,
                close_at
            FROM ticket_settings
            WHERE id = 1
        """)

        settings = (
            cur.fetchone()
        )

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
        int(
            seat["next_seat"]
        )
        - 1
    )

    remaining = max(
        total - used,
        0,
    )

    sale = get_sale_state()

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

        sale_state=
            sale["state"],

        open_at_value=
            dt_local_value(
                settings[
                    "open_at"
                ]
                if settings
                else None
            ),

        close_at_value=
            dt_local_value(
                settings[
                    "close_at"
                ]
                if settings
                else None
            ),
    )


# =========================================================
# ADMIN - SCHEDULE
# =========================================================

@app.route(
    "/admin/schedule",
    methods=["POST"],
)
@admin_required
def update_schedule():

    open_value = (
        request.form.get(
            "open_at",
            "",
        )
        .strip()
    )

    close_value = (
        request.form.get(
            "close_at",
            "",
        )
        .strip()
    )

    reset_queue = (
        request.form.get(
            "reset_queue"
        )
        == "1"
    )

    try:

        open_at = (
            parse_kst_datetime(
                open_value
            )
        )

        close_at = (
            parse_kst_datetime(
                close_value
            )
        )

    except ValueError:

        return (
            "시간 형식이 올바르지 않습니다.",
            400,
        )

    if not open_at:

        return (
            "오픈 시간을 입력해주세요.",
            400,
        )

    if (
        close_at
        and close_at
        <= open_at
    ):

        return (
            "마감 시간은 오픈 시간보다 뒤여야 합니다.",
            400,
        )

    conn = get_db()
    cur = conn.cursor()

    try:

        cur.execute("""
            INSERT INTO
                ticket_settings
            (
                id,
                open_at,
                close_at
            )
            VALUES (
                1,
                %s,
                %s
            )

            ON CONFLICT (id)

            DO UPDATE SET
                open_at =
                    EXCLUDED.open_at,

                close_at =
                    EXCLUDED.close_at
        """, (
            open_at,
            close_at,
        ))

        conn.commit()

    finally:

        cur.close()
        conn.close()

    # 중요
    # 관리자 시간 변경 즉시 캐시 제거
    clear_sale_cache()

    if reset_queue:
        reset_queue_data()

    return redirect(
        "/admin"
    )


# =========================================================
# ADMIN - SEATS
# =========================================================

@app.route(
    "/admin/seats",
    methods=["POST"],
)
@admin_required
def update_seats():

    try:

        total_seats = int(
            request.form.get(
                "total_seats",
                "0",
            )
        )

    except ValueError:

        return (
            "좌석 수가 올바르지 않습니다.",
            400,
        )

    if total_seats < 1:

        return (
            "전체 좌석은 1석 이상이어야 합니다.",
            400,
        )

    conn = get_db()
    cur = conn.cursor()

    try:

        cur.execute("""
            SELECT
                next_seat
            FROM seat_counter
            WHERE id = 1
        """)

        row = (
            cur.fetchone()
        )

        if not row:

            return (
                "seat_counter가 없습니다.",
                500,
            )

        used = (
            int(
                row[
                    "next_seat"
                ]
            )
            - 1
        )

        if (
            total_seats
            < used
        ):

            return (
                f"이미 {used}석이 사용되어 "
                "전체 좌석을 그보다 작게 "
                "설정할 수 없습니다.",
                400,
            )

        cur.execute("""
            UPDATE seat_counter
            SET total_seats = %s
            WHERE id = 1
        """, (
            total_seats,
        ))

        conn.commit()

    finally:

        cur.close()
        conn.close()

    return redirect(
        "/admin"
    )


# =========================================================
# EXCEL
# =========================================================

@app.route(
    "/admin/excel"
)
@admin_required
def excel():

    conn = get_db()

    try:

        df = pd.read_sql_query("""
            SELECT

                id AS 번호,

                student_number
                    AS 학번,

                seat_number
                    AS 좌석,

                created_at
                    AS 신청시간

            FROM test_reservations

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
            sheet_name=
                "테스트신청자",
        )

    output.seek(0)

    return send_file(
        output,

        as_attachment=True,

        download_name=
            "학생_티켓팅_테스트.xlsx",

        mimetype=(
            "application/"
            "vnd.openxmlformats-"
            "officedocument."
            "spreadsheetml.sheet"
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

    reset_queue_data()

    return redirect(
        "/admin"
    )


# =========================================================
# RESET TEST
# =========================================================

@app.route(
    "/admin/reset-test",
    methods=["POST"],
)
@admin_required
def reset_test():

    reset_queue_data()

    conn = get_db()
    cur = conn.cursor()

    try:

        cur.execute("""
            TRUNCATE TABLE
                test_reservations
            RESTART IDENTITY
        """)

        cur.execute("""
            UPDATE seat_counter
            SET next_seat = 1
            WHERE id = 1
        """)

        conn.commit()

    finally:

        cur.close()
        conn.close()

    return redirect(
        "/admin"
    )


# =========================================================
# DEBUG QUEUE
# =========================================================

@app.route(
    "/admin/debug"
)
@admin_required
def debug_queue():

    cleanup_expired_active()

    return jsonify({
        "max_active":
            MAX_ACTIVE,

        "active_count":
            int(
                redis.zcard(
                    ACTIVE_KEY
                )
                or 0
            ),

        "waiting_count":
            int(
                redis.zcard(
                    WAITING_KEY
                )
                or 0
            ),

        "active_users":
            redis.zrange(
                ACTIVE_KEY,
                0,
                -1,
            )
            or [],

        "waiting_users":
            redis.zrange(
                WAITING_KEY,
                0,
                -1,
            )
            or [],
    })


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    print()
    print(
        "=============================="
    )

    print(
        " UJHS STUDENT TICKETING TEST"
    )

    print(
        " MAX_ACTIVE:",
        MAX_ACTIVE,
    )

    print(
        " APPLY_TIME_LIMIT:",
        APPLY_TIME_LIMIT,
    )

    print(
        " SALE CACHE:",
        SALE_CACHE_SECONDS,
        "seconds",
    )

    print(
        "=============================="
    )

    print()

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True,
    )