import json
import os
import random
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from functools import wraps
from io import BytesIO
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import extensions
from psycopg2.pool import PoolError, ThreadedConnectionPool
from dotenv import load_dotenv
from flask import (Flask, Response, jsonify, redirect, render_template, request, send_file, session,)
from upstash_redis import Redis

load_dotenv()

app = Flask(__name__)

SECRET_KEY = os.getenv("SECRET_KEY")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

if not SECRET_KEY:
    raise RuntimeError("SECRET_KEY가 없습니다.")
if not SUPABASE_DB_URL:
    raise RuntimeError("SUPABASE_DB_URL이 없습니다.")
if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
    raise RuntimeError("Upstash Redis 환경변수가 없습니다.")

app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

MAX_ACTIVE = int(os.getenv("MAX_ACTIVE", "30"))
APPLY_TIME_LIMIT = int(os.getenv("APPLY_TIME_LIMIT", "180"))
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "change-me")

DB_POOL_MIN = max(1, int(os.getenv("DB_POOL_MIN", "1")))
DB_POOL_MAX = max(DB_POOL_MIN, int(os.getenv("DB_POOL_MAX", "8")))
DB_POOL_WAIT_SECONDS = float(os.getenv("DB_POOL_WAIT_SECONDS", "5"))
LOCAL_CACHE_SECONDS = float(os.getenv("LOCAL_CACHE_SECONDS", "1"))
ACTIVE_HEARTBEAT_TTL = int(os.getenv("ACTIVE_HEARTBEAT_TTL", "15"))
ADMIT_TRIGGER_RATE = float(os.getenv("ADMIT_TRIGGER_RATE", "0.02"))

if ACTIVE_HEARTBEAT_TTL < 2:
    raise RuntimeError("ACTIVE_HEARTBEAT_TTL은 2초 이상이어야 합니다.")
if not 0 <= ADMIT_TRIGGER_RATE <= 1:
    raise RuntimeError("ADMIT_TRIGGER_RATE는 0과 1 사이여야 합니다.")

KST = ZoneInfo("Asia/Seoul")

# REDIS
redis = Redis(
    url=UPSTASH_REDIS_REST_URL,
    token=UPSTASH_REDIS_REST_TOKEN,
)

KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "ticket:v3")
WAITING_KEY = f"{KEY_PREFIX}:waiting"
ACTIVE_KEY = f"{KEY_PREFIX}:active"
ACTIVE_DEADLINE_KEY = f"{KEY_PREFIX}:active_deadline"
LAST_NUMBER_KEY = f"{KEY_PREFIX}:last_number"
LOCK_KEY = f"{KEY_PREFIX}:admit_lock"
SALE_CACHE_KEY = f"{KEY_PREFIX}:sale"
SEAT_CACHE_KEY = f"{KEY_PREFIX}:seat"

_db_pool = ThreadedConnectionPool(
    minconn=DB_POOL_MIN,
    maxconn=DB_POOL_MAX,
    dsn=SUPABASE_DB_URL,
    cursor_factory=psycopg2.extras.RealDictCursor,
    connect_timeout=5,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=3,
)
_db_slots = threading.BoundedSemaphore(DB_POOL_MAX)


class DatabaseBusyError(RuntimeError):
    pass


def get_db():
    """새 연결을 만들지 않고 pool에서 연결을 빌린다."""
    if not _db_slots.acquire(timeout=DB_POOL_WAIT_SECONDS):
        raise DatabaseBusyError("DB 연결 풀이 가득 찼습니다.")

    try:
        conn = _db_pool.getconn()
    except (PoolError, Exception):
        _db_slots.release()
        raise

    try:
        if conn.closed:
            _db_pool.putconn(conn, close=True)
            conn = _db_pool.getconn()
        conn.autocommit = False
        return conn
    except Exception:
        try:
            _db_pool.putconn(conn, close=True)
        finally:
            _db_slots.release()
        raise


def release_db(conn, discard=False):
    """연결을 닫지 않고 pool에 반환한다. 망가진 연결만 폐기한다."""
    if conn is None:
        return

    try:
        if not conn.closed and conn.status != extensions.STATUS_READY:
            conn.rollback()
        _db_pool.putconn(conn, close=bool(discard or conn.closed))
    except Exception:
        try:
            _db_pool.putconn(conn, close=True)
        except Exception:
            pass
    finally:
        _db_slots.release()


_local_sale = {"expires": 0.0, "data": None}
_local_seat = {"expires": 0.0, "data": None}


def _set_local(cache, data):
    cache["data"] = data
    cache["expires"] = time.monotonic() + LOCAL_CACHE_SECONDS


def _get_local(cache):
    if cache["data"] is not None and time.monotonic() < cache["expires"]:
        return cache["data"]
    return None


def _redis_json_get(key):
    raw = redis.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _redis_json_set(key, data):
    redis.set(key, json.dumps(data, separators=(",", ":")))


def _load_sale_from_db():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT open_at, close_at FROM ticket_settings WHERE id = 1")
        row = cur.fetchone()
        if not row:
            data = {"open": None, "close": None}
        else:
            data = {
                "open": row["open_at"].timestamp() if row["open_at"] else None,
                "close": row["close_at"].timestamp() if row["close_at"] else None,
            }
        _redis_json_set(SALE_CACHE_KEY, data)
        _set_local(_local_sale, data)
        return data
    finally:
        cur.close()
        release_db(conn)


def get_sale_schedule(force_db=False):
    if not force_db:
        local = _get_local(_local_sale)
        if local is not None:
            return local

        cached = _redis_json_get(SALE_CACHE_KEY)
        if cached is not None:
            _set_local(_local_sale, cached)
            return cached

    return _load_sale_from_db()


def set_sale_schedule_cache(open_at, close_at):
    data = {
        "open": open_at.timestamp() if open_at else None,
        "close": close_at.timestamp() if close_at else None,
    }
    _redis_json_set(SALE_CACHE_KEY, data)
    _set_local(_local_sale, data)


def get_sale_state():
    schedule = get_sale_schedule()
    now_timestamp = time.time()
    open_ts = schedule.get("open")
    close_ts = schedule.get("close")

    if not open_ts:
        state = "not_configured"
        is_open = False
    elif now_timestamp < float(open_ts):
        state = "before"
        is_open = False
    elif close_ts and now_timestamp >= float(close_ts):
        state = "closed"
        is_open = False
    else:
        state = "open"
        is_open = True

    return {
        "state": state,
        "is_open": is_open,
        "open_at": datetime.fromtimestamp(float(open_ts), timezone.utc) if open_ts else None,
        "close_at": datetime.fromtimestamp(float(close_ts), timezone.utc) if close_ts else None,
        "now": datetime.now(timezone.utc),
    }


def _load_seat_from_db():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT next_seat, total_seats FROM seat_counter WHERE id = 1")
        row = cur.fetchone()
        if not row:
            data = {"next": 1, "total": 0}
        else:
            data = {"next": int(row["next_seat"]), "total": int(row["total_seats"])}
        _redis_json_set(SEAT_CACHE_KEY, data)
        _set_local(_local_seat, data)
        return data
    finally:
        cur.close()
        release_db(conn)


def get_seat_snapshot(force_db=False):
    if not force_db:
        local = _get_local(_local_seat)
        if local is not None:
            return local

        cached = _redis_json_get(SEAT_CACHE_KEY)
        if cached is not None:
            _set_local(_local_seat, cached)
            return cached

    return _load_seat_from_db()


def set_seat_snapshot(next_seat, total_seats):
    data = {"next": int(next_seat), "total": int(total_seats)}
    _redis_json_set(SEAT_CACHE_KEY, data)
    _set_local(_local_seat, data)


def get_remaining_seats():
    data = get_seat_snapshot()
    return max(int(data["total"]) - int(data["next"]) + 1, 0)

def dt_local_value(dt):
    if not dt:
        return ""
    return dt.astimezone(KST).strftime("%Y-%m-%dT%H:%M")


def parse_kst_datetime(value):
    if not value:
        return None
    naive = datetime.strptime(value, "%Y-%m-%dT%H:%M")
    return naive.replace(tzinfo=KST).astimezone(timezone.utc)

def now_ts():
    return int(time.time())


def cleanup_expired_active():
    # Lua가 heartbeat ZSET과 실제 마감 HASH를 함께 정리한다.
    admit_users()


def active_count():
    cleanup_expired_active()
    return int(redis.zcard(ACTIVE_KEY) or 0)


def waiting_count():
    return int(redis.zcard(WAITING_KEY) or 0)


def admit_users():
    """만료 정리와 waiting -> active 이동을 Redis 안에서 원자 처리한다."""
    script = """
    local now = tonumber(ARGV[1])
    local max_active = tonumber(ARGV[2])
    local heartbeat_expiry = now + tonumber(ARGV[3])
    local apply_deadline = now + tonumber(ARGV[4])

    local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)
    if #expired > 0 then
        redis.call('ZREM', KEYS[2], unpack(expired))
        redis.call('HDEL', KEYS[3], unpack(expired))
    end

    local slots = max_active - redis.call('ZCARD', KEYS[2])
    if slots <= 0 then return 0 end

    local users = redis.call('ZRANGE', KEYS[1], 0, slots - 1)
    for _, token in ipairs(users) do
        redis.call('ZADD', KEYS[2], heartbeat_expiry, token)
        redis.call('HSET', KEYS[3], token, apply_deadline)
        redis.call('ZREM', KEYS[1], token)
    end
    return #users
    """
    return int(redis.eval(
        script,
        keys=[WAITING_KEY, ACTIVE_KEY, ACTIVE_DEADLINE_KEY],
        args=[str(now_ts()), str(MAX_ACTIVE), str(ACTIVE_HEARTBEAT_TTL), str(APPLY_TIME_LIMIT)],
    ) or 0)


def get_active_expiry(token):
    if not token:
        return None
    heartbeat_expiry = redis.zscore(ACTIVE_KEY, token)
    deadline = redis.hget(ACTIVE_DEADLINE_KEY, token)
    if heartbeat_expiry is None:
        return None
    now = now_ts()
    # 배포 직전 active 사용자는 기존 ZSET score가 실제 마감이었다.
    # HASH가 없으면 그 값을 deadline으로 승격해 진행 중인 신청을 보존한다.
    if deadline is None and int(float(heartbeat_expiry)) > now:
        deadline = int(float(heartbeat_expiry))
        redis.hset(ACTIVE_DEADLINE_KEY, token, deadline)
        redis.zadd(ACTIVE_KEY, {token: now + ACTIVE_HEARTBEAT_TTL})
        heartbeat_expiry = now + ACTIVE_HEARTBEAT_TTL
    if int(float(heartbeat_expiry)) <= now or int(deadline) <= now:
        redis.zrem(ACTIVE_KEY, token)
        redis.hdel(ACTIVE_DEADLINE_KEY, token)
        return None
    return int(deadline)


def refresh_active_heartbeat(token):
    """살아 있는 active 사용자만 TTL을 연장하고 절대 마감은 유지한다."""
    if not token:
        return None
    script = """
    local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
    local deadline = redis.call('HGET', KEYS[2], ARGV[1])
    local now = tonumber(ARGV[2])
    if not score or tonumber(score) <= now then
        redis.call('ZREM', KEYS[1], ARGV[1])
        redis.call('HDEL', KEYS[2], ARGV[1])
        return false
    end
    -- 구버전 active score(180초 마감)를 새 HASH 구조로 무중단 승격한다.
    if not deadline then
        deadline = score
        redis.call('HSET', KEYS[2], ARGV[1], deadline)
    end
    if tonumber(deadline) <= now then
        redis.call('ZREM', KEYS[1], ARGV[1])
        redis.call('HDEL', KEYS[2], ARGV[1])
        return false
    end
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[1])
    return deadline
    """
    result = redis.eval(
        script,
        keys=[ACTIVE_KEY, ACTIVE_DEADLINE_KEY],
        args=[token, str(now_ts()), str(ACTIVE_HEARTBEAT_TTL)],
    )
    return int(result) if result else None


def user_is_active(token):
    return get_active_expiry(token) is not None


def user_is_waiting(token):
    return bool(token and redis.zscore(WAITING_KEY, token) is not None)


def get_waiting_rank(token):
    rank = redis.zrank(WAITING_KEY, token)
    return None if rank is None else int(rank)


def remove_user(token):
    if not token:
        return
    redis.zrem(WAITING_KEY, token)
    redis.zrem(ACTIVE_KEY, token)
    redis.hdel(ACTIVE_DEADLINE_KEY, token)


def reset_queue_data():
    redis.delete(WAITING_KEY)
    redis.delete(ACTIVE_KEY)
    redis.delete(ACTIVE_DEADLINE_KEY)
    redis.delete(LAST_NUMBER_KEY)
    redis.delete(LOCK_KEY)


def clear_session():
    for key in [
        "queue_token",
        "queue_no",
        "completed",
        "student_name",
        "people_count",
        "seat_numbers",
    ]:
        session.pop(key, None)

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
                {"WWW-Authenticate": 'Basic realm="Admin"'},
            )
        return func(*args, **kwargs)

    return wrapper


def valid_name(value):
    return bool(re.fullmatch(r"[가-힣a-zA-Z\s]{2,20}", value or ""))


def valid_phone(value):
    return bool(re.fullmatch(r"01[0-9]-\d{3,4}-\d{4}", value or ""))

def warm_shared_caches():
    """worker 시작 시 DB pool의 연결을 이용해 표시용 cache를 미리 준비한다."""
    try:
        _load_sale_from_db()
        _load_seat_from_db()
    except Exception as error:
        print("CACHE WARMUP WARNING:", repr(error))

warm_shared_caches()

# error / health
@app.errorhandler(DatabaseBusyError)
def handle_db_busy(_error):
    return "현재 접속자가 많습니다. 잠시 후 다시 시도해주세요.", 503


@app.route("/healthz")
def healthz():
    # Render check용
    return jsonify({"ok": True}), 200


# main
@app.route("/")
def index():
    old_token = session.get("queue_token")
    if old_token:
        remove_user(old_token)
    clear_session()

    # 대부분 local/Redis cache에서 끝나므로 DB 연결을 만들지 x
    remaining = get_remaining_seats()
    sale = get_sale_state()

    open_at = sale["open_at"]
    close_at = sale["close_at"]

    return render_template(
        "index.html",
        remaining=remaining,
        sale_state=sale["state"],
        open_at_text=(
            f"{open_at.astimezone(KST).month}월 "
            f"{open_at.astimezone(KST).day}일 "
            f"{open_at.astimezone(KST).hour}시 "
            f"{open_at.astimezone(KST).minute:02d}분 오픈 예정"
            if open_at
            else None
        ),
        close_at_text=(close_at.astimezone(KST).strftime("%m월 %d일 %H시 %M분") if close_at else None),
        open_at_ms=(int(open_at.timestamp() * 1000) if open_at else None),
        close_at_ms=(int(close_at.timestamp() * 1000) if close_at else None),
        server_now_ms=int(sale["now"].timestamp() * 1000),
    )


# enter
@app.route("/enter")
def enter():
    # DB 조회 없음. schedule은 local/Redis cache.
    if not get_sale_state()["is_open"]:
        return redirect("/")
    if get_remaining_seats() <= 0:
        return redirect("/")

    token = session.get("queue_token")
    if token and user_is_active(token):
        return redirect("/apply")
    if token and user_is_waiting(token):
        return redirect("/waiting")

    token = str(uuid.uuid4())
    queue_no = int(redis.incr(LAST_NUMBER_KEY))
    session["queue_token"] = token
    session["queue_no"] = queue_no

    redis.zadd(WAITING_KEY, {token: queue_no})
    admit_users()

    return redirect("/apply" if user_is_active(token) else "/waiting")



# waiting / status
@app.route("/waiting")
def waiting():
    token = session.get("queue_token")
    if not token:
        return redirect("/")

    if not get_sale_state()["is_open"]:
        remove_user(token)
        clear_session()
        return redirect("/")

    if user_is_active(token):
        return redirect("/apply")
    if not user_is_waiting(token):
        return redirect("/")

    return render_template("waiting.html")


@app.route("/queue/status")
def queue_status():
    """
    핫패스. Supabase DB는 호출하지 않는다.
    멀리 있는 대기자는 ZRANK 정도만 확인하고,
    앞쪽 몇 명만 빈 슬롯 입장을 시도한다.
    """
    token = session.get("queue_token")
    queue_no = session.get("queue_no")
    if not token:
        return jsonify({"valid": False}), 401

    if not get_sale_state()["is_open"]:
        remove_user(token)
        clear_session()
        return jsonify({"valid": False, "closed": True})

    expiry = get_active_expiry(token)
    if expiry is not None:
        return jsonify(
            {
                "valid": True,
                "can_enter": True,
                "queue_no": queue_no,
                "waiting_count": 0,
            }
        )

    rank = get_waiting_rank(token)
    if rank is None:
        # 다른 요청이 방금 원자적으로 active로 옮겼을 수 있다.
        expiry = get_active_expiry(token)
        if expiry is not None:
            return jsonify({"valid": True, "can_enter": True, "queue_no": queue_no, "waiting_count": 0})
        return jsonify({"valid": False})

    # 맨 앞 소수만 admit 로직을 시도.
    # 1000명이 동시에 lock을 두드리는 현상을 막음.
    if rank < 3 or random.random() < ADMIT_TRIGGER_RATE:
        admit_users()
        expiry = get_active_expiry(token)
        if expiry is not None:
            return jsonify(
                {
                    "valid": True,
                    "can_enter": True,
                    "queue_no": queue_no,
                    "waiting_count": 0,
                }
            )
        rank = get_waiting_rank(token)
        if rank is None:
            expiry = get_active_expiry(token)
            if expiry is not None:
                return jsonify({"valid": True, "can_enter": True, "queue_no": queue_no, "waiting_count": 0})
            return jsonify({"valid": False})

    return jsonify(
        {
            "valid": True,
            "can_enter": False,
            "queue_no": queue_no,
            "waiting_count": max(rank, 0),
        }
    )


# apply / leave
@app.route("/apply")
def apply():
    token = session.get("queue_token")
    if not token:
        return redirect("/")

    if not get_sale_state()["is_open"]:
        remove_user(token)
        clear_session()
        return redirect("/")

    expiry = get_active_expiry(token)
    if expiry is None:
        if user_is_waiting(token):
            return redirect("/waiting")
        clear_session()
        return redirect("/")

    remaining_time = max(expiry - now_ts(), 0)
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
        remaining_time=remaining_time,
    )


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    deadline = refresh_active_heartbeat(session.get("queue_token"))
    if deadline is None:
        return jsonify({"valid": False}), 401
    return jsonify({"valid": True, "remaining_time": max(deadline - now_ts(), 0)})


@app.route("/leave", methods=["POST"])
def leave():
    token = session.get("queue_token")
    if token:
        remove_user(token)
    clear_session()
    admit_users()
    return jsonify({"ok": True})


@app.route("/reserve", methods=["POST"])
def reserve():
    token = session.get("queue_token")
    if not token or get_active_expiry(token) is None:
        clear_session()
        return redirect("/")

    if not get_sale_state()["is_open"]:
        remove_user(token)
        clear_session()
        return redirect("/")

    student_name = request.form.get("student_name", "").strip()
    student_phone = request.form.get("student_phone", "").strip()
    parent_name = request.form.get("parent_name", "").strip()
    parent_phone = request.form.get("parent_phone", "").strip()

    try:
        people_count = int(request.form.get("people_count", "0"))
    except (TypeError, ValueError):
        people_count = 0

    if not valid_name(student_name):
        return "학생 이름을 정확히 입력해주세요.", 400
    if not valid_phone(student_phone):
        return "학생 전화번호를 정확히 입력해주세요.", 400
    if not valid_name(parent_name):
        return "보호자 이름을 정확히 입력해주세요.", 400
    if not valid_phone(parent_phone):
        return "보호자 전화번호를 정확히 입력해주세요.", 400
    if people_count not in (1, 2):
        return "신청 인원이 올바르지 않습니다.", 400

    conn = None
    cur = None
    try:
        conn = get_db()
        cur = conn.cursor()

        # 최종 좌석 배정만 DB transaction + row lock으로 처리.
        cur.execute(
            """
            SELECT next_seat, total_seats
            FROM seat_counter
            WHERE id = 1
            FOR UPDATE
            """
        )
        seat = cur.fetchone()
        if not seat:
            conn.rollback()
            return "좌석 데이터가 없습니다.", 500

        next_seat = int(seat["next_seat"])
        total_seats = int(seat["total_seats"])
        last_seat = next_seat + people_count - 1

        if last_seat > total_seats:
            conn.rollback()
            set_seat_snapshot(next_seat, total_seats)
            return "남은 좌석이 부족합니다.", 409

        seat_list = list(range(next_seat, next_seat + people_count))
        seat_numbers = ", ".join(map(str, seat_list))

        cur.execute(
            """
            INSERT INTO reservations (
                student_name,
                student_phone,
                parent_name,
                parent_phone,
                people_count,
                seat_numbers
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                student_name,
                student_phone,
                parent_name,
                parent_phone,
                people_count,
                seat_numbers,
            ),
        )

        cur.execute(
            """
            UPDATE seat_counter
            SET next_seat = next_seat + %s
            WHERE id = 1
            """,
            (people_count,),
        )
        conn.commit()

        # DB 커밋 뒤 표시용 cache 동기화
        set_seat_snapshot(next_seat + people_count, total_seats)

    except psycopg2.errors.UniqueViolation:
        if conn:
            conn.rollback()
        return "이미 신청된 학생 전화번호입니다.", 409
    except psycopg2.OperationalError as error:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print("DB OPERATIONAL ERROR:", repr(error))
        return "DB 연결이 일시적으로 불안정합니다. 다시 시도해주세요.", 503
    except Exception as error:
        if conn:
            conn.rollback()
        print("RESERVE ERROR:", repr(error))
        return "신청 처리 중 오류가 발생했습니다.", 500
    finally:
        if cur:
            cur.close()
        if conn:
            release_db(conn, discard=bool(conn.closed))

    session["completed"] = True
    session["student_name"] = student_name
    session["people_count"] = people_count
    session["seat_numbers"] = seat_numbers

    remove_user(token)
    session.pop("queue_token", None)
    session.pop("queue_no", None)
    admit_users()

    return redirect("/success")


# 성공
@app.route("/success")
def success():
    if not session.get("completed"):
        return redirect("/")

    student_name = session.get("student_name")
    people_count = session.get("people_count")
    seat_numbers = session.get("seat_numbers")

    session.pop("completed", None)
    session.pop("student_name", None)
    session.pop("people_count", None)
    session.pop("seat_numbers", None)

    return render_template(
        "success.html",
        student_name=student_name,
        people_count=people_count,
        seat_numbers=seat_numbers,
    )


# admin
@app.route("/admin")
@admin_required
def admin():
    admit_users()

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                id,
                student_name,
                student_phone,
                parent_name,
                parent_phone,
                people_count,
                seat_numbers,
                created_at
            FROM reservations
            ORDER BY id ASC
            """
        )
        reservations = cur.fetchall()

        cur.execute("SELECT next_seat, total_seats FROM seat_counter WHERE id = 1")
        seat = cur.fetchone()

        cur.execute("SELECT open_at, close_at FROM ticket_settings WHERE id = 1")
        settings = cur.fetchone()
    finally:
        cur.close()
        release_db(conn)

    if not seat:
        return "seat_counter가 없습니다.", 500

    total = int(seat["total_seats"])
    used = int(seat["next_seat"]) - 1
    remaining = max(total - used, 0)
    # admin DB 값을 cache와 먼저 맞춘 뒤 상태를 계산.
    set_seat_snapshot(int(seat["next_seat"]), total)
    if settings:
        set_sale_schedule_cache(settings["open_at"], settings["close_at"])
    sale = get_sale_state()

    return render_template(
        "admin.html",
        reservations=reservations,
        total_seats=total,
        used_seats=used,
        remaining=remaining,
        active_count=active_count(),
        waiting_count=waiting_count(),
        max_active=MAX_ACTIVE,
        sale_state=sale["state"],
        open_at_value=dt_local_value(settings["open_at"] if settings else None),
        close_at_value=dt_local_value(settings["close_at"] if settings else None),
    )


@app.route("/admin/schedule", methods=["POST"])
@admin_required
def update_schedule():
    open_value = request.form.get("open_at", "").strip()
    close_value = request.form.get("close_at", "").strip()
    reset_queue = request.form.get("reset_queue") == "1"

    try:
        open_at = parse_kst_datetime(open_value)
        close_at = parse_kst_datetime(close_value)
    except ValueError:
        return "시간 형식이 올바르지 않습니다.", 400

    if not open_at:
        return "오픈 시간을 입력해주세요.", 400
    if close_at and close_at <= open_at:
        return "마감 시간은 오픈 시간보다 뒤여야 합니다.", 400

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO ticket_settings (id, open_at, close_at)
            VALUES (1, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET open_at = EXCLUDED.open_at, close_at = EXCLUDED.close_at
            """,
            (open_at, close_at),
        )
        conn.commit()
    finally:
        cur.close()
        release_db(conn)

    set_sale_schedule_cache(open_at, close_at)
    if reset_queue:
        reset_queue_data()

    return redirect("/admin")


@app.route("/admin/seats", methods=["POST"])
@admin_required
def update_seats():
    try:
        total_seats = int(request.form.get("total_seats", "0"))
    except ValueError:
        return "좌석 수가 올바르지 않습니다.", 400

    if total_seats < 1:
        return "전체 좌석은 1석 이상이어야 합니다.", 400

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT next_seat FROM seat_counter WHERE id = 1")
        row = cur.fetchone()
        if not row:
            return "seat_counter가 없습니다.", 500

        next_seat = int(row["next_seat"])
        used = next_seat - 1
        if total_seats < used:
            return f"이미 {used}석이 사용되어 전체 좌석을 그보다 작게 설정할 수 없습니다.", 400

        cur.execute("UPDATE seat_counter SET total_seats = %s WHERE id = 1", (total_seats,))
        conn.commit()
    finally:
        cur.close()
        release_db(conn)

    set_seat_snapshot(next_seat, total_seats)
    return redirect("/admin")


@app.route("/admin/excel")
@admin_required
def excel():
    conn = get_db()
    try:
        df = pd.read_sql_query(
            """
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
            """,
            conn,
        )
    finally:
        release_db(conn)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="입시설명회신청자")
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="운정고_입시설명회_신청자.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/admin/reset-queue", methods=["POST"])
@admin_required
def reset_queue():
    reset_queue_data()
    return redirect("/admin")


@app.route("/admin/reset-test", methods=["POST"])
@app.route("/admin/reset-reservations", methods=["POST"])
@admin_required
def reset_test():
    reset_queue_data()

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE reservations RESTART IDENTITY")
        cur.execute("UPDATE seat_counter SET next_seat = 1 WHERE id = 1 RETURNING total_seats")
        row = cur.fetchone()
        conn.commit()
        total_seats = int(row["total_seats"]) if row else 0
    finally:
        cur.close()
        release_db(conn)

    set_seat_snapshot(1, total_seats)
    return redirect("/admin")


@app.route("/admin/debug")
@admin_required
def debug_queue():
    cleanup_expired_active()
    return jsonify(
        {
            "max_active": MAX_ACTIVE,
            "active_count": int(redis.zcard(ACTIVE_KEY) or 0),
            "waiting_count": int(redis.zcard(WAITING_KEY) or 0),
            "active_users": redis.zrange(ACTIVE_KEY, 0, -1) or [],
            "active_deadlines": redis.hgetall(ACTIVE_DEADLINE_KEY) or {},
            "waiting_users": redis.zrange(WAITING_KEY, 0, -1) or [],
            "db_pool_min_per_worker": DB_POOL_MIN,
            "db_pool_max_per_worker": DB_POOL_MAX,
        }
    )

# 로컬
if __name__ == "__main__":
    print("=" * 42)
    print(" UJHS STUDENT TICKETING - OPTIMIZED")
    print(" MAX_ACTIVE:", MAX_ACTIVE)
    print(" APPLY_TIME_LIMIT:", APPLY_TIME_LIMIT)
    print(" DB_POOL:", f"{DB_POOL_MIN}~{DB_POOL_MAX} / worker")
    print(" LOCAL_CACHE_SECONDS:", LOCAL_CACHE_SECONDS)
    print("=" * 42)
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
