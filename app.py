import os
import uuid
from io import BytesIO

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, render_template, request, redirect, session, jsonify, send_file
from upstash_redis import Redis

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-this-secret-key")

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
TOTAL_SEATS = int(os.getenv("TOTAL_SEATS", 100))
ENTRY_LIMIT = int(os.getenv("ENTRY_LIMIT", 30))

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)


def get_db():
    return psycopg2.connect(
        SUPABASE_DB_URL,
        cursor_factory=psycopg2.extras.RealDictCursor
    )


def get_remaining_seats():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT next_seat, total_seats
        FROM seat_counter
        WHERE id = 1
    """)

    data = cur.fetchone()
    cur.close()
    conn.close()

    if not data:
        return 0

    return max(data["total_seats"] - data["next_seat"] + 1, 0)


@app.route("/")
def index():
    if "queue_no" not in session:
        queue_no = redis.incr("queue:last_number")
        token = str(uuid.uuid4())

        session["queue_no"] = queue_no
        session["queue_token"] = token

        redis.hset(f"queue:user:{token}", values={
            "queue_no": queue_no,
            "status": "waiting"
        })

        redis.expire(f"queue:user:{token}", 60 * 30)

        if redis.get("queue:allowed_until") is None:
            redis.set("queue:allowed_until", ENTRY_LIMIT)

    return redirect("/waiting")


@app.route("/waiting")
def waiting():
    return render_template("waiting.html")


@app.route("/queue/status")
def queue_status():
    queue_no = int(session.get("queue_no", 999999999))
    allowed_until = int(redis.get("queue:allowed_until") or 0)

    waiting_count = max(queue_no - allowed_until, 0)

    return jsonify({
        "can_enter": queue_no <= allowed_until,
        "queue_no": queue_no,
        "waiting_count": waiting_count
    })


@app.route("/apply")
def apply():
    queue_no = int(session.get("queue_no", 999999999))
    allowed_until = int(redis.get("queue:allowed_until") or 0)

    if queue_no > allowed_until:
        return redirect("/waiting")

    remaining = get_remaining_seats()

    if remaining <= 0:
        return "모든 좌석이 마감되었습니다."

    return render_template("apply.html", remaining=remaining)


@app.route("/reserve", methods=["POST"])
def reserve():
    queue_no = int(session.get("queue_no", 999999999))
    allowed_until = int(redis.get("queue:allowed_until") or 0)

    if queue_no > allowed_until:
        return redirect("/waiting")

    student_name = request.form["student_name"].strip()
    student_phone = request.form["student_phone"].strip()
    parent_name = request.form["parent_name"].strip()
    parent_phone = request.form["parent_phone"].strip()
    people_count = int(request.form["people_count"])

    if people_count not in [1, 2]:
        return "신청 인원이 올바르지 않습니다."

    conn = get_db()
    cur = conn.cursor()

    try:
        conn.autocommit = False

        cur.execute("""
            SELECT next_seat, total_seats
            FROM seat_counter
            WHERE id = 1
            FOR UPDATE
        """)

        seat_data = cur.fetchone()

        next_seat = seat_data["next_seat"]
        total_seats = seat_data["total_seats"]

        if next_seat + people_count - 1 > total_seats:
            conn.rollback()
            return "남은 좌석이 부족합니다."

        seat_list = list(range(next_seat, next_seat + people_count))
        seat_numbers = ", ".join(map(str, seat_list))

        cur.execute("""
            INSERT INTO reservations (
                student_name,
                student_phone,
                parent_name,
                parent_phone,
                people_count,
                seat_numbers
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            student_name,
            student_phone,
            parent_name,
            parent_phone,
            people_count,
            seat_numbers
        ))

        cur.execute("""
            UPDATE seat_counter
            SET next_seat = next_seat + %s
            WHERE id = 1
        """, (people_count,))

        conn.commit()

        redis.incrby("queue:allowed_until", 1)

        session["completed"] = True
        session["student_name"] = student_name
        session["people_count"] = people_count
        session["seat_numbers"] = seat_numbers

        return redirect("/success")

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return "이미 신청된 학생 전화번호입니다."

    except Exception as e:
        conn.rollback()
        return f"신청 중 오류 발생: {e}"

    finally:
        cur.close()
        conn.close()


@app.route("/success")
def success():
    if not session.get("completed"):
        return redirect("/")

    return render_template(
        "success.html",
        student_name=session.get("student_name"),
        people_count=session.get("people_count"),
        seat_numbers=session.get("seat_numbers")
    )


@app.route("/admin")
def admin():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM reservations
        ORDER BY id ASC
    """)
    reservations = cur.fetchall()

    cur.execute("""
        SELECT next_seat, total_seats
        FROM seat_counter
        WHERE id = 1
    """)
    seat_data = cur.fetchone()

    cur.close()
    conn.close()

    used_seats = seat_data["next_seat"] - 1
    remaining = seat_data["total_seats"] - used_seats

    return render_template(
        "admin.html",
        reservations=reservations,
        used_seats=used_seats,
        remaining=remaining,
        total_seats=seat_data["total_seats"]
    )


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

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="신청자목록")

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="입시설명회_신청자목록.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.route("/admin/reset-queue")
def reset_queue():
    redis.delete("queue:last_number")
    redis.delete("queue:allowed_until")
    redis.set("queue:allowed_until", ENTRY_LIMIT)
    return "대기열 초기화 완료"


@app.route("/admin/open-more/<int:count>")
def open_more(count):
    redis.incrby("queue:allowed_until", count)
    return f"{count}명 추가 입장 허용 완료"


if __name__ == "__main__":
    app.run(debug=True)