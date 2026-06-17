# 그냥 db만드는용 (테스트 버전임)

import sqlite3

TOTAL_SEATS = 100  # 총 좌석 수

conn = sqlite3.connect("ticketing.db")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_name TEXT NOT NULL,
    student_phone TEXT NOT NULL,
    parent_name TEXT NOT NULL,
    parent_phone TEXT NOT NULL,
    people_count INTEGER NOT NULL,
    seat_numbers TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS seat_counter (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    next_seat INTEGER NOT NULL,
    total_seats INTEGER NOT NULL
)
""")

cur.execute("""
INSERT OR IGNORE INTO seat_counter (id, next_seat, total_seats)
VALUES (1, 1, ?)
""", (TOTAL_SEATS,))

conn.commit()
conn.close()

print("DB 생성 완료")