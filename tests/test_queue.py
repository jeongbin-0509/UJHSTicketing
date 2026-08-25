def add_waiters(app, count, start=0):
    tokens = [f"user-{number}" for number in range(start, start + count)]
    app.redis.zadd(app.WAITING_KEY, {token: number for number, token in enumerate(tokens, start=start)})
    return tokens


def test_admit_is_atomic_and_never_exceeds_capacity(app_module, monkeypatch):
    app = app_module
    monkeypatch.setattr(app, "now_ts", lambda: 1000)
    tokens = add_waiters(app, 40)

    assert app.admit_users() == app.MAX_ACTIVE
    assert app.redis.zcard(app.ACTIVE_KEY) == app.MAX_ACTIVE
    assert app.redis.zcard(app.WAITING_KEY) == 10
    assert set(app.redis.zrange(app.ACTIVE_KEY, 0, -1)) == set(tokens[:app.MAX_ACTIVE])
    assert len(app.redis.hgetall(app.ACTIVE_DEADLINE_KEY)) == app.MAX_ACTIVE

    assert app.admit_users() == 0
    assert app.redis.zcard(app.ACTIVE_KEY) == app.MAX_ACTIVE


def test_expired_ghosts_are_reclaimed_and_queue_progresses(app_module, monkeypatch):
    app = app_module
    clock = {"now": 1000}
    monkeypatch.setattr(app, "now_ts", lambda: clock["now"])
    add_waiters(app, app.MAX_ACTIVE + 5)
    app.admit_users()

    clock["now"] += app.ACTIVE_HEARTBEAT_TTL + 1
    assert app.admit_users() == 5
    assert app.redis.zcard(app.WAITING_KEY) == 0
    assert app.redis.zcard(app.ACTIVE_KEY) == 5
    assert len(app.redis.hgetall(app.ACTIVE_DEADLINE_KEY)) == 5


def test_heartbeat_refreshes_presence_but_not_absolute_deadline(app_module, monkeypatch):
    app = app_module
    clock = {"now": 1000}
    monkeypatch.setattr(app, "now_ts", lambda: clock["now"])
    token = add_waiters(app, 1)[0]
    app.admit_users()
    original_deadline = int(app.redis.hget(app.ACTIVE_DEADLINE_KEY, token))

    clock["now"] = 1005
    assert app.refresh_active_heartbeat(token) == original_deadline
    assert int(app.redis.zscore(app.ACTIVE_KEY, token)) == 1005 + app.ACTIVE_HEARTBEAT_TTL
    assert int(app.redis.hget(app.ACTIVE_DEADLINE_KEY, token)) == original_deadline

    clock["now"] = original_deadline + 1
    assert app.refresh_active_heartbeat(token) is None
    assert app.redis.zscore(app.ACTIVE_KEY, token) is None
    assert app.redis.hget(app.ACTIVE_DEADLINE_KEY, token) is None


def test_existing_active_user_is_migrated_without_being_kicked(app_module, monkeypatch):
    app = app_module
    monkeypatch.setattr(app, "now_ts", lambda: 1000)
    app.redis.zadd(app.ACTIVE_KEY, {"legacy-user": 1180})

    assert app.get_active_expiry("legacy-user") == 1180
    assert int(app.redis.hget(app.ACTIVE_DEADLINE_KEY, "legacy-user")) == 1180
    assert int(app.redis.zscore(app.ACTIVE_KEY, "legacy-user")) == 1000 + app.ACTIVE_HEARTBEAT_TTL


def test_status_rechecks_active_when_waiting_rank_disappears(app_module, monkeypatch):
    app = app_module
    monkeypatch.setattr(app, "get_sale_state", lambda: {"is_open": True})
    monkeypatch.setattr(app, "get_active_expiry", lambda _token: 1200)
    monkeypatch.setattr(app, "get_waiting_rank", lambda _token: None)

    with app.app.test_client() as client:
        with client.session_transaction() as session:
            session["queue_token"] = "moved-user"
            session["queue_no"] = 7
        response = client.get("/queue/status")

    assert response.status_code == 200
    assert response.get_json() == {
        "valid": True,
        "can_enter": True,
        "queue_no": 7,
        "waiting_count": 0,
    }
