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


def test_enter_queue_issues_unique_numbers_and_respects_capacity(app_module, monkeypatch):
    app = app_module
    monkeypatch.setattr(app, "now_ts", lambda: 1000)

    results = [app.enter_queue(f"entrant-{number}") for number in range(100)]

    assert [queue_no for queue_no, _state, _rank in results] == list(range(1, 101))
    assert sum(state == 1 for _queue_no, state, _rank in results) == app.MAX_ACTIVE
    assert app.redis.zcard(app.ACTIVE_KEY) == app.MAX_ACTIVE
    assert app.redis.zcard(app.WAITING_KEY) == 100 - app.MAX_ACTIVE


def test_expired_ghosts_are_reclaimed_and_queue_progresses(app_module, monkeypatch):
    app = app_module
    clock = {"now": 1000}
    monkeypatch.setattr(app, "now_ts", lambda: clock["now"])
    add_waiters(app, app.MAX_ACTIVE + 5)
    app.admit_users()

    clock["now"] += app.ACTIVE_INITIAL_TTL + 1
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
    assert int(app.redis.zscore(app.ACTIVE_KEY, "legacy-user")) == 1180


def test_status_rechecks_active_when_waiting_rank_disappears(app_module, monkeypatch):
    app = app_module
    monkeypatch.setattr(app, "get_sale_state", lambda: {"is_open": True})
    monkeypatch.setattr(app, "get_remaining_seats", lambda: 10)
    monkeypatch.setattr(app, "get_queue_state", lambda _token, trigger_admit=False: (1, 0, 1200))

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


def test_status_uses_single_rank_lookup_for_regular_waiter(app_module, monkeypatch):
    app = app_module
    monkeypatch.setattr(app, "get_sale_state", lambda: {"is_open": True})
    monkeypatch.setattr(app, "get_remaining_seats", lambda: 10)
    monkeypatch.setattr(app.random, "random", lambda: 1.0)
    app.redis.zadd(app.WAITING_KEY, {
        "ahead-1": 1,
        "ahead-2": 2,
        "ahead-3": 3,
        "regular-waiter": 10,
    })

    calls = {"rank": 0, "state": 0}
    original_zrank = app.redis.zrank
    original_state = app.get_queue_state

    def counted_zrank(*args, **kwargs):
        calls["rank"] += 1
        return original_zrank(*args, **kwargs)

    def counted_state(*args, **kwargs):
        calls["state"] += 1
        return original_state(*args, **kwargs)

    monkeypatch.setattr(app.redis, "zrank", counted_zrank)
    monkeypatch.setattr(app, "get_queue_state", counted_state)

    with app.app.test_client() as client:
        with client.session_transaction() as session:
            session["queue_token"] = "regular-waiter"
            session["queue_no"] = 10
        response = client.get("/queue/status")

    assert response.status_code == 200
    assert response.get_json()["waiting_count"] == 3
    assert calls == {"rank": 1, "state": 0}


def test_shared_progress_is_cached_and_does_not_calculate_personal_rank(app_module, monkeypatch):
    app = app_module
    app._local_queue_progress.update({"expires": 0.0, "data": None})
    app.redis.zadd(app.WAITING_KEY, {"waiting-31": 31, "waiting-32": 32})
    app.redis.zadd(app.ACTIVE_KEY, {"active-user": 9999})

    calls = {"count": 0}
    original_eval = app.redis.eval

    def counted_eval(*args, **kwargs):
        calls["count"] += 1
        return original_eval(*args, **kwargs)

    monkeypatch.setattr(app.redis, "eval", counted_eval)
    first = app.get_queue_progress()
    second = app.get_queue_progress()

    assert first == {
        "front_queue_no": 31,
        "waiting_count": 2,
        "active_count": 1,
    }
    assert second == first
    assert calls["count"] == 1


def test_progress_endpoint_returns_shared_cacheable_state(app_module, monkeypatch):
    app = app_module
    monkeypatch.setattr(app, "get_sale_state", lambda: {"is_open": True})
    monkeypatch.setattr(app, "get_remaining_seats", lambda: 50)
    monkeypatch.setattr(app, "get_queue_progress", lambda: {
        "front_queue_no": 21,
        "waiting_count": 200,
        "active_count": 30,
    })

    with app.app.test_client() as client:
        response = client.get("/queue/progress")

    assert response.status_code == 200
    assert response.get_json()["front_queue_no"] == 21
    assert response.get_json()["exact_threshold"] == app.QUEUE_EXACT_THRESHOLD
    assert response.headers["Cache-Control"].startswith("public, max-age=3")
