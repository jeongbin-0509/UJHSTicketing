import importlib
import os
import sys
from pathlib import Path

import fakeredis
import psycopg2.pool
import pytest
import upstash_redis

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class RedisAdapter:
    def __init__(self):
        self.inner = fakeredis.FakeRedis(decode_responses=True)

    def eval(self, script, keys=None, args=None):
        keys = keys or []
        args = args or []
        return self.inner.eval(script, len(keys), *keys, *args)

    def __getattr__(self, name):
        return getattr(self.inner, name)


class DummyCursor:
    def __init__(self):
        self.query = ""

    def execute(self, query, _params=None):
        self.query = query

    def fetchone(self):
        if "ticket_settings" in self.query:
            return {"open_at": None, "close_at": None}
        if "seat_counter" in self.query:
            return {"next_seat": 1, "total_seats": 10000}
        return None

    def close(self):
        pass


class DummyConnection:
    closed = False
    status = 1
    autocommit = False

    def cursor(self):
        return DummyCursor()

    def rollback(self):
        pass


class DummyPool:
    def __init__(self, *args, **kwargs):
        self.connection = DummyConnection()

    def getconn(self):
        return self.connection

    def putconn(self, _connection, close=False):
        pass


@pytest.fixture(scope="session")
def app_module():
    os.environ.update({
        "SECRET_KEY": "test-secret",
        "SUPABASE_DB_URL": "postgresql://unused",
        "UPSTASH_REDIS_REST_URL": "https://unused",
        "UPSTASH_REDIS_REST_TOKEN": "unused",
        "REDIS_KEY_PREFIX": "test:ticket",
        "TICKETING_ENABLED": "1",
    })
    fake = RedisAdapter()
    original_pool = psycopg2.pool.ThreadedConnectionPool
    original_redis = upstash_redis.Redis
    psycopg2.pool.ThreadedConnectionPool = DummyPool
    upstash_redis.Redis = lambda *args, **kwargs: fake
    try:
        sys.modules.pop("app", None)
        module = importlib.import_module("app")
        yield module
    finally:
        psycopg2.pool.ThreadedConnectionPool = original_pool
        upstash_redis.Redis = original_redis


@pytest.fixture(autouse=True)
def clean_redis(app_module):
    app_module.redis.flushall()
