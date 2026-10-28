import json
from datetime import UTC, datetime

import redis
from fastapi.testclient import TestClient

from pmdata.api.app import app, get_conn, get_redis


class FakeConn:
    """代替数据库连接：记下执行的 SQL 和参数，返回预先给定的行。"""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]


def client_with(rows):
    conn = FakeConn(rows)
    app.dependency_overrides[get_conn] = lambda: conn
    # 不用 with TestClient(...)：那样会执行 lifespan 去连真的 Postgres
    return TestClient(app), conn


def teardown_function():
    app.dependency_overrides.clear()


def test_health():
    client, _ = client_with([{"latest": datetime(2026, 10, 28, 9, 0, tzinfo=UTC), "age_seconds": 1.5}])
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "latest_metric_at": "2026-10-28T09:00:00+00:00", "age_seconds": 1.5}


def test_markets_passes_hours():
    client, conn = client_with([{"market_id": "0xM", "question": "Q?", "mid": 0.45}])
    r = client.get("/api/markets?hours=6")
    assert r.status_code == 200
    assert r.json() == [{"market_id": "0xM", "question": "Q?", "mid": 0.45}]
    assert conn.calls[0][1] == (6,)


def test_markets_rejects_bad_hours():
    client, conn = client_with([])
    assert client.get("/api/markets?hours=0").status_code == 422
    assert client.get("/api/markets?hours=1000").status_code == 422
    assert conn.calls == []


def test_history_404_when_empty():
    client, conn = client_with([])
    r = client.get("/api/markets/0xM/history?minutes=30")
    assert r.status_code == 404
    assert conn.calls[0][1] == (1, "0xM", 30)


def test_history_bucket():
    client, conn = client_with([{"second": "t", "mid": 0.5}])
    assert client.get("/api/markets/0xM/history?minutes=60&bucket_seconds=60").status_code == 200
    assert conn.calls[0][1] == (60, "0xM", 60)


def test_cors_allows_dev_frontend_only():
    client, _ = client_with([])
    ok = client.get("/api/markets", headers={"Origin": "http://localhost:5173"})
    assert ok.headers["access-control-allow-origin"] == "http://localhost:5173"
    other = client.get("/api/markets", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in other.headers


class FakeRedis:
    def __init__(self, data=None, fail=False):
        self.data, self.fail = data or {}, fail

    def get(self, key):
        if self.fail:
            raise redis.ConnectionError("连不上")
        return self.data.get(key)


TOKENS = [
    {"market_id": "0xM", "question": "Q?", "slug": "q", "end_date": None, "outcome": "Yes", "outcome_index": 1,
     "asset_id": "Y", "mid": 0.45, "best_bid": 0.4, "best_ask": 0.5, "spread": 0.1, "microprice": 0.44,
     "imbalance_5": 0.1, "bid_depth_5c": 10.0, "ask_depth_5c": 8.0, "logodds_chg_60m": 0.02, "updated_at": None},
    {"market_id": "0xM", "question": "Q?", "slug": "q", "end_date": None, "outcome": "No", "outcome_index": 2,
     "asset_id": "N", "mid": 0.55, "best_bid": 0.5, "best_ask": 0.6, "spread": 0.1, "microprice": 0.56,
     "imbalance_5": -0.1, "bid_depth_5c": 8.0, "ask_depth_5c": 10.0, "logodds_chg_60m": -0.02, "updated_at": None},
]


def client_with_redis(rows, r):
    client, conn = client_with(rows)
    app.dependency_overrides[get_redis] = lambda: r
    return client, conn


def test_market_info():
    client, _ = client_with(TOKENS)
    j = client.get("/api/markets/0xM").json()
    assert j["question"] == "Q?"
    assert [o["outcome"] for o in j["outcomes"]] == ["Yes", "No"]


def test_market_404():
    client, _ = client_with([])
    assert client.get("/api/markets/0xnope").status_code == 404


def test_book_from_redis():
    raw = json.dumps({"asset_id": "N", "market": "0xM", "exchange_ts": 1000, "written_at": "t",
                      "bids": [["0.50", "3"]], "asks": [["0.60", "4.5"]]})
    client, _ = client_with_redis(TOKENS, FakeRedis({"book:N": raw}))
    j = client.get("/api/markets/0xM/book?outcome_index=2").json()
    assert j["available"] is True and j["outcome"] == "No"
    assert j["bids"] == [[0.5, 3.0]] and j["asks"] == [[0.6, 4.5]]


def test_book_missing_is_unavailable_not_error():
    client, _ = client_with_redis(TOKENS, FakeRedis())
    j = client.get("/api/markets/0xM/book").json()
    assert j["available"] is False and j["bids"] == []


def test_book_unknown_outcome_404():
    client, _ = client_with_redis(TOKENS, FakeRedis())
    assert client.get("/api/markets/0xM/book?outcome_index=3").status_code == 404


def test_book_redis_down_503():
    client, _ = client_with_redis(TOKENS, FakeRedis(fail=True))
    assert client.get("/api/markets/0xM/book").status_code == 503


def test_sparklines_grouped_and_cached():
    from pmdata.api import app as app_module
    app_module.sparkline_cache.items.clear()
    rows = [{"market_id": "0xA", "bucket": datetime(2026, 10, 28, 9, 0, tzinfo=UTC), "mid": 0.4},
            {"market_id": "0xA", "bucket": datetime(2026, 10, 28, 9, 30, tzinfo=UTC), "mid": 0.5},
            {"market_id": "0xB", "bucket": datetime(2026, 10, 28, 9, 0, tzinfo=UTC), "mid": 0.9}]
    client, conn = client_with(rows)
    j = client.get("/api/sparklines").json()
    assert j["0xA"] == [[1793178000000, 0.4], [1793179800000, 0.5]]
    assert list(j) == ["0xA", "0xB"]
    client.get("/api/sparklines")
    assert len(conn.calls) == 1          # 第二次走缓存，没查数据库
