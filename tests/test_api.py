from datetime import UTC, datetime

from fastapi.testclient import TestClient

from pmdata.api.app import app, get_conn


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
