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
    assert r.json() == [{"market_id": "0xM", "question": "Q?", "mid": 0.45, "twin_gap": None, "twin_mid_gap": None}]
    assert conn.calls[0][1] == (6,)
    assert len(conn.calls) == 1   # 没有孪生就不查关系


def test_markets_rejects_bad_hours():
    client, conn = client_with([])
    assert client.get("/api/markets?hours=0").status_code == 422
    assert client.get("/api/markets?hours=1000").status_code == 422
    assert conn.calls == []


def test_depth_quality_404_for_unknown_market():
    client, _ = client_with([])
    assert client.get("/api/markets/0xM/depth-quality").status_code == 404


def test_depth_quality_passes_market_and_hours():
    client, conn = client_with([{"minute": "t", "depth_usd": 100.0, "suspicious_usd": 49.0, "levels": []}])
    r = client.get("/api/markets/0xM/depth-quality?hours=6")
    assert r.status_code == 200
    assert r.json()["latest"]["suspicious_usd"] == 49.0
    assert len(r.json()["history"]) == 1
    assert [c[1] for c in conn.calls[1:]] == [("0xM",), ("0xM", 6)]


def test_depth_quality_rejects_bad_hours():
    client, conn = client_with([])
    assert client.get("/api/markets/0xM/depth-quality?hours=0").status_code == 422
    assert client.get("/api/markets/0xM/depth-quality?hours=1000").status_code == 422
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


def test_trades_empty_is_not_404():
    client, conn = client_with([])
    r = client.get("/api/markets/0xM/trades?minutes=360&bucket_seconds=300")
    assert r.status_code == 200
    assert r.json() == []
    assert conn.calls[0][1] == (300, "0xM", 360)


def test_trades_bucket_at_least_one_minute():
    client, conn = client_with([])
    assert client.get("/api/markets/0xM/trades?bucket_seconds=15").status_code == 422
    assert conn.calls == []


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
     "asset_id": "Y", "topic": "Releases", "company": "OpenAI",
     "mid": 0.45, "best_bid": 0.4, "best_ask": 0.5, "spread": 0.1, "microprice": 0.44,
     "imbalance_5": 0.1, "bid_depth_5c": 10.0, "ask_depth_5c": 8.0, "logodds_chg_60m": 0.02, "updated_at": None},
    {"market_id": "0xM", "question": "Q?", "slug": "q", "end_date": None, "outcome": "No", "outcome_index": 2,
     "asset_id": "N", "topic": "Releases", "company": "OpenAI",
     "mid": 0.55, "best_bid": 0.5, "best_ask": 0.6, "spread": 0.1, "microprice": 0.56,
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
    assert (j["topic"], j["company"]) == ("Releases", "OpenAI")
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



def twin_row(**kw):
    return {"market_a": "0xLOW", "market_b": "0xHIGH", "relation": "implies", "strength": "strict", "condition": "",
            "differences": ["boundary"], "confidence": "high", "reason": "r", "source": "ai", "judged_at": None,
            "asset_a": "YL", "question_a": "Below?", "slug_a": "low", "asset_b": "YH", "question_b": "Above?",
            "slug_b": "high", **kw}


def book_raw(bid, ask):
    return json.dumps({"bids": [[str(bid), "1"]] if bid else [], "asks": [[str(ask), "1"]] if ask else []})


def test_twins_from_current_market_view():
    # 表里是 A=0xLOW implies B=0xHIGH；从 0xHIGH 看就是 implied_by，缺口按两边买一卖一算
    client, conn = client_with_redis([twin_row()], FakeRedis({"book:YL": book_raw(0.50, 0.52),
                                                             "book:YH": book_raw(0.44, 0.45)}))
    j = client.get("/api/markets/0xHIGH/twins").json()
    t = j["twins"][0]
    assert t["market_id"] == "0xLOW" and t["question"] == "Below?" and t["relation"] == "implied_by"
    assert t["this_quote"] == {"bid": 0.44, "ask": 0.45}
    assert t["gap"] == {"this": "Yes", "other": "No", "cost": 0.95, "edge": 0.05}
    assert t["mid_edge"] == 0.065           # 0.445 + (1 − 0.51)
    assert conn.calls[0][1] == {"m": "0xHIGH"}


def test_twins_sorted_by_edge_and_missing_book():
    rows = [twin_row(market_b="0xB1", asset_b="YB1", question_b="Gone?", relation="exclusive"),
            twin_row(market_b="0xB2", asset_b="YB2", question_b="Big gap?", relation="exclusive")]
    client, _ = client_with_redis(rows, FakeRedis({"book:YL": book_raw(0.6, 0.62), "book:YB2": book_raw(0.5, 0.52)}))
    j = client.get("/api/markets/0xLOW/twins").json()
    assert [t["question"] for t in j["twins"]] == ["Big gap?", "Gone?"]   # 盘口不可用的排最后
    assert j["twins"][1]["gap"] is None and j["twins"][1]["other_quote"] is None


def test_markets_best_twin_gap():
    # 列表和关系共用一个 FakeConn：两种行的字段合在一起
    row = {**twin_row(relation="exclusive"), "market_id": "0xLOW", "twins": 1}
    client, conn = client_with_redis([row], FakeRedis({"book:YL": book_raw(0.6, 0.62), "book:YH": book_raw(0.5, 0.52)}))
    j = client.get("/api/markets").json()
    assert j[0]["twin_gap"] == 0.1          # 两边买 No：0.4 + 0.5
    assert j[0]["twin_mid_gap"] == 0.12     # 按中间价：0.39 + 0.49
    assert conn.calls[1][1] == {"m": None}


def test_replay_passes_tokens_and_window(monkeypatch):
    from pmdata.api import app as app_module
    seen = {}

    def fake(market, tokens, start, end):
        seen.update(market=market, tokens=tokens, start=start, end=end)
        return {"initial": {1: None, 2: {"bids": [[0.5, 3.0]], "asks": []}}, "events": []}, True

    monkeypatch.setattr(app_module.replay_bronze, "replay", fake)
    client, _ = client_with(TOKENS)
    j = client.get("/api/markets/0xM/replay?start=2026-10-29T12:00:00&minutes=15").json()
    assert seen["tokens"] == {"Y": 1, "N": 2}
    assert seen["start"] == datetime(2026, 10, 29, 12, 0, tzinfo=UTC)   # 不带时区按 UTC
    assert seen["end"] == datetime(2026, 10, 29, 12, 15, tzinfo=UTC)
    assert j["complete"] is True
    assert [(o["outcome"], o["initial"]) for o in j["outcomes"]] == [
        ("Yes", None), ("No", {"bids": [[0.5, 3.0]], "asks": []})]


def test_replay_window_at_most_an_hour():
    client, conn = client_with(TOKENS)
    assert client.get("/api/markets/0xM/replay?start=2026-10-29T12:00:00Z&minutes=61").status_code == 422
    assert conn.calls == []


def test_replay_periods(monkeypatch):
    from pmdata.api import app as app_module
    monkeypatch.setattr(app_module.replay_bronze, "periods", lambda m: [
        (datetime(2026, 10, 29, 12, 0, tzinfo=UTC), datetime(2026, 10, 29, 13, 0, tzinfo=UTC))])
    client, _ = client_with(TOKENS)
    assert client.get("/api/markets/0xM/replay/periods").json() == [
        {"start": "2026-10-29T12:00:00+00:00", "end": "2026-10-29T13:00:00+00:00"}]


def test_signals_passes_hours():
    client, conn = client_with([{"event_id": "0xM:1", "status": "final", "change_points": 5.0}])
    r = client.get("/api/signals?hours=24")
    assert r.status_code == 200
    assert r.json() == [{"event_id": "0xM:1", "status": "final", "change_points": 5.0}]
    assert conn.calls[0][1] == (24,)
    assert client.get("/api/signals?hours=1000").status_code == 422
