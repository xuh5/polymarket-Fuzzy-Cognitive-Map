import json
from datetime import UTC, datetime

from pmdata.common.config import TOPIC_METRICS_BOOK
from pmdata.signals.detector import JumpDetector
from pmdata.sink.pg_writer import (
    BOOK_COLS,
    DEPTH_COLS,
    EVENT_COLS,
    EXEC_COLS,
    TOKEN_COLS,
    TRADE_COLS,
    UPSERTS,
    to_rows,
    upsert_sql,
)


class FakeMsg:
    def __init__(self, topic, value, partition=0, offset=0):
        self._t, self._p, self._o = topic, partition, offset
        self._v = None if value is None else (value if isinstance(value, bytes) else json.dumps(value).encode())

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v


def book_rec(second, mid):
    return {"market": "0xM", "asset_id": "Y", "second": second, "exchange_ts": second + 5,
            "received_at": "2026-10-22T09:00:00.100+00:00", "best_bid": 0.4, "best_ask": 0.5, "mid": mid,
            "computed_at": "2026-10-22T09:00:01.000+00:00"}


def test_book_rows_dedup_within_batch():
    rows = to_rows([FakeMsg("pm.metrics.book", book_rec(1000, 0.45)),
                    FakeMsg("pm.metrics.book", book_rec(1000, 0.46)),     # 上游重复输出同一秒：后到的覆盖
                    FakeMsg("pm.metrics.book", book_rec(2000, 0.47))])
    book = rows["metrics_book"]
    assert len(book) == 2
    row = dict(zip(BOOK_COLS, book[("Y", datetime.fromtimestamp(1, UTC))], strict=True))
    assert row["mid"] == 0.46 and row["market_id"] == "0xM"
    assert row["exchange_ts"] == datetime.fromtimestamp(1.005, UTC)
    assert row["received_at"].tzinfo is not None
    assert row["logodds_chg_60m"] is None          # 缺的字段写 null


def test_trade_rows():
    rec = {"market": "0xM", "asset_id": "Y", "minute": 60000, "trades": 2, "volume": 14.0, "notional": 6.0,
           "buy_volume": 10.0, "sell_volume": 4.0, "exchange_ts": 61000,
           "received_at": "2026-10-22T09:00:00.100+00:00", "computed_at": "2026-10-22T09:00:01.000+00:00"}
    [row] = to_rows([FakeMsg("pm.metrics.trades", rec)])["metrics_trades"].values()
    row = dict(zip(TRADE_COLS, row, strict=True))
    assert row["minute"] == datetime.fromtimestamp(60, UTC) and row["buy_volume"] == 10.0


def test_depth_rows_dedup_and_keep_levels():
    rec = {"market": "0xM", "ts": 60000, "depth_usd": 100.0, "suspicious_usd": 49.0,
           "levels": [{"side": "BUY", "price": 0.49, "usd": 49.0, "flashes": 5, "filled_usd": 0}],
           "computed_at": "2026-10-22T09:00:01.000+00:00"}
    rows = to_rows([FakeMsg("pm.metrics.depth", dict(rec, suspicious_usd=1.0)),
                    FakeMsg("pm.metrics.depth", rec)])["depth_quality"]      # 上游重复输出同一分钟：后到的覆盖
    assert len(rows) == 1
    [row] = rows.values()
    row = dict(zip(DEPTH_COLS, row, strict=True))
    assert row["minute"] == datetime.fromtimestamp(60, UTC) and row["suspicious_usd"] == 49.0
    assert row["levels"].obj == rec["levels"]


def test_execution_rows_dedup_and_skip_missing_hash():
    rec = {"market": "0xM", "asset_id": "Y", "tx_hash": "0xT", "ts": 60000, "side": "BUY", "fee_rate_bps": 0,
           "price": 0.52, "size": 40.0, "notional": 20.8, "levels": 1, "worst_price": 0.52,
           "pre_bid": 0.5, "pre_ask": 0.52, "pre_mid": 0.51, "pre_spread": 0.02, "pre_match": "exact", "cost": 0.01,
           "mid_1s": 0.515, "mid_10s": 0.515, "mid_60s": 0.52, "mid_300s": None,
           "maker_pnl_1s": 0.005, "maker_pnl_10s": 0.005, "maker_pnl_60s": 0.0, "maker_pnl_300s": None,
           "computed_at": "2026-10-22T09:05:01.000+00:00"}
    rows = to_rows([FakeMsg("pm.metrics.execution", dict(rec, cost=9.0)),
                    FakeMsg("pm.metrics.execution", rec),                  # 重启后重复输出：后到的覆盖
                    FakeMsg("pm.metrics.execution", dict(rec, tx_hash=None))])["execution_quality"]
    assert list(rows) == [("0xT", "Y")]
    row = dict(zip(EXEC_COLS, rows[("0xT", "Y")], strict=True))
    assert row["market_id"] == "0xM" and row["ts"] == datetime.fromtimestamp(60, UTC)
    assert row["cost"] == 0.01 and row["maker_pnl_300s"] is None


def test_token_rows_from_markets_and_tombstone_skipped():
    market = {"id": "1", "conditionId": "0xM", "question": "Q?", "slug": "q", "version": "v1",
              "endDate": "2026-12-31T00:00:00Z", "outcomes": '["Yes", "No"]', "clobTokenIds": '["111", "222"]'}
    rows = to_rows([FakeMsg("pm.raw.markets", {"source": "gamma", "payload": market}),
                    FakeMsg("pm.raw.markets", None),
                    FakeMsg("pm.metrics.book", b"not json")])
    tokens = {k: dict(zip(TOKEN_COLS, v, strict=True)) for k, v in rows["tokens"].items()}
    assert tokens["111"]["outcome"] == "Yes" and tokens["111"]["outcome_index"] == 1
    assert tokens["222"]["outcome"] == "No" and tokens["222"]["market_id"] == "0xM"
    assert rows["market_labels"] == {"0xM": ("0xM", "Other", None, "rules")}
    assert rows["metrics_book"] == {}


def test_upsert_sql():
    sql = upsert_sql("t", ["a", "b", "c"], ["a"], ", x = now()")
    assert sql == ("insert into t (a, b, c) values (%s, %s, %s) "
                   "on conflict (a) do update set b = excluded.b, c = excluded.c, x = now()")


def test_event_rows_from_detector_output():
    det = JumpDetector(clock=lambda: "2026-10-22T09:05:00.000+00:00")
    det.outcomes = {"Y": 1}
    for sec, mid in ((0, 0.41), (300, 0.45)):
        [ev] = det.apply(TOPIC_METRICS_BOOK, {
            "market": "0xM", "asset_id": "Y", "second": sec * 1000, "book_since": 0, "best_bid": mid - 0.01,
            "best_ask": mid + 0.01, "mid": mid, "spread": 0.02}) or [None]
    rows = to_rows([FakeMsg("pm.events", dict(ev, status="open")), FakeMsg("pm.events", dict(ev, status="closed"))])
    [row] = rows["signal_events"].values()
    row = dict(zip(EVENT_COLS, row, strict=True))
    assert row["status"] == "closed" and row["market_id"] == "0xM"
    assert row["peak_at"] == datetime.fromtimestamp(300, UTC) and row["book_since"] == datetime.fromtimestamp(0, UTC)
    assert row["move_type"] == "both" and row["mid_after_30m"] is None
    assert "where array_position" in UPSERTS["signal_events"]      # 状态不倒退


class KeyedMsg(FakeMsg):
    def __init__(self, topic, key, value):
        super().__init__(topic, value)
        self._k = key

    def key(self):
        return self._k.encode()


def twin_payload(status="ok", relations=None):
    return {"event_a": "1", "event_b": "2", "fingerprint": "fp", "status": status, "error": None,
            "model": "claude:haiku", "cost_usd": 0.01, "judged_at": "2026-11-20T01:00:00.000Z",
            "relations": relations if relations is not None else [
                {"market_a": "0xA", "market_b": "0xB", "relation": "exclusive", "strength": "strict",
                 "condition": "", "differences": ["d"], "confidence": "high", "reason": "r"}]}


def test_twins_rows_keep_last_and_tombstone():
    rows = to_rows([KeyedMsg("pm.twins", "1:2", {"source": "twins", "payload": twin_payload("error")}),
                    KeyedMsg("pm.twins", "1:2", {"source": "twins", "payload": twin_payload()}),
                    KeyedMsg("pm.twins", "3:4", None)])
    assert rows["twins"]["1:2"]["status"] == "ok"      # 同一组只留最后一条
    assert rows["twins"]["3:4"] is None                # tombstone


class FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append(("execute", sql.split()[0], params))

    def executemany(self, sql, seq):
        self.calls.append(("executemany", sql.split()[0], list(seq)))


def test_write_twins_retires_then_upserts():
    from pmdata.sink.pg_writer import RELATION_UPSERT, write_twins
    cur = FakeCursor()
    write_twins(cur, {"1:2": twin_payload(), "3:4": None, "5:6": dict(twin_payload("error"), event_a="5")})
    kinds = [(c[0], c[1]) for c in cur.calls]
    # 1:2 成功：记判断 → 这组 AI 的旧关系先退役 → 写新关系（恢复成在用）
    assert kinds[:3] == [("execute", "insert"), ("execute", "update"), ("executemany", "insert")]
    assert cur.calls[2][2][0][:5] == ("0xA", "0xB", "1", "2", "exclusive")
    # 3:4 tombstone：关系全部退役、判断记成 retired；5:6 失败：只记判断，不动关系
    assert kinds[3:] == [("execute", "update"), ("execute", "update"), ("execute", "insert")]
    assert "retired_at = null" in RELATION_UPSERT and "source = 'ai'" in RELATION_UPSERT
