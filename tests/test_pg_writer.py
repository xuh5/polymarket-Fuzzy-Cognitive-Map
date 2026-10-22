import json
from datetime import UTC, datetime

from pmdata.sink.pg_writer import BOOK_COLS, TOKEN_COLS, TRADE_COLS, to_rows, upsert_sql


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


def test_token_rows_from_markets_and_tombstone_skipped():
    market = {"id": "1", "conditionId": "0xM", "question": "Q?", "slug": "q", "version": "v1",
              "endDate": "2026-12-31T00:00:00Z", "outcomes": '["Yes", "No"]', "clobTokenIds": '["111", "222"]'}
    rows = to_rows([FakeMsg("pm.raw.markets", {"source": "gamma", "payload": market}),
                    FakeMsg("pm.raw.markets", None),
                    FakeMsg("pm.metrics.book", b"not json")])
    tokens = {k: dict(zip(TOKEN_COLS, v, strict=True)) for k, v in rows["tokens"].items()}
    assert tokens["111"]["outcome"] == "Yes" and tokens["111"]["outcome_index"] == 1
    assert tokens["222"]["outcome"] == "No" and tokens["222"]["market_id"] == "0xM"
    assert rows["metrics_book"] == {}


def test_upsert_sql():
    sql = upsert_sql("t", ["a", "b", "c"], ["a"], ", x = now()")
    assert sql == ("insert into t (a, b, c) values (%s, %s, %s) "
                   "on conflict (a) do update set b = excluded.b, c = excluded.c, x = now()")
