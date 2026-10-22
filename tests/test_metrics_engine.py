import math

import pytest

from pmdata.common.config import TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES
from pmdata.metrics.engine import MetricsEngine, logit


def env(payload, source="clob_ws"):
    return {"source": source, "received_at": "R", "payload": payload}


def book(ts, bids, asks, asset="Y", market="0xM"):
    return env({"event_type": "book", "market": market, "asset_id": asset, "timestamp": str(ts),
                "bids": [{"price": p, "size": s} for p, s in bids],
                "asks": [{"price": p, "size": s} for p, s in asks]})


def change(ts, side, price, size, asset="Y", market="0xM"):
    return env({"event_type": "price_change", "market": market, "timestamp": str(ts),
                "price_changes": [{"asset_id": asset, "side": side, "price": price, "size": size}]})


def trade(ts, side, price, size, asset="Y", market="0xM"):
    return env({"event_type": "last_trade_price", "market": market, "asset_id": asset, "timestamp": str(ts),
                "side": side, "price": price, "size": size})


@pytest.fixture
def engine():
    return MetricsEngine(clock=lambda: "NOW")


def book_records(out):
    return [r for topic, _, r in out if topic == TOPIC_METRICS_BOOK]


def test_one_record_per_second_with_last_state(engine):
    assert engine.apply(book(1000, [("0.40", "10")], [("0.50", "10")])) == []
    assert engine.apply(change(1500, "BUY", "0.42", "5")) == []        # 同一秒内不输出
    out = engine.apply(change(2100, "BUY", "0.43", "5"))               # 下一秒的消息到了，上一秒结束
    [rec] = book_records(out)
    assert rec["second"] == 1000
    assert rec["best_bid"] == 0.42                                     # 是这一秒最后的状态
    assert rec["computed_at"] == "NOW"


def test_book_metrics(engine):
    engine.apply(book(1000, [("0.40", "30"), ("0.38", "10")], [("0.50", "10"), ("0.60", "50")]))
    [rec] = book_records(engine.flush())
    assert rec["mid"] == pytest.approx(0.45)
    assert rec["spread"] == pytest.approx(0.10)
    # 买一 30 份、卖一 10 份：买方力量大，加权中间价靠近卖一
    assert rec["microprice"] == pytest.approx((0.40 * 10 + 0.50 * 30) / 40)
    assert rec["imbalance_1"] == pytest.approx((30 - 10) / 40)
    assert rec["imbalance_5"] == pytest.approx((40 - 60) / 100)
    # 中间价 0.45 ±5 分 = [0.40, 0.50]：买盘只有 0.40 一档，卖盘只有 0.50 一档
    assert rec["bid_depth_5c"] == pytest.approx(0.40 * 30)
    assert rec["ask_depth_5c"] == pytest.approx(0.50 * 10)
    assert rec["bid_depth_1c"] == 0 and rec["ask_depth_1c"] == 0


def test_one_side_empty(engine):
    engine.apply(book(1000, [("0.40", "30")], []))
    [rec] = book_records(engine.flush())
    assert rec["best_bid"] == 0.40 and rec["best_ask"] is None
    assert rec["mid"] is None and rec["microprice"] is None and rec["bid_depth_1c"] is None
    assert rec["imbalance_1"] == 1.0


def test_logodds_change(engine):
    engine.apply(book(0, [("0.09", "1")], [("0.11", "1")]))           # 中间价 0.10
    engine.apply(change(60_000, "BUY", "0.19", "1"))                  # 1 分钟后
    engine.apply(change(60_000, "SELL", "0.21", "1"))
    engine.apply(change(60_000, "SELL", "0.11", "0"))
    engine.apply(change(60_000, "BUY", "0.09", "0"))                   # 中间价 0.20
    recs = book_records(engine.flush())
    assert recs[-1]["mid"] == pytest.approx(0.20)
    assert recs[-1]["logodds_chg_1m"] == pytest.approx(logit(0.20) - logit(0.10))
    assert recs[-1]["logodds_chg_5m"] is None                          # 5 分钟前还没数据


def test_logit_clips_extremes():
    assert math.isfinite(logit(0)) and math.isfinite(logit(1))
    assert logit(0.5) == 0


def test_gap_flushes_then_stops_until_snapshot(engine):
    engine.apply(book(1000, [("0.40", "1")], [("0.50", "1")]))
    out = engine.apply(env({"event_type": "gap", "market": "0xM"}, source="collector"))
    assert len(book_records(out)) == 1                                 # 断线前那一秒照常输出
    engine.apply(change(5000, "BUY", "0.41", "1"))                     # 作废期间
    assert book_records(engine.flush()) == []


def test_trade_window(engine):
    engine.apply(trade(1_000, "BUY", "0.40", "10"))
    engine.apply(trade(30_000, "SELL", "0.50", "4"))
    out = engine.apply(trade(61_000, "BUY", "0.40", "1"))              # 下一分钟的成交到了，上一分钟结束
    [(topic, key, rec)] = out
    assert (topic, key) == (TOPIC_METRICS_TRADES, "0xM")
    assert rec["minute"] == 0 and rec["trades"] == 2
    assert rec["volume"] == 14 and rec["buy_volume"] == 10 and rec["sell_volume"] == 4
    assert rec["notional"] == pytest.approx(4 + 2)


def test_other_market_does_not_close_window(engine):
    engine.apply(book(1000, [("0.40", "1")], [("0.50", "1")]))
    # 别的市场在别的分区，它的时间推进不代表这个市场这一秒的消息都到齐了
    assert engine.apply(book(9000, [("0.1", "1")], [], asset="Z", market="0xOther")) == []
    # 运行程序按时钟兜底输出：Y 的第 1 秒早结束了；Z 的第 9 秒到 9000ms 时还没结束
    assert [r["asset_id"] for r in book_records(engine.flush(up_to_ms=9000))] == ["Y"]
