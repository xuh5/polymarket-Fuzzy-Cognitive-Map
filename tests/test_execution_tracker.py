import pytest

from pmdata.execution.tracker import ExecutionTracker

MARKET = "0xM"
T0 = 2021          # 成交消息的交易所时间；对应的挂单减少在它之前 21 毫秒
LAST = T0 + 300_000 + 1   # 过了最后一个 markout（5 分钟）


def book(ts=1000, bids=(("0.50", "100"),), asks=(("0.52", "100"), ("0.53", "100")), asset="Y"):
    return {"source": "clob_ws", "payload": {
        "event_type": "book", "market": MARKET, "asset_id": asset, "timestamp": str(ts),
        "bids": [{"price": p, "size": s} for p, s in bids],
        "asks": [{"price": p, "size": s} for p, s in asks]}}


def change(ts, side, price, size, asset="Y", more=()):
    """一条盘口消息；more = 同一条消息里的其他价位 [(side, price, size)]"""
    items = [(side, price, size), *more]
    return {"source": "clob_ws", "payload": {
        "event_type": "price_change", "market": MARKET, "timestamp": str(ts),
        "price_changes": [{"asset_id": asset, "side": sd, "price": pr, "size": sz} for sd, pr, sz in items]}}


def trade(ts, price, size, side, tx="T1", asset="Y"):
    return {"source": "clob_ws", "payload": {
        "event_type": "last_trade_price", "market": MARKET, "asset_id": asset, "timestamp": str(ts),
        "price": price, "size": size, "side": side, "fee_rate_bps": "0", "transaction_hash": tx}}


def gap():
    return {"source": "collector", "payload": {"event_type": "gap", "market": MARKET,
                                               "disconnected_at": None, "reconnected_at": None}}


def tracker():
    t = ExecutionTracker()
    t.apply(book())
    return t


def run(t, *envs, until=LAST):
    out = []
    for e in envs:
        out += t.apply(e)
    return out + t.advance(until)


def test_pre_book_is_taken_before_the_matching_decrease():
    # 卖一 0.52 被整档吃光，减少先到、成交消息后到：成交前的卖一应该是 0.52，不是吃光后的 0.53
    t = tracker()
    [r] = run(t, change(T0 - 21, "SELL", "0.52", "0"), trade(T0, "0.52", "100", "BUY"))
    assert (r["pre_bid"], r["pre_ask"], r["pre_mid"]) == (0.50, 0.52, 0.51)
    assert r["pre_spread"] == pytest.approx(0.02)
    assert r["pre_match"] == "exact"
    assert (r["levels"], r["size"], r["price"], r["worst_price"]) == (1, 100.0, 0.52, 0.52)
    assert r["notional"] == pytest.approx(52.0)
    assert r["cost"] == pytest.approx(0.01)   # 比中间价多付 1 分


def test_sweep_through_two_levels():
    # 一笔吃单吃穿两档：0.52 吃光、0.53 吃掉 50，交易所每档一条盘口消息、时间戳相同；
    # 成交消息只有一条，均价 0.52333 四舍五入成 0.523
    t = tracker()
    [r] = run(t, change(T0 - 21, "SELL", "0.52", "0"), change(T0 - 21, "SELL", "0.53", "50"),
              trade(T0, "0.523", "150", "BUY"))
    assert r["pre_match"] == "sweep"
    assert (r["levels"], r["worst_price"], r["size"]) == (2, 0.53, 150.0)
    assert r["pre_ask"] == 0.52
    assert r["cost"] == pytest.approx(0.523 - 0.51)


def test_decreases_at_different_times_are_not_a_sweep():
    # 两档在不同时刻减少（比如一个是撤单），均价对不上任何一档：找不到
    t = tracker()
    [r] = run(t, change(T0 - 30, "SELL", "0.52", "0"), change(T0 - 21, "SELL", "0.53", "50"),
              trade(T0, "0.523", "150", "BUY"))
    assert r["pre_match"] == "none"
    assert r["levels"] is None


def test_markouts_use_the_mid_at_each_horizon():
    t = tracker()
    out = run(t, change(T0 - 21, "SELL", "0.52", "0"), trade(T0, "0.52", "100", "BUY"),
              # 成交后盘口：买一 0.50、卖一 0.53，中间价 0.515；4 秒时买一抬到 0.52，中间价 0.525
              change(4000, "BUY", "0.52", "10"))
    [r] = out
    assert r["mid_1s"] == pytest.approx(0.515)     # 1 秒到期（3021）时还没抬
    assert r["mid_10s"] == pytest.approx(0.525)
    assert r["mid_300s"] == pytest.approx(0.525)
    assert r["maker_pnl_1s"] == pytest.approx(0.005)    # 挂单方 0.52 卖出，1 秒后中间价 0.515：赚半分
    assert r["maker_pnl_10s"] == pytest.approx(-0.005)  # 10 秒后中间价 0.525：亏半分


def test_output_only_after_the_last_horizon():
    t = tracker()
    assert t.apply(change(T0 - 21, "SELL", "0.52", "0")) == []
    assert t.apply(trade(T0, "0.52", "100", "BUY")) == []
    assert t.advance(T0 + 300_000) == []      # 正好到期还不算过
    assert len(t.advance(T0 + 300_001)) == 1
    assert t.advance(T0 + 400_000) == []      # 只输出一次


def test_unmatched_trade_falls_back_to_the_current_book():
    # 对应的减少在 2 秒以前，配不上：退回成交消息到达时的盘口（0.52 已经被吃光，卖一是 0.53）
    t = tracker()
    [r] = run(t, change(T0 - 2500, "SELL", "0.52", "0"), trade(T0, "0.52", "100", "BUY"))
    assert r["pre_match"] == "none"
    assert r["pre_ask"] == 0.53


def test_sell_side_signs():
    # 主动卖：在买一 0.50 成交，成本是中间价 0.51 − 0.50 = 1 分；
    # 之后买一掉到 0.48、卖一压到 0.50，中间价 0.49：挂单方 0.50 买入，亏 1 分
    t = tracker()
    [r] = run(t, change(T0 - 21, "BUY", "0.50", "60"), trade(T0, "0.50", "40", "SELL"),
              change(T0 + 100, "BUY", "0.50", "0"), change(T0 + 101, "BUY", "0.48", "10"),
              change(T0 + 102, "SELL", "0.50", "10"))
    assert r["side"] == "SELL"
    assert r["cost"] == pytest.approx(0.01)
    assert r["mid_1s"] == pytest.approx(0.49)
    assert r["maker_pnl_1s"] == pytest.approx(-0.01)


def test_gap_leaves_later_mids_empty():
    t = tracker()
    [r] = run(t, change(T0 - 21, "SELL", "0.52", "0"), trade(T0, "0.52", "100", "BUY"), gap())
    assert r["mid_1s"] is None and r["maker_pnl_300s"] is None
    assert r["cost"] == pytest.approx(0.01)    # 成交前的数据不受影响


def test_trade_without_a_usable_book_is_skipped():
    t = ExecutionTracker()
    assert run(t, trade(T0, "0.52", "100", "BUY")) == []
    assert t.skipped == 1


def test_drain_outputs_unfinished_executions():
    t = tracker()
    t.apply(change(T0 - 21, "SELL", "0.52", "0"))
    t.apply(trade(T0, "0.52", "100", "BUY"))
    t.apply(change(T0 + 5000, "BUY", "0.51", "10"))   # 只过了 1 秒那个点
    [r] = t.drain()
    assert r["mid_1s"] == pytest.approx(0.515)
    assert r["mid_10s"] is None
    assert t.drain() == []
