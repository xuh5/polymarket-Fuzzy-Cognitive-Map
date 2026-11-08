from pmdata.depth.tracker import DepthTracker

MARKET = "0xM"
BEST = ("0.50", "0.52")


def book(ts=1000, bids=(("0.50", "100"),), asks=(("0.52", "100"),), asset="Y"):
    return {"source": "clob_ws", "payload": {
        "event_type": "book", "market": MARKET, "asset_id": asset, "timestamp": str(ts),
        "bids": [{"price": p, "size": s} for p, s in bids],
        "asks": [{"price": p, "size": s} for p, s in asks]}}


def change(ts, side, price, size, best=BEST, asset="Y"):
    return {"source": "clob_ws", "payload": {
        "event_type": "price_change", "market": MARKET, "timestamp": str(ts),
        "price_changes": [{"asset_id": asset, "side": side, "price": price, "size": size,
                           "best_bid": best[0], "best_ask": best[1]}]}}


def trade(ts, price, size, side, asset="Y"):
    return {"source": "clob_ws", "payload": {
        "event_type": "last_trade_price", "market": MARKET, "asset_id": asset, "timestamp": str(ts),
        "price": price, "size": size, "side": side}}


def marker(event_type):
    return {"source": "collector", "payload": {"event_type": event_type, "market": MARKET}}


def tracker():
    t = DepthTracker(outcomes={"Y": 1, "N": 2})
    t.apply(book())
    return t


def flash(t, ts, price="0.49", size="3000", side="BUY", life=1000):
    """挂一个块，life 毫秒后全撤。"""
    t.apply(change(ts, side, price, size))
    t.apply(change(ts + life, side, price, "0"))


def flashes(t, count=5, start=2000, **kw):
    for i in range(count):
        flash(t, start + i * 5000, **kw)


def report(t, up_to=62_000):
    return t.advance(up_to)


def test_five_flashes_flag_the_level_with_its_current_amount():
    t = tracker()
    flashes(t)
    t.apply(change(40_000, "BUY", "0.49", "3000"))   # 现在又挂着
    out = report(t)
    assert len(out) == 1
    s = out[0]
    assert s["market"] == MARKET and s["ts"] == 60_000
    assert s["suspicious_usd"] == 1470
    assert s["depth_usd"] == 50 + 1470 + 52
    assert s["levels"] == [{"side": "BUY", "price": 0.49, "usd": 1470, "flagged_usd": 1470, "flashes": 5,
                            "filled_usd": 0, "reactions": 0}]


def test_four_flashes_is_not_enough():
    t = tracker()
    flashes(t, count=4)
    t.apply(change(40_000, "BUY", "0.49", "3000"))
    assert report(t)[0]["suspicious_usd"] == 0


def test_flagged_level_with_nothing_resting_adds_no_amount():
    t = tracker()
    flashes(t)
    s = report(t)[0]
    assert s["suspicious_usd"] == 0 and s["levels"] == []


def test_flash_expires_after_an_hour():
    t = tracker()
    flashes(t)
    t.apply(change(3_660_000, "BUY", "0.49", "3000"))   # 挂着，但 5 次闪现都超过 1 小时了
    out = t.advance(3_730_000)
    last = [s for s in out if s["ts"] == 3_720_000][0]
    assert last["suspicious_usd"] == 0


def test_filled_lot_is_not_a_flash():
    t = tracker()
    for i in range(5):
        ts = 2000 + i * 5000
        t.apply(change(ts, "BUY", "0.49", "3000"))
        t.apply(trade(ts + 1000, "0.49", "300", "SELL"))    # 成交 10%，超过 5%
        t.apply(change(ts + 1000, "BUY", "0.49", "0"))
    t.apply(change(40_000, "BUY", "0.49", "3000"))
    assert report(t)[0]["suspicious_usd"] == 0


def test_trade_on_second_outcome_is_converted():
    t = tracker()
    for i in range(5):
        ts = 2000 + i * 5000
        t.apply(change(ts, "BUY", "0.49", "3000"))
        t.apply(trade(ts + 1000, "0.51", "300", "BUY", asset="N"))   # 买 No 0.51 = 吃 Yes 买盘 0.49
        t.apply(change(ts + 1000, "BUY", "0.49", "0"))
    t.apply(change(40_000, "BUY", "0.49", "3000"))
    assert report(t)[0]["suspicious_usd"] == 0


def test_trade_arriving_after_the_decrease_still_matches():
    t = tracker()
    for i in range(5):
        ts = 2000 + i * 5000
        t.apply(change(ts, "BUY", "0.49", "3000"))
        t.apply(change(ts + 1000, "BUY", "0.49", "0"))
        t.apply(trade(ts + 2500, "0.49", "300", "SELL"))    # 比盘口变化晚到 1.5 秒
    t.apply(change(40_000, "BUY", "0.49", "3000"))
    assert report(t)[0]["suspicious_usd"] == 0


def test_trade_outside_the_window_does_not_match():
    t = tracker()
    for i in range(5):
        ts = 2000 + i * 5000
        t.apply(change(ts, "BUY", "0.49", "3000"))
        t.apply(change(ts + 1000, "BUY", "0.49", "0"))
        t.apply(trade(ts + 3500, "0.49", "300", "SELL"))    # 晚 2.5 秒，配不上
    t.apply(change(40_000, "BUY", "0.49", "3000"))
    assert report(t)[0]["levels"][0]["flashes"] == 5


def test_cancel_hits_newest_lot():
    t = tracker()
    # 老块 3000 + 新块 3000；撤 3000 应该撤新块（闪现），老块还在
    t.apply(change(2000, "BUY", "0.49", "3000"))
    for i in range(5):
        ts = 3000 + i * 2000
        t.apply(change(ts, "BUY", "0.49", "6000"))
        t.apply(change(ts + 1000, "BUY", "0.49", "3000"))
    out = report(t)
    assert out[0]["levels"][0]["flashes"] == 5
    assert out[0]["suspicious_usd"] == 1470


def test_trade_hits_oldest_lot():
    t = tracker()
    t.apply(change(2000, "BUY", "0.49", "3000"))
    size = 3000
    for i in range(5):
        ts = 3000 + i * 2000
        t.apply(change(ts, "BUY", "0.49", str(size + 3000)))
        # 成交 300 份：算在老块上，新块成交 0、撤掉后是闪现；算错到新块上就是成交 10%，不算闪现
        t.apply(trade(ts + 500, "0.49", "300", "SELL"))
        size -= 300
        t.apply(change(ts + 500, "BUY", "0.49", str(size + 3000)))
        t.apply(change(ts + 1000, "BUY", "0.49", str(size)))
    level = report(t)[0]["levels"][0]
    assert (level["usd"], level["flashes"], level["filled_usd"]) == (735, 5, 0)


def test_small_far_or_long_lived_lots_are_not_flashes():
    small, far, slow = tracker(), tracker(), tracker()
    flashes(small, size="1000")      # $490
    small.apply(change(40_000, "BUY", "0.49", "1000"))
    flashes(far, price="0.40")       # 离买一 10 分
    far.apply(change(40_000, "BUY", "0.40", "3000"))
    assert report(small)[0]["levels"] == []
    assert report(far)[0]["levels"] == []
    flashes(slow, life=600_000, start=2000)
    slow.apply(change(700_000, "BUY", "0.49", "3000"))
    assert all(s["levels"] == [] for s in slow.advance(800_000))


def test_snapshot_lots_are_not_flashes():
    t = DepthTracker(outcomes={"Y": 1, "N": 2})
    t.apply(book(bids=(("0.50", "100"), ("0.49", "3000"))))
    t.apply(change(2000, "BUY", "0.49", "0"))
    report(t)
    assert t.markets[MARKET].flashes == {}


def test_only_first_outcome_counts():
    t = tracker()
    t.apply(book(asset="N", bids=(("0.48", "9999"),), asks=(("0.50", "9999"),)))
    t.apply(change(2000, "BUY", "0.48", "5000", asset="N"))
    s = report(t)[0]
    assert s["depth_usd"] == 50 + 52      # No 的盘口没有再算一遍


def test_depth_only_within_five_cents_on_both_sides():
    t = DepthTracker(outcomes={"Y": 1, "N": 2})
    t.apply(book(bids=(("0.50", "100"), ("0.45", "100"), ("0.40", "100")), asks=(("0.52", "100"), ("0.60", "100"))))
    assert report(t)[0]["depth_usd"] == 50 + 45 + 52


def test_changes_are_held_for_two_seconds():
    t = tracker()
    t.apply(change(2000, "BUY", "0.49", "3000"))
    assert 0.49 not in t.markets[MARKET].book["BUY"]
    t.apply(change(4000, "BUY", "0.48", "10"))
    assert 0.49 in t.markets[MARKET].book["BUY"]
    assert 0.48 not in t.markets[MARKET].book["BUY"]


def test_message_ordering_uses_each_markets_own_clock():
    t = tracker()
    other = {"source": "clob_ws", "payload": {
        "event_type": "price_change", "market": "0xOther", "timestamp": "99999999", "price_changes": []}}
    t.apply(change(2000, "BUY", "0.49", "3000"))
    assert t.apply(other) == []
    assert 0.49 not in t.markets[MARKET].book["BUY"]


def test_gap_invalidates_book_but_keeps_flash_history():
    t = tracker()
    flashes(t)
    t.apply(change(40_000, "BUY", "0.49", "3000"))
    t.apply(marker("gap"))
    assert t.markets[MARKET].book is None
    assert len(t.markets[MARKET].flashes[("BUY", 0.49)]) == 5
    t.apply(change(41_000, "BUY", "0.49", "9000"))
    assert t.ignored == 1
    assert report(t) == []                              # 盘口不可用时不输出
    t.apply(book(ts=50_000, bids=(("0.50", "100"), ("0.49", "3000"))))
    assert report(t)[0]["suspicious_usd"] == 0          # 快照里的量不知道挂了多久，不算


def test_gap_processes_changes_received_before_it():
    t = tracker()
    for i in range(5):
        flash(t, 2000 + i * 5000)
    t.apply(marker("gap"))                              # 最后一次撤单还压着，断线前要处理完
    assert len(t.markets[MARKET].flashes[("BUY", 0.49)]) == 5


def test_unsubscribed_and_resolved_forget_the_market():
    t = tracker()
    t.apply(marker("unsubscribed"))
    assert MARKET not in t.markets
    t = tracker()
    t.apply({"source": "clob_ws", "payload": {"event_type": "market_resolved", "market": MARKET}})
    assert MARKET not in t.markets


def test_snapshot_does_not_overwrite_live_book():
    t = tracker()
    t.apply(change(2000, "BUY", "0.49", "3000"))
    t.apply(change(5000, "BUY", "0.48", "10"))
    t.apply(book(ts=6000, bids=(("0.50", "1"),)))       # 成交后的快照，时间可能比流旧，不覆盖
    assert 0.49 in t.markets[MARKET].book["BUY"]


def test_rest_snapshot_starts_the_book():
    t = DepthTracker(outcomes={"Y": 1, "N": 2})
    msg = book()
    del msg["payload"]["event_type"]
    msg["source"] = "clob_rest"
    t.apply(msg)
    assert report(t)[0]["depth_usd"] == 50 + 52


def test_quiet_market_outputs_every_interval_with_same_state():
    t = tracker()
    out = t.advance(190_000)
    assert [s["ts"] for s in out] == [60_000, 120_000, 180_000]
    assert t.advance(190_000) == []


def resting(t, ts=30_000, size="3000"):
    """先闪现 5 次让价位被标记，再挂一笔留着。"""
    flashes(t)
    t.apply(change(ts, "BUY", "0.49", size))


def test_only_recently_placed_money_is_suspicious():
    t = tracker()
    t.apply(change(2000, "BUY", "0.49", "3000"))      # 老块，到 720 秒时已经挂了 10 分钟以上
    for i in range(5):
        ts = 700_000 + i * 2000
        t.apply(change(ts, "BUY", "0.49", "6000"))
        t.apply(change(ts + 1000, "BUY", "0.49", "3000"))
    t.apply(change(712_000, "BUY", "0.49", "4000"))   # 新挂 1000 份，$490
    s = [x for x in t.advance(722_000) if x["ts"] == 720_000][0]
    assert s["suspicious_usd"] == 490
    assert s["levels"][0]["usd"] == 1960 and s["levels"][0]["flagged_usd"] == 490


def test_flagged_level_with_only_old_money_is_not_listed():
    t = tracker()
    t.apply(change(2000, "BUY", "0.49", "3000"))
    for i in range(5):
        ts = 3000 + i * 2000
        t.apply(change(ts, "BUY", "0.49", "6000"))
        t.apply(change(ts + 1000, "BUY", "0.49", "3000"))
    out = [x for x in t.advance(662_000) if x["ts"] == 660_000][0]
    assert out["levels"] == [] and out["suspicious_usd"] == 0


def test_cancel_within_a_second_of_a_trade_counts_as_reaction():
    t = tracker()
    resting(t)
    t.apply(trade(40_000, "0.50", "10", "SELL"))      # 主动卖吃买盘：买盘这一侧受影响
    t.apply(change(40_500, "BUY", "0.49", "1000"))    # 撤掉 2/3
    assert report(t)[0]["levels"][0]["reactions"] == 1


def test_reaction_needs_half_the_level_within_a_second():
    small_cut, late_cut = tracker(), tracker()
    resting(small_cut)
    small_cut.apply(trade(40_000, "0.50", "10", "SELL"))
    small_cut.apply(change(40_500, "BUY", "0.49", "2000"))    # 只撤 1/3
    resting(late_cut)
    late_cut.apply(trade(40_000, "0.50", "10", "SELL"))
    late_cut.apply(change(41_500, "BUY", "0.49", "1000"))     # 1.5 秒后才撤
    assert report(small_cut)[0]["levels"][0]["reactions"] == 0
    assert report(late_cut)[0]["levels"][0]["reactions"] == 0


def test_filled_amount_is_not_a_reaction():
    t = tracker()
    resting(t)
    t.apply(trade(40_100, "0.49", "2000", "SELL"))            # 这笔成交吃掉了减少的量
    t.apply(change(40_200, "BUY", "0.49", "1000"))
    assert report(t)[0]["levels"][0]["reactions"] == 0


def test_trades_on_the_other_side_do_not_count():
    t = tracker()
    resting(t)
    t.apply(trade(40_000, "0.52", "10", "BUY"))               # 主动买吃卖盘，买盘这一侧没受影响
    t.apply(change(40_500, "BUY", "0.49", "1000"))
    assert report(t)[0]["levels"][0]["reactions"] == 0


def test_trade_on_second_outcome_triggers_the_converted_side():
    t = tracker()
    resting(t)
    t.apply(trade(40_000, "0.50", "10", "BUY", asset="N"))    # 买 No = 吃 Yes 买盘
    t.apply(change(40_500, "BUY", "0.49", "1000"))
    assert report(t)[0]["levels"][0]["reactions"] == 1


def test_a_split_trade_counts_once():
    t = tracker()
    resting(t)
    t.apply(trade(40_000, "0.50", "10", "SELL"))
    t.apply(trade(40_100, "0.50", "10", "SELL"))              # 同一笔大单拆出来的
    t.apply(change(40_500, "BUY", "0.49", "1000"))
    assert report(t)[0]["levels"][0]["reactions"] == 1


def test_reaction_alone_does_not_flag_a_level():
    t = tracker()
    t.apply(change(30_000, "BUY", "0.49", "3000"))
    t.apply(trade(40_000, "0.50", "10", "SELL"))
    t.apply(change(40_500, "BUY", "0.49", "1000"))
    assert report(t)[0]["levels"] == []
