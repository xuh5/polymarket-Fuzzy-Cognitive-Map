from pmdata.health.quality import StreamQuality

T = "2026-10-29T10:00:00.000+00:00"


def env(payload, source="clob_ws", received_at=T):
    return {"source": source, "received_at": received_at, "payload": payload}


def book(ts=1000, bid="0.40", ask="0.60"):
    return env({"event_type": "book", "market": "M", "asset_id": "Y", "timestamp": str(ts),
                "bids": [{"price": bid, "size": "10"}], "asks": [{"price": ask, "size": "10"}]})


def change(ts, price, size, side, best_bid, best_ask, received_at=T):
    return env({"event_type": "price_change", "market": "M", "timestamp": str(ts), "price_changes": [
        {"asset_id": "Y", "price": price, "size": size, "side": side, "best_bid": best_bid, "best_ask": best_ask}]},
        received_at=received_at)


def counts(q):
    return dict(q.minutes["2026-10-29T10:00"])


def test_matching_book_is_checked_not_mismatched():
    q = StreamQuality()
    for e in [book(), change(1001, "0.45", "5", "BUY", "0.45", "0.6"), book(1002)]:
        q.apply(e)
    c = counts(q)
    assert c["checked"] == 1 and c.get("mismatched", 0) == 0


def test_split_update_compared_after_whole_timestamp():
    # 同一时刻拆成两条：第一条带的已经是两条都改完的最优价，中间状态不能算对不上
    q = StreamQuality()
    for e in [book(), change(1001, "0.45", "5", "BUY", "0.46", "0.6"),
              change(1001, "0.46", "5", "BUY", "0.46", "0.6"), book(1002)]:
        q.apply(e)
    assert counts(q)["checked"] == 1 and counts(q).get("mismatched", 0) == 0


def test_missed_message_shows_as_mismatch():
    q = StreamQuality()
    for e in [book(), change(1001, "0.45", "5", "BUY", "0.47", "0.6"), book(1002)]:   # 漏了 0.47 那一条
        q.apply(e)
    assert counts(q)["mismatched"] == 1


def test_out_of_order_and_ignored():
    q = StreamQuality()
    q.apply(change(1000, "0.45", "5", "BUY", "0.45", "0.6"))   # 还没快照：忽略
    q.apply(change(999, "0.44", "5", "BUY", "0.45", "0.6"))    # 时间倒退
    c = counts(q)
    assert c["ignored"] == 2 and c["out_of_order"] == 1 and c["messages"] == 2


def test_gap_resets_order_and_no_counting_during_warmup():
    q = StreamQuality()
    q.counting = False
    q.apply(change(1000, "0.45", "5", "BUY", "0.45", "0.6"))
    assert q.minutes == {}
    q.counting = True
    q.apply(env({"event_type": "gap", "market": "M"}, source="collector"))
    q.apply(change(900, "0.45", "5", "BUY", "0.45", "0.6"))    # 重连后从头比，不算乱序
    assert counts(q).get("out_of_order", 0) == 0
    assert q.rows()[0][0] == "2026-10-29T10:00:00+00:00"
