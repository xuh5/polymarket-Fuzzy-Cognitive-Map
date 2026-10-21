from decimal import Decimal

from pmdata.book.orderbook import BookStore


def book_msg(asset, bids, asks, ts="1000", market="0xM", source="clob_ws"):
    payload = {"market": market, "asset_id": asset, "timestamp": ts,
               "bids": [{"price": p, "size": s} for p, s in bids],
               "asks": [{"price": p, "size": s} for p, s in asks]}
    if source == "clob_ws":
        payload["event_type"] = "book"
    return {"source": source, "payload": payload}


def change_msg(*changes, ts="2000", market="0xM"):
    return {"source": "clob_ws", "payload": {
        "event_type": "price_change", "market": market, "timestamp": ts,
        "price_changes": [{"asset_id": a, "side": side, "price": p, "size": s} for a, side, p, s in changes]}}


def marker(event_type, market="0xM"):
    return {"source": "collector", "payload": {"event_type": event_type, "market": market}}


def test_snapshot_then_best_prices():
    store = BookStore()
    # 原数组不是按最优价排序的（实测买盘从低到高），不能拿第一档当买一
    assert store.apply(book_msg("Y", [("0.40", "10"), ("0.45", "5")], [("0.50", "7"), ("0.48", "3")])) == ["Y"]
    book = store.get("Y")
    assert book.best_bid() == Decimal("0.45")
    assert book.best_ask() == Decimal("0.48")
    assert book.top("BUY", 2) == [(Decimal("0.45"), Decimal("5")), (Decimal("0.40"), Decimal("10"))]
    assert book.top("SELL", 1) == [(Decimal("0.48"), Decimal("3"))]


def test_price_change_size_is_total_and_zero_removes():
    store = BookStore()
    store.apply(book_msg("Y", [("0.45", "5")], [("0.48", "3")]))
    store.apply(change_msg(("Y", "BUY", "0.45", "8")))      # 覆盖成 8，不是 5 + 8
    store.apply(change_msg(("Y", "SELL", "0.48", "0")))     # 0 = 这一档没了
    store.apply(change_msg(("Y", "SELL", "0.49", "2")))     # 新的一档
    book = store.get("Y")
    assert book.bids == {Decimal("0.45"): Decimal("8")}
    assert book.asks == {Decimal("0.49"): Decimal("2")}
    assert book.exchange_ts == 2000


def test_price_string_formats_are_same_level():
    store = BookStore()
    store.apply(book_msg("Y", [("0.5", "5")], []))
    store.apply(change_msg(("Y", "BUY", "0.50", "0")))
    assert store.get("Y").bids == {}


def test_one_message_updates_both_tokens():
    store = BookStore()
    store.apply(book_msg("Y", [("0.45", "5")], []))
    store.apply(book_msg("N", [], [("0.55", "5")]))
    assert store.apply(change_msg(("Y", "BUY", "0.46", "1"), ("N", "SELL", "0.54", "1"), ("Y", "BUY", "0.45", "0"))) \
        == ["Y", "N"]
    assert store.get("Y").best_bid() == Decimal("0.46")
    assert store.get("N").best_ask() == Decimal("0.54")


def test_change_before_snapshot_is_ignored():
    store = BookStore()
    assert store.apply(change_msg(("Y", "BUY", "0.45", "5"))) == []
    assert store.get("Y") is None
    assert store.ignored == 1


def test_gap_invalidates_until_next_snapshot():
    store = BookStore()
    store.apply(book_msg("Y", [("0.45", "5")], []))
    store.apply(book_msg("Z", [("0.10", "5")], [], market="0xOther"))
    store.apply(marker("gap"))
    assert store.get("Y") is None
    assert store.get("Z") is not None                      # 别的市场不受影响
    store.apply(change_msg(("Y", "BUY", "0.46", "1")))     # 作废期间的变化被忽略
    assert store.ignored == 1
    store.apply(book_msg("Y", [("0.44", "2")], [], source="clob_rest"))   # REST 补拉的快照同样有效
    assert store.get("Y").bids == {Decimal("0.44"): Decimal("2")}


def test_unsubscribed_and_resolved_drop_market():
    store = BookStore()
    store.apply(book_msg("Y", [("0.45", "5")], []))
    store.apply(marker("unsubscribed"))
    assert "Y" not in store.books
    store.apply(book_msg("Y", [("0.45", "5")], []))
    store.apply({"source": "clob_ws", "payload": {"event_type": "market_resolved", "market": "0xM"}})
    assert "Y" not in store.books


def test_events_that_do_not_touch_book():
    store = BookStore()
    store.apply(book_msg("Y", [("0.45", "5")], []))
    for et in ("last_trade_price", "tick_size_change"):
        assert store.apply({"source": "clob_ws", "payload": {"event_type": et, "market": "0xM", "asset_id": "Y"}}) == []
    assert store.get("Y").bids == {Decimal("0.45"): Decimal("5")}
