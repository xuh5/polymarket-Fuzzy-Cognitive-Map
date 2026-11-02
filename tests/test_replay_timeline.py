import json
from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pmdata.book.orderbook import BookStore
from pmdata.replay import bronze
from pmdata.replay.timeline import build

TOKENS = {"Y": 1, "N": 2}


def book_msg(asset, bids, asks, ts):
    return {"source": "clob_ws", "payload": {
        "event_type": "book", "market": "0xM", "asset_id": asset, "timestamp": str(ts),
        "bids": [{"price": p, "size": s} for p, s in bids], "asks": [{"price": p, "size": s} for p, s in asks]}}


def change_msg(ts, *changes):
    return {"source": "clob_ws", "payload": {
        "event_type": "price_change", "market": "0xM", "timestamp": str(ts),
        "price_changes": [{"asset_id": a, "side": side, "price": p, "size": s} for a, side, p, s in changes]}}


def trade_msg(ts, asset="Y", side="BUY", price="0.45", size="10"):
    return {"source": "clob_ws", "payload": {
        "event_type": "last_trade_price", "market": "0xM", "asset_id": asset, "timestamp": str(ts),
        "side": side, "price": price, "size": size}}


def gap_msg(received_at):
    return {"source": "collector", "received_at": received_at,
            "payload": {"event_type": "gap", "market": "0xM", "reason": "重连"}}


def both_books(ts):
    return [book_msg("Y", [("0.40", "10"), ("0.44", "5")], [("0.46", "3")], ts),
            book_msg("N", [("0.54", "3")], [("0.56", "5"), ("0.60", "10")], ts)]


def apply_events(initial, events):
    """前端要做的事：从起点把每条变化依次套上去"""
    books = {o: None if b is None else {"bid": dict(b["bids"]), "ask": dict(b["asks"])} for o, b in initial.items()}
    for e in events:
        if e["type"] == "book":
            books[e["o"]] = {"bid": dict(e["bids"]), "ask": dict(e["asks"])}
        elif e["type"] == "change":
            side = books[e["o"]][e["side"]]
            if e["size"] == 0:
                side.pop(e["price"], None)
            else:
                side[e["price"]] = e["size"]
        elif e["type"] in ("gap", "closed"):
            books = {o: None for o in books}
    return books


def test_messages_before_start_build_initial_only():
    envs = [*both_books(1000), change_msg(2000, ("Y", "BUY", "0.44", "8")),
            change_msg(5000, ("Y", "SELL", "0.47", "2"))]
    r = build(envs, TOKENS, 3000, 9000)
    assert r["initial"][1] == {"bids": [[0.44, 8.0], [0.40, 10.0]], "asks": [[0.46, 3.0]]}   # 已经套上 2000 那条
    assert r["initial"][2] == {"bids": [[0.54, 3.0]], "asks": [[0.56, 5.0], [0.60, 10.0]]}
    assert r["events"] == [{"t": 5000, "type": "change", "o": 1, "side": "ask", "price": 0.47, "size": 2.0}]


def test_stops_after_end():
    envs = [*both_books(1000), change_msg(5000, ("Y", "BUY", "0.41", "1")), change_msg(9001, ("Y", "BUY", "0.42", "1"))]
    assert [e["t"] for e in build(envs, TOKENS, 3000, 9000)["events"]] == [5000]


def test_no_book_yet_means_unavailable_and_changes_hidden():
    # 起点前没有清单：盘口不可用，在它上面的变化实时也忽略，回放里不出现
    envs = [change_msg(4000, ("Y", "BUY", "0.41", "1")), *both_books(5000), change_msg(6000, ("Y", "BUY", "0.41", "2"))]
    r = build(envs, TOKENS, 3000, 9000)
    assert r["initial"] == {1: None, 2: None}
    assert [(e["type"], e["t"]) for e in r["events"]] == [("book", 5000), ("book", 5000), ("change", 6000)]


def test_gap_invalidates_until_next_book():
    envs = [*both_books(1000), gap_msg("1970-01-01T00:00:05+00:00"),
            change_msg(6000, ("Y", "BUY", "0.41", "1")), *both_books(7000)]
    r = build(envs, TOKENS, 3000, 9000)
    assert [(e["type"], e["t"]) for e in r["events"]] == [("gap", 5000), ("book", 7000), ("book", 7000)]


def test_trade_event():
    envs = [*both_books(1000), trade_msg(5000, asset="N", side="SELL", price="0.55", size="12.5")]
    assert build(envs, TOKENS, 3000, 9000)["events"] == [
        {"t": 5000, "type": "trade", "o": 2, "side": "SELL", "price": 0.55, "size": 12.5}]


def test_empty_window_uses_state_at_end_of_input():
    r = build([*both_books(1000)], TOKENS, 3000, 9000)
    assert r["initial"][1]["asks"] == [[0.46, 3.0]] and r["events"] == []


def test_applying_events_reproduces_book_store():
    envs = [*both_books(1000), change_msg(4000, ("Y", "BUY", "0.44", "0"), ("N", "SELL", "0.56", "0")),
            change_msg(5000, ("Y", "BUY", "0.45", "7")), gap_msg("1970-01-01T00:00:06+00:00"),
            change_msg(6500, ("Y", "BUY", "0.46", "1")), *both_books(7000), change_msg(8000, ("N", "BUY", "0.55", "4"))]
    r = build(envs, TOKENS, 3000, 9000)
    store = BookStore()
    for env in envs:
        store.apply(env)
    got = apply_events(r["initial"], r["events"])
    for aid, o in TOKENS.items():
        book = store.get(aid)
        assert got[o] == {"bid": {float(p): float(s) for p, s in book.bids.items()},
                          "ask": {float(p): float(s) for p, s in book.asks.items()}}


def write_bronze(root, rows):
    """按 archiver 的列写一个迷你 bronze 文件"""
    day = root / "date=2026-10-29"
    day.mkdir(parents=True)
    pq.write_table(pa.table({
        "kafka_partition": [r[0] for r in rows], "kafka_offset": [r[1] for r in rows],
        "kafka_timestamp": pa.array([r[2] for r in rows], pa.timestamp("ms", tz="UTC")),
        "source": ["clob_ws"] * len(rows), "event_type": [r[3]["payload"]["event_type"] for r in rows],
        "market": ["0xM"] * len(rows), "value": [json.dumps(r[3]) for r in rows]}),
        day / "part-1.parquet")


def at(minute):
    return datetime(2026, 10, 29, 12, minute, tzinfo=UTC)


def test_bronze_start_is_oldest_latest_book_and_load_dedupes(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "CLOB_DIR", tmp_path)
    y1, n1, y2 = book_msg("Y", [], [], 1), book_msg("N", [], [], 1), book_msg("Y", [], [], 2)
    ch = change_msg(3, ("Y", "BUY", "0.4", "1"))
    write_bronze(tmp_path, [(0, 1, at(1), y1), (0, 2, at(2), n1), (0, 3, at(5), y2),
                            (0, 4, at(20), ch), (0, 4, at(20), ch)])   # 最后一条是 archiver 重复写的
    with duckdb.connect() as con:
        # Y 最近一份在 12:05，N 在 12:02：要从 12:02 开始，N 才有起点
        assert bronze.find_start(con, "0xM", at(10), 2) == at(2)
        envs = bronze.load(con, "0xM", at(2), at(30))
    assert [e["payload"]["timestamp"] for e in envs] == ["1", "2", "3"]


def test_bronze_periods_split_at_unsubscribe(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "CLOB_DIR", tmp_path)
    ch, unsub = change_msg(1, ("Y", "BUY", "0.4", "1")), {"payload": {"event_type": "unsubscribed"}}
    unsub_at = datetime(2026, 10, 29, 12, 10, 59, 900000, tzinfo=UTC)
    stray_at = datetime(2026, 10, 29, 12, 11, 0, 100000, tzinfo=UTC)   # 退订后 0.2 秒的在途消息，跨了分钟
    write_bronze(tmp_path, [(0, 1, at(1), ch), (0, 2, at(5), ch), (0, 3, unsub_at, unsub),
                            (0, 4, stray_at, ch), (0, 5, at(40), ch), (0, 6, at(50), ch)])
    assert bronze.scan_periods() == {"0xM": [(at(1), unsub_at), (at(40), at(50))]}
