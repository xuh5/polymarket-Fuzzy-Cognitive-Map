import asyncio
import json

import pytest

from pmdata.ingest import collect_clob


class FakeProducer:
    """代替真正的 Kafka producer：只把要写的消息记下来。"""

    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery=None):
        self.messages.append((topic, key, json.loads(value)))

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        return 0


def selection(*markets):
    return [
        {"condition_id": m, "question": f"Q {m}", "tokens": [{"token_id": f"{m}-yes"}, {"token_id": f"{m}-no"}]}
        for m in markets
    ]


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr(collect_clob, "make_producer", FakeProducer)
    c = collect_clob.Collector(selection("0xA", "0xB"))
    c.conn_id = "conn1"
    return c


class TestMarketKey:
    def test_uses_market_field(self, collector):
        assert collector.market_key({"market": "0xZ", "asset_id": "0xA-yes"}) == "0xZ"

    def test_falls_back_to_asset_id_lookup(self, collector):
        assert collector.market_key({"asset_id": "0xA-no"}) == "0xA"

    def test_unknown_asset_counts_warning_and_returns_none(self, collector):
        assert collector.market_key({"asset_id": "nope"}) is None
        assert collector.stats["无法确定市场"] == 1


class TestHandleFrame:
    def test_array_frame_is_split_one_message_per_event(self, collector):
        frame = [{"event_type": "book", "market": "0xA", "asset_id": "0xA-yes"},
                 {"event_type": "book", "market": "0xA", "asset_id": "0xA-no"}]
        collector.handle_frame(json.dumps(frame), "2026-10-18T00:00:00.000+00:00")

        msgs = collector.producer.messages
        assert [(m[1], m[2]["seq"], m[2]["frame_index"]) for m in msgs] == [("0xA", 1, 0), ("0xA", 1, 1)]
        assert msgs[0][0] == "pm.raw.clob"
        assert msgs[0][2]["conn_id"] == "conn1"
        assert msgs[0][2]["payload"] == frame[0]          # 原始数据一字不改
        assert collector.books_seen == {"0xA-yes", "0xA-no"}

    def test_new_market_is_counted_but_not_written(self, collector):
        collector.handle_frame(json.dumps({"event_type": "new_market", "market": "0xNEW"}), "t")
        assert collector.producer.messages == []
        assert collector.stats["new_market"] == 1

    def test_non_json_frame_is_counted(self, collector):
        collector.handle_frame("not json", "t")
        assert collector.stats["非 JSON 帧"] == 1
        assert collector.seq == 1


def test_refresh_writes_markers_for_added_and_removed_markets(collector):
    asyncio.run(collector.apply_selection(selection("0xB", "0xC")))   # 0xA 移出，0xC 加入

    markers = {(m[1], m[2]["payload"]["event_type"]) for m in collector.producer.messages}
    assert markers == {("0xA", "unsubscribed"), ("0xC", "gap")}
    assert set(collector.subscribed) == {"0xB-yes", "0xB-no", "0xC-yes", "0xC-no"}
    assert collector.asset_to_market["0xA-yes"] == "0xA"   # 退订后迟到的消息仍能换算 key


def test_refresh_with_empty_selection_keeps_old_list(collector):
    asyncio.run(collector.apply_selection([]))
    assert set(collector.subscribed) == {"0xA-yes", "0xA-no", "0xB-yes", "0xB-no"}
