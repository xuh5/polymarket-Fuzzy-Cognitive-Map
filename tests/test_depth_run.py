import json

from pmdata.depth.run import Runner


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery=None):
        self.messages.append((topic, key, json.loads(value)))

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        return 0


class FakeMsg:
    def __init__(self, partition, offset, env):
        self._p, self._o, self._v = partition, offset, json.dumps(env).encode()

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v


def book(ts):
    return {"source": "clob_ws", "payload": {
        "event_type": "book", "market": "0xM", "asset_id": "Y", "timestamp": str(ts),
        "bids": [{"price": "0.5", "size": "100"}], "asks": [{"price": "0.52", "size": "100"}]}}


def runner(producer, consumer=None):
    r = Runner(consumer=consumer, producer=producer)
    r.tracker.outcomes = {"Y": 1}
    return r


def test_warmup_updates_state_but_does_not_output():
    p = FakeProducer()
    r = runner(p)
    r.resume, r.position = {0: 2}, {0: 0}
    r.handle(FakeMsg(0, 0, book(1000)))
    r.handle(FakeMsg(0, 1, book(70_000)))        # 这条让第 60 秒到期，但还在预热：不输出
    assert p.messages == []
    assert r.caught_up()
    r.handle(FakeMsg(0, 2, book(135_000)))       # 到了上次提交的位置：第 120 秒到期，输出
    assert [(t, k, m["ts"]) for t, k, m in p.messages] == [("pm.metrics.depth", "0xM", 120_000)]
    assert p.messages[0][2]["depth_usd"] == 50 + 52
    assert p.messages[0][2]["computed_at"]


def test_idle_advance_only_after_caught_up():
    p = FakeProducer()
    r = runner(p)
    r.resume, r.position = {0: 5}, {0: 0}
    r.handle(FakeMsg(0, 0, book(1000)))
    r.flush_idle(now_ms=65_000)                  # 预热中：电脑时间不能拿来推进
    assert p.messages == []
    r.position[0] = 5
    r.flush_idle(now_ms=65_000)
    assert [m[2]["ts"] for m in p.messages] == [60_000]


def test_commit_skips_partitions_still_warming():
    class FakeConsumer:
        committed = None

        def commit(self, offsets, asynchronous):
            self.committed = [(tp.partition, tp.offset) for tp in offsets]

    c = FakeConsumer()
    r = runner(FakeProducer(), c)
    r.resume, r.position = {0: 10, 1: 10}, {0: 12, 1: 3}
    r.commit()
    assert c.committed == [(0, 12)]
