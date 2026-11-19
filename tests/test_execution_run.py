import json

from pmdata.execution.run import Runner

T0 = 2021


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
        "bids": [{"price": "0.50", "size": "100"}], "asks": [{"price": "0.52", "size": "100"}]}}


def take_ask(ts):
    return {"source": "clob_ws", "payload": {
        "event_type": "price_change", "market": "0xM", "timestamp": str(ts),
        "price_changes": [{"asset_id": "Y", "side": "SELL", "price": "0.52", "size": "60"}]}}


def trade(ts):
    return {"source": "clob_ws", "payload": {
        "event_type": "last_trade_price", "market": "0xM", "asset_id": "Y", "timestamp": str(ts),
        "price": "0.52", "size": "40", "side": "BUY", "fee_rate_bps": "0", "transaction_hash": "0xT"}}


def feed(r, offsets_envs):
    for off, env in offsets_envs:
        r.handle(FakeMsg(0, off, env))


def test_warmup_updates_state_but_does_not_output():
    p = FakeProducer()
    r = Runner(consumer=None, producer=p)
    r.resume, r.position = {0: 4}, {0: 0}
    feed(r, [(0, book(1000)), (1, take_ask(T0 - 21)), (2, trade(T0)),
             (3, book(T0 + 400_000))])                 # 这条让 5 分钟到期，但还在预热：不输出
    assert p.messages == []
    assert r.caught_up()


def test_output_after_resume_and_marked_with_computed_at():
    p = FakeProducer()
    r = Runner(consumer=None, producer=p)
    r.resume, r.position = {0: 3}, {0: 0}
    feed(r, [(0, book(1000)), (1, take_ask(T0 - 21)), (2, trade(T0)),
             (3, book(T0 + 400_000))])                 # 到了上次提交的位置：输出
    [(topic, key, rec)] = p.messages
    assert (topic, key) == ("pm.metrics.execution", "0xM")
    assert rec["tx_hash"] == "0xT" and rec["pre_ask"] == 0.52 and rec["pre_match"] == "exact"
    assert rec["computed_at"]


def test_idle_advance_only_after_caught_up():
    p = FakeProducer()
    r = Runner(consumer=None, producer=p)
    r.resume, r.position = {0: 5}, {0: 0}
    feed(r, [(0, book(1000)), (1, take_ask(T0 - 21)), (2, trade(T0))])
    r.flush_idle(now_ms=T0 + 400_000)                  # 预热中：电脑时间不能拿来推进
    assert p.messages == []
    r.position[0] = 5
    r.flush_idle(now_ms=T0 + 400_000)
    assert len(p.messages) == 1


def test_commit_skips_partitions_still_warming():
    class FakeConsumer:
        committed = None

        def commit(self, offsets, asynchronous):
            self.committed = [(tp.partition, tp.offset) for tp in offsets]

    c = FakeConsumer()
    r = Runner(consumer=c, producer=FakeProducer())
    r.resume, r.position = {0: 10, 1: 10}, {0: 12, 1: 3}
    r.commit()
    assert c.committed == [(0, 12)]
