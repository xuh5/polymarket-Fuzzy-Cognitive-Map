import json

from pmdata.metrics import run
from pmdata.metrics.run import Runner, plan_start


class TestPlanStart:
    def test_restart_warms_up_from_24h_ago(self):
        assert plan_start(committed=500, warm=100, low=0, high=900) == (100, 500)

    def test_first_start_outputs_from_end(self):
        assert plan_start(committed=None, warm=100, low=0, high=900) == (100, 900)

    def test_stopped_longer_than_warmup_reads_from_commit(self):
        assert plan_start(committed=50, warm=100, low=0, high=900) == (50, 50)

    def test_no_message_in_warmup_window(self):
        assert plan_start(committed=900, warm=None, low=0, high=900) == (900, 900)

    def test_old_data_deleted_by_retention(self):
        assert plan_start(committed=None, warm=None, low=300, high=200) == (300, 200)


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


def book(ts, bid):
    return {"source": "clob_ws", "received_at": "R", "payload": {
        "event_type": "book", "market": "0xM", "asset_id": "Y", "timestamp": str(ts),
        "bids": [{"price": bid, "size": "1"}], "asks": [{"price": "0.9", "size": "1"}]}}


def test_warmup_updates_state_but_does_not_output():
    producer = FakeProducer()
    r = Runner(consumer=None, producer=producer)
    r.resume, r.position = {0: 2}, {0: 0}
    r.handle(FakeMsg(0, 0, book(1000, "0.1")))
    assert not r.caught_up()
    r.handle(FakeMsg(0, 1, book(2000, "0.2")))     # 结束第 1 秒，但还在预热：不输出
    assert producer.messages == []
    assert r.caught_up()                            # 下一条要读的就是上次提交的位置
    r.handle(FakeMsg(0, 2, book(3000, "0.3")))     # 到了上次提交的位置：结束第 2 秒，输出
    assert [m[2]["best_bid"] for m in producer.messages] == [0.2]


def test_idle_flush_only_after_caught_up(monkeypatch):
    producer = FakeProducer()
    r = Runner(consumer=None, producer=producer)
    r.resume, r.position = {0: 5}, {0: 0}
    r.handle(FakeMsg(0, 0, book(1000, "0.1")))
    r.flush_idle(now_ms=10**13)                     # 预热中：不能用现在的时间兜底
    assert producer.messages == []
    r.position[0] = 5
    r.flush_idle(now_ms=10**13)
    assert len(producer.messages) == 1


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
    assert run.TOPIC_CLOB == "pm.raw.clob"
