import json

import redis

from pmdata.book.cache import FULL_SYNC_SECONDS, TTL_SECONDS, BookCache
from pmdata.book.orderbook import BookStore


class FakeRedis:
    """代替 Redis：pipeline 里的命令在 execute 时才生效，和真的一样。"""

    def __init__(self):
        self.data = {}
        self.ttl = {}
        self.published = []
        self.fail = False
        self._ops = []

    def pipeline(self, transaction=False):
        self._ops = []
        return self

    def set(self, key, value, ex=None):
        self._ops.append(("set", key, value, ex))

    def delete(self, key):
        self._ops.append(("delete", key))

    def publish(self, channel, msg):
        self._ops.append(("publish", channel, msg))

    def scan_iter(self, pattern):
        prefix = pattern.rstrip("*")
        return [k.encode() for k in self.data if k.startswith(prefix)]

    def execute(self):
        if self.fail:
            raise redis.ConnectionError("连不上")
        for op in self._ops:
            if op[0] == "set":
                self.data[op[1]], self.ttl[op[1]] = op[2], op[3]
            elif op[0] == "delete":
                self.data.pop(op[1], None)
            else:
                self.published.append(json.loads(op[2]))


def book_env(aid="Y", ts=1000):
    return {"source": "clob_ws", "payload": {
        "event_type": "book", "market": "0xM", "asset_id": aid, "timestamp": str(ts),
        "bids": [{"price": "0.40", "size": "10"}, {"price": "0.45", "size": "5"}],
        "asks": [{"price": "0.55", "size": "7"}, {"price": "0.50", "size": "3"}]}}


def change_env(ts, price="0.46", size="2"):
    return {"source": "clob_ws", "payload": {
        "event_type": "price_change", "market": "0xM", "timestamp": str(ts),
        "price_changes": [{"asset_id": "Y", "side": "BUY", "price": price, "size": size}]}}


def gap_env():
    return {"source": "collector", "payload": {"event_type": "gap", "market": "0xM"}}


def setup():
    store, r = BookStore(), FakeRedis()
    return store, r, BookCache(r, clock=lambda: "2026-10-28T10:00:00.000+00:00")


def test_writes_book_sorted_with_ttl_and_publishes():
    store, r, cache = setup()
    store.apply(book_env())
    cache.sync(store, now=0)
    v = json.loads(r.data["book:Y"])
    assert v["bids"] == [["0.45", "5"], ["0.40", "10"]]     # 价高在前
    assert v["asks"] == [["0.50", "3"], ["0.55", "7"]]      # 价低在前
    assert v["market"] == "0xM" and v["exchange_ts"] == 1000
    assert r.ttl["book:Y"] == TTL_SECONDS
    assert r.published == [{"asset_id": "Y", "market": "0xM"}]


def test_only_changed_books_are_written_between_full_syncs():
    store, r, cache = setup()
    store.apply(book_env())
    cache.sync(store, now=0)
    cache.sync(store, now=1)                  # 没变：不写也不通知
    assert len(r.published) == 1
    store.apply(change_env(ts=2000))
    cache.sync(store, now=2)
    assert json.loads(r.data["book:Y"])["bids"][0] == ["0.46", "2"]
    assert len(r.published) == 2


def test_same_millisecond_change_after_sync_is_written():
    # 交易所会把同一时刻的更新拆成几条消息（时间戳相同）；同步刚好卡在中间时，后面那条也要写进去
    store, r, cache = setup()
    store.apply(book_env(ts=1000))
    cache.sync(store, now=0)
    store.apply(change_env(ts=1000))
    cache.sync(store, now=1)
    assert json.loads(r.data["book:Y"])["bids"][0] == ["0.46", "2"]


def test_full_sync_rewrites_without_publishing():
    store, r, cache = setup()
    store.apply(book_env())
    cache.sync(store, now=0)
    del r.data["book:Y"]                      # 比如 Redis 重启丢了
    cache.sync(store, now=FULL_SYNC_SECONDS)
    assert "book:Y" in r.data
    assert len(r.published) == 1


def test_gap_deletes_key():
    store, r, cache = setup()
    store.apply(book_env())
    cache.sync(store, now=0)
    store.apply(gap_env())
    cache.sync(store, now=1)
    assert "book:Y" not in r.data


def test_full_sync_removes_leftover_keys():
    store, r, cache = setup()
    r.data["book:OLD"] = "{}"                 # metrics 重启前写的、现在已经没有的盘口
    store.apply(book_env())
    cache.sync(store, now=0)
    assert set(r.data) == {"book:Y"}


def test_redis_down_does_not_raise_and_retries_full():
    store, r, cache = setup()
    store.apply(book_env())
    r.fail = True
    cache.sync(store, now=0)                  # 只打警告
    assert r.data == {}
    r.fail = False
    cache.sync(store, now=1)                  # 下一秒全量补上
    assert "book:Y" in r.data
