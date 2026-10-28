"""把当前订单簿写进 Redis，给网站后端读：订单簿只在 metrics 里算一份，别的服务都读它的结果。

Redis 里的格式（网站后端依赖它，改了要同步改 pmdata.api）：
- 键 book:<asset_id>，值是 JSON：{asset_id, market, exchange_ts（最后一次更新的交易所时间，毫秒）,
  written_at（UTC ISO）, bids: [[价格, 数量], ...]（价高在前）, asks: [[价格, 数量], ...]（价低在前）}；
  价格和数量是字符串，Decimal 原样，不丢精度
- 盘口作废（断线）、市场退订 / 结算：删掉这个键。网站读不到 = 这个盘口暂时不可用
- 每个键 TTL_SECONDS 后过期：metrics 挂了，旧盘口最多留这么久，不会一直被当成最新的显示
- 盘口变了写入后，在频道 book-updates 发 {asset_id, market}，网站后端订阅它做实时推送

每秒同步一次：只写变了的盘口（看 Book 的改动次数 version，不用 engine 告诉我们），删掉作废的。
每 FULL_SYNC_SECONDS 全量同步一次：重写所有可用盘口（刷新过期时间、补上之前写失败的），删掉 Redis 里多出来的键
（比如 metrics 重启前写的、现在已经作废的）。
写 Redis 失败只打警告、下次改成全量同步，不影响指标输出：这个缓存不在 Kafka 那条「不丢不重复」的链路上，丢了随时能重写。
"""
import json
import logging
import math

import redis

from pmdata.common.timeutil import now_iso

log = logging.getLogger("book-cache")

KEY_PREFIX = "book:"
CHANNEL = "book-updates"
TTL_SECONDS = 120
FULL_SYNC_SECONDS = 30


def book_json(asset_id, book, written_at):
    return json.dumps({
        "asset_id": asset_id,
        "market": book.market,
        "exchange_ts": book.exchange_ts,
        "written_at": written_at,
        "bids": [[str(p), str(s)] for p, s in book.top("BUY", None)],
        "asks": [[str(p), str(s)] for p, s in book.top("SELL", None)],
    })


class BookCache:
    def __init__(self, client, clock=now_iso):
        self.client = client
        self.clock = clock
        self.written = {}            # asset_id → 写进 Redis 的那份盘口的 (Book 对象 id, version)
        self.last_full = -math.inf   # 上次全量同步的时间（time.monotonic()）

    def sync(self, store, now):
        """把 store（BookStore）同步到 Redis。now 是 time.monotonic()，决定这次要不要全量。"""
        full = now - self.last_full >= FULL_SYNC_SECONDS
        written_at = self.clock()
        pipe = self.client.pipeline(transaction=False)
        new_written = dict(self.written)
        valid = {aid: b for aid, b in store.books.items() if b.valid}

        for aid, book in valid.items():
            # 带上对象 id：盘口被删掉又重建时 version 从 0 重新数，光比 version 可能碰巧相等
            ver = (id(book), book.version)
            changed = self.written.get(aid) != ver
            if changed or full:
                pipe.set(KEY_PREFIX + aid, book_json(aid, book, written_at), ex=TTL_SECONDS)
                new_written[aid] = ver
            if changed:
                pipe.publish(CHANNEL, json.dumps({"asset_id": aid, "market": book.market}))

        stale = set(self.written) - set(valid)
        try:
            if full:
                stale |= {k.decode()[len(KEY_PREFIX):] for k in self.client.scan_iter(KEY_PREFIX + "*")} - set(valid)
            for aid in stale:
                pipe.delete(KEY_PREFIX + aid)
                new_written.pop(aid, None)
            pipe.execute()
        except redis.RedisError as e:
            log.warning("写 Redis 失败（下次全量同步）：%s", e)
            self.last_full = -math.inf
            return
        self.written = new_written
        if full:
            self.last_full = now
