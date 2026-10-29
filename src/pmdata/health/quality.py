"""行情流质量：乱序和「盘口对不上」，纯逻辑，不碰 Kafka 和数据库。

Polymarket 的行情消息没有序号，丢没丢消息不能直接数，所以用两个间接指标：
- 乱序（out_of_order）：同一市场的 price_change，Kafka 里排在后面的，交易所时间反而更早。
  我们按市场分区，管道内部不会打乱顺序，数到的是交易所推送本身的乱序（实测一天约 180 次，几乎都只差 1～7 毫秒）。
  只比 price_change 之间：best_bid_ask 等别的事件和 price_change 的时间戳本来就差 1 毫秒，混着比全是假乱序。
- 盘口对不上（mismatched）：每条 price_change 带着交易所改完之后的买一卖一，本地重建的订单簿和它不一样，
  就说明中间漏了消息（或顺序错了）。交易所会把同一时刻的一次更新拆成几条消息，
  所以同一个 token 同一时间戳的消息全部处理完再比（等这个 token 出现新时间戳、新快照或缺口标记时再比）。
  交易所用 best_bid=0 / best_ask=1 表示这一侧为空。checked 是比了多少次。
- 盘口不可用时忽略的 price_change（ignored）：断线后到下一个快照之间的变化，正常情况下只在重连后的几秒里出现。

按收到时间（received_at）归到分钟：Status 页面要看的是「那一分钟管道怎么样」。
"""
from collections import Counter
from decimal import Decimal

from pmdata.book.orderbook import BookStore

FIELDS = ["messages", "checked", "mismatched", "out_of_order", "ignored"]

SCHEMA = """
create table if not exists health_stream_quality (
    minute timestamptz primary key,   -- 按收到时间（received_at）归到分钟
    messages integer not null,        -- 这一分钟收到的 pm.raw.clob 消息数
    checked integer not null,         -- 比对了几次盘口
    mismatched integer not null,      -- 其中本地盘口和交易所给的买一卖一对不上的次数
    out_of_order integer not null,    -- price_change 交易所时间倒退的次数
    ignored integer not null          -- 盘口不可用时被忽略的变化条数
);
"""

# 程序重启时，重启那一分钟在内存里只有重启之后的计数，比库里已有的少；取较大的，不让数字变小
UPSERT = (f"insert into health_stream_quality (minute, {', '.join(FIELDS)}) "
          f"values (%s, {', '.join(['%s'] * len(FIELDS))}) on conflict (minute) do update set "
          + ", ".join(f"{f} = greatest(health_stream_quality.{f}, excluded.{f})" for f in FIELDS))


class StreamQuality:
    def __init__(self):
        self.store = BookStore()
        self.pending = {}       # asset_id → (交易所时间戳, 交易所给的买一卖一, 分钟)，等同一时刻的消息到齐再比
        self.last_ts = {}       # 市场 → 上一条 price_change 的交易所时间
        self.minutes = {}       # 分钟（ISO 字符串前 16 位）→ Counter
        self.counting = True    # 预热时 False：只更新订单簿，不计数

    def _count(self, minute, field, n=1):
        if self.counting and n:
            self.minutes.setdefault(minute, Counter())[field] += n

    def _settle(self, aid):
        ts, theirs, minute = self.pending.pop(aid)
        book = self.store.get(aid)
        if book is None:
            return
        self._count(minute, "checked")
        if (book.best_bid() or Decimal(0), book.best_ask() or Decimal(1)) != theirs:
            self._count(minute, "mismatched")

    def apply(self, env):
        p = env["payload"]
        minute = env["received_at"][:16]
        self._count(minute, "messages")

        # 先把「同一时刻已经到齐」的 token 比掉，再处理这一条
        if env["source"] == "collector":
            market = p.get("market")
            hit = [a for a in self.pending if a in self.store.books and self.store.books[a].market == market]
        elif p.get("event_type") == "price_change":
            aids = {c["asset_id"] for c in p["price_changes"]}
            hit = [a for a in aids if a in self.pending and self.pending[a][0] != p["timestamp"]]
        else:
            hit = [p.get("asset_id")] if p.get("asset_id") in self.pending else []
        for aid in hit:
            self._settle(aid)

        if env["source"] == "collector" and p.get("event_type") == "gap":
            self.last_ts.pop(p.get("market"), None)   # 重连后重新开始比

        ignored = self.store.ignored
        self.store.apply(env)
        self._count(minute, "ignored", self.store.ignored - ignored)

        if p.get("event_type") == "price_change":
            ts = int(p["timestamp"])
            market = p.get("market")
            if market in self.last_ts and ts < self.last_ts[market]:
                self._count(minute, "out_of_order")
            self.last_ts[market] = max(ts, self.last_ts.get(market, ts))
            for c in p["price_changes"]:
                self.pending[c["asset_id"]] = (p["timestamp"], (Decimal(c["best_bid"]), Decimal(c["best_ask"])), minute)

    def rows(self):
        """[(分钟, messages, checked, mismatched, out_of_order, ignored)]，给数据库 upsert 用。"""
        return [(m + ":00+00:00", *(c[f] for f in FIELDS)) for m, c in sorted(self.minutes.items())]

    def drop_before(self, minute):
        """丢掉早于 minute（同样是 ISO 前 16 位）的计数，内存里只留最近的。"""
        for m in [m for m in self.minutes if m < minute]:
            del self.minutes[m]
