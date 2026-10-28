"""本地订单簿重建：纯逻辑，不碰 Kafka、不连网。

输入是 pm.raw.clob 的一条信封（已经 json 解析成 dict），输出是更新后的内存盘口。
不关心消息从哪来：阶段 3 实时从 Kafka 读、阶段 5 从 Parquet 回放，喂进来的是同样的信封，结果就一样。

规则：
- book 快照（WebSocket 推的，或重连后 REST 补拉的）：这个 token 的盘口整份替换，之后才算「可用」
- price_change：size 是这个价位变化后的总挂单量（不是增量），0 表示这一档没了
- gap 标记：这个市场断过线，中间的变化可能漏了，盘口作废，等下一个快照
- unsubscribed / market_resolved：这个市场不再有数据，盘口删掉
- 还没收到快照（或作废后）收到的 price_change 直接忽略：在不完整的盘口上改一档，结果也是错的
"""
from decimal import Decimal


class Book:
    """一个 token 的订单簿。价格和数量都用 Decimal：交易所给的是字符串，用 float 会有 0.1 + 0.2 这种误差。"""

    def __init__(self, market):
        self.market = market
        self.bids = {}   # 价格 → 数量
        self.asks = {}
        self.valid = False
        self.exchange_ts = None   # 最后一次更新的交易所时间（毫秒）
        self.version = 0          # 改过几次；同一毫秒可能改好几次，所以判断「变没变」用它，不用 exchange_ts

    def replace(self, bids, asks, exchange_ts):
        self.bids = {Decimal(x["price"]): Decimal(x["size"]) for x in bids}
        self.asks = {Decimal(x["price"]): Decimal(x["size"]) for x in asks}
        self.valid = True
        self.exchange_ts = exchange_ts
        self.version += 1

    def set_level(self, side, price, size, exchange_ts):
        levels = self.bids if side == "BUY" else self.asks
        price, size = Decimal(price), Decimal(size)
        if size == 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        self.exchange_ts = exchange_ts
        self.version += 1

    def best_bid(self):
        return max(self.bids) if self.bids else None

    def best_ask(self):
        return min(self.asks) if self.asks else None

    def top(self, side, n):
        """前 n 档 [(价格, 数量)]，从最优价开始。BUY = 买盘（价高优先），SELL = 卖盘（价低优先）。"""
        levels = self.bids if side == "BUY" else self.asks
        return sorted(levels.items(), reverse=(side == "BUY"))[:n]


class BookStore:
    """所有 token 的订单簿，按 asset_id 存。"""

    def __init__(self):
        self.books = {}   # asset_id → Book
        self.ignored = 0  # 盘口不可用时收到、被忽略的 price_change 条数

    def get(self, asset_id):
        """可用的盘口；没有或已作废返回 None。"""
        book = self.books.get(asset_id)
        return book if book and book.valid else None

    def apply(self, env):
        """处理一条信封，返回这次盘口有变化的 asset_id 列表（成交、tick_size_change 等不改盘口的返回空）。"""
        p = env["payload"]
        if env["source"] == "collector":
            if p["event_type"] == "gap":
                for book in self.books.values():
                    if book.market == p["market"]:
                        book.valid = False
            elif p["event_type"] == "unsubscribed":
                self._drop_market(p["market"])
            return []

        # REST 补拉的快照没有 event_type，结构和 book 一样
        event_type = "book" if env["source"] == "clob_rest" else p.get("event_type")
        if event_type == "book":
            aid = p["asset_id"]
            self.books.setdefault(aid, Book(p["market"])).replace(p["bids"], p["asks"], int(p["timestamp"]))
            return [aid]
        if event_type == "price_change":
            changed = []
            for c in p["price_changes"]:
                book = self.get(c["asset_id"])
                if book is None:
                    self.ignored += 1
                    continue
                book.set_level(c["side"], c["price"], c["size"], int(p["timestamp"]))
                if c["asset_id"] not in changed:
                    changed.append(c["asset_id"])
            return changed
        if event_type == "market_resolved":
            self._drop_market(p["market"])
        return []

    def _drop_market(self, market):
        for aid in [aid for aid, b in self.books.items() if b.market == market]:
            del self.books[aid]
