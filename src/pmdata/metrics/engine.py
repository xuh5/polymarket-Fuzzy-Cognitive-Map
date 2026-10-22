"""实时指标计算：纯逻辑，不碰 Kafka。输入 pm.raw.clob 的信封，输出要写进 pm.metrics.* 的指标。

时间一律用交易所时间（payload.timestamp），不用电脑当前时间：这样实时跑和阶段 5 回放算出来的结果一样。
- 盘口类（按 token）：盘口变了就记下「这一秒有变化」，等这一秒结束再输出这一秒最后的状态，每个 token 每秒最多一条。
  交易所会把同一时刻的更新拆成几条消息，等这一秒结束再算也顺便避开了只改了一半的状态。
- 成交类（按 token）：按 1 分钟窗口累计，窗口结束后输出。按 token 而不是按市场：主动买 Yes 和主动买 No 意思正好相反，
  合在一起就看不出流向了。

「这一秒 / 这一分钟结束了」怎么判断：同一个市场的消息在同一个分区里、按顺序到达，
所以收到这个市场一条更晚的消息，之前的就都结束了（apply 里自动处理）。
冷门市场可能很久才来下一条，所以运行程序每秒再调一次 flush(现在 − 宽限时间) 把它们输出。
回放时最后调一次 flush() 输出剩下的。
"""
import bisect
import math
from decimal import Decimal

from pmdata.book.orderbook import BookStore
from pmdata.common.config import TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES
from pmdata.common.timeutil import now_iso

TOP_N = 5                                         # 盘口不平衡看前几档
DEPTH_BANDS = {"1c": Decimal("0.01"), "5c": Decimal("0.05")}   # 深度：中间价上下多少以内
CHANGE_WINDOWS = {"1m": 60, "5m": 300, "15m": 900, "60m": 3600}  # 价格变化的时间尺度（秒）
LOGIT_EPS = 0.001                                 # 价格贴着 0 或 1 时 log-odds 会变成无穷大，先夹到 [0.001, 0.999]


def logit(p):
    """对数几率 ln(p / (1 − p))：0.01→0.02（概率翻倍）比 0.50→0.51 变化大得多，直接拿价格相减看不出来。"""
    p = min(max(p, LOGIT_EPS), 1 - LOGIT_EPS)
    return math.log(p / (1 - p))


def num(x):
    return None if x is None else float(x)


class MidHistory:
    """一个 token 每秒的中间价，用来查「N 秒前的中间价」。只保留够算最长时间尺度的部分。"""

    KEEP = max(CHANGE_WINDOWS.values())

    def __init__(self):
        self.secs = []
        self.mids = []

    def add(self, sec, mid):
        self.secs.append(sec)
        self.mids.append(mid)
        # 删掉用不到的旧数据，但要留一条 ≤ sec − KEEP 的，查 60 分钟前时要用
        cut = bisect.bisect_right(self.secs, sec - self.KEEP) - 1
        if cut > 0:
            del self.secs[:cut]
            del self.mids[:cut]

    def as_of(self, sec):
        """sec 这一秒（含）之前最后一次的中间价；那时还没有数据就返回 None。"""
        i = bisect.bisect_right(self.secs, sec) - 1
        return self.mids[i] if i >= 0 else None


class MetricsEngine:
    def __init__(self, clock=now_iso):
        self.store = BookStore()
        self.clock = clock          # 「算完时间」，测延迟用；测试里换成固定值
        self.pending = {}           # asset_id → (秒, 最后一条消息的 received_at)：这一秒盘口变过、还没输出
        self.history = {}           # asset_id → MidHistory
        self.trades = {}            # (market, asset_id, 分钟) → 这一分钟的成交累计

    def apply(self, env):
        """处理一条信封，返回 [(topic, key, 指标 dict)]：因为这条消息而「结束」了的秒 / 分钟的指标。"""
        p = env["payload"]
        if env["source"] == "collector":
            # 断线 / 退订：先把这个市场攒着的都输出（那时的盘口是好的），再让盘口作废
            out = self.flush(market=p["market"])
            self.store.apply(env)
            return out

        ts = int(p["timestamp"])
        out = self.flush(up_to_ms=ts, market=p["market"])
        if p.get("event_type") == "last_trade_price":
            self._add_trade(p, ts, env["received_at"])
        for aid in self.store.apply(env):
            self.pending[aid] = (ts // 1000, env["received_at"])
        return out

    def flush(self, up_to_ms=None, market=None):
        """输出在 up_to_ms 之前已经结束的秒 / 分钟（None = 全部）；market 不为空时只管这个市场。"""
        out = []
        for aid, (sec, received_at) in list(self.pending.items()):
            book = self.store.books.get(aid)
            if book is None:        # 市场已退订 / 已结算，盘口没了
                del self.pending[aid]
                continue
            if market not in (None, book.market) or (up_to_ms is not None and (sec + 1) * 1000 > up_to_ms):
                continue
            del self.pending[aid]
            rec = self._book_metrics(aid, sec, received_at)
            if rec:
                out.append((TOPIC_METRICS_BOOK, book.market, rec))
        for k in list(self.trades):
            m, _, minute = k
            if market not in (None, m) or (up_to_ms is not None and (minute + 1) * 60000 > up_to_ms):
                continue
            rec = self.trades.pop(k)
            rec["computed_at"] = self.clock()
            out.append((TOPIC_METRICS_TRADES, m, rec))
        return out

    def _book_metrics(self, aid, sec, received_at):
        book = self.store.get(aid)
        if book is None:            # 断线后盘口作废，等快照
            return None
        bid, ask = book.best_bid(), book.best_ask()
        rec = {"market": book.market, "asset_id": aid, "second": sec * 1000,
               "exchange_ts": book.exchange_ts, "received_at": received_at,
               "best_bid": num(bid), "best_ask": num(ask)}

        bids, asks = book.top("BUY", TOP_N), book.top("SELL", TOP_N)
        for n in (1, TOP_N):
            b = sum(s for _, s in bids[:n])
            a = sum(s for _, s in asks[:n])
            rec[f"imbalance_{n}"] = num((b - a) / (b + a)) if b + a else None

        mid = None
        if bid is not None and ask is not None:
            mid = (bid + ask) / 2
            bid_size, ask_size = book.bids[bid], book.asks[ask]
            # 加权中间价：买一挂得多（买方力量大）时更靠近卖一
            rec["microprice"] = num((bid * ask_size + ask * bid_size) / (bid_size + ask_size))
        else:
            rec["microprice"] = None
        rec["mid"] = num(mid)
        rec["spread"] = num(ask - bid) if mid is not None else None

        for name, band in DEPTH_BANDS.items():
            # 离中间价 band 以内一共挂了多少美元（价格 × 份数）
            if mid is None:
                rec[f"bid_depth_{name}"] = rec[f"ask_depth_{name}"] = None
                continue
            rec[f"bid_depth_{name}"] = num(sum(p * s for p, s in book.bids.items() if p >= mid - band))
            rec[f"ask_depth_{name}"] = num(sum(p * s for p, s in book.asks.items() if p <= mid + band))

        hist = self.history.setdefault(aid, MidHistory())
        for name, seconds in CHANGE_WINDOWS.items():
            before = hist.as_of(sec - seconds)
            rec[f"logodds_chg_{name}"] = logit(mid) - logit(before) if mid is not None and before is not None else None
        if mid is not None:
            hist.add(sec, float(mid))

        rec["computed_at"] = self.clock()
        return rec

    def _add_trade(self, p, ts, received_at):
        minute = ts // 60000
        k = (p["market"], p["asset_id"], minute)
        w = self.trades.get(k)
        if w is None:
            w = self.trades[k] = {"market": p["market"], "asset_id": p["asset_id"], "minute": minute * 60000,
                                  "trades": 0, "volume": 0.0, "notional": 0.0, "buy_volume": 0.0, "sell_volume": 0.0}
        price, size = Decimal(p["price"]), Decimal(p["size"])
        w["trades"] += 1
        w["volume"] += float(size)
        w["notional"] += float(price * size)
        # side 是吃单方（主动方）的方向：和官方 Data API taker_only 的成交逐笔对过，869 笔全部一致
        w["buy_volume" if p["side"] == "BUY" else "sell_volume"] += float(size)
        w["exchange_ts"] = ts
        w["received_at"] = received_at
