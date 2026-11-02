"""跳动检测：纯逻辑，不碰 Kafka。输入 pm.metrics.book / pm.metrics.trades 的指标，输出要写进 pm.events 的事件。

什么算一次跳动（用户 2026-11-02 同意）：第一个选项（二元市场的 Yes）的中间价和 5 分钟前比，变了 ≥ 3 个点。
门槛故意放低，什么算重要交给网页上的筛选；所以每次跳动都把筛选要用的特征记全：
- 怎么动的：买一卖一一起动（重新定价）/ 价差变宽（一边的挂单撤了）/ 价差变窄（一边新挂上）
- 期间成交：笔数、金额、方向（看涨 = 买 Yes + 卖 No），和平时比是几倍
- 事后：5 分钟、30 分钟后的价格（保持住了还是回去了）
同一个市场隔不到 5 分钟又跳，合并成一次，记下来回反向了几次（一张小单反复挂撤会让中间价来回跳）。

一次跳动写三次（同一个 event_id，后写的覆盖先写的）：
open（刚过门槛，马上写，推送用）→ closed（5 分钟没再过门槛，峰值和期间成交都定了）
→ final（峰值 30 分钟后，补上事后价格）。

时间一律用指标里的交易所时间，回放和实时结果一样。「过了多久」靠同一市场更晚的指标推进，
冷门市场靠运行程序定时调 flush(现在 − 宽限)；回放结束调 flush()。
只和同一段连续盘口（book_since 相同）里的价格比：重新订阅、长时间断线前后的价格不比。
"""
import bisect

from pmdata.common.config import TOPIC_METRICS_BOOK, TOPIC_METRICS_TRADES
from pmdata.common.timeutil import now_iso
from pmdata.metrics.engine import logit

WINDOW = 300            # 秒：和 5 分钟前比
CANDIDATE = 3.0         # 个点：候选门槛（网页默认只显示 ≥ 5 个点，用户自己调）
MERGE = 300             # 秒：同一市场隔不到这么久又过门槛，算同一次
AFTER = (300, 1800)     # 事后看峰值之后 5 分钟、30 分钟的价格
BASELINE = 86400        # 平时的成交量：前 24 小时
SPREAD_MOVE = 0.02      # 价差变宽 / 变窄超过 2¢ 才算「一边撤了 / 一边补上」
KEEP = WINDOW + max(AFTER) + 600   # 每秒盘口留多久（秒）
COVERAGE_BREAK = 3600   # 秒：两段盘口之间断开超过这么久（退订、停机），自己记的成交就不全了，基准重新攒 24 小时


class Series:
    """一个 token 同一段连续盘口里每秒的盘口（指标 dict），查「某一秒那一刻」的状态。"""

    def __init__(self, since):
        self.since = since
        self.secs = []
        self.rows = []

    def add(self, sec, row):
        self.secs.append(sec)
        self.rows.append(row)
        cut = bisect.bisect_right(self.secs, sec - KEEP) - 1   # 留一条 ≤ sec − KEEP 的，as_of 要用
        if cut > 0:
            del self.secs[:cut]
            del self.rows[:cut]

    def as_of(self, sec):
        """sec 这一秒（含）之前最后的盘口；这一段那时还没开始就是 None。"""
        i = bisect.bisect_right(self.secs, sec) - 1
        return self.rows[i] if i >= 0 else None

    @property
    def last(self):
        return self.secs[-1] if self.secs else None


class MarketState:
    def __init__(self):
        self.series = None      # 第一个选项的盘口
        self.covered_since = None   # 成交从什么时候起一直在记（毫秒）；断线几十秒只漏一点点成交，不算断
        self.trades = {}        # 分钟（毫秒）→ [笔数, 金额, 看涨份数, 看跌份数]，两个选项合在一起
        self.open = None        # 正在进行的跳动
        self.closed = []        # 已结束、等 30 分钟后补事后价格的跳动
        self.last_ms = None     # 第一个选项最后一条盘口指标的时间


def move_type(before, peak, d):
    """买一卖一怎么动的：both = 两边一起朝同一方向动了至少一半（重新定价）；
    wider / narrower = 价差变宽（一边撤了）/ 变窄（一边新挂上）；other = 别的情况"""
    bid_moved = (peak["best_bid"] - before["best_bid"]) * 100
    ask_moved = (peak["best_ask"] - before["best_ask"]) * 100
    if all(m * d > 0 and abs(m) >= abs(d) / 2 for m in (bid_moved, ask_moved)):
        return "both"
    if peak["spread"] > before["spread"] + SPREAD_MOVE:
        return "wider"
    if peak["spread"] < before["spread"] - SPREAD_MOVE:
        return "narrower"
    return "other"


def depth(row):
    return (row.get("bid_depth_5c") or 0) + (row.get("ask_depth_5c") or 0)


class JumpDetector:
    def __init__(self, clock=now_iso):
        self.clock = clock
        self.outcomes = {}      # asset_id → 选项序号（1 = 第一个选项），运行程序从 pm.raw.markets 填
        self.volume24h = {}     # 市场 → Gamma 的 24 小时成交额（美元）；自己的成交数据不满 24 小时时用它
        self.markets = {}

    def apply(self, topic, rec):
        """处理一条指标，返回因此产生的事件列表。"""
        index = self.outcomes.get(rec["asset_id"])
        if index is None:
            return []
        mk = self.markets.setdefault(rec["market"], MarketState())
        if topic == TOPIC_METRICS_TRADES:
            self._add_trades(mk, rec, index)
            return []
        if topic != TOPIC_METRICS_BOOK or index != 1:
            return []

        sec = rec["second"] // 1000
        out = []
        if mk.series is not None and mk.series.since != rec.get("book_since"):
            out += self._end_series(rec["market"], mk)
        if mk.series is None:
            since = rec.get("book_since")
            if since is None or mk.covered_since is None or since - mk.last_ms > COVERAGE_BREAK * 1000:
                mk.covered_since = since
            mk.series = Series(since)
        mk.last_ms = rec["second"]
        out += self._advance(rec["market"], mk, sec)
        if rec.get("mid") is None:
            return out
        before = mk.series.as_of(sec - WINDOW)
        mk.series.add(sec, rec)
        if before is None or before.get("mid") is None:
            return out
        d = (rec["mid"] - before["mid"]) * 100
        if abs(d) < CANDIDATE:
            return out

        ep = mk.open
        if ep is None:
            ep = mk.open = {"market": rec["market"], "asset_id": rec["asset_id"], "start": sec, "last": sec,
                            "sign": d > 0, "reversals": 0, "since": mk.series.since}
            self._set_peak(ep, sec, d, before, rec)
            out.append(self._event(mk, ep, "open"))
            return out
        if (d > 0) != ep["sign"]:
            ep["reversals"] += 1
            ep["sign"] = d > 0
        ep["last"] = sec
        if abs(d) > abs(ep["change"]):
            self._set_peak(ep, sec, d, before, rec)
        return out

    def flush(self, up_to_ms=None):
        """时间推进到 up_to_ms（None = 回放结束，全部收尾），返回因此结束 / 补完的事件。"""
        out = []
        for market, mk in self.markets.items():
            if up_to_ms is None:
                out += self._end_series(market, mk)
            else:
                out += self._advance(market, mk, up_to_ms // 1000)
        return out

    @staticmethod
    def _set_peak(ep, sec, d, before, rec):
        ep.update(peak=sec, change=d, before=before, at_peak=rec)

    def _add_trades(self, mk, rec, index):
        w = mk.trades.setdefault(rec["minute"], [0, 0.0, 0.0, 0.0])
        w[0] += rec["trades"]
        w[1] += rec["notional"]
        # 看涨 = 主动买第一个选项 + 主动卖第二个选项
        buy, sell = rec["buy_volume"], rec["sell_volume"]
        w[2] += buy if index == 1 else sell
        w[3] += sell if index == 1 else buy
        cut = rec["minute"] - (BASELINE + 3600) * 1000
        for m in [m for m in mk.trades if m < cut]:
            del mk.trades[m]

    def _advance(self, market, mk, sec):
        """时间到了 sec：5 分钟没再过门槛的跳动结束；峰值 30 分钟后的补上事后价格。"""
        out = []
        ep = mk.open
        if ep is not None and sec > ep["last"] + MERGE:
            mk.open = None
            out.append(self._close(mk, ep, ended=False))
        for ep in [ep for ep in mk.closed if sec >= ep["peak"] + AFTER[-1]]:
            mk.closed.remove(ep)
            out.append(self._final(mk, ep, ended=False))
        return out

    def _end_series(self, market, mk):
        """这一段连续盘口结束了（重新订阅 / 长时间断线 / 回放结束）：手上的跳动都收尾，到不了的事后价格留空。"""
        out = []
        if mk.open is not None:
            out.append(self._close(mk, mk.open, ended=True))
            mk.open = None
        for ep in mk.closed:
            out.append(self._final(mk, ep, ended=True))
        mk.closed = []
        mk.series = None
        return out

    def _after(self, mk, ep, seconds, ended):
        """峰值 seconds 秒后的中间价。时间到了而盘口没再变，就是最后的价格；
        ended = 这一段盘口已经结束（重新订阅 / 断线 / 回放结束），结束前没到那一刻就不知道，留空。"""
        s, t = mk.series, ep["peak"] + seconds
        if s is None or s.since != ep["since"] or (ended and s.last < t):
            return None
        row = s.as_of(t)
        return row["mid"] if row else None

    def _close(self, mk, ep, ended):
        ep["mid_after_5m"] = self._after(mk, ep, AFTER[0], ended)
        mk.closed.append(ep)
        return self._event(mk, ep, "closed")

    def _final(self, mk, ep, ended):
        ep["mid_after_30m"] = self._after(mk, ep, AFTER[1], ended)
        return self._event(mk, ep, "final")

    def _event(self, mk, ep, status):
        b, p, peak_ms = ep["before"], ep["at_peak"], ep["peak"] * 1000
        # 跳动期间（峰值前 5 分钟）的成交；按分钟存，和这 5 分钟有重叠的分钟都算上
        lo = peak_ms - WINDOW * 1000
        win = [v for m, v in mk.trades.items() if m + 60000 > lo and m <= peak_ms]
        trades, notional = sum(v[0] for v in win), sum(v[1] for v in win)
        bull, bear = sum(v[2] for v in win), sum(v[3] for v in win)
        # 平时：自己已经连续记了 24 小时成交就用自己的前 24 小时成交额，否则用 Gamma 的 volume24hr
        if mk.covered_since is not None and mk.covered_since <= peak_ms - BASELINE * 1000:
            base = sum(v[1] for m, v in mk.trades.items() if peak_ms - BASELINE * 1000 <= m < lo)
            source = "own"
        else:
            base, source = self.volume24h.get(ep["market"]), "gamma" if ep["market"] in self.volume24h else None
        normal = base * WINDOW / BASELINE if base else None   # 平时 5 分钟的成交额
        return {
            "event_id": f"{ep['market']}:{ep['start']}", "kind": "jump", "status": status,
            "market": ep["market"], "asset_id": ep["asset_id"],
            "start": ep["start"] * 1000, "peak": peak_ms, "last": ep["last"] * 1000, "book_since": ep["since"],
            "mid_before": b["mid"], "mid_peak": p["mid"],
            "change_points": ep["change"], "change_logodds": logit(p["mid"]) - logit(b["mid"]),
            "bid_before": b["best_bid"], "ask_before": b["best_ask"],
            "bid_peak": p["best_bid"], "ask_peak": p["best_ask"],
            "spread_before": b["spread"], "spread_peak": p["spread"], "depth_before": depth(b),
            "move_type": move_type(b, p, ep["change"]), "reversals": ep["reversals"],
            "trades": trades, "notional": notional, "bullish_volume": bull, "bearish_volume": bear,
            "flow": (bull - bear) / (bull + bear) if bull + bear else None,
            "baseline_notional_24h": base, "baseline_source": source,
            "volume_ratio": notional / normal if normal else None,
            "mid_after_5m": ep.get("mid_after_5m"), "mid_after_30m": ep.get("mid_after_30m"),
            "computed_at": self.clock(),
        }
