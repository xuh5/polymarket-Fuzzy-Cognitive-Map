"""假深度识别：纯逻辑，不碰 Kafka、不连网。输入 pm.raw.clob 的信封，实时和回放喂进来的一样，结果就一样。

行情只给每个价位的总量，没有单张订单，所以把一个价位的量拆成一笔笔「挂单块」来推算：
- 量变多：多出来的部分算一个新块
- 量变少：先看前后 2 秒内这个价位有没有成交，配上的算被吃掉（先挂的先成交），其余算撤单（后挂的先撤）

判定规则（2026-11-08 按 bronze 探索定的）：
- 闪现：一个块 >= $1000、挂出时离本侧最优价 5 分内、成交不到 5%、10 分钟内就没了
- 可疑价位：过去 1 小时闪现 >= 5 次
- 可疑金额：可疑价位上挂了不到 10 分钟的部分。挂得久的（包括快照里本来就有的）不算，它们不是闪现的单
- 见势就撤（附加说明，单独不算可疑）：有成交后 1 秒内，附近一个 >= $1000 的价位被撤掉一半以上
单看「没成交就撤」区分不了好坏（大单 86% 都这样），反复闪现才是少数价位才有的特征。
闪现也可能是做市机器人在正常调价，这里只陈述发生了什么，不判断意图。

几个容易想错的地方：
- 同一笔挂单在 Yes、No 两边盘口各出现一次（Yes 买 0.59 = No 卖 0.41），只算第一个选项的盘口，否则金额翻倍；
  第二个选项上的成交换算到第一个选项的价格和方向。
- 被吃掉还是被撤走要等成交消息，而成交消息可能比盘口变化晚到，所以盘口变化先压 2 秒再处理。
  加减量必须一起压：只压减量的话，后撤的单会撤到压着的这 2 秒里新挂的块上。
- 交易所每笔成交都会推一份完整快照，时间戳可能比当前的流还旧，所以快照只在盘口不可用时用来起步，平时不覆盖。
- 时间一律用交易所时间，回放结果才和实时一致；每个市场只按自己的消息推进（别的市场在别的分区，回放时顺序不同）。
  冷门市场很久没有新消息，靠运行程序定时调 advance()。
- 这里算的是统计量，用 float：比 Decimal 快，几位小数的误差不影响结论。
"""
import bisect
from collections import deque
from dataclasses import dataclass

EPS = 1e-6
MATCH_WINDOW_MS = 2_000      # 盘口变少时，前后这么久内的成交算配得上（实测时间差中位数 21 毫秒）
TRADE_KEEP_MS = 60_000       # 成交在内存里留多久（只用来配对）
NEAR_PRICE = 0.05            # 离最优价多近算「看得见的深度」
FLASH_MIN_USD = 1000
FLASH_MAX_LIFE_MS = 600_000
FLASH_MAX_FILL = 0.05
FLASH_WINDOW_MS = 3_600_000
MIN_FLASHES = 5
REACTION_WINDOW_MS = 1_000   # 有成交后这么久内撤单算「见势就撤」（人手来不及）
REACTION_MIN_CANCEL = 0.5    # 撤掉这一档多大比例才算
REACTION_COOLDOWN_MS = 1_000  # 一笔大单常拆成好几笔成交，同一侧隔不到这么久只算一次
INTERVAL_MS = 60_000         # 每个市场多久输出一次
NO_LIMIT = float("inf")


@dataclass(slots=True)
class Lot:
    added_at: int | None     # None：快照里本来就有，不知道挂了多久
    remaining: float
    size: float
    distance: float | None   # 挂出时离本侧最优价多远
    filled: float = 0.0


class Level:
    __slots__ = ("total", "lots")

    def __init__(self, total=0.0, lots=()):
        self.total = total
        self.lots = deque(lots)   # 从旧到新


class MarketState:
    def __init__(self, market):
        self.market = market
        self.book = None          # {"BUY": {价格: Level}, "SELL": {...}}；None = 盘口不可用
        self.best = (0.0, 1.0)    # 买一、卖一
        self.pending = deque()    # 压着还没处理的盘口变化 (时间, [变化])
        self.trades = {}          # (挂单方一侧, 价格) → [[时间, 没配上的份额]]，按时间排序
        self.flashes = {}         # (一侧, 价格) → deque[(结束时间, 成交金额)]
        self.reactions = {}       # (一侧, 价格) → deque[(时间,)]：见势就撤的记录
        self.trade_events = []    # 还没开始观察的成交 (时间, 挂单方一侧)，按时间排序
        self.windows = []         # 正在观察的成交：看接下来 1 秒内附近价位撤了多少
        self.last_window = {}     # 一侧 → 最近一次开始观察的时间
        self.watermark = 0        # 这个市场见过的最新交易所时间
        self.next_output = None


def distance(side, price, best_bid, best_ask):
    """离本侧最优价多远，自己就是最优价时为 0；这一侧为空时 None（交易所用买一 0 / 卖一 1 表示空）。"""
    if (side == "BUY" and best_bid <= 0) or (side == "SELL" and (best_ask <= 0 or best_ask >= 1)):
        return None
    return round(max(best_bid - price if side == "BUY" else price - best_ask, 0.0), 4)


def _price(s):
    return round(float(s), 4)   # "0.5" 和 "0.50" 是同一个价位


class DepthTracker:
    def __init__(self, outcomes=None, interval_ms=INTERVAL_MS):
        self.outcomes = outcomes or {}   # asset_id → 选项序号（1 起）
        self.interval_ms = interval_ms
        self.markets = {}                # 市场 ID → MarketState
        self.ignored = 0                 # 盘口不可用时收到、被忽略的 price_change 条数
        self.reaction_count = 0          # 累计见势就撤的次数

    def apply(self, env):
        """处理一条信封，返回这次到期的市场汇总（格式见 _summary）。"""
        p = env["payload"]
        if env["source"] == "collector":
            return self._on_marker(p)
        event_type = "book" if env["source"] == "clob_rest" else p.get("event_type")   # REST 补拉的快照没有 event_type
        if event_type == "book":
            return self._on_book(p)
        if event_type == "price_change":
            return self._on_price_change(p)
        if event_type == "last_trade_price":
            return self._on_trade(p)
        if event_type == "market_resolved":
            return self._on_marker({"event_type": "unsubscribed", "market": p["market"]})
        return []

    def advance(self, up_to_ms):
        """冷门市场靠它推进：up_to_ms 由运行程序给（电脑时间减宽限），之前的盘口变化和该输出的汇总都算完。"""
        out = []
        for state in self.markets.values():
            out += self._drain(state, max(state.watermark, up_to_ms))
        return out

    def _state(self, market):
        if market not in self.markets:
            self.markets[market] = MarketState(market)
        return self.markets[market]

    def _on_marker(self, p):
        state = self.markets.get(p["market"])
        if state is None:
            return []
        out = self._drain(state, NO_LIMIT, tail=False)   # 断线之前收到的变化是有效的，先处理完
        state.book, state.next_output = None, None
        state.trade_events, state.windows = [], []
        if p["event_type"] == "unsubscribed":
            del self.markets[p["market"]]
        # 断线（gap）不清闪现记录：挂单行为的历史不因为我们断了几秒而失效
        return out

    def _on_book(self, p):
        if self.outcomes.get(p["asset_id"]) != 1:
            return []
        state = self._state(p["market"])
        ts = int(p["timestamp"])
        if state.book is None:
            bids = {_price(x["price"]): float(x["size"]) for x in p["bids"]}
            asks = {_price(x["price"]): float(x["size"]) for x in p["asks"]}
            state.book = {
                side: {px: Level(sz, [Lot(None, sz, sz, None)]) for px, sz in src.items() if sz > EPS}
                for side, src in (("BUY", bids), ("SELL", asks))
            }
            state.best = (max(bids, default=0.0), min(asks, default=1.0))
            state.next_output = (ts // self.interval_ms + 1) * self.interval_ms
        return self._touch(state, ts)

    def _on_price_change(self, p):
        state = self._state(p["market"])
        ts = int(p["timestamp"])
        if state.book is None:
            self.ignored += 1
            return self._touch(state, ts)
        changes = [(c["side"], _price(c["price"]), float(c["size"]), float(c["best_bid"]), float(c["best_ask"]))
                   for c in p["price_changes"] if self.outcomes.get(c["asset_id"]) == 1]
        state.pending.append((ts, changes))
        return self._touch(state, ts)

    def _on_trade(self, p):
        state = self._state(p["market"])
        ts, price, size = int(p["timestamp"]), float(p["price"]), float(p["size"])
        outcome = self.outcomes.get(p["asset_id"])
        if outcome == 1:   # 主动买第一个选项，吃的是卖盘
            key = ("SELL" if p["side"] == "BUY" else "BUY", round(price, 4))
        elif outcome == 2:  # 主动买第二个选项 = 主动卖第一个选项，吃的是第一个选项的买盘，价格换算成 1 − p
            key = (p["side"], round(1 - price, 4))
        else:
            return self._touch(state, ts)
        trades = state.trades.setdefault(key, [])
        bisect.insort(trades, [ts, size])
        keep_from = bisect.bisect_left(trades, [ts - TRADE_KEEP_MS])
        del trades[:keep_from]
        if state.book is not None:
            bisect.insort(state.trade_events, (ts, key[0]))
        return self._touch(state, ts)

    def _touch(self, state, ts):
        state.watermark = max(state.watermark, ts)
        return self._drain(state, state.watermark)

    def _drain(self, state, up_to, tail=True):
        """处理压够 2 秒的盘口变化。每条变化处理前，先输出它之前到期的汇总（那一刻的盘口就是现在这样）。"""
        out = []
        while state.pending and state.pending[0][0] + MATCH_WINDOW_MS <= up_to:
            ts, changes = state.pending.popleft()
            self._open_windows(state, ts)
            self._close_windows(state, ts)
            self._outputs(state, ts, out)
            for change in changes:
                self._change(state, ts, *change)
        if tail:
            self._open_windows(state, up_to - MATCH_WINDOW_MS)
            self._close_windows(state, up_to - MATCH_WINDOW_MS)
            self._outputs(state, up_to - MATCH_WINDOW_MS, out)
        return out

    def _outputs(self, state, until, out):
        if state.book is None:
            return
        while state.next_output <= until:
            out.append(self._summary(state, state.next_output))
            state.next_output += self.interval_ms

    @staticmethod
    def _open_windows(state, until):
        """until 之前的成交：记下那一刻挂单方一侧 5¢ 内的大价位，接下来 1 秒看它们撤了多少。"""
        if state.book is None:
            return
        while state.trade_events and state.trade_events[0][0] <= until:
            t, side = state.trade_events.pop(0)
            if t - state.last_window.get(side, -REACTION_COOLDOWN_MS) < REACTION_COOLDOWN_MS:
                continue
            state.last_window[side] = t
            levels = {}
            for price, level in state.book[side].items():
                d = distance(side, price, *state.best)
                if d is not None and d <= NEAR_PRICE and level.total * price >= FLASH_MIN_USD:
                    levels[price] = [level.total * price, 0.0]   # [当时的金额, 之后撤掉的金额]
            if levels:
                state.windows.append((t, side, levels))

    def _close_windows(self, state, ts):
        """观察满 1 秒的成交：撤掉一半以上的价位记一次见势就撤。"""
        remaining = []
        for window in state.windows:
            t, side, levels = window
            if ts <= t + REACTION_WINDOW_MS:
                remaining.append(window)
                continue
            for price, (usd, cancelled) in levels.items():
                if cancelled / usd >= REACTION_MIN_CANCEL:
                    state.reactions.setdefault((side, price), deque()).append((t,))
                    self.reaction_count += 1
        state.windows = remaining

    def _change(self, state, ts, side, price, new_size, best_bid, best_ask):
        state.best = (best_bid, best_ask)
        levels = state.book[side]
        level = levels.get(price)
        delta = new_size - (level.total if level else 0.0)
        if abs(delta) < EPS:
            return
        if delta > 0:
            level = levels.setdefault(price, Level())
            level.lots.append(Lot(ts, delta, delta, distance(side, price, best_bid, best_ask)))
            level.total = new_size
            return
        fill = self._match_trades(state, side, price, ts, -delta)
        cancel = -delta - fill
        if cancel > EPS:
            for t, window_side, window_levels in state.windows:
                if window_side == side and t < ts <= t + REACTION_WINDOW_MS and price in window_levels:
                    window_levels[price][1] += cancel * price
        while fill > EPS and level.lots:     # 被吃掉：先挂的先成交
            lot = level.lots[0]
            x = min(lot.remaining, fill)
            lot.remaining -= x
            lot.filled += x
            fill -= x
            if lot.remaining <= EPS:
                self._close(state, side, price, level.lots.popleft(), ts)
        while cancel > EPS and level.lots:   # 撤单：后挂的先撤
            lot = level.lots[-1]
            x = min(lot.remaining, cancel)
            lot.remaining -= x
            cancel -= x
            if lot.remaining <= EPS:
                self._close(state, side, price, level.lots.pop(), ts)
        if new_size <= EPS:
            for lot in level.lots:
                self._close(state, side, price, lot, ts)
            del levels[price]
        else:
            level.total = new_size

    @staticmethod
    def _match_trades(state, side, price, ts, amount):
        """在时间窗口内配同价位、挂单方一侧还没配上的成交，返回配上的份额。"""
        trades = state.trades.get((side, price))
        if not trades:
            return 0.0
        got = 0.0
        i = bisect.bisect_left(trades, [ts - MATCH_WINDOW_MS])
        while i < len(trades) and trades[i][0] <= ts + MATCH_WINDOW_MS and got < amount - EPS:
            x = min(trades[i][1], amount - got)
            trades[i][1] -= x
            got += x
            i += 1
        return got

    @staticmethod
    def _close(state, side, price, lot, ts):
        """一个块没了：够得上闪现就记一笔。快照里本来就有的块不知道挂了多久，不算。"""
        if (lot.added_at is not None and lot.distance is not None and lot.distance <= NEAR_PRICE
                and lot.size * price >= FLASH_MIN_USD and lot.filled / lot.size < FLASH_MAX_FILL
                and ts - lot.added_at < FLASH_MAX_LIFE_MS):
            state.flashes.setdefault((side, price), deque()).append((ts, lot.filled * price))

    def _summary(self, state, ts):
        """这一刻离最优价 5 分内的深度（第一个选项的盘口，买卖两侧）、其中可疑的金额，和每个可疑价位。"""
        cutoff = ts - FLASH_WINDOW_MS
        for series in (state.flashes, state.reactions):
            for key in list(series):
                q = series[key]
                while q and q[0][0] < cutoff:
                    q.popleft()
                if not q:
                    del series[key]
        depth = suspicious = 0.0
        flagged = []
        for side, levels in state.book.items():
            for price, level in levels.items():
                d = distance(side, price, *state.best)
                if d is None or d > NEAR_PRICE:
                    continue
                usd = level.total * price
                depth += usd
                flashes = state.flashes.get((side, price), ())
                if len(flashes) < MIN_FLASHES:
                    continue
                recent = sum(lot.remaining * price for lot in level.lots
                             if lot.added_at is not None and ts - lot.added_at < FLASH_MAX_LIFE_MS)
                if recent > EPS:
                    suspicious += recent
                    flagged.append({"side": side, "price": price, "usd": round(usd, 2), "flagged_usd": round(recent, 2),
                                    "flashes": len(flashes), "filled_usd": round(sum(f for _, f in flashes), 2),
                                    "reactions": len(state.reactions.get((side, price), ()))})
        flagged.sort(key=lambda x: -x["flagged_usd"])
        return {"market": state.market, "ts": ts, "depth_usd": round(depth, 2), "suspicious_usd": round(suspicious, 2),
                "levels": flagged}
