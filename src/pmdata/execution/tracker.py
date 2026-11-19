"""成交质量：纯逻辑，不碰 Kafka、不连网。输入 pm.raw.clob 的信封，实时和回放喂进来的一样，结果就一样。

每一笔吃单输出一条：
- 成交前的买一、卖一、中间价、价差
- 吃了几档、成交量、成交额、均价
- 吃单成本：均价比成交前中间价多付多少（每股）
- 挂单方事后赚多少：按成交后 1 秒 / 10 秒 / 1 分钟 / 5 分钟的中间价算（每股，markout）

WebSocket 的成交消息（2026-11-18 和官方 Data API 逐笔对过）：一笔吃单只推一条，给的是吃单方的总量和均价
（均价四舍五入到 0.001），不是每一档一条。吃穿 10 档、对面 24 个挂单方的成交也只有一条。所以「吃了几档」
不能数成交消息，要看盘口：吃穿几档时交易所每档推一条盘口消息，但这些消息的交易所时间戳完全相同，
所以按（token, 挂单那一侧, 交易所时间戳）把减少的价位归成一组。

「成交前」怎么取（2026-11-18 用 bronze 11-16 全天 6080 笔核对）：
交易所先推盘口减少、约 21 毫秒后才推成交消息（97% 如此），所以处理到成交消息时盘口已经扣掉了这笔成交，
直接取会有 21% 的成交看起来「成交在中间」甚至方向反了。改为找这笔成交对应的那条盘口消息，取它处理之前的买一卖一：
- exact：只吃一档。挂单那一侧、正好在成交价的价位减少了
- sweep：吃穿多档。同一时刻挂单那一侧几个价位一起减少，价格把成交均价夹在中间；吃了几档 = 这几个价位
- none：都没找到，退回成交消息到达时的盘口（吃了几档未知）
都只看成交前 2 秒内、还没被别的成交用过的盘口消息，取最近的一条。

时间一律用交易所时间。每个市场只按自己的消息推进：这个市场来了一条交易所时间 T 的消息，
就把「到期时间 < T」的 markout 用处理这条消息之前的中间价补上（= 到期那一刻的中间价）。
冷门市场很久没有新消息，靠运行程序定时调 advance()。盘口不可用（断线、退订）时对应的中间价为 None。
统计量用 float，盘口本身仍是 BookStore 里的 Decimal。
"""
from collections import defaultdict, deque
from decimal import Decimal

from pmdata.book.orderbook import BookStore

HORIZONS = (1, 10, 60, 300)      # markout 看成交后几秒
MATCH_WINDOW_MS = 2_000          # 成交前这么久内的盘口减少算配得上（实测中位数早 21 毫秒）
HALF_TICK = Decimal("0.0005")    # 成交均价四舍五入到 0.001，比较时留半个 tick


def _f(x):
    return None if x is None else float(x)


def _mid(book):
    if book is None:
        return None
    bid, ask = book.best_bid(), book.best_ask()
    return None if bid is None or ask is None else float(bid + ask) / 2


class ExecutionTracker:
    def __init__(self):
        self.store = BookStore()
        self.decreases = defaultdict(deque)   # asset_id → [同一时刻某一侧减少的价位]，只留 MATCH_WINDOW_MS 内的
        self.open = {}                         # (交易哈希, asset_id) → 还没等完 markout 的吃单
        self.by_market = defaultdict(set)      # 市场 → 上面的 key，推进时只看这个市场的
        self.skipped = 0                       # 成交时盘口不可用、没法算的笔数

    # ---------- 对外 ----------

    def apply(self, env):
        """处理一条信封，返回这次等完了 markout、可以输出的吃单列表。"""
        p = env["payload"]
        out = []
        if env["source"] == "collector":
            self.store.apply(env)
            return out
        ts = p.get("timestamp")
        if p.get("market") and ts is not None:
            out = self._settle(p["market"], int(ts))
        event_type = "book" if env["source"] == "clob_rest" else p.get("event_type")
        if event_type == "price_change":
            self._record_decreases(p)
        elif event_type == "last_trade_price":
            self._on_trade(p)
            return out
        self.store.apply(env)
        return out

    def advance(self, up_to_ms):
        """所有市场的「到期时间 < up_to_ms」的 markout 用当前中间价补上（运行程序用电脑时间定时调）。"""
        out = []
        for market in list(self.by_market):
            out += self._settle(market, up_to_ms)
        return out

    def drain(self):
        """回放结束：剩下没等完的也输出，没到期的 markout 留 None。"""
        out = [self._finish(e) for e in self.open.values()]
        self.open.clear()
        self.by_market.clear()
        return out

    # ---------- 内部 ----------

    def _record_decreases(self, p):
        """记下每个 token 每一侧在这一时刻减少了哪些价位，以及这一时刻第一条消息处理之前的买一卖一。"""
        ts = int(p["timestamp"])
        for c in p["price_changes"]:
            aid = c["asset_id"]
            book = self.store.get(aid)
            if book is None:
                continue
            levels = book.bids if c["side"] == "BUY" else book.asks
            price = Decimal(c["price"])
            old = levels.get(price)
            if old is None or Decimal(c["size"]) >= old:
                continue
            q = self.decreases[aid]
            g = next((x for x in reversed(q) if x["ts"] == ts and x["side"] == c["side"] and not x["used"]), None)
            if g is None:
                g = dict(ts=ts, side=c["side"], prices=set(), used=False, pre=(book.best_bid(), book.best_ask()))
                q.append(g)
                while q and ts - q[0]["ts"] > MATCH_WINDOW_MS:
                    q.popleft()
            g["prices"].add(price)

    def _match(self, aid, ts, side, price):
        """找这笔成交对应的盘口消息 → (记录, exact/sweep)；找不到 (None, None)。"""
        maker_side = "SELL" if side == "BUY" else "BUY"
        recent = [g for g in reversed(self.decreases[aid])
                  if not g["used"] and g["side"] == maker_side and 0 <= ts - g["ts"] <= MATCH_WINDOW_MS]
        for g in recent:
            if price in g["prices"] and len(g["prices"]) == 1:
                return g, "exact"
            # 吃穿多档时均价可能正好等于其中一档，所以多个价位的组只看均价夹没夹在中间
            if len(g["prices"]) > 1 and min(g["prices"]) - HALF_TICK <= price <= max(g["prices"]) + HALF_TICK:
                return g, "sweep"
        return None, None

    def _on_trade(self, p):
        aid, ts, side = p["asset_id"], int(p["timestamp"]), p["side"]
        key = (p.get("transaction_hash"), aid)
        if key in self.open:
            return
        book = self.store.get(aid)
        if book is None:
            self.skipped += 1
            return
        price = Decimal(p["price"])
        g, how = self._match(aid, ts, side, price)
        if g:
            g["used"] = True
            pre = g["pre"]
            prices = [price] if how == "exact" else sorted(g["prices"])
            levels = len(prices)
            worst = max(prices) if side == "BUY" else min(prices)
        else:
            pre, levels, worst = (book.best_bid(), book.best_ask()), None, None
        ex = dict(market=p["market"], asset_id=aid, tx_hash=p.get("transaction_hash"), ts=ts, side=side,
                  fee_rate_bps=int(p["fee_rate_bps"]) if p.get("fee_rate_bps") else None,
                  price=price, size=Decimal(p["size"]), levels=levels, worst=worst,
                  pre=pre, pre_match=how or "none", mids={})
        self.open[key] = ex
        self.by_market[ex["market"]].add(key)

    def _settle(self, market, now_ts):
        out = []
        keys = self.by_market.get(market)
        if not keys:
            return out
        for key in list(keys):
            ex = self.open[key]
            for h in HORIZONS:
                if h not in ex["mids"] and ex["ts"] + h * 1000 < now_ts:
                    ex["mids"][h] = _mid(self.store.get(ex["asset_id"]))
            if len(ex["mids"]) == len(HORIZONS):
                out.append(self._finish(ex))
                del self.open[key]
                keys.discard(key)
        if not keys:
            del self.by_market[market]
        return out

    @staticmethod
    def _finish(ex):
        sign = 1 if ex["side"] == "BUY" else -1
        price, size = float(ex["price"]), float(ex["size"])
        bid, ask = (_f(x) for x in ex["pre"])
        mid = (bid + ask) / 2 if bid is not None and ask is not None else None
        rec = dict(
            market=ex["market"], asset_id=ex["asset_id"], tx_hash=ex["tx_hash"], ts=ex["ts"], side=ex["side"],
            fee_rate_bps=ex["fee_rate_bps"],
            price=price, size=size, notional=price * size, levels=ex["levels"], worst_price=_f(ex["worst"]),
            pre_bid=bid, pre_ask=ask, pre_mid=mid, pre_spread=ask - bid if mid is not None else None,
            pre_match=ex["pre_match"],
            # 吃单方比成交前中间价多付多少（每股）；正数 = 吃单付出的成本
            cost=sign * (price - mid) if mid is not None else None,
        )
        for h in HORIZONS:
            m = ex["mids"].get(h)
            rec[f"mid_{h}s"] = m
            # 挂单方按 h 秒后的中间价算，每股赚多少（markout）；正数 = 挂单方赚
            rec[f"maker_pnl_{h}s"] = sign * (price - m) if m is not None else None
        return rec
