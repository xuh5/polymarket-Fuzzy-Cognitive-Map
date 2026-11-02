"""回放（模块 3）：把一个市场一段时间的原始消息整理成「起点订单簿 + 之后的每一次变化」，给网页播放。

纯逻辑，不碰文件：输入是按 Kafka 顺序排好的信封（从上一份官方完整清单开始），输出可以直接转成 JSON。
内部就用实时那份 BookStore，规则完全一样，所以前端把 events 依次套到 initial 上，得到的就是当时本地订单簿的样子：
- 只输出 BookStore 真正改了的变化：盘口不可用时的 price_change 实时也忽略，回放里同样不出现
- 时间一律用交易所时间（毫秒）；断线标记没有交易所时间，用收到时间
- 不按秒切帧：挂了零点几秒就撤的单也要看得到

events 的几种：
- {t, type: "change", o, side: "bid"|"ask", price, size}  这一档变成 size（0 = 这一档没了）
- {t, type: "book", o, bids, asks}                         官方完整清单，整份替换
- {t, type: "trade", o, side, price, size}                 成交；side 是吃单方方向（BUY = 主动买这个选项）
- {t, type: "gap"}                                         断过线，两个选项的盘口都作废，等下一份 book
- {t, type: "closed"}                                      退订 / 结算，之后没有数据是正常的
o 是选项序号（outcome_index，Yes = 1）；价格、数量是数字；bids 价高在前，asks 价低在前。
"""
from datetime import datetime

from pmdata.book.orderbook import BookStore


def event_ms(env):
    ts = env["payload"].get("timestamp")
    if ts is not None:
        return int(ts)
    return int(datetime.fromisoformat(env["received_at"]).timestamp() * 1000)


def levels(book):
    bids = sorted(book.bids.items(), reverse=True)
    asks = sorted(book.asks.items())
    return [[float(p), float(s)] for p, s in bids], [[float(p), float(s)] for p, s in asks]


def snapshot(store, asset_id):
    book = store.get(asset_id)
    if book is None:
        return None
    bids, asks = levels(book)
    return {"bids": bids, "asks": asks}


def build(envs, tokens, start_ms, end_ms):
    """envs：按 Kafka 顺序的信封；tokens：asset_id → 选项序号。

    时间早于 start_ms 的消息只用来把订单簿推到起点，不输出；一出现时间 ≥ start_ms 的消息就进入窗口
    （之后交易所时间偶尔倒退几毫秒也照常输出，不回头），出现时间 > end_ms 的就停。
    返回 {initial: {选项序号: {bids, asks} 或 None（起点时盘口不可用）}, events: [...]}
    """
    store = BookStore()
    initial = None
    events = []
    for env in envs:
        t = event_ms(env)
        if t > end_ms:
            break
        if initial is None and t >= start_ms:
            initial = {o: snapshot(store, aid) for aid, o in tokens.items()}
        if initial is None:
            store.apply(env)
            continue

        p = env["payload"]
        if env["source"] == "collector":
            store.apply(env)
            if p["event_type"] in ("gap", "unsubscribed"):
                events.append({"t": t, "type": "gap" if p["event_type"] == "gap" else "closed"})
            continue
        event_type = "book" if env["source"] == "clob_rest" else p.get("event_type")
        if event_type == "price_change":
            usable = {c["asset_id"] for c in p["price_changes"] if store.get(c["asset_id"]) is not None}
            store.apply(env)
            for c in p["price_changes"]:
                if c["asset_id"] in usable and c["asset_id"] in tokens:
                    events.append({"t": t, "type": "change", "o": tokens[c["asset_id"]],
                                   "side": "bid" if c["side"] == "BUY" else "ask",
                                   "price": float(c["price"]), "size": float(c["size"])})
        elif event_type == "book":
            store.apply(env)
            if p["asset_id"] in tokens:
                bids, asks = levels(store.get(p["asset_id"]))
                events.append({"t": t, "type": "book", "o": tokens[p["asset_id"]], "bids": bids, "asks": asks})
        elif event_type == "last_trade_price":
            if p["asset_id"] in tokens:
                events.append({"t": t, "type": "trade", "o": tokens[p["asset_id"]], "side": p["side"],
                               "price": float(p["price"]), "size": float(p["size"])})
        elif event_type == "market_resolved":
            store.apply(env)
            events.append({"t": t, "type": "closed"})

    # 窗口里一条消息都没有：起点就是处理完所有早于窗口的消息之后的样子
    if initial is None:
        initial = {o: snapshot(store, aid) for aid, o in tokens.items()}
    return {"initial": initial, "events": events}
