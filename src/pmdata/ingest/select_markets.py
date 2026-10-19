"""从 pm.raw.markets 读出所有市场，筛出要订阅的前 N 个市场和它们的 token。"""
import json
import re
from datetime import UTC, datetime, timedelta

from pmdata.common.config import TOPIC_MARKETS
from pmdata.common.kafka import latest_by_key

TOP_N = 30
MIN_DAYS_LEFT = 2   # 离到期不足这么多天的市场不订阅


def load_latest_markets():
    """读完 pm.raw.markets，返回每个市场的最新数据（已关闭市场的 tombstone 会把它删掉）。"""
    return [e["payload"] for e in latest_by_key(TOPIC_MARKETS, "select-markets").values()]


def get_token_ids(market):
    """按官方文档的规则拿 token ID：看 version 选字段，不看哪个字段有值。

    v1 的 clobTokenIds 是 JSON 字符串，要先解析；v2 的 positionIds 本身就是列表。
    版本不认识、数量对不上、ID 不是纯数字，都返回 None（跳过这个市场）。
    """
    version = market.get("version")
    if version == "v1":
        raw = market.get("clobTokenIds")
        ids = json.loads(raw) if raw else None
    elif version == "v2":
        ids = market.get("positionIds")
    else:
        return None

    outcomes = json.loads(market.get("outcomes") or "[]")
    if not ids or len(ids) != 2 or len(outcomes) != 2:
        return None
    if not all(isinstance(i, str) and re.fullmatch(r"\d+", i) for i in ids):
        return None
    # 下标对齐：outcomes[0] 对应 ids[0]，二元市场里 0 是 Yes、1 是 No
    return [{"outcome": o, "token_id": i} for o, i in zip(outcomes, ids, strict=True)]


def days_left(market, now):
    """离到期还有几天；没有 endDate 返回 None。已过期会是负数。"""
    end = market.get("endDate")
    if not end:
        return None
    return (datetime.fromisoformat(end) - now) / timedelta(days=1)


def select_markets(markets, top_n=TOP_N, min_days_left=MIN_DAYS_LEFT, now=None):
    """能下单 + 有订单簿 + 离到期至少 min_days_left 天 → 按 24 小时成交额从高到低 → 取前 top_n 个。

    没有 endDate 的市场判断不了什么时候结束，也排除。
    """
    now = now or datetime.now(UTC)
    candidates = []
    for m in markets:
        if not (m.get("acceptingOrders") and m.get("enableOrderBook")):
            continue
        left = days_left(m, now)
        if left is None or left < min_days_left:   # 已过期的 left < 0，也在这里排掉
            continue
        tokens = get_token_ids(m)
        if tokens is None:
            continue
        candidates.append({
            "market_id": str(m["id"]),
            "condition_id": m["conditionId"],   # CLOB 里的市场 ID，WebSocket 消息的 market 字段就是它
            "question": m.get("question"),
            "volume24hr": float(m.get("volume24hr") or 0),  # 没有这个字段 = 24 小时没成交
            "end_date": m["endDate"],
            "tokens": tokens,
        })
    candidates.sort(key=lambda c: c["volume24hr"], reverse=True)
    return candidates[:top_n]


def main():
    markets = load_latest_markets()
    selected = select_markets(markets)
    print(f"共 {len(markets)} 个市场，选出 {len(selected)} 个：\n")
    for rank, s in enumerate(selected, 1):
        print(f"{rank:2}. [{s['market_id']}] 24h 成交 ${s['volume24hr']:,.0f}  "
              f"到期 {s['end_date'][:10]}  {s['question']}")
        for t in s["tokens"]:
            print(f"      {t['outcome']:<4} {t['token_id']}")


if __name__ == "__main__":
    main()
