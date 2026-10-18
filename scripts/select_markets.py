"""从 pm.raw.markets 读出所有市场，筛出要订阅的前 N 个市场和它们的 token。"""
import json
import os
import re
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")   # 容器里设成 kafka:19092
TOPIC = "pm.raw.markets"
TOP_N = 30
MIN_DAYS_LEFT = 2   # 离到期不足这么多天的市场不订阅


def load_latest_markets():
    """从头读完整个 topic，每个市场只留最新一条。

    compact 是 Kafka 在后台慢慢做的，同一个市场可能暂时还有多条旧消息，
    所以这里自己按 key 去重：后读到的覆盖先读到的。
    """
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "select-markets",
        "enable.auto.commit": False,   # 每次都从头读，不需要记住读到哪
    })
    meta = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
    partitions = [TopicPartition(TOPIC, p, 0) for p in meta.partitions]

    # 先记下每个分区现在的末尾位置，读到那里就算读完
    remaining = {}
    for tp in partitions:
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        if high > low:
            remaining[tp.partition] = high
    consumer.assign(partitions)

    latest = {}
    while remaining:
        msg = consumer.poll(10)
        if msg is None:
            raise TimeoutError(f"读取超时，还没读完的分区：{sorted(remaining)}")
        if msg.error():
            raise RuntimeError(msg.error())
        key = msg.key().decode()
        if msg.value() is None:
            latest.pop(key, None)   # tombstone：这个市场已经关闭，从名录里删掉
        else:
            latest[key] = json.loads(msg.value())["payload"]
        if msg.offset() + 1 >= remaining.get(msg.partition(), 0):
            remaining.pop(msg.partition(), None)

    consumer.close()
    return list(latest.values())


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
    return [{"outcome": o, "token_id": i} for o, i in zip(outcomes, ids)]


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
    now = now or datetime.now(timezone.utc)
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
        print(f"{rank:2}. [{s['market_id']}] 24h 成交 ${s['volume24hr']:,.0f}  到期 {s['end_date'][:10]}  {s['question']}")
        for t in s["tokens"]:
            print(f"      {t['outcome']:<4} {t['token_id']}")


if __name__ == "__main__":
    main()
