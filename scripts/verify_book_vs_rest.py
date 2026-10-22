"""阶段 3 验收：本地重建的订单簿 vs 官方 REST 快照（POST /books），逐档对比。

用和 metrics 程序同一份 BookStore 代码：先从 pm.raw.clob 往回预热 WARMUP_HOURS 小时、追上实时，
然后每隔 --every 秒向官方要一次所有可用 token 的盘口。每个快照带交易所时间戳 T，
拿本地「处理到 T 为止」的盘口来比：继续读 Kafka，这个市场一来时间 > T 的消息，就在处理它之前比
（没有更晚的消息就在最后宽限 SETTLE_SECONDS 秒后比当前状态）。
每个价位的数量都相同才算这个盘口一致。

用法：python scripts/verify_book_vs_rest.py [--rounds 10] [--every 60] [--warmup-hours 24]
"""
import argparse
import json
import time
from collections import Counter
from decimal import Decimal

import requests
from confluent_kafka import Consumer, TopicPartition

from pmdata.book.orderbook import BookStore
from pmdata.common.config import CLOB_REST_URL, KAFKA_BOOTSTRAP, TOPIC_CLOB

SETTLE_SECONDS = 20

parser = argparse.ArgumentParser(description="本地订单簿 vs 官方 REST 快照")
parser.add_argument("--rounds", type=int, default=10, help="抽查几轮")
parser.add_argument("--every", type=int, default=60, help="每轮间隔秒数")
parser.add_argument("--warmup-hours", type=float, default=24, help="预热往回多少小时")
args = parser.parse_args()

consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "pm-verify-book-rest",
                     "enable.auto.commit": False})
parts = consumer.list_topics(TOPIC_CLOB, timeout=10).topics[TOPIC_CLOB].partitions
since = int(time.time() * 1000 - args.warmup_hours * 3600 * 1000)
starts = consumer.offsets_for_times([TopicPartition(TOPIC_CLOB, p, since) for p in parts], timeout=10)
ends = {p: consumer.get_watermark_offsets(TopicPartition(TOPIC_CLOB, p), timeout=10)[1] for p in parts}
consumer.assign([tp if tp.offset >= 0 else TopicPartition(TOPIC_CLOB, tp.partition, ends[tp.partition])
                 for tp in starts])

store = BookStore()
pending = {}            # asset_id → (T, 官方快照, 第几轮, 组别)
stats = Counter()
examples = []


def levels(entries):
    return {Decimal(x["price"]): Decimal(x["size"]) for x in entries}


def compare(aid, why):
    t, rest, rnd, group = pending.pop(aid)
    book = store.get(aid)
    if book is None:
        stats["本地盘口不可用（断线等快照）"] += 1
        return
    stats[f"比较时机：{why}"] += 1
    if group != "对齐":
        stats["官方快照比本地旧（无法对齐）"] += 1
        stats["  其中仍然完全一致" if _diff(book, rest, rnd, t, record=False) == 0 else "  其中有差异"] += 1
        return
    stats["比较的盘口"] += 1
    bad = _diff(book, rest, rnd, t, record=True)
    stats["完全一致的盘口" if bad == 0 else "有差异的盘口"] += 1


def _diff(book, rest, rnd, t, record):
    bad = 0
    for side, ours, theirs in (("BUY", book.bids, levels(rest["bids"])), ("SELL", book.asks, levels(rest["asks"]))):
        for price in ours.keys() | theirs.keys():
            if record:
                stats["比较的价位"] += 1
            if ours.get(price) != theirs.get(price):
                bad += 1
                if record:
                    stats["不一致的价位"] += 1
                    if len(examples) < 10:
                        examples.append((rnd, rest["asset_id"][:10], side, str(price), str(ours.get(price)),
                                         str(theirs.get(price)), t, book.exchange_ts))
    return bad


def handle(msg):
    env = json.loads(msg.value())
    p = env["payload"]
    if env["source"] != "collector" and p.get("timestamp"):
        ts = int(p["timestamp"])
        market = p["market"]
        for aid in [a for a, (t, rest, _, _) in pending.items() if rest["market"] == market and ts > t]:
            compare(aid, "收到更晚的消息")
    store.apply(env)


# 预热：读到启动时的末尾
t0 = time.time()
caught = {p: ends[p] <= (tp.offset if tp.offset >= 0 else ends[p]) for p, tp in zip(parts, starts, strict=True)}
n = 0
while not all(caught.values()):
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    handle(msg)
    n += 1
    if msg.offset() + 1 >= ends[msg.partition()]:
        caught[msg.partition()] = True
print(f"预热完成：{n:,} 条，{time.time() - t0:.0f} 秒；可用盘口 {sum(b.valid for b in store.books.values())} 个")

http = requests.Session()
next_round, rnd = time.time(), 0
deadline = None
while True:
    now = time.time()
    if rnd < args.rounds and now >= next_round:
        rnd += 1
        next_round = now + args.every
        aids = [a for a, b in store.books.items() if b.valid and a not in pending]
        resp = http.post(f"{CLOB_REST_URL}/books", json=[{"token_id": a} for a in aids], timeout=15)
        resp.raise_for_status()
        for rest in resp.json():
            t = int(rest["timestamp"])
            book = store.get(rest["asset_id"])
            group = "对齐" if book is None or book.exchange_ts <= t else "官方旧"
            pending[rest["asset_id"]] = (t, rest, rnd, group)
        print(f"第 {rnd} 轮：要了 {len(aids)} 个 token 的快照")
        if rnd == args.rounds:
            deadline = now + SETTLE_SECONDS
    if deadline and now >= deadline:
        break
    msg = consumer.poll(0.2)
    if msg is not None and not msg.error():
        handle(msg)
for aid in list(pending):
    compare(aid, f"{SETTLE_SECONDS} 秒内没有更晚的消息，比当前状态")
consumer.close()

print("\n结果：")
for k, v in sorted(stats.items()):
    print(f"  {k}：{v:,}")
if stats["比较的盘口"]:
    print(f"  能对齐的：盘口一致率 {stats['完全一致的盘口'] / stats['比较的盘口']:.2%}，"
          f"价位一致率 {1 - stats['不一致的价位'] / stats['比较的价位']:.4%}")
for e in examples:
    print("  不一致：第 {} 轮 token {}… {} 价位 {}：本地 {} / 官方 {}（官方时间 {}，本地最后更新 {}）".format(*e))
