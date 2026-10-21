"""核对（阶段 3）：把 bronze 按 offset 顺序回放进本地订单簿，
比对算出的买一卖一 vs price_change 里交易所给的 best_bid/best_ask。

交易所会把同一时刻的一次更新拆成好几条消息，每条带的最优价都是「全部改完之后」的，
所以按（token, 交易所时间戳）分组：同一时刻的消息全部处理完再比。
交易所用 best_bid=0 表示买盘为空、best_ask=1 表示卖盘为空。

用法：python scripts/verify_book_replay.py [日期，如 2026-10-20；默认全部]
"""
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal

import duckdb

from pmdata.book.orderbook import BookStore
from pmdata.common.config import DATA_DIR

day = sys.argv[1] if len(sys.argv) > 1 else "*"
rel = duckdb.connect().sql(f"""
    select value from read_parquet('{(DATA_DIR / "bronze/pm.raw.clob").as_posix()}/date={day}/*.parquet')
    where coalesce(event_type, '') not in ('new_market', 'best_bid_ask', 'last_trade_price')
    qualify row_number() over (partition by kafka_partition, kafka_offset order by kafka_timestamp) = 1
    order by kafka_partition, kafka_offset
""")
store = BookStore()
pending = {}   # asset_id → (交易所时间戳, 交易所给的最优价)，等这个 token 出现新时间戳或别的事件时再比
checked = 0
bad = Counter()   # (分钟, 市场) → 次数


def settle(aid):
    global checked
    ts, theirs = pending.pop(aid)
    book = store.get(aid)
    if book is None:
        return
    checked += 1
    if (book.best_bid() or Decimal(0), book.best_ask() or Decimal(1)) != theirs:
        minute = datetime.fromtimestamp(int(ts) // 1000, UTC).strftime("%m-%d %H:%M")
        bad[(minute, book.market[:10])] += 1


while batch := rel.fetchmany(50000):
    for (value,) in batch:
        env = json.loads(value)
        p = env["payload"]
        if env["source"] == "collector":
            hit = [a for a in pending if a in store.books and store.books[a].market == p["market"]]
        elif p.get("event_type") == "price_change":
            aids = {c["asset_id"] for c in p["price_changes"]}
            hit = [a for a in pending if a in aids and pending[a][0] != p["timestamp"]]
        else:
            hit = [a for a in pending if a == p.get("asset_id")]
        for aid in hit:
            settle(aid)
        store.apply(env)
        if p.get("event_type") == "price_change":
            for c in p["price_changes"]:
                pending[c["asset_id"]] = (p["timestamp"], (Decimal(c["best_bid"]), Decimal(c["best_ask"])))
for aid in list(pending):
    settle(aid)

total = sum(bad.values())
print(f"比对 {checked:,} 次，不一致 {total:,}（{total / max(checked, 1):.4%}）；"
      f"盘口不可用时忽略的变化 {store.ignored:,} 条")
for (minute, market), n in sorted(bad.items()):
    print(f"  {minute} UTC  市场 {market}…  {n} 次")
