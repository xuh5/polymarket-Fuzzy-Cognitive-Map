"""一次性核对（阶段 2 验收用）：用 Python 直接解析 bronze 原始 JSON，重算成交和买一卖一变化，和 gold 表逐条比对。

不经过 SQL、用另一套独立实现重算，才能发现 SQL 自己的 bug。只比对仓库建表时已读到的 offset（之后 archiver 新写的不算）。
挑 4 个最近 1 小时最活跃的市场 + 4 个随机市场。先在 dbt/ 下跑 dbt build。

用法：python scripts/verify_gold_vs_bronze.py [随机种子]
"""
import json
import random
import sys
from decimal import Decimal

import duckdb
import pyarrow.compute as pc
import pyarrow.dataset as ds

from pmdata.common.config import DATA_DIR

random.seed(int(sys.argv[1]) if len(sys.argv) > 1 else 7)
wh = duckdb.connect(str(DATA_DIR / "warehouse.duckdb"), read_only=True)
markets = [r[0] for r in wh.sql("select distinct market_id from gold.fct_top_of_book order by 1").fetchall()]
# 4 个最近最活跃的（正在采集）+ 4 个随机的
active = [r[0] for r in wh.sql("""select market_id from gold.fct_top_of_book
    where exchange_ts > (select max(exchange_ts) from gold.fct_top_of_book) - interval 1 hour
    group by 1 order by count(*) desc limit 4""").fetchall()]
picked = active + random.sample([m for m in markets if m not in active], 4)
# 仓库建表时 bronze 读到了每个分区的哪个 offset：只比对这之前的，之后 archiver 新写的不算
cutoff = dict(wh.sql("""select kafka_partition, max(kafka_offset) from (
    select kafka_partition, kafka_offset from silver.stg_clob__price_changes union all
    select kafka_partition, kafka_offset from silver.stg_clob__trades union all
    select kafka_partition, kafka_offset from silver.stg_clob__book_levels union all
    select kafka_partition, kafka_offset from silver.stg_clob__markers) group by 1""").fetchall())
print("截止 offset:", cutoff)

# 只用 pyarrow 读原始文件，不经过 SQL
bronze = ds.dataset(str(DATA_DIR / "bronze" / "pm.raw.clob"), format="parquet", partitioning="hive")
cols = ["kafka_partition", "kafka_offset", "market", "event_type", "source", "value"]
tbl = bronze.to_table(columns=cols, filter=pc.field("market").isin(picked))
recs = {}
for r in tbl.to_pylist():          # 按 partition+offset 去重（at-least-once 可能重复）
    if r["kafka_offset"] > cutoff[r["kafka_partition"]]:
        continue
    recs[(r["kafka_partition"], r["kafka_offset"])] = r
ordered = sorted(recs.values(), key=lambda r: (r["kafka_partition"], r["kafka_offset"]))

def D(s):
    return Decimal(s) if s is not None else None


all_ok = True
for m in picked:
    my_trades, my_tob, last = set(), [], {}
    for r in (x for x in ordered if x["market"] == m):
        p = json.loads(r["value"])["payload"]
        po = (r["kafka_partition"], r["kafka_offset"])
        if r["event_type"] == "last_trade_price":
            my_trades.add((f"{po[0]}-{po[1]}", p["asset_id"], D(p["price"]), D(p["size"]), p["side"]))
        elif r["event_type"] == "price_change":
            for i, ch in enumerate(p["price_changes"], 1):
                cur = (D(ch["best_bid"]), D(ch["best_ask"]))
                if last.get(ch["asset_id"], "首次") != cur:
                    my_tob.append((f"{po[0]}-{po[1]}-{i}", ch["asset_id"], *cur))
                last[ch["asset_id"]] = cur
        elif r["event_type"] == "book" or r["source"] == "clob_rest":
            bids = [D(x["price"]) for x in p["bids"]]
            asks = [D(x["price"]) for x in p["asks"]]
            cur = (max(bids) if bids else None, min(asks) if asks else None)
            if last.get(p["asset_id"], "首次") != cur:
                my_tob.append((f"{po[0]}-{po[1]}-0", p["asset_id"], *cur))
            last[p["asset_id"]] = cur
    g_trades = set(wh.execute("select trade_id, asset_id, price, size, side from gold.fct_trades "
                              "where market_id = ?", [m]).fetchall())
    g_tob = set(wh.execute("select tob_id, asset_id, best_bid, best_ask from gold.fct_top_of_book "
                           "where market_id = ?", [m]).fetchall())
    ok = my_trades == g_trades and set(my_tob) == g_tob
    all_ok &= ok
    print(f"{'一致' if ok else '不一致'}  {m[:12]}…  成交 Python {len(my_trades)} / gold {len(g_trades)}   "
          f"买一卖一变化 Python {len(my_tob)} / gold {len(g_tob)}"
          + ("" if ok else f"  只在Python:{len(set(my_tob)-g_tob)} 只在gold:{len(g_tob-set(my_tob))}"))
print("全部一致" if all_ok else "有不一致")
