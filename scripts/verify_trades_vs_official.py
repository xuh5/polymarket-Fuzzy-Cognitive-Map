"""一次性核对（阶段 2 验收用）：和 Polymarket 官方 Data API v2 的成交记录对比，按 (transaction_hash, token_id) 配对。

接口：GET https://data-api.polymarket.com/v2/trades?condition=<conditionId>&filter_amount=0&taker_only=true
- filter_amount 默认会滤掉 0.01 份以下的小单，必须设 0；start/end 只对按钱包查询生效，按市场只能从新往旧翻页；
- timestamp 是链上区块时间（秒），所以靠哈希配对，不靠时间。
挑窗口内一直在采集、成交最多的 5 个市场。先在 dbt/ 下跑 dbt build。

用法：python scripts/verify_trades_vs_official.py --start 2026-10-20T13:00 --end 2026-10-20T19:30
"""
import argparse
import time
from collections import defaultdict
from datetime import UTC, datetime

import duckdb
import requests

from pmdata.common.config import DATA_DIR

parser = argparse.ArgumentParser(description="和官方 Data API v2 对比成交")
parser.add_argument("--start", required=True, help="窗口开始（UTC），如 2026-10-20T13:00")
parser.add_argument("--end", required=True, help="窗口结束（UTC）")
args = parser.parse_args()
START = datetime.fromisoformat(args.start).replace(tzinfo=UTC)
END = datetime.fromisoformat(args.end).replace(tzinfo=UTC)
wh = duckdb.connect(str(DATA_DIR / "warehouse.duckdb"), read_only=True)
wh.execute("set TimeZone='UTC'")

# 窗口内一直在采集、成交最多的 5 个市场：窗口开始前就有数据，窗口结束后还有数据
markets = wh.execute("""
    select market_id, any_value(m.question) from gold.fct_trades f join gold.dim_markets m using (market_id)
    where market_id in (select market_id from gold.fct_top_of_book group by 1
                        having min(exchange_ts) < ? and max(exchange_ts) > ?)
      and exchange_ts >= ? and exchange_ts < ?
    group by 1 order by count(*) desc limit 5""", [START, END, START, END]).fetchall()


def official_trades(cond):
    """从最新往回翻页，翻到区块时间早于窗口开始为止（按市场查时 start/end 参数不生效）。"""
    out, cursor = [], None
    while True:
        params = {"condition": cond, "filter_amount": 0, "taker_only": "true", "limit": 500}
        if cursor:
            params = {"condition": cond, "cursor": cursor, "filter_amount": 0, "taker_only": "true"}
        r = requests.get("https://data-api.polymarket.com/v2/trades", params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        out += body["data"]
        cursor = body["pagination"].get("next_cursor")
        if not cursor or not body["data"] or min(t["timestamp"] for t in body["data"]) < START.timestamp():
            return out
        time.sleep(0.3)


for cond, question in markets:
    off = [t for t in official_trades(cond) if START.timestamp() <= t["timestamp"] < END.timestamp()]
    ours = wh.execute("""select transaction_hash, asset_id, size, price, side, exchange_ts from gold.fct_trades
        where market_id = ? and exchange_ts >= ? and exchange_ts < ?""", [cond, START, END]).fetchall()
    o_sz, m_sz = defaultdict(float), defaultdict(float)
    for t in off:
        o_sz[(t["transaction_hash"].lower(), t["token_id"])] += t["size"]
    for h, a, s, *_ in ours:
        m_sz[(h.lower(), a)] += float(s)
    both = o_sz.keys() & m_sz.keys()
    size_ok = sum(abs(o_sz[k] - m_sz[k]) < 1e-6 for k in both)
    only_off, only_ours = o_sz.keys() - m_sz.keys(), m_sz.keys() - o_sz.keys()
    print(f"\n{question[:70]}")
    print(f"  官方成交行 {len(off)}（{len(o_sz)} 个 tx+token）  我们 {len(ours)} 行（{len(m_sz)} 个 tx+token）")
    print(f"  配上 {len(both)} 个，其中数量一致 {size_ok} 个；"
          f"只在官方 {len(only_off)} 个；只在我们 {len(only_ours)} 个")
    for k in list(only_off)[:3]:
        t = next(t for t in off if (t["transaction_hash"].lower(), t["token_id"]) == k)
        print(f"    只在官方：{datetime.fromtimestamp(t['timestamp'], UTC):%H:%M:%S} {t['outcome']} {t['side']} "
              f"{t['price']} × {t['size']}  tx {k[0][:12]}…")
    for k in list(only_ours)[:3]:
        r = next(r for r in ours if (r[0].lower(), r[1]) == k)
        print(f"    只在我们：{r[5]:%H:%M:%S} {r[4]} {r[3]} × {r[2]}  tx {k[0][:12]}…")
    for k in [k for k in both if abs(o_sz[k] - m_sz[k]) >= 1e-6][:3]:
        print(f"    数量不同：tx {k[0][:12]}… 官方 {o_sz[k]} / 我们 {m_sz[k]}")
