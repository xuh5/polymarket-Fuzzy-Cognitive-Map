"""管道延迟：每分钟把 metrics_book 里这一分钟写进数据库的指标汇总成 p50 / p99，一分钟一行写进 health_latency。

每条盘口指标带着四个时间，相邻两个相减就是一段延迟：
- 交易所 → 收到（exchange_ts → received_at）：网络 + 本机时钟误差（本机时钟偏慢时会是负数）
- 收到 → 算完（received_at → computed_at）：metrics 每秒输出一次、要等这一秒结束 + 最多 3 秒宽限，是设计上的等待
- 算完 → 入库（computed_at → inserted_at）：经过 Kafka、pg-sink 每批写一次
- 端到端（exchange_ts → inserted_at）
received_at、computed_at、inserted_at 都来自 Docker 里的容器，用的是同一个时钟；只有交易所时间是别人的钟。

按 inserted_at 归到分钟。每次重算最近 RECOMPUTE_MINUTES 个已经结束的分钟（upsert），晚到的行也能算进去；
程序启动时把 metrics_book 里现存的（7 天）全部算一遍。metrics_book 只留 7 天，这张表一直保留（一天才 1440 行）。
"""
import logging
from datetime import UTC, datetime, timedelta

log = logging.getLogger("health-latency")

RECOMPUTE_MINUTES = 5
SEGMENTS = {                       # 列名前缀 → (起点, 终点)
    "exch_recv": ("exchange_ts", "received_at"),
    "recv_comp": ("received_at", "computed_at"),
    "comp_ins": ("computed_at", "inserted_at"),
    "end_to_end": ("exchange_ts", "inserted_at"),
}
QUANTILES = {"p50": 0.5, "p99": 0.99}
COLS = [f"{seg}_{q}_ms" for seg in SEGMENTS for q in QUANTILES]

SCHEMA = f"""
create table if not exists health_latency (
    minute timestamptz primary key,   -- 按写进数据库的时间（inserted_at）归到分钟
    rows integer not null,            -- 这一分钟写进了多少条盘口指标
    {", ".join(f"{c} double precision" for c in COLS)}
);
"""

AGGREGATES = ",\n    ".join(
    f"percentile_cont({q}) within group (order by extract(epoch from {end} - {start}) * 1000)"
    for start, end in SEGMENTS.values() for q in QUANTILES.values())

# second（交易所时间）上有索引，先用它粗筛；指标从交易所到入库最多几十秒（预热补输出时），留 1 小时足够
UPSERT = f"""
insert into health_latency (minute, rows, {", ".join(COLS)})
select date_trunc('minute', inserted_at), count(*),
    {AGGREGATES}
from metrics_book
where inserted_at >= %(start)s and inserted_at < %(end)s
  and second >= %(start)s - interval '1 hour'
  and exchange_ts is not null and received_at is not null and computed_at is not null
group by 1
on conflict (minute) do update set rows = excluded.rows, {", ".join(f"{c} = excluded.{c}" for c in COLS)}
"""


def record(conn, start=None):
    """重算 [start, 当前这一分钟开始) 的每一分钟；start 为空就是最近 RECOMPUTE_MINUTES 分钟。"""
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    if start is None:
        start = end - timedelta(minutes=RECOMPUTE_MINUTES)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(UPSERT, {"start": start, "end": end})
        return cur.rowcount


def backfill(conn):
    """启动时把 metrics_book 里现存的都算一遍（7 天约 1 万分钟）。"""
    first = conn.execute("select min(inserted_at) from metrics_book").fetchone()[0]
    if first is not None:
        n = record(conn, first.replace(second=0, microsecond=0))
        log.info("补算延迟：%d 分钟", n)
