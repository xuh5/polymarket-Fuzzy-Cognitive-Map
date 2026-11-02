"""回放用的 bronze 读取：往前找起点（上一份官方完整清单），读出这个市场从起点到窗口结束的原始消息。

不另存快照：交易所每次成交、每次重连都会推完整清单，同一市场一半情况 3 分钟内就有一份；
2026-11-01 实测 10 个跳动，还原前后 10 分钟大多 0.2～0.5 秒，最慢 2.2 秒（起点在 48 小时前）。
每次只读一个市场的消息，所以快；archiver 攒 5 分钟才写文件，最近几分钟的数据还读不到。
"""
import json
from datetime import UTC, datetime, timedelta
from functools import lru_cache

import duckdb

from pmdata.common.config import DATA_DIR
from pmdata.replay.timeline import build

CLOB_DIR = DATA_DIR / "bronze" / "pm.raw.clob"
MAX_LOOKBACK_DAYS = 3
# Kafka 时间（收到时间）比交易所时间晚几百毫秒，按 Kafka 时间筛时窗口末尾多读一点
MARGIN = timedelta(seconds=10)


def from_ms(ms):
    # 时间在 SQL 里先转成毫秒数再取回：DuckDB 把带时区的时间交给 Python 要用 pytz，镜像里没装
    return None if ms is None else datetime.fromtimestamp(ms / 1000, UTC)


def day_globs(lo, hi):
    """lo～hi 之间（UTC 日期，含两头）有 Parquet 文件的日期目录；采集停过的日期没有目录，read_parquet 会报错，跳过"""
    out = []
    d = lo.date()
    while d <= hi.date():
        path = CLOB_DIR / f"date={d}"
        if path.is_dir() and any(path.glob("*.parquet")):
            out.append(f"{path.as_posix()}/*.parquet")
        d += timedelta(days=1)
    return out


def find_start(con, market, start, n_tokens):
    """每个选项在 start 之前最近一份完整清单，取其中最早的时间（Kafka 时间）。

    按天往前扩大范围找，最多 MAX_LOOKBACK_DAYS 天；还找不齐就从最远那天开始读，
    起点时没有清单的选项盘口就是「不可用」，等窗口里来了清单再显示。
    """
    for back in range(MAX_LOOKBACK_DAYS + 1):
        lo = start - timedelta(days=back)
        files = day_globs(lo, start)
        if not files:
            continue
        found, ts = con.execute("""
            select count(*), epoch_ms(min(ts)) from (
              select json_extract_string(value, '$.payload.asset_id') as aid, max(kafka_timestamp) as ts
              from read_parquet(?, union_by_name = true)
              where market = ? and (event_type = 'book' or source = 'clob_rest') and kafka_timestamp <= ?
              group by 1)""", [files, market, start]).fetchone()
        if found >= n_tokens:
            return from_ms(ts)
    return start - timedelta(days=MAX_LOOKBACK_DAYS)


def load(con, market, since, until):
    """这个市场 Kafka 时间在 since～until 之间的信封，按 Kafka 顺序、去重（archiver 是至少一次，可能有重复）"""
    files = day_globs(since, until)
    if not files:
        return []
    rows = con.execute("""
        select value from read_parquet(?, union_by_name = true)
        where market = ? and kafka_timestamp between ? and ?
          and coalesce(event_type, '') not in ('best_bid_ask', 'tick_size_change')
        qualify row_number() over (partition by kafka_partition, kafka_offset) = 1
        order by kafka_partition, kafka_offset""", [files, market, since, until]).fetchall()
    return [json.loads(v) for (v,) in rows]


def archived_until(con):
    """bronze 里最新一条 clob 消息的 Kafka 时间；比它晚的还在 Kafka 里没写下来"""
    dirs = sorted(p for p in CLOB_DIR.glob("date=*") if any(p.glob("*.parquet")))
    if not dirs:
        return None
    return from_ms(con.execute("select epoch_ms(max(kafka_timestamp)) from read_parquet(?)",
                               [f"{dirs[-1].as_posix()}/*.parquet"]).fetchone()[0])


def _replay(market, tokens, start, end):
    with duckdb.connect() as con:
        since = find_start(con, market, start, len(tokens))
        envs = load(con, market, since, end + MARGIN)
    return build(envs, dict(tokens), int(start.timestamp() * 1000), int(end.timestamp() * 1000))


# 同一段被第二个人打开时直接返回；只缓存已经完整归档的窗口（还在归档的下次再读会多出数据）
_cached = lru_cache(maxsize=32)(_replay)


def replay(market, tokens, start, end):
    """tokens：asset_id → 选项序号。返回 (build 的结果, 这段是否已经完整归档)。结果可能被缓存共用，别修改。"""
    with duckdb.connect() as con:
        until = archived_until(con)
    key = tuple(sorted(tokens.items()))
    if until is not None and end + MARGIN <= until:
        return _cached(market, key, start, end), True
    return _replay(market, key, start, end), False
