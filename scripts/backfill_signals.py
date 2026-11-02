"""一次性回填：用 bronze 回放过去几天，把 signals 服务上线之前的跳动补写进 pm.events。

和实时同一套代码：原始消息 → MetricsEngine → JumpDetector。区别只有两处：
- 「平时的几倍」用的 Gamma 24 小时成交额：实时用当时最新的名录；这里用 bronze 里每小时的名录快照，取峰值那一刻的。
- 只写在 --cutoff（signals 第一次开始输出的时刻）之前已经走到 final 的跳动；
  那时还没结束的，实时程序预热时有它们的状态，会自己写 closed / final，这里不碰，免得 Kafka 里新版本被旧版本盖掉。

回放按「每天 → 每个分区 → offset」的顺序读，同一市场的消息严格有序（同一市场在同一分区）。
在宿主机跑（读 data/bronze 比容器快得多），7 天约 20 分钟。

    python scripts/backfill_signals.py --start 2026-10-26 --save events.json   # 只回放、存文件，先看结果
    python scripts/backfill_signals.py --load events.json                        # 确认没问题再写 Kafka
"""
import argparse
import bisect
import json
import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

import duckdb

from pmdata.common.config import DATA_DIR, TOPIC_EVENTS
from pmdata.common.kafka import make_producer
from pmdata.ingest.select_markets import get_token_ids
from pmdata.metrics.engine import MetricsEngine
from pmdata.replay.bronze import CLOB_DIR
from pmdata.signals.detector import BASELINE, WINDOW, JumpDetector

MARKETS_DIR = DATA_DIR / "bronze" / "pm.raw.markets"
CUTOFF = "2026-11-02T14:24:46Z"   # signals 容器第一次开始输出（docker logs signals 的「从 … 开始输出」）
MARGIN_MS = 10_000                # 边界上多写 10 秒：两边都写的事件 event_id 相同，后写的覆盖，不会重复


def market_files(start, end):
    days = [start - timedelta(days=1) + timedelta(days=i) for i in range((end - start).days + 2)]
    return [f"{(MARKETS_DIR / f'date={d}').as_posix()}/*.parquet" for d in days if (MARKETS_DIR / f"date={d}").is_dir()]


def load_markets(con, files):
    """bronze 名录快照 → ({asset_id: 选项序号}, {市场: [(毫秒, Gamma 24 小时成交额)] 按时间排})"""
    con.execute(f"""create temp table snap as
        select json_extract_string(value, '$.payload.conditionId') as market, epoch_ms(kafka_timestamp) as ts,
               try_cast(json_extract_string(value, '$.payload.volume24hr') as double) as v,
               json_extract_string(value, '$.payload.version') as version,
               json_extract_string(value, '$.payload.clobTokenIds') as clob_ids,
               json_extract(value, '$.payload.positionIds')::varchar as position_ids,
               json_extract_string(value, '$.payload.outcomes') as outcomes
        from read_parquet({files}, union_by_name = true)
        where value is not null""")
    outcomes = {}
    for version, clob_ids, position_ids, outs in con.execute("""
            select arg_max(version, ts), arg_max(clob_ids, ts), arg_max(position_ids, ts), arg_max(outcomes, ts)
            from snap group by market""").fetchall():
        m = {"version": version, "clobTokenIds": clob_ids, "outcomes": outs,
             "positionIds": json.loads(position_ids) if position_ids else None}
        for i, t in enumerate(get_token_ids(m) or [], 1):
            outcomes[t["token_id"]] = i
    volume = defaultdict(list)
    for market, ts, v in con.execute("select market, ts, v from snap where v is not null order by ts").fetchall():
        volume[market].append((ts, v))
    return outcomes, volume


def gamma_baseline(ev, volume):
    """自己的成交不满 24 小时的跳动：用峰值那一刻最近一次名录快照的 volume24hr（和 detector._event 同一个算法）。"""
    snaps = volume.get(ev["market"], [])
    i = bisect.bisect_right(snaps, (ev["peak"], float("inf"))) - 1
    if i < 0:
        return
    base = snaps[i][1]
    normal = base * WINDOW / BASELINE
    ev.update(baseline_notional_24h=base, baseline_source="gamma",
              volume_ratio=ev["notional"] / normal if normal else None)


def main():
    parser = argparse.ArgumentParser(description="用 bronze 回放补写 signals 上线之前的跳动")
    parser.add_argument("--start", type=date.fromisoformat, help="从哪天开始回放（UTC 日期）")
    parser.add_argument("--end", type=date.fromisoformat, default=datetime.now(UTC).date(), help="回放到哪天（含）")
    parser.add_argument("--cutoff", default=CUTOFF, help="只写这之前已经 final 的跳动（UTC）")
    parser.add_argument("--save", help="回放结果存到这个 JSON 文件，不写 Kafka")
    parser.add_argument("--load", help="不回放，直接把 --save 存的文件写进 Kafka")
    args = parser.parse_args()
    if args.load:
        with open(args.load, encoding="utf-8") as f:
            write(json.load(f))
        return
    if args.start is None:
        parser.error("要么给 --start 回放，要么给 --load")
    cutoff_ms = int(datetime.fromisoformat(args.cutoff).timestamp() * 1000) + MARGIN_MS

    con = duckdb.connect(config={"threads": 4})   # 名录每条 JSON 约 8KB，线程多了解析时内存吃不消
    t0 = time.time()
    outcomes, volume = load_markets(con, market_files(args.start, args.end))
    print(f"名录：{len(outcomes)} 个 token，{len(volume)} 个市场有成交额快照（{time.time() - t0:.0f} 秒）", flush=True)

    engine = MetricsEngine(clock=lambda: "")
    detector = JumpDetector()
    detector.outcomes = outcomes   # volume24h 留空：Gamma 基准回放完按峰值时刻补
    done = {}                      # event_id → cutoff 之前写出的 final 版本
    n = 0

    def take(events, at_ms):
        for ev in events:
            if ev["status"] == "final" and at_ms < cutoff_ms:
                done[ev["event_id"]] = ev

    d = args.start
    while d <= args.end:
        path = CLOB_DIR / f"date={d}"
        if path.is_dir():
            cur = con.execute(f"""select value from read_parquet('{path.as_posix()}/*.parquet', union_by_name = true)
                where coalesce(event_type, '') not in ('best_bid_ask', 'tick_size_change')
                qualify row_number() over (partition by kafka_partition, kafka_offset) = 1
                order by kafka_partition, kafka_offset""")
            while rows := cur.fetchmany(50000):
                for (v,) in rows:
                    for topic, _, rec in engine.apply(json.loads(v)):
                        take(detector.apply(topic, rec), rec.get("second") or rec["minute"])
                    n += 1
            print(f"{d}：累计 {n} 条消息，{time.time() - t0:.0f} 秒，{len(done)} 次跳动", flush=True)
        d += timedelta(days=1)
    # 回放结束时还没走完的跳动不写（实时程序会写）

    events = sorted(done.values(), key=lambda e: e["peak"])
    for ev in events:
        if ev["baseline_source"] is None:
            gamma_baseline(ev, volume)
    by_source = defaultdict(int)
    for ev in events:
        by_source[ev["baseline_source"]] += 1
    print(f"要写的跳动 {len(events)} 次（≥5 个点 {sum(abs(e['change_points']) >= 5 for e in events)} 次），"
          f"基准：{dict(by_source)}")
    if events:
        first, last = (datetime.fromtimestamp(events[i]["peak"] / 1000, UTC) for i in (0, -1))
        print(f"峰值时间 {first:%m-%d %H:%M} ～ {last:%m-%d %H:%M} UTC")
    if args.save:
        with open(args.save, "w", encoding="utf-8") as f:
            json.dump(events, f)
        print("已存到", args.save)
    else:
        write(events)


def write(events):
    producer = make_producer()
    for ev in events:
        producer.produce(TOPIC_EVENTS, key=ev["event_id"], value=json.dumps(ev))
        producer.poll(0)
    left = producer.flush(60)
    print(f"写完 {len(events)} 条" if left == 0 else f"还有 {left} 条没确认写入")


if __name__ == "__main__":
    main()
