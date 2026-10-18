"""检查 pm.raw.clob 一段时间内的数据质量：断线、缺口、每小时消息量、重复、缺字段。

默认检查最近 24 小时；--hours 指定小时数。只读 Kafka，不写任何东西。
"""
import argparse
import json
import os
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")   # 容器里设成 kafka:19092
TOPIC = "pm.raw.clob"
LOW_HOUR_RATIO = 0.5   # 某小时消息量低于中位数的这个比例，就标出来


def parse_time(s):
    return datetime.fromisoformat(s) if s else None


def read_since(since):
    """用 Kafka 的按时间定位，从 since 那一刻开始读到当前末尾。"""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "check-clob",
        "enable.auto.commit": False,
    })
    meta = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
    since_ms = int(since.timestamp() * 1000)
    # 问 Kafka：每个分区里，时间戳 >= since 的第一条消息在哪个 offset
    starts = consumer.offsets_for_times(
        [TopicPartition(TOPIC, p, since_ms) for p in meta.partitions], timeout=10)

    remaining = {}
    for tp in starts:
        _, high = consumer.get_watermark_offsets(tp, timeout=10)
        if tp.offset >= 0 and tp.offset < high:   # offset 为 -1 表示这个分区在 since 之后没有消息
            remaining[tp.partition] = high
    consumer.assign([tp for tp in starts if tp.partition in remaining])

    while remaining:
        msg = consumer.poll(10)
        if msg is None:
            raise TimeoutError(f"读取超时，还没读完的分区：{sorted(remaining)}")
        if msg.error():
            raise RuntimeError(msg.error())
        yield msg
        if msg.offset() + 1 >= remaining.get(msg.partition(), 0):
            remaining.pop(msg.partition(), None)
    consumer.close()


def main(hours):
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)
    print(f"检查 {TOPIC}：{since:%Y-%m-%d %H:%M} ~ {now:%Y-%m-%d %H:%M} UTC（{hours:g} 小时）\n")

    total = 0
    by_type = Counter()
    by_hour = Counter()
    by_minute = Counter()
    no_received_at = 0
    seen_frames = set()
    duplicates = 0
    connections = {}              # conn_id → 这次连接的缺口信息（缺口标记每个市场一条，按连接去重）
    first = last = None

    for msg in read_since(since):
        if msg.value() is None:
            continue
        e = json.loads(msg.value())
        total += 1
        payload = e.get("payload") or {}
        by_type[(e.get("source"), payload.get("event_type", "(rest book)"))] += 1

        received_at = parse_time(e.get("received_at"))
        if received_at is None:
            no_received_at += 1
            continue
        first = min(first or received_at, received_at)
        last = max(last or received_at, received_at)
        by_hour[received_at.strftime("%m-%d %H:00")] += 1
        by_minute[received_at.strftime("%m-%d %H:%M")] += 1

        # 重复：同一连接、同一帧、同一下标的消息出现两次
        if e.get("seq") is not None:
            frame = (e.get("conn_id"), e["seq"], e.get("frame_index"))
            duplicates += frame in seen_frames
            seen_frames.add(frame)

        if payload.get("event_type") == "gap" and payload.get("reason") != "加入订阅名单":
            connections.setdefault(e.get("conn_id"), payload)

    if total == 0:
        print("这段时间没有消息")
        return

    # ---------- 输出 ----------
    span_hours = (last - first).total_seconds() / 3600
    print(f"消息总数 {total:,}，覆盖 {first:%m-%d %H:%M:%S} ~ {last:%m-%d %H:%M:%S}（{span_hours:.1f} 小时）")
    print(f"缺 received_at 的消息 {no_received_at} 条；重复消息 {duplicates} 条\n")

    print("按类型：")
    for (source, event_type), n in by_type.most_common():
        print(f"  {source:<10} {event_type:<18} {n:>10,}")

    per_sec = total / max((last - first).total_seconds(), 1)
    peak_minute, peak_n = by_minute.most_common(1)[0]
    print(f"\n吞吐量：平均 {per_sec:.1f} 条/秒；峰值分钟 {peak_minute} 共 {peak_n:,} 条（{peak_n / 60:.1f} 条/秒）")

    # 断线和缺口：每次连接写一组缺口标记，disconnected_at 为空表示采集器刚启动
    restarts = [g for g in connections.values() if g.get("disconnected_at") is None]
    reconnects = [g for g in connections.values() if g.get("disconnected_at")]
    gap_seconds = [
        (parse_time(g["reconnected_at"]) - parse_time(g["disconnected_at"])).total_seconds()
        for g in reconnects
    ]
    refreshes = by_type[("collector", "unsubscribed")]
    print(f"\n名单刷新共移出市场 {refreshes} 次")
    if not connections:
        print("这段时间内没有新连接：采集器全程在线，没有断线")
    else:
        print(f"这段时间内新连接 {len(connections)} 次：采集器启动 {len(restarts)} 次，断线重连 {len(reconnects)} 次")
    for g, sec in zip(reconnects, gap_seconds):
        print(f"  断线 {g['disconnected_at'][5:19]} → 恢复 {g['reconnected_at'][11:19]}，缺口 {sec:.1f} 秒（{g['reason']}）")
    for g in restarts:
        print(f"  采集器启动 {g['reconnected_at'][5:19]}（之前停了多久要看上一条消息的时间）")
    if gap_seconds:
        covered = (last - first).total_seconds()
        print(f"  断线缺口合计 {sum(gap_seconds):.1f} 秒，占覆盖时长 {sum(gap_seconds) / covered:.4%}")

    # 每小时消息量，低于中位数一半的标出来（首尾两个小时不完整，不参与判断）
    hours_sorted = sorted(by_hour)
    middle = hours_sorted[1:-1] or hours_sorted
    median = statistics.median(by_hour[h] for h in middle)
    print(f"\n每小时消息量（中位数 {median:,.0f}）：")
    for h in hours_sorted:
        n = by_hour[h]
        flag = "  ← 偏低" if h in middle and n < LOW_HOUR_RATIO * median else ""
        print(f"  {h}  {n:>8,}  {'█' * round(40 * n / max(by_hour.values()))}{flag}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=float, default=24, help="检查最近多少小时，默认 24")
    args = parser.parse_args()
    main(args.hours)
