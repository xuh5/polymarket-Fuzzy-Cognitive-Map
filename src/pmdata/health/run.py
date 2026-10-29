"""管道健康程序（容器 health）：把管道自己的运行状况写进 Postgres，给 Status 页面和告警用。

一个循环里做这几件事：
- 每 LAG_EVERY 秒记录一次各 consumer group 的消费积压（lag.py），顺便把上一分钟的延迟汇总一下（latency.py）
- 其余时间用自己的 consumer（group = pm-health）读 pm.raw.clob：
  - 挑出缺口标记记成断线重连（reconnects.py），重连记录一直保留（一天才几条）
  - 自己维护一份订单簿，统计乱序和「盘口对不上」（quality.py），每分钟一行写进 health_stream_quality
- 和 metrics 一样，订单簿在内存里，快照只在采集器重连时才有，所以每次启动先往回倒 WARMUP_HOURS 小时预热：
  只更新订单簿、不计数，读到上次提交的位置之后才开始计数（从哪读、从哪计数的规则和 metrics 共用 plan_start）。

可靠性：重连记录先写数据库、提交事务，成功后才提交 offset（at-least-once），重复写入按 conn_id 忽略。
质量计数在内存里攒着，每分钟把最近的分钟整份 upsert 一次；崩溃重启时重启那一分钟会少算一点。
写数据库出错直接退出，让 Docker 重启。
"""
import argparse
import json
import logging
import signal
import time
from datetime import UTC, datetime, timedelta

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from pmdata.common.config import KAFKA_BOOTSTRAP, PG_DSN, TOPIC_CLOB
from pmdata.common.log import setup_logging
from pmdata.health import lag, latency, quality, reconnects
from pmdata.metrics.run import WARMUP_HOURS, plan_start

log = logging.getLogger("health")

GROUP_ID = "pm-health"
LAG_EVERY = 60
CLEANUP_SECONDS = 3600
BATCH_SIZE = 5000
KEEP_MINUTES = 120     # 质量计数在内存里留多久（追积压时晚到的分钟还能补上）


class Reader:
    """pm.raw.clob 的读取进度：分区分配时决定从哪预热、从哪开始计数。只跑一个实例。"""

    def __init__(self, consumer):
        self.consumer = consumer
        self.quality = quality.StreamQuality()
        self.resume = {}     # 分区 → 从哪个 offset 开始计数
        self.position = {}   # 分区 → 下一条要读的 offset

    def on_assign(self, consumer, partitions):
        committed = {tp.partition: tp.offset for tp in consumer.committed(partitions, timeout=10)}
        since = int(time.time() * 1000) - WARMUP_HOURS * 3600 * 1000
        warm = {tp.partition: tp.offset for tp in consumer.offsets_for_times(
            [TopicPartition(TOPIC_CLOB, tp.partition, since) for tp in partitions], timeout=10)}
        self.quality = quality.StreamQuality()   # 重新预热，旧状态不要了
        self.resume, self.position = {}, {}
        assign = []
        for tp in partitions:
            p = tp.partition
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            start, resume = plan_start(committed[p] if committed[p] >= 0 else None,
                                       warm[p] if warm[p] >= 0 else None, low, high)
            self.resume[p], self.position[p] = resume, start
            assign.append(TopicPartition(TOPIC_CLOB, p, start))
            log.info("分区 %d：从 %d 开始预热，从 %d 开始计数（预热 %d 条）", p, start, resume, resume - start)
        consumer.assign(assign)

    def handle(self, msg):
        p = msg.partition()
        self.quality.counting = msg.offset() >= self.resume[p]
        self.quality.apply(json.loads(msg.value()))
        self.position[p] = msg.offset() + 1

    def commit(self):
        """预热中的分区不提交（不能把提交位置往回拨）。"""
        offsets = [TopicPartition(TOPIC_CLOB, p, pos) for p, pos in self.position.items() if pos >= self.resume[p]]
        if offsets:
            self.consumer.commit(offsets=offsets, asynchronous=False)


def write_reconnects(conn, msgs):
    rows = reconnects.reconnect_rows(m.value() for m in msgs)
    if not rows:
        return
    with conn.transaction(), conn.cursor() as cur:
        for row in rows.values():
            cur.execute(reconnects.INSERT, row)
            if cur.rowcount:   # 只给第一次写进去的打日志（同一次重连在每个分区都有标记）
                _, back, _down, gap, reason = row
                log.info("重连 %s，停了 %s 秒（%s）", back.isoformat(timespec="seconds"),
                         "?" if gap is None else f"{gap:.1f}", reason)


def write_quality(conn, q):
    rows = q.rows()
    if rows:
        with conn.transaction(), conn.cursor() as cur:
            cur.executemany(quality.UPSERT, rows)
        last = dict(zip(["minute", *quality.FIELDS], rows[-1], strict=True))
        log.info("行情质量（%s）：%s", last.pop("minute")[:16], last)
    q.drop_before((datetime.now(UTC) - timedelta(minutes=KEEP_MINUTES)).isoformat()[:16])


def main():
    parser = argparse.ArgumentParser(description="管道健康指标 → Postgres")
    parser.add_argument("--group-id", default=GROUP_ID, help="consumer group（试运行时换一个，不影响正式进度）")
    parser.add_argument("--duration", type=int, help="运行多少秒后退出；不填则一直运行")
    args = parser.parse_args()
    setup_logging()

    # 自动提交：单独的查询（比如补算延迟时查 min）不会悄悄开一个没人提交的事务；要原子写入的地方用 conn.transaction()
    conn = psycopg.connect(PG_DSN, autocommit=True)
    with conn.transaction():
        conn.execute(lag.SCHEMA)
        conn.execute(reconnects.SCHEMA)
        conn.execute(latency.SCHEMA)
        conn.execute(quality.SCHEMA)
    latency.backfill(conn)
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": args.group_id,
        "enable.auto.commit": False,       # 数据库提交成功后手动提交
    })
    reader = Reader(consumer)

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([TOPIC_CLOB], on_assign=reader.on_assign)
    log.info("开始（group=%s），积压每 %d 秒记一次，盯 %s", args.group_id, LAG_EVERY, lag.GROUPS)
    deadline = time.monotonic() + args.duration if args.duration else None
    next_lag = time.monotonic()
    last_cleanup = 0.0
    try:
        while not stopping and (deadline is None or time.monotonic() < deadline):
            now = time.monotonic()
            if now >= next_lag:
                next_lag += LAG_EVERY
                lag.record(conn, admin)
                latency.record(conn)
                write_quality(conn, reader.quality)
            if now - last_cleanup >= CLEANUP_SECONDS:
                lag.cleanup(conn)
                last_cleanup = now

            msgs = [m for m in consumer.consume(num_messages=BATCH_SIZE, timeout=0.5) if not m.error()]
            if not msgs:
                continue
            for m in msgs:
                reader.handle(m)
            write_reconnects(conn, msgs)
            reader.commit()
        write_quality(conn, reader.quality)
        log.info("停止")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
