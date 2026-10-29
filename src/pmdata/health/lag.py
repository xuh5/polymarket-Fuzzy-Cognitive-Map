"""Kafka 消费积压监控：每个 consumer group 还有多少条消息没处理完。

每 INTERVAL_SECONDS 秒问一次 Kafka：
- 每个 group 已提交的 offset（处理到哪了）——不用写 topic 名单，group 提交过哪些分区就查哪些
- 这些分区现在的末尾 offset（topic 写到哪了）
积压 = 末尾 − 已提交，每个分区一行写进 Postgres 表 health_consumer_lag，给 Status 页面和告警用。

只读 Kafka，不加入被监控的 group，不会影响它们的分区分配和进度。
注意 archiver 本来就攒满 5 分钟才提交一次，所以它平时就有几分钟的积压，是正常的。
查 Kafka 出错只跳过这一轮（缺的那几分钟本身就说明出了问题）；写数据库出错直接退出，让 Docker 重启。
"""
import argparse
import logging
import signal
import time
from datetime import UTC, datetime

import psycopg
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from pmdata.common.config import KAFKA_BOOTSTRAP, PG_DSN
from pmdata.common.log import setup_logging

log = logging.getLogger("health-lag")

# 要盯的 consumer group，新加程序时在这里加一行
GROUPS = ["pm-archiver", "pm-metrics", "pm-pg-sink"]
INTERVAL_SECONDS = 60
RETENTION_DAYS = 7
CLEANUP_SECONDS = 3600
TIMEOUT = 10

SCHEMA = """
create table if not exists health_consumer_lag (
    ts timestamptz not null,
    group_id text not null,
    topic text not null,
    partition integer not null,
    committed bigint not null,      -- 已提交的 offset（下一条要处理的位置）
    end_offset bigint not null,     -- 分区末尾 offset（下一条要写入的位置）
    lag bigint not null,            -- 还没处理的条数 = end_offset − committed
    primary key (ts, group_id, topic, partition)
);
create index if not exists health_consumer_lag_group_ts on health_consumer_lag (group_id, ts);
"""

INSERT = ("insert into health_consumer_lag (ts, group_id, topic, partition, committed, end_offset, lag) "
          "values (%s, %s, %s, %s, %s, %s, %s) on conflict do nothing")


def lag_rows(ts, committed, ends):
    """committed：{group: {(topic, partition): offset}}；ends：{(topic, partition): 末尾 offset} → 表的行。

    没提交过的分区（offset < 0）跳过；已提交比末尾还大（刚好读到 Kafka 两次查询之间新写的消息）按 0 算。
    """
    rows = []
    for group, offsets in committed.items():
        for (topic, p), off in sorted(offsets.items()):
            if off < 0 or (topic, p) not in ends:
                continue
            end = ends[(topic, p)]
            rows.append((ts, group, topic, p, off, end, max(end - off, 0)))
    return rows


def fetch(admin, groups):
    """查 Kafka：各 group 已提交的 offset + 这些分区的末尾 offset。"""
    committed = {}
    for group in groups:
        res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result(TIMEOUT)
        committed[group] = {(tp.topic, tp.partition): tp.offset for tp in res.topic_partitions}
    # 先查已提交、后查末尾：末尾只会更大，积压不会被算成负数
    tps = {tp for offsets in committed.values() for tp in offsets}
    ends = {}
    if tps:
        futures = admin.list_offsets({TopicPartition(t, p): OffsetSpec.latest() for t, p in tps},
                                     request_timeout=TIMEOUT)
        ends = {(tp.topic, tp.partition): f.result(TIMEOUT).offset for tp, f in futures.items()}
    return committed, ends


def cleanup(conn):
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(f"delete from health_consumer_lag where ts < now() - interval '{RETENTION_DAYS} days'")
        log.info("清理 %d 天前的数据：%d 行", RETENTION_DAYS, cur.rowcount)


def main():
    parser = argparse.ArgumentParser(description="Kafka 消费积压 → Postgres")
    parser.add_argument("--once", action="store_true", help="只查一轮、打印结果，不写数据库")
    args = parser.parse_args()
    setup_logging()

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    if args.once:
        committed, ends = fetch(admin, GROUPS)
        for _, group, topic, p, off, end, lag in lag_rows(datetime.now(UTC), committed, ends):
            print(f"{group:12} {topic:18} p{p}  已提交 {off:>10}  末尾 {end:>10}  积压 {lag:>7}")
        return

    conn = psycopg.connect(PG_DSN)
    with conn.transaction():
        conn.execute(SCHEMA)

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("开始，盯 %s，每 %d 秒一次", GROUPS, INTERVAL_SECONDS)
    last_cleanup = 0.0
    next_run = time.monotonic()
    try:
        while not stopping:
            if time.monotonic() < next_run:
                time.sleep(0.5)
                continue
            next_run += INTERVAL_SECONDS
            ts = datetime.now(UTC).replace(microsecond=0)
            try:
                committed, ends = fetch(admin, GROUPS)
            except Exception as e:   # Kafka 暂时连不上：这一轮跳过
                log.warning("查询 Kafka 失败，跳过这一轮：%s", e)
                continue
            rows = lag_rows(ts, committed, ends)
            with conn.transaction(), conn.cursor() as cur:
                cur.executemany(INSERT, rows)
            totals = {}
            for _, group, *_rest, lag in rows:
                totals[group] = totals.get(group, 0) + lag
            log.info("积压 %s", totals)
            if time.monotonic() - last_cleanup >= CLEANUP_SECONDS:
                cleanup(conn)
                last_cleanup = time.monotonic()
        log.info("停止")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
