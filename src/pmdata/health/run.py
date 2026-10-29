"""管道健康程序（容器 health）：把管道自己的运行状况写进 Postgres，给 Status 页面和告警用。

一个循环里做两件事：
- 每 LAG_EVERY 秒记录一次各 consumer group 的消费积压（lag.py）
- 其余时间用自己的 consumer（group = pm-health）读 pm.raw.clob，挑出缺口标记记成断线重连（reconnects.py）
  第一次启动从 Kafka 现存最早的消息读起，所以会补上过去 7 天的重连记录；重连记录一直保留（一天才几条）。

可靠性：重连记录先写数据库、提交事务，成功后才提交 offset（at-least-once），重复写入按 conn_id 忽略。
写数据库出错直接退出，让 Docker 重启。
"""
import argparse
import logging
import signal
import time

import psycopg
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from pmdata.common.config import KAFKA_BOOTSTRAP, PG_DSN, TOPIC_CLOB
from pmdata.common.log import setup_logging
from pmdata.health import lag, reconnects

log = logging.getLogger("health")

GROUP_ID = "pm-health"
LAG_EVERY = 60
CLEANUP_SECONDS = 3600
BATCH_SIZE = 5000


def main():
    parser = argparse.ArgumentParser(description="管道健康指标 → Postgres")
    parser.add_argument("--group-id", default=GROUP_ID, help="consumer group（试运行时换一个，不影响正式进度）")
    parser.add_argument("--duration", type=int, help="运行多少秒后退出；不填则一直运行")
    args = parser.parse_args()
    setup_logging()

    conn = psycopg.connect(PG_DSN)
    with conn.transaction():
        conn.execute(lag.SCHEMA)
        conn.execute(reconnects.SCHEMA)
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": args.group_id,
        "enable.auto.commit": False,       # 数据库提交成功后手动提交
        "auto.offset.reset": "earliest",   # 第一次启动把 Kafka 里现存的重连都补上
    })

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([TOPIC_CLOB])
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
            if now - last_cleanup >= CLEANUP_SECONDS:
                lag.cleanup(conn)
                last_cleanup = now

            msgs = [m for m in consumer.consume(num_messages=BATCH_SIZE, timeout=0.5) if not m.error()]
            if not msgs:
                continue
            rows = reconnects.reconnect_rows(m.value() for m in msgs)
            if rows:
                with conn.transaction(), conn.cursor() as cur:
                    for row in rows.values():
                        cur.execute(reconnects.INSERT, row)
                        if cur.rowcount:   # 只给第一次写进去的打日志（同一次重连在每个分区都有标记）
                            _, back, _down, gap, reason = row
                            log.info("重连 %s，停了 %s 秒（%s）", back.isoformat(timespec="seconds"),
                                     "?" if gap is None else f"{gap:.1f}", reason)
            consumer.commit(asynchronous=False)
        log.info("停止")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
