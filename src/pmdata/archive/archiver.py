"""归档程序：持续读取三个 raw topic，原样写成 Parquet（bronze 层）。

一个 consumer（group = pm-archiver）同时订阅三个 topic，每个 topic 在内存里攒一批，
满 5 分钟或攒够 10 万条就写成文件：data/bronze/<topic>/date=YYYY-MM-DD/part-xxx.parquet。

可靠性（at-least-once）：关掉自动提交，文件写成功（先写 .tmp 再改名）之后才提交 offset。
写完文件、提交之前崩溃 → 重启后这批会重读重写，产生重复行，但不会丢；
每行都带 kafka_partition + kafka_offset（在一个 topic 内唯一），下游（dbt silver 层）按它去重。
"""
import argparse
import json
import logging
import signal
import time
import uuid
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, TopicPartition

from pmdata.common.config import DATA_DIR, KAFKA_BOOTSTRAP, TOPIC_CLOB, TOPIC_COMMENTS, TOPIC_MARKETS
from pmdata.common.log import setup_logging

log = logging.getLogger("archiver")

GROUP_ID = "pm-archiver"
FLUSH_SECONDS = 300      # 每批最多攒 5 分钟
MAX_ROWS = 100_000       # 每批最多攒 10 万条（首次启动补读积压数据时防止内存撑爆）

# 三个 topic 共有的列；value 是 Kafka 消息原文（整条信封 JSON，一个字不改），tombstone 为 null
COMMON_FIELDS = [
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("kafka_key", pa.string()),
    ("kafka_timestamp", pa.timestamp("ms", tz="UTC")),
    ("source", pa.string()),
    ("received_at", pa.string()),
]
# 各 topic 额外从信封/payload 里抽出来的列，只为方便过滤，完整内容以 value 为准
EXTRA_FIELDS = {
    TOPIC_CLOB: [
        ("event_type", pa.string()),
        ("market", pa.string()),
        ("conn_id", pa.string()),
        ("seq", pa.int64()),
        ("frame_index", pa.int32()),
    ],
    TOPIC_COMMENTS: [("event_id", pa.string())],
    TOPIC_MARKETS: [("is_tombstone", pa.bool_())],
}
TOPICS = list(EXTRA_FIELDS)


def schema_for(topic):
    return pa.schema(COMMON_FIELDS + EXTRA_FIELDS[topic] + [("value", pa.string())])


def to_row(msg):
    """把一条 Kafka 消息变成一行。解析不了的消息也照样归档（抽取的列为空），绝不丢。"""
    topic = msg.topic()
    raw = msg.value()
    key = msg.key()
    _, ts_ms = msg.timestamp()
    row = {
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "kafka_key": key.decode() if key is not None else None,
        "kafka_timestamp": datetime.fromtimestamp(ts_ms / 1000, UTC),
        "value": raw.decode() if raw is not None else None,
    }
    env = {}
    if raw is not None:
        try:
            env = json.loads(raw)
        except ValueError:
            log.warning("%s p%d offset %d 不是合法 JSON，只存原文", topic, msg.partition(), msg.offset())
    payload = env.get("payload")
    payload = payload if isinstance(payload, dict) else {}

    row["source"] = env.get("source")
    row["received_at"] = env.get("received_at")
    if topic == TOPIC_CLOB:
        row["event_type"] = payload.get("event_type")
        row["market"] = payload.get("market")
        row["conn_id"] = env.get("conn_id")
        row["seq"] = env.get("seq")
        row["frame_index"] = env.get("frame_index")
    elif topic == TOPIC_COMMENTS:
        event_id = env.get("event_id")
        row["event_id"] = str(event_id) if event_id is not None else None
    elif topic == TOPIC_MARKETS:
        row["is_tombstone"] = raw is None
    return row


def write_files(topic, rows, base_dir):
    """按 Kafka 时间戳的 UTC 日期分组，每个日期写一个 Parquet 文件，返回写出的文件路径。

    先写 .tmp 再改名：改名是原子操作，目录里只会出现完整的 .parquet，不会有写了一半的文件。
    """
    by_date = {}
    for r in rows:
        by_date.setdefault(r["kafka_timestamp"].strftime("%Y-%m-%d"), []).append(r)

    schema = schema_for(topic)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    paths = []
    for date, group in sorted(by_date.items()):
        folder = base_dir / topic / f"date={date}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"part-{stamp}-{uuid.uuid4().hex[:6]}.parquet"
        tmp = path.with_name(path.name + ".tmp")
        pq.write_table(pa.Table.from_pylist(group, schema=schema), tmp, compression="zstd")
        tmp.replace(path)
        paths.append(path)
    return paths


class Batch:
    """某个 topic 正在攒的一批消息，以及每个分区读到的最后一个 offset。"""

    def __init__(self):
        self.rows = []
        self.last_offsets = {}
        self.started = time.monotonic()

    def add(self, msg):
        self.rows.append(to_row(msg))
        self.last_offsets[msg.partition()] = msg.offset()

    def due(self, flush_seconds, max_rows):
        if not self.rows:
            return False
        return len(self.rows) >= max_rows or time.monotonic() - self.started >= flush_seconds


class Archiver:
    def __init__(self, consumer, base_dir, flush_seconds=FLUSH_SECONDS, max_rows=MAX_ROWS):
        self.consumer = consumer
        self.base_dir = base_dir
        self.flush_seconds = flush_seconds
        self.max_rows = max_rows
        self.batches = {t: Batch() for t in TOPICS}

    def handle(self, msg):
        self.batches[msg.topic()].add(msg)

    def flush(self, topic):
        """写文件 → 成功后才提交 offset。写文件失败会抛异常、不提交，重启后从上次提交的位置重读。"""
        batch = self.batches[topic]
        if not batch.rows:
            return
        paths = write_files(topic, batch.rows, self.base_dir)
        # 提交的 offset 是「下一条要读的位置」，所以是最后一条 + 1
        offsets = [TopicPartition(topic, p, o + 1) for p, o in batch.last_offsets.items()]
        self.consumer.commit(offsets=offsets, asynchronous=False)
        files = ", ".join(str(p.relative_to(self.base_dir)) for p in paths)
        log.info("%s 写入 %d 条 → %s", topic, len(batch.rows), files)
        self.batches[topic] = Batch()

    def flush_due(self):
        for topic, batch in self.batches.items():
            if batch.due(self.flush_seconds, self.max_rows):
                self.flush(topic)

    def flush_all(self):
        for topic in TOPICS:
            self.flush(topic)


def make_consumer(group_id):
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,         # 文件写成功后手动提交
        "auto.offset.reset": "earliest",     # 第一次启动（group 还没提交过 offset）时从最早的数据开始
    })


def main():
    parser = argparse.ArgumentParser(description="把三个 raw topic 归档成 Parquet（bronze 层）")
    parser.add_argument("--flush-seconds", type=int, default=FLUSH_SECONDS, help="每批最多攒多少秒")
    parser.add_argument("--max-rows", type=int, default=MAX_ROWS, help="每批最多攒多少条")
    parser.add_argument("--duration", type=int, help="运行多少秒后写出剩余数据并退出；不填则一直运行")
    parser.add_argument("--group-id", default=GROUP_ID, help="consumer group（试运行时换一个，不影响正式进度）")
    args = parser.parse_args()
    setup_logging()

    base_dir = DATA_DIR / "bronze"
    consumer = make_consumer(args.group_id)
    archiver = Archiver(consumer, base_dir, args.flush_seconds, args.max_rows)

    def on_revoke(c, partitions):
        # 分区要被分给别的 consumer 前，先把手里攒的写掉并提交，避免别人重读
        log.info("分区被收回，先写出手里的数据")
        archiver.flush_all()

    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)   # docker stop 发的是 SIGTERM
    signal.signal(signal.SIGINT, stop)    # Ctrl+C

    consumer.subscribe(TOPICS, on_revoke=on_revoke)
    log.info("开始归档 %s → %s（group=%s）", TOPICS, base_dir, args.group_id)
    deadline = time.monotonic() + args.duration if args.duration else None
    try:
        while not stopping and (deadline is None or time.monotonic() < deadline):
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.warning("Kafka 错误：%s", msg.error())
                else:
                    archiver.handle(msg)
            archiver.flush_due()
        log.info("停止，写出剩余数据后退出")
        archiver.flush_all()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
