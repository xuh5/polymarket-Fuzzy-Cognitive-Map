"""Kafka 公共工具：创建 producer、按项目约定打信封、把一个 topic 读到当前末尾。"""
import json

from confluent_kafka import Consumer, Producer, TopicPartition

from pmdata.common.config import KAFKA_BOOTSTRAP


def make_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,   # 防止网络重试导致重复写入
        "compression.type": "lz4",
    })


def envelope(source, received_at, payload, **extra):
    """项目统一的消息信封：source、received_at 在前，各 topic 自己的字段居中，原始数据放 payload。"""
    return json.dumps({"source": source, "received_at": received_at, **extra, "payload": payload})


def read_to_end(topic, group_id, since=None):
    """把 topic 从起点读到「调用这一刻」的末尾，读完就返回（不会一直等新消息）。

    since 为 None 时从每个分区现存最早的消息开始（被保留策略删掉的部分自然跳过）；
    传入 datetime 时用 Kafka 的按时间定位（offsets_for_times），从那一刻开始读。
    不提交 offset：每次调用都是独立的一次完整读取。
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    try:
        meta = consumer.list_topics(topic, timeout=10).topics[topic]
        partitions = [TopicPartition(topic, p) for p in meta.partitions]

        if since is None:
            starts = []
            for tp in partitions:
                low, _ = consumer.get_watermark_offsets(tp, timeout=10)
                starts.append(TopicPartition(topic, tp.partition, low))
        else:
            since_ms = int(since.timestamp() * 1000)
            # 问 Kafka：每个分区里，时间戳 >= since 的第一条消息在哪个 offset（-1 表示没有）
            starts = consumer.offsets_for_times(
                [TopicPartition(topic, tp.partition, since_ms) for tp in partitions], timeout=10)

        # 先记下每个分区现在的末尾位置，读到那里就算读完
        remaining = {}
        for tp in starts:
            _, high = consumer.get_watermark_offsets(tp, timeout=10)
            if 0 <= tp.offset < high:
                remaining[tp.partition] = high
        consumer.assign([tp for tp in starts if tp.partition in remaining])

        while remaining:
            msg = consumer.poll(10)
            if msg is None:
                raise TimeoutError(f"读取 {topic} 超时，还没读完的分区：{sorted(remaining)}")
            if msg.error():
                raise RuntimeError(msg.error())
            yield msg
            if msg.offset() + 1 >= remaining.get(msg.partition(), 0):
                remaining.pop(msg.partition(), None)
    finally:
        consumer.close()


def latest_by_key(topic, group_id):
    """读完一个 compact topic，返回 {key: 解析后的信封}，每个 key 只留最新一条。

    compact 是 Kafka 在后台慢慢做的，同一个 key 可能暂时还有多条旧消息，所以这里自己去重：
    后读到的覆盖先读到的；读到 tombstone（value 为空）就把这个 key 删掉。
    """
    latest = {}
    for msg in read_to_end(topic, group_id):
        key = msg.key().decode()
        if msg.value() is None:
            latest.pop(key, None)
        else:
            latest[key] = json.loads(msg.value())
    return latest
