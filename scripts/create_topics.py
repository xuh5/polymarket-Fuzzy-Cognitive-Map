"""创建项目需要的 Kafka topic。可以重复运行：已存在的会跳过。"""
import os

from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")   # 容器里设成 kafka:19092
SEVEN_DAYS_MS = str(7 * 24 * 60 * 60 * 1000)

# (topic 名, 分区数, 额外配置)
TOPICS = [
    ("pm.raw.markets",  3, {"cleanup.policy": "compact"}),   # 每个市场只留最新一条
    ("pm.raw.clob",     3, {"retention.ms": SEVEN_DAYS_MS}),  # 订单簿+成交，保留 7 天
    ("pm.raw.comments", 3, {"retention.ms": SEVEN_DAYS_MS}),  # 评论，保留 7 天
]


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics)

    to_create = [
        NewTopic(name, num_partitions=parts, replication_factor=1, config=cfg)
        for name, parts, cfg in TOPICS
        if name not in existing
    ]

    for name, _, _ in TOPICS:
        if name in existing:
            print(f"已存在  {name}")

    if not to_create:
        return

    for name, future in admin.create_topics(to_create).items():
        try:
            future.result()  # 等 Kafka 确认建好
            print(f"已创建  {name}")
        except Exception as e:
            print(f"失败    {name}: {e}")


if __name__ == "__main__":
    main()