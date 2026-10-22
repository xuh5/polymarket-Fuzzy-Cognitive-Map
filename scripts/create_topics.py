"""创建项目需要的 Kafka topic。可以重复运行：

不存在的就创建；已存在的把配置更新成下面写的样子（配置以代码为准）。分区数只能加不能减，这里不改。
"""
from confluent_kafka.admin import AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, NewTopic, ResourceType

from pmdata.common.config import (
    KAFKA_BOOTSTRAP,
    TOPIC_CLOB,
    TOPIC_COMMENTS,
    TOPIC_MARKETS,
    TOPIC_METRICS_BOOK,
    TOPIC_METRICS_TRADES,
)

SEVEN_DAYS_MS = str(7 * 24 * 60 * 60 * 1000)

# (topic 名, 分区数, 配置)。配置里值为 None 表示删掉这项、恢复 Kafka 默认值
TOPICS = [
    (TOPIC_MARKETS,  3, {"cleanup.policy": "compact"}),   # 每个市场只留最新一条
    (TOPIC_CLOB,     3, {"retention.ms": SEVEN_DAYS_MS}),  # 订单簿+成交，保留 7 天
    # 评论：key 是评论 ID，每条评论只留最新版本、永久保留（重启时读一遍就知道哪些存过）
    (TOPIC_COMMENTS, 3, {"cleanup.policy": "compact", "retention.ms": None}),
    # 阶段 3 实时指标（key 是市场 ID，和 pm.raw.clob 一致）；算错了可以从 raw 重算，保留 7 天就够
    (TOPIC_METRICS_BOOK,   3, {"retention.ms": SEVEN_DAYS_MS}),
    (TOPIC_METRICS_TRADES, 3, {"retention.ms": SEVEN_DAYS_MS}),
]


def main():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics)

    to_create = [
        NewTopic(name, num_partitions=parts, replication_factor=1,
                 config={k: v for k, v in cfg.items() if v is not None})
        for name, parts, cfg in TOPICS
        if name not in existing
    ]
    if to_create:
        for name, future in admin.create_topics(to_create).items():
            try:
                future.result()  # 等 Kafka 确认建好
                print(f"已创建  {name}")
            except Exception as e:
                print(f"失败    {name}: {e}")

    # 已存在的 topic：逐项设置或删除配置，只动这里列出的项
    to_alter = [
        ConfigResource(ResourceType.TOPIC, name, incremental_configs=[
            ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET if v is not None
                        else AlterConfigOpType.DELETE)
            for k, v in cfg.items()
        ])
        for name, _, cfg in TOPICS
        if name in existing
    ]
    if to_alter:
        for res, future in admin.incremental_alter_configs(to_alter).items():
            try:
                future.result()
                print(f"已同步配置  {res.name}")
            except Exception as e:
                print(f"同步失败    {res.name}: {e}")


if __name__ == "__main__":
    main()
