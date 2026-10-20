-- 采集器自己写的标记。粒度：一行 = 某个市场的一个缺口标记（gap）或退订标记（unsubscribed）
-- gap：这个市场之前的订单簿状态作废，等下一个快照；disconnected_at 为空表示刚启动或刚加入订阅名单
-- unsubscribed：被移出订阅名单，之后没数据是正常的
with source as (
    select *
    from {{ source('bronze', 'clob') }}
    where source = 'collector'
    {{ dedup_kafka() }}
)

select
    kafka_partition,
    kafka_offset,
    market                                                          as market_id,
    event_type                                                      as marker_type,
    value ->> '$.payload.reason'                                    as reason,
    cast(value ->> '$.payload.disconnected_at' as timestamptz)      as disconnected_at,
    cast(value ->> '$.payload.reconnected_at' as timestamptz)       as reconnected_at,
    cast(value ->> '$.payload.at' as timestamptz)                   as unsubscribed_at,
    conn_id,
    cast(received_at as timestamptz)                                as received_at,
    date
from source
