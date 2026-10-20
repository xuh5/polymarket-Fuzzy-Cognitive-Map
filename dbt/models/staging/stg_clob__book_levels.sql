-- 订单簿快照。粒度：一行 = 某次快照里某一侧的一档（价格 + 挂单量）
-- 来源两种：WebSocket 推的 book 事件，和重连后 REST 补拉的快照（source = clob_rest，没有 event_type）
-- 注意：某一侧整个为空时，这一侧没有行（空订单簿的快照在这张表里看不到，要看 bronze）
with source as (
    select *
    from {{ source('bronze', 'clob') }}
    where event_type = 'book' or source = 'clob_rest'
    {{ dedup_kafka() }}
),

snapshots as (
    select
        kafka_partition,
        kafka_offset,
        -- 一条消息就是一次快照，用它在 Kafka 里的位置当快照编号
        kafka_partition || '-' || kafka_offset                      as snapshot_id,
        source,
        market                                                      as market_id,
        value ->> '$.payload.asset_id'                              as asset_id,
        value ->> '$.payload.hash'                                  as book_hash,
        {{ ms_to_ts("value ->> '$.payload.timestamp'") }}           as exchange_ts,
        cast(received_at as timestamptz)                            as received_at,
        date,
        cast(value -> '$.payload.bids' as json[])                   as bids,
        cast(value -> '$.payload.asks' as json[])                   as asks
    from source
),

levels as (
    select * exclude (bids, asks), 'BUY' as side,
        unnest(bids) as level, generate_subscripts(bids, 1) as level_index
    from snapshots
    union all
    select * exclude (bids, asks), 'SELL' as side,
        unnest(asks) as level, generate_subscripts(asks, 1) as level_index
    from snapshots
)

select
    snapshot_id,
    kafka_partition,
    kafka_offset,
    source,
    market_id,
    asset_id,
    side,
    -- 档位在原始数组里的下标（从 1 开始）。原始数组不是按「最优价在前」排的，不要当成第几档
    level_index,
    cast(level ->> 'price' as decimal(12, 6))                       as price,
    cast(level ->> 'size' as decimal(20, 6))                        as size,
    book_hash,
    exchange_ts,
    received_at,
    date
from levels
