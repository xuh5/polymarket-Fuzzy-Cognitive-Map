-- 最优买卖价。粒度：一行 = 某个 token 的最优买价 / 卖价变了一次（best_bid_ask 事件）
with source as (
    select *
    from {{ source('bronze', 'clob') }}
    where event_type = 'best_bid_ask'
    {{ dedup_kafka() }}
)

select
    kafka_partition,
    kafka_offset,
    market                                                          as market_id,
    value ->> '$.payload.asset_id'                                  as asset_id,
    cast(value ->> '$.payload.best_bid' as decimal(12, 6))          as best_bid,
    cast(value ->> '$.payload.best_ask' as decimal(12, 6))          as best_ask,
    cast(value ->> '$.payload.spread' as decimal(12, 6))            as spread,
    {{ ms_to_ts("value ->> '$.payload.timestamp'") }}               as exchange_ts,
    cast(received_at as timestamptz)                                as received_at,
    date
from source
