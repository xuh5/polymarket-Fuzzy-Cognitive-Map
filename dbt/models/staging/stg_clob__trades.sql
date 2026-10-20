-- 成交。粒度：一行 = 一笔成交（WebSocket 的 last_trade_price 事件）
with source as (
    select *
    from {{ source('bronze', 'clob') }}
    where event_type = 'last_trade_price'
    {{ dedup_kafka() }}
)

select
    kafka_partition,
    kafka_offset,
    market                                                          as market_id,
    value ->> '$.payload.asset_id'                                  as asset_id,
    value ->> '$.payload.side'                                      as side,
    cast(value ->> '$.payload.price' as decimal(12, 6))             as price,
    cast(value ->> '$.payload.size' as decimal(20, 6))              as size,
    cast(value ->> '$.payload.fee_rate_bps' as integer)             as fee_rate_bps,
    value ->> '$.payload.transaction_hash'                          as transaction_hash,
    -- 交易所时间（payload 里的毫秒时间戳）和我们收到的时间都留着，两者相减就是延迟
    {{ ms_to_ts("value ->> '$.payload.timestamp'") }}               as exchange_ts,
    cast(received_at as timestamptz)                                as received_at,
    date
from source
