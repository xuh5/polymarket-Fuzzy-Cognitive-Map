-- 盘口变化。粒度：一行 = 某个 token 某个价位的一次数量变化
-- 一条 price_change 消息里有多个变化（实测几乎都是 2 个：同一市场的 Yes 和 No），这里展开成多行
with source as (
    select *
    from {{ source('bronze', 'clob') }}
    where event_type = 'price_change'
    {{ dedup_kafka() }}
),

exploded as (
    select
        kafka_partition,
        kafka_offset,
        market                                                      as market_id,
        {{ ms_to_ts("value ->> '$.payload.timestamp'") }}           as exchange_ts,
        cast(received_at as timestamptz)                            as received_at,
        date,
        -- 两个 unnest 并排展开：change 是数组里的每个元素，change_index 是它的下标（从 1 开始）
        unnest(cast(value -> '$.payload.price_changes' as json[]))              as change,
        generate_subscripts(cast(value -> '$.payload.price_changes' as json[]), 1) as change_index
    from source
)

select
    kafka_partition,
    kafka_offset,
    change_index,
    market_id,
    change ->> 'asset_id'                                           as asset_id,
    -- BUY = 买盘（bid）这一侧，SELL = 卖盘（ask）这一侧
    change ->> 'side'                                               as side,
    cast(change ->> 'price' as decimal(12, 6))                      as price,
    -- 这个价位变化后的总挂单量（不是增量）；0 表示这一档没了
    cast(change ->> 'size' as decimal(20, 6))                       as size,
    -- 变化之后这个 token 的最优买价 / 卖价
    cast(change ->> 'best_bid' as decimal(12, 6))                   as best_bid,
    cast(change ->> 'best_ask' as decimal(12, 6))                   as best_ask,
    change ->> 'hash'                                               as book_hash,
    exchange_ts,
    received_at,
    date
from exploded
