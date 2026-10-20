-- 市场名录。粒度：一行 = 某个市场在某一次（每小时一次）拉取时的状态
-- tombstone（市场从开放列表里消失 = 已关闭）也保留一行，is_tombstone = true，其余字段为空
with source as (
    select *
    from {{ source('bronze', 'markets') }}
    {{ dedup_kafka() }}
)

select
    kafka_partition,
    kafka_offset,
    kafka_key                                                       as gamma_market_id,
    is_tombstone,
    -- CLOB 里的市场 ID：WebSocket 消息的 market 字段就是它，和 clob 表 join 用这个
    value ->> '$.payload.conditionId'                               as market_id,
    value ->> '$.payload.question'                                  as question,
    value ->> '$.payload.slug'                                      as slug,
    value ->> '$.payload.events[0].id'                              as event_id,
    value ->> '$.payload.events[0].title'                           as event_title,
    value ->> '$.payload.version'                                   as version,
    -- outcomes 是「JSON 字符串里装着数组」，要先把字符串当 JSON 再转成数组
    cast(json(value ->> '$.payload.outcomes') as varchar[])         as outcomes,
    -- token ID 按 version 取：v1 在 clobTokenIds（同样是 JSON 字符串），v2 在 positionIds（本身就是数组）
    case value ->> '$.payload.version'
        when 'v1' then cast(json(value ->> '$.payload.clobTokenIds') as varchar[])
        when 'v2' then cast(value -> '$.payload.positionIds' as varchar[])
    end                                                             as token_ids,
    cast(value ->> '$.payload.endDate' as timestamptz)              as end_date,
    -- 没有 volume24hr 字段表示 24 小时没成交
    coalesce(cast(value ->> '$.payload.volume24hr' as double), 0)   as volume_24hr,
    cast(value ->> '$.payload.liquidityNum' as double)              as liquidity,
    cast(value ->> '$.payload.acceptingOrders' as boolean)          as accepting_orders,
    cast(value ->> '$.payload.enableOrderBook' as boolean)          as enable_order_book,
    cast(value ->> '$.payload.closed' as boolean)                   as closed,
    cast(value ->> '$.payload.negRisk' as boolean)                  as neg_risk,
    cast(value ->> '$.payload.orderPriceMinTickSize' as decimal(12, 6)) as tick_size,
    cast(value ->> '$.payload.orderMinSize' as double)              as min_order_size,
    -- taker 手续费率（模拟盘要用）
    cast(value ->> '$.payload.feeSchedule.rate' as double)          as taker_fee_rate,
    cast(value -> '$.tag_slugs' as varchar[])                       as tag_slugs,
    cast(received_at as timestamptz)                                as fetched_at,
    kafka_timestamp,
    date
from source
