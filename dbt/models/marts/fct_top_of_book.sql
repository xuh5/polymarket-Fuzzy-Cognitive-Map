-- 事实：盘口（最优买卖价）变化。粒度：一行 = 某个 token 的最优买价或卖价变了一次
-- 来源：price_change 里带的变化后最优价（实测比 best_bid_ask 事件全得多：一天 28.9 万次 vs 9 千次，覆盖市场 51 vs 29），
-- 加上订单簿快照算出的最优价（重连拿到新快照的那一刻最优价可能已经变了，只看 price_change 会漏掉）。
-- 只保留最优价真的变了的行：挂单量变了但买一卖一没变的不算。
with from_price_changes as (
    select
        market_id, asset_id, kafka_partition, kafka_offset, change_index,
        'price_change'                          as source_event,
        best_bid, best_ask, exchange_ts, received_at, date
    from {{ ref('stg_clob__price_changes') }}
),

from_snapshots as (
    select
        market_id, asset_id, kafka_partition, kafka_offset,
        0                                       as change_index,
        'book'                                  as source_event,
        -- 快照里买盘最高价 = 买一，卖盘最低价 = 卖一（某一侧为空时就是 null）
        max(price) filter (where side = 'BUY')  as best_bid,
        min(price) filter (where side = 'SELL') as best_ask,
        any_value(exchange_ts)                  as exchange_ts,
        any_value(received_at)                  as received_at,
        any_value(date)                         as date
    from {{ ref('stg_clob__book_levels') }}
    group by market_id, asset_id, kafka_partition, kafka_offset
),

combined as (
    select * from from_price_changes
    union all
    select * from from_snapshots
),

flagged as (
    select
        *,
        -- 同一个市场的事件都在同一个分区里，按 offset 排就是它们真实的先后顺序
        (best_bid, best_ask) is distinct from lag((best_bid, best_ask)) over (
            partition by asset_id order by kafka_offset, change_index
        )                                       as is_changed
    from combined
)

select
    kafka_partition || '-' || kafka_offset || '-' || change_index as tob_id,
    market_id,
    asset_id,
    exchange_ts,
    received_at,
    source_event,
    best_bid,
    best_ask,
    best_ask - best_bid                         as spread,
    (best_bid + best_ask) / 2                   as mid_price,
    date
from flagged
where is_changed
