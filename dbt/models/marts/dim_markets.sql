-- 维度：市场。粒度：一行 = 一个市场，取名录里最近一次的状态
-- 已关闭（从开放列表消失、写了 tombstone）的市场也保留，用 is_delisted 标出来——历史成交还要查得到它的问题文本
with snapshots as (
    select * from {{ ref('stg_markets__snapshots') }}
),

-- 每个市场最后一条消息：如果是 tombstone，说明已经关闭
latest_any as (
    select gamma_market_id, is_tombstone, kafka_timestamp
    from snapshots
    qualify row_number() over (partition by gamma_market_id order by kafka_offset desc) = 1
),

-- 每个市场最后一次有内容的快照（同一个 key 在同一个分区，offset 越大越新）
latest_live as (
    select *
    from snapshots
    where not is_tombstone
    qualify row_number() over (partition by gamma_market_id order by kafka_offset desc) = 1
),

first_seen as (
    select gamma_market_id, min(fetched_at) as first_seen_at
    from snapshots
    group by 1
)

select
    l.market_id,
    l.gamma_market_id,
    l.question,
    l.slug,
    l.event_id,
    l.event_title,
    l.outcomes,
    l.token_ids,
    l.end_date,
    l.volume_24hr,
    l.liquidity,
    l.tick_size,
    l.min_order_size,
    l.taker_fee_rate,
    l.neg_risk,
    l.tag_slugs,
    a.is_tombstone                                          as is_delisted,
    case when a.is_tombstone then a.kafka_timestamp end     as delisted_at,
    f.first_seen_at,
    l.fetched_at                                            as last_snapshot_at
from latest_live as l
join latest_any as a using (gamma_market_id)
join first_seen as f using (gamma_market_id)
