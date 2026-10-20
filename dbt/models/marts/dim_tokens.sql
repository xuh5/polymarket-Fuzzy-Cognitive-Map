-- 维度：token（选项）。粒度：一行 = 一个 token
-- clob 消息里只有一长串 asset_id，靠这张表才知道它是哪个市场的 Yes 还是 No
with markets as (
    select market_id, token_ids, outcomes
    from {{ ref('dim_markets') }}
),

exploded as (
    select
        market_id,
        unnest(token_ids)                       as asset_id,
        unnest(outcomes)                        as outcome,
        generate_subscripts(token_ids, 1)       as outcome_index
    from markets
)

select
    asset_id,
    market_id,
    outcome,
    -- 1 = outcomes 里的第一个（二元市场里是 Yes），2 = 第二个（No）
    outcome_index
from exploded
