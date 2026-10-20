-- 事实：成交。粒度：一行 = 一笔成交
-- 只放 ID 和数字；问题文本、Yes/No 通过 market_id / asset_id join dim_markets、dim_tokens 拿
select
    kafka_partition || '-' || kafka_offset      as trade_id,
    market_id,
    asset_id,
    exchange_ts,
    received_at,
    side,
    price,
    size,
    -- 成交金额（美元）= 价格 × 份数
    price * size                                as notional,
    fee_rate_bps,
    transaction_hash,
    date
from {{ ref('stg_clob__trades') }}
