-- 对账测试：silver 各表的行数要和 bronze 原始消息对得上（查出的行 = 对不上的事件类型）
-- 去重后的消息条数：成交、最优价、标记各一条消息一行；price_change 一条消息展开成数组长度那么多行；
-- 快照按「快照数」比（空的那一侧不产生行，所以不比档位数）。
-- bronze 在 dbt build 期间可能被 archiver 追加新文件，所以只比对 silver 建表时已经读到的 offset 范围。
with bronze as (
    select * from {{ source('bronze', 'clob') }}
    {{ dedup_kafka() }}
),

cutoff as (
    select kafka_partition, max(kafka_offset) as max_offset from (
        select kafka_partition, kafka_offset from {{ ref('stg_clob__price_changes') }}
        union all select kafka_partition, kafka_offset from {{ ref('stg_clob__trades') }}
        union all select kafka_partition, kafka_offset from {{ ref('stg_clob__book_levels') }}
        union all select kafka_partition, kafka_offset from {{ ref('stg_clob__best_bid_ask') }}
        union all select kafka_partition, kafka_offset from {{ ref('stg_clob__markers') }}
    ) group by 1
),

expected as (
    select
        case when source = 'collector' then 'markers'
             when event_type = 'book' or source = 'clob_rest' then 'book'
             else event_type end as kind,
        sum(case when event_type = 'price_change'
                 then json_array_length(value -> '$.payload.price_changes') else 1 end) as n
    from bronze
    join cutoff using (kafka_partition)
    where kafka_offset <= max_offset
    group by 1
),

actual as (
    select 'last_trade_price' as kind, count(*) as n from {{ ref('stg_clob__trades') }}
    union all select 'price_change', count(*) from {{ ref('stg_clob__price_changes') }}
    union all select 'book', count(distinct snapshot_id) from {{ ref('stg_clob__book_levels') }}
    union all select 'best_bid_ask', count(*) from {{ ref('stg_clob__best_bid_ask') }}
    union all select 'markers', count(*) from {{ ref('stg_clob__markers') }}
)

select a.kind, e.n as bronze_rows, a.n as silver_rows
from actual as a
left join expected as e using (kind)
where e.n is distinct from a.n
