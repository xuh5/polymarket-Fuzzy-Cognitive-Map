-- 评论。粒度：一行 = 一条评论（只留最新版本）
-- 采集器在评论新增或有变化（内容、点赞数等）时写一个新版本；同一条评论的版本在同一个分区，offset 大的更新
with source as (
    select *
    from {{ source('bronze', 'comments') }}
    {{ dedup_kafka() }}
),

latest as (
    select *
    from source
    qualify row_number() over (partition by kafka_key order by kafka_offset desc) = 1
)

select
    kafka_partition,
    kafka_offset,
    kafka_key                                                       as comment_id,
    event_id,
    value ->> '$.payload.parentCommentID'                           as parent_comment_id,
    value ->> '$.payload.body'                                      as body,
    value ->> '$.payload.userAddress'                               as user_address,
    value ->> '$.payload.profile.name'                              as user_name,
    cast(value ->> '$.payload.reactionCount' as integer)            as reaction_count,
    cast(value ->> '$.payload.reportCount' as integer)              as report_count,
    cast(value ->> '$.payload.createdAt' as timestamptz)            as created_at,
    cast(value ->> '$.payload.updatedAt' as timestamptz)            as updated_at,
    cast(received_at as timestamptz)                                as received_at
from latest
