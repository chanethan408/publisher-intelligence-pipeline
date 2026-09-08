{{
    config(
        materialized='view'
    )
}}

with staging_videos as (

    select *
    from {{ ref('stg_youtube__videos') }}

),

ordered_metrics as (

    select
        video_id,
        channel_id,
        video_title,
        channel_title,
        published_at,
        snapshot_date,
        view_count,
        like_count,
        comment_count,
        
        -- Window calculation: Previous snapshot metrics per video
        lag(view_count, 1) over (
            partition by video_id 
            order by snapshot_date asc
        ) as prev_view_count,
        
        lag(like_count, 1) over (
            partition by video_id 
            order by snapshot_date asc
        ) as prev_like_count,

        lag(comment_count, 1) over (
            partition by video_id 
            order by snapshot_date asc
        ) as prev_comment_count

    from staging_videos

),

calculated_deltas as (

    select
        video_id,
        channel_id,
        video_title,
        channel_title,
        published_at,
        snapshot_date,
        view_count,
        like_count,
        comment_count,

        -- Daily incremental deltas (defaulting to 0 if previous snapshot is missing or counts drop)
        case 
            when prev_view_count is null then 0
            when view_count >= prev_view_count then view_count - prev_view_count
            else 0
        end as daily_views,

        case 
            when prev_like_count is null then 0
            when like_count >= prev_like_count then like_count - prev_like_count
            else 0
        end as daily_likes,

        case 
            when prev_comment_count is null then 0
            when comment_count >= prev_comment_count then comment_count - prev_comment_count
            else 0
        end as daily_comments,

        -- Safe division invariant for engagement rate: (likes + comments) / views
        round(
            (coalesce(like_count, 0) + coalesce(comment_count, 0)) / 
            nullif(view_count, 0) * 100,
            4
        ) as engagement_rate_pct

    from ordered_metrics

)

select * from calculated_deltas