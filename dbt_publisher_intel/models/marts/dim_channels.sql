{{
    config(
        materialized='table'
    )
}}

with video_metrics as (

    select * 
    from {{ ref('int_daily_video_metrics') }}

),

aggregated_channels as (

    select
        channel_id,
        channel_title,
        min(published_at) as first_video_published_at,
        max(published_at) as latest_video_published_at,
        count(distinct video_id) as total_tracked_videos,
        max(snapshot_date) as last_telemetry_date
    from video_metrics
    group by 1, 2

)

select * from aggregated_channels