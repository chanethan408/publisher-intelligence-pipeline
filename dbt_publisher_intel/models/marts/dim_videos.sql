{{
    config(
        materialized='table'
    )
}}

with video_metrics as (

    select * 
    from {{ ref('int_daily_video_metrics') }}

),

deduplicated_videos as (

    select
        video_id,
        channel_id,
        video_title,
        published_at,
        snapshot_date as metadata_last_updated_date
    from video_metrics
    qualify row_number() over (
        partition by video_id 
        order by snapshot_date desc
    ) = 1

)

select * from deduplicated_videos