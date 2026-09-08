{{
    config(
        materialized='incremental',
        unique_key=['video_id', 'snapshot_date'],
        incremental_strategy='merge'
    )
}}

with daily_metrics as (

    select *
    from {{ ref('int_daily_video_metrics') }}
    {% if is_incremental() %}
        -- 2-day lookback window for late-arriving telemetry corrections
        where snapshot_date >= dateadd('day', -2, (select max(snapshot_date) from {{ this }}))
    {% endif %}

),

final_facts as (

    select
        -- Deterministic Surrogate Primary Key
        md5(concat(coalesce(video_id, ''), '|', coalesce(snapshot_date::string, ''))) as performance_pk,
        
        -- Foreign Keys & Natural Keys
        video_id,
        channel_id,
        snapshot_date,
        
        -- Daily Snapshot Telemetry Deltas
        daily_views,
        daily_likes,
        daily_comments,
        
        -- Cumulative Totals
        view_count as cumulative_views,
        like_count as cumulative_likes,
        comment_count as cumulative_comments,
        
        -- Derived Ratios
        engagement_rate_pct,
        
        -- Audit Metadata
        current_timestamp() as dbt_updated_at

    from daily_metrics

)

select * from final_facts