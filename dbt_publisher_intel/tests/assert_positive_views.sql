-- View counts and daily deltas cannot be negative
select
    performance_pk,
    video_id,
    snapshot_date,
    cumulative_views,
    daily_views
from {{ ref('fct_daily_video_performance') }}
where cumulative_views < 0
   or daily_views < 0