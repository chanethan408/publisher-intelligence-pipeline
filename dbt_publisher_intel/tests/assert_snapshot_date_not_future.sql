-- Telemetry snapshot cannot be dated in the future
select
    performance_pk,
    video_id,
    snapshot_date
from {{ ref('fct_daily_video_performance') }}
where snapshot_date > current_date()