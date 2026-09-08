{{
    config(
        materialized='view'
    )
}}

with raw_source as (

    select
        raw_payload,
        execution_date,
        ingested_at,
        source_file
    from {{ source('raw_ingest', 'raw_videos') }}

),

extracted_fields as (

    select
        -- Identifiers
        raw_payload:video_id::string as video_id,
        raw_payload:channel_id::string as channel_id,
        
        -- Descriptive Attributes
        raw_payload:title::string as video_title,
        raw_payload:channel_title::string as channel_title,
        raw_payload:description::string as video_description,
        raw_payload:published_at::timestamp_ntz as published_at,
        
        -- Telemetry Metrics
        coalesce(raw_payload:view_count::integer, 0) as view_count,
        coalesce(raw_payload:like_count::integer, 0) as like_count,
        coalesce(raw_payload:comment_count::integer, 0) as comment_count,
        
        -- Semi-structured Topic Categories
        raw_payload:topic_categories as topic_categories,

        -- Partition & Snapshot Metadata
        raw_payload:snapshot_date::date as snapshot_date,
        raw_payload:extracted_at::timestamp_tz as extracted_at,
        execution_date,
        ingested_at,
        source_file
    from raw_source

),

deduplicated as (

    select *
    from extracted_fields
    -- Invariant: strictly 1 row per video per snapshot date, preserving latest ingest
    qualify row_number() over (
        partition by video_id, snapshot_date 
        order by ingested_at desc
    ) = 1

)

select * from deduplicated