-- =====================================================================
-- 01_snowflake_setup.sql
-- Database, schemas, warehouse, file formats, stages, and raw tables.
-- =====================================================================

-- 1. Create Virtual Warehouse (X-Small with auto-suspend)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Virtual warehouse for publisher intelligence ELT workloads';

-- 2. Create Database & Schemas
CREATE DATABASE IF NOT EXISTS PUBLISHER_DWH;

-- RAW_INGEST: Landing schema for raw S3 JSON payloads
CREATE SCHEMA IF NOT EXISTS PUBLISHER_DWH.RAW_INGEST;

-- Analytical schemas populated by dbt
CREATE SCHEMA IF NOT EXISTS PUBLISHER_DWH.STAGING;
CREATE SCHEMA IF NOT EXISTS PUBLISHER_DWH.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS PUBLISHER_DWH.MARTS;

USE WAREHOUSE COMPUTE_WH;
USE DATABASE PUBLISHER_DWH;
USE SCHEMA RAW_INGEST;

-- 3. File Format for Gzipped JSON Payloads
CREATE OR REPLACE FILE FORMAT PUBLISHER_DWH.RAW_INGEST.FF_JSON_GZIP
    TYPE = 'JSON'
    COMPRESSION = 'GZIP'
    STRIP_OUTER_ARRAY = TRUE
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = TRUE;

-- 4. Target Raw Table to Store VARIANT Payloads
-- Invariant: (raw_payload VARIANT, execution_date DATE, ingested_at TIMESTAMP_NTZ, source_file STRING)
CREATE TABLE IF NOT EXISTS PUBLISHER_DWH.RAW_INGEST.RAW_VIDEOS (
    raw_payload     VARIANT         COMMENT 'Raw extracted JSON object representing a single video snapshot',
    execution_date  DATE            COMMENT 'Logical execution/partition date from S3 path',
    ingested_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when row was loaded into Snowflake',
    source_file     STRING          COMMENT 'Fully qualified S3 source file path'
);

-- 5. External S3 Stage
-- NOTE: In production, substitute CREDENTIALS with your Storage Integration.
-- If using key-based access locally, load keys securely via environment variables or secret manager.
-- CREATE OR REPLACE STAGE PUBLISHER_DWH.RAW_INGEST.STAGE_S3_RAW
--     URL = 's3://publisher-telemetry-lake-chanethan408/raw/entity=videos/'
--     CREDENTIALS = (
--         AWS_KEY_ID = '<AWS_KEY_ID>'
--         AWS_SECRET_KEY = '<AWS_SECRET_KEY>'
--     )
--     FILE_FORMAT = PUBLISHER_DWH.RAW_INGEST.FF_JSON_GZIP;