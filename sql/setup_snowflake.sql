-- ============================================================
-- RETAIL INTELLIGENCE PLATFORM — Snowflake Setup Script
-- Run as ACCOUNTADMIN or SYSADMIN
-- ============================================================

-- Step 1: Create a dedicated role for this project
-- NOTE: Requires ACCOUNTADMIN or USERADMIN role. Comment out if not available.
-- USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS RETAIL_ANALYST;
GRANT ROLE RETAIL_ANALYST TO ROLE SYSADMIN;
-- USE ROLE SYSADMIN;

-- Step 2: Create the virtual warehouse
-- XSMALL is fine for development; scales up for production loads
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 120          -- auto-pause after 2 min idle (saves credits)
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Retail Intelligence Platform';

GRANT USAGE ON WAREHOUSE RETAIL_WH TO ROLE RETAIL_ANALYST;

-- Step 3: Create three separate databases (medallion architecture)
CREATE DATABASE IF NOT EXISTS RAW_DB
    COMMENT = 'Layer 1 — Raw ingested data, never modified';

CREATE DATABASE IF NOT EXISTS STAGING_DB
    COMMENT = 'Layer 2 — Cleaned, cast, deduplicated data + business logic';

CREATE DATABASE IF NOT EXISTS MARTS_DB
    COMMENT = 'Layer 3 — Final analytics-ready tables for BI tools';

-- Step 4: Create schemas inside each database
CREATE SCHEMA IF NOT EXISTS RAW_DB.RAW;
CREATE SCHEMA IF NOT EXISTS STAGING_DB.STG;
CREATE SCHEMA IF NOT EXISTS MARTS_DB.MARTS;

-- Step 5: Grant privileges to the analyst role
GRANT USAGE ON DATABASE RAW_DB TO ROLE RETAIL_ANALYST;
GRANT USAGE ON DATABASE STAGING_DB TO ROLE RETAIL_ANALYST;
GRANT USAGE ON DATABASE MARTS_DB TO ROLE RETAIL_ANALYST;

GRANT USAGE ON SCHEMA RAW_DB.RAW TO ROLE RETAIL_ANALYST;
GRANT USAGE ON SCHEMA STAGING_DB.STG TO ROLE RETAIL_ANALYST;
GRANT USAGE ON SCHEMA MARTS_DB.MARTS TO ROLE RETAIL_ANALYST;

GRANT ALL ON SCHEMA RAW_DB.RAW TO ROLE RETAIL_ANALYST;
GRANT ALL ON SCHEMA STAGING_DB.STG TO ROLE RETAIL_ANALYST;
GRANT ALL ON SCHEMA MARTS_DB.MARTS TO ROLE RETAIL_ANALYST;

-- Step 6: Create a pipeline run manifest table
-- This tracks every data load — a professional practice most beginners skip
USE DATABASE RAW_DB;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    table_name      VARCHAR(100)    NOT NULL,
    source_file     VARCHAR(500),
    rows_loaded     INTEGER,
    rows_failed     INTEGER         DEFAULT 0,
    load_status     VARCHAR(20)     DEFAULT 'STARTED',   -- STARTED / SUCCESS / FAILED
    error_message   VARCHAR(2000),
    started_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    completed_at    TIMESTAMP_NTZ,
    loaded_by       VARCHAR(100)    DEFAULT CURRENT_USER(),
    PRIMARY KEY (run_id)
);

-- Step 7: Create raw tables with VARIANT columns for flexible ingestion
-- Orders — structured CSV source
CREATE TABLE IF NOT EXISTS RAW_DB.RAW.raw_orders (
    order_id        VARCHAR(50),
    customer_id     VARCHAR(50),
    product_id      VARCHAR(50),
    order_date      VARCHAR(50),    -- keep as string in RAW; cast in staging
    order_status    VARCHAR(30),
    quantity        VARCHAR(20),
    unit_price      VARCHAR(20),
    discount_pct    VARCHAR(20),
    shipping_city   VARCHAR(100),
    shipping_country VARCHAR(100),
    _loaded_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500),
    _run_id         VARCHAR(36)
);

-- Products — JSON source stored as VARIANT (Snowflake's semi-structured type)
CREATE TABLE IF NOT EXISTS RAW_DB.RAW.raw_products (
    raw_data        VARIANT,        -- entire JSON object preserved
    _loaded_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500),
    _run_id         VARCHAR(36)
);

-- Customers
CREATE TABLE IF NOT EXISTS RAW_DB.RAW.raw_customers (
    customer_id     VARCHAR(50),
    customer_name   VARCHAR(200),
    email           VARCHAR(200),
    phone           VARCHAR(50),
    city            VARCHAR(100),
    country         VARCHAR(100),
    segment         VARCHAR(50),
    signup_date     VARCHAR(50),
    _loaded_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500),
    _run_id         VARCHAR(36)
);

-- Web events — JSON source
CREATE TABLE IF NOT EXISTS RAW_DB.RAW.raw_web_events (
    raw_data        VARIANT,
    _loaded_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500),
    _run_id         VARCHAR(36)
);

-- Verify everything was created correctly
SHOW TABLES IN SCHEMA RAW_DB.RAW;


SELECT 'raw_orders'     AS tbl, COUNT(*) AS row_count FROM RAW_DB.RAW.raw_orders    UNION ALL
SELECT 'raw_customers'  AS tbl, COUNT(*) AS row_count FROM RAW_DB.RAW.raw_customers  UNION ALL
SELECT 'raw_products'   AS tbl, COUNT(*) AS row_count FROM RAW_DB.RAW.raw_products   UNION ALL
SELECT 'raw_web_events' AS tbl, COUNT(*) AS row_count FROM RAW_DB.RAW.raw_web_events;
