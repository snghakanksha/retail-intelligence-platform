USE WAREHOUSE RETAIL_WH;
USE ROLE ACCOUNTADMIN;

-- ═══════════════════════════════════════════════════════════
-- SNOWFLAKE TASKS — Automated Pipeline Scheduling
-- This makes your project production-grade
-- ═══════════════════════════════════════════════════════════

-- Task 1: Refresh staging tables (runs daily at 6am UTC)
CREATE OR REPLACE TASK STAGING_DB.STG.task_refresh_staging
    WAREHOUSE = RETAIL_WH
    SCHEDULE  = 'USING CRON 0 6 * * * UTC'
    COMMENT   = 'Daily refresh of all staging tables from RAW layer'
AS
BEGIN
    CREATE OR REPLACE TABLE STAGING_DB.STG.stg_orders AS
    SELECT
        order_id, customer_id, product_id,
        TRY_TO_DATE(order_date, 'YYYY-MM-DD') AS order_date,
        LOWER(TRIM(order_status)) AS order_status,
        TRY_TO_NUMBER(quantity) AS quantity,
        TRY_TO_DECIMAL(unit_price, 10, 2) AS unit_price,
        TRY_TO_DECIMAL(discount_pct, 5, 2) AS discount_pct,
        ROUND(TRY_TO_NUMBER(quantity) * TRY_TO_DECIMAL(unit_price, 10, 2) *
            (1 - COALESCE(TRY_TO_DECIMAL(discount_pct,5,2), 0) / 100), 2) AS line_revenue,
        CASE WHEN TRY_TO_DECIMAL(discount_pct,5,2) > 0 THEN TRUE ELSE FALSE END AS is_discounted,
        UPPER(TRIM(shipping_city)) AS shipping_city,
        UPPER(TRIM(shipping_country)) AS shipping_country,
        DATE_TRUNC('month', TRY_TO_DATE(order_date, 'YYYY-MM-DD')) AS order_month,
        DAYOFWEEK(TRY_TO_DATE(order_date, 'YYYY-MM-DD')) AS order_day_of_week,
        _loaded_at, _source_file, _run_id,
        CURRENT_TIMESTAMP() AS _stg_updated_at
    FROM RAW_DB.RAW.raw_orders
    WHERE order_id IS NOT NULL
      AND TRY_TO_DATE(order_date, 'YYYY-MM-DD') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1;
END;

-- Task 2: Refresh marts (runs daily at 7am UTC — 1 hour AFTER staging)
CREATE OR REPLACE TASK MARTS_DB.MARTS.task_refresh_marts
    WAREHOUSE = RETAIL_WH
    SCHEDULE  = 'USING CRON 0 7 * * * UTC'
    COMMENT   = 'Daily refresh of all mart tables from staging layer'
AS
BEGIN
    -- Refresh sales mart
    CREATE OR REPLACE TABLE MARTS_DB.MARTS.mart_sales AS
    SELECT
        o.order_date, o.order_month, o.order_day_of_week,
        p.category, p.sub_category, o.shipping_country, o.order_status,
        COUNT(DISTINCT o.order_id)                       AS total_orders,
        COUNT(DISTINCT o.customer_id)                    AS unique_customers,
        SUM(o.line_revenue)                              AS total_revenue,
        ROUND(AVG(o.line_revenue), 2)                    AS avg_order_value,
        SUM(o.quantity)                                  AS total_units_sold,
        SUM(CASE WHEN o.is_discounted THEN 1 ELSE 0 END) AS discounted_orders,
        ROUND(SUM(CASE WHEN o.is_discounted THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*),0), 2) AS discount_rate_pct,
        SUM(o.line_revenue - (p.unit_cost * o.quantity)) AS total_gross_profit,
        ROUND(SUM(o.line_revenue - (p.unit_cost * o.quantity)) * 100.0 / NULLIF(SUM(o.line_revenue),0), 2) AS gross_profit_margin_pct
    FROM STAGING_DB.STG.stg_orders o
    LEFT JOIN STAGING_DB.STG.stg_products p ON o.product_id = p.product_id
    WHERE o.order_status = 'completed'
    GROUP BY 1,2,3,4,5,6,7;

    -- Refresh anomalies mart
    CREATE OR REPLACE TABLE MARTS_DB.MARTS.mart_anomalies AS
    WITH daily_revenue AS (
        SELECT order_date, SUM(line_revenue) AS daily_revenue, COUNT(DISTINCT order_id) AS daily_orders
        FROM STAGING_DB.STG.stg_orders WHERE order_status = 'completed' GROUP BY 1
    ),
    stats AS (
        SELECT AVG(daily_revenue) AS mean_rev, STDDEV(daily_revenue) AS stddev_rev,
               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY daily_revenue) AS q1,
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY daily_revenue) AS q3
        FROM daily_revenue
    )
    SELECT d.order_date, d.daily_revenue, d.daily_orders, s.mean_rev, s.stddev_rev,
           ROUND((d.daily_revenue - s.mean_rev) / NULLIF(s.stddev_rev, 0), 2) AS z_score,
           s.q1 - 1.5 * (s.q3 - s.q1) AS iqr_lower_bound,
           s.q3 + 1.5 * (s.q3 - s.q1) AS iqr_upper_bound,
           CASE
               WHEN ABS((d.daily_revenue - s.mean_rev) / NULLIF(s.stddev_rev, 0)) > 2 THEN TRUE
               WHEN d.daily_revenue < s.q1 - 1.5 * (s.q3 - s.q1) THEN TRUE
               WHEN d.daily_revenue > s.q3 + 1.5 * (s.q3 - s.q1) THEN TRUE
               ELSE FALSE
           END AS is_anomaly,
           CASE
               WHEN d.daily_revenue > s.mean_rev + 2 * s.stddev_rev THEN 'Revenue Spike'
               WHEN d.daily_revenue < s.mean_rev - 2 * s.stddev_rev THEN 'Revenue Drop'
               ELSE 'Normal'
           END AS anomaly_type
    FROM daily_revenue d CROSS JOIN stats s ORDER BY d.order_date;
END;

-- Activate both tasks (they are SUSPENDED by default)
ALTER TASK STAGING_DB.STG.task_refresh_staging RESUME;
ALTER TASK MARTS_DB.MARTS.task_refresh_marts RESUME;

-- Verify tasks were created and are active
SHOW TASKS IN SCHEMA STAGING_DB.STG;
SHOW TASKS IN SCHEMA MARTS_DB.MARTS;