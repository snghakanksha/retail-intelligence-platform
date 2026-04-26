USE WAREHOUSE RETAIL_WH;

CREATE OR REPLACE TABLE STAGING_DB.STG.stg_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    TRY_TO_DATE(order_date, 'YYYY-MM-DD')                                    AS order_date,
    LOWER(TRIM(order_status))                                                AS order_status,
    TRY_TO_NUMBER(quantity)                                                  AS quantity,
    TRY_TO_DECIMAL(unit_price, 10, 2)                                       AS unit_price,
    TRY_TO_DECIMAL(discount_pct, 5, 2)                                      AS discount_pct,
    ROUND(TRY_TO_NUMBER(quantity) * TRY_TO_DECIMAL(unit_price, 10, 2) * (1 - COALESCE(TRY_TO_DECIMAL(discount_pct,5,2), 0) / 100), 2) AS line_revenue,
    CASE WHEN TRY_TO_DECIMAL(discount_pct,5,2) > 0 THEN TRUE ELSE FALSE END AS is_discounted,
    UPPER(TRIM(shipping_city))                                               AS shipping_city,
    UPPER(TRIM(shipping_country))                                            AS shipping_country,
    DATE_TRUNC('month', TRY_TO_DATE(order_date, 'YYYY-MM-DD'))              AS order_month,
    DAYOFWEEK(TRY_TO_DATE(order_date, 'YYYY-MM-DD'))                        AS order_day_of_week,
    _loaded_at,
    _source_file,
    _run_id,
    CURRENT_TIMESTAMP() AS _stg_updated_at
FROM RAW_DB.RAW.raw_orders
WHERE order_id IS NOT NULL
  AND TRY_TO_DATE(order_date, 'YYYY-MM-DD') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1;

CREATE OR REPLACE TABLE STAGING_DB.STG.stg_customers AS
SELECT
    customer_id,
    INITCAP(TRIM(customer_name))                     AS customer_name,
    LOWER(TRIM(email))                               AS email,
    TRIM(phone)                                      AS phone,
    INITCAP(TRIM(city))                              AS city,
    INITCAP(TRIM(country))                           AS country,
    INITCAP(TRIM(segment))                           AS segment,
    TRY_TO_DATE(signup_date, 'YYYY-MM-DD')           AS signup_date,
    DATEDIFF('day', TRY_TO_DATE(signup_date, 'YYYY-MM-DD'), CURRENT_DATE()) AS customer_age_days,
    CASE WHEN email IS NULL OR email = '' THEN TRUE ELSE FALSE END           AS is_incomplete,
    _loaded_at,
    CURRENT_TIMESTAMP() AS _stg_updated_at
FROM RAW_DB.RAW.raw_customers
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) = 1;

CREATE OR REPLACE TABLE STAGING_DB.STG.stg_products AS
SELECT
    raw_data:product_id::VARCHAR                     AS product_id,
    raw_data:product_name::VARCHAR                   AS product_name,
    raw_data:category::VARCHAR                       AS category,
    raw_data:sub_category::VARCHAR                   AS sub_category,
    raw_data:supplier::VARCHAR                       AS supplier,
    raw_data:unit_cost::DECIMAL(10,2)               AS unit_cost,
    raw_data:unit_price::DECIMAL(10,2)              AS unit_price,
    raw_data:stock_qty::INTEGER                      AS stock_qty,
    raw_data:is_active::BOOLEAN                      AS is_active,
    ROUND(raw_data:unit_price::DECIMAL(10,2) - raw_data:unit_cost::DECIMAL(10,2), 2) AS gross_margin_amount,
    ROUND((raw_data:unit_price::DECIMAL(10,2) - raw_data:unit_cost::DECIMAL(10,2)) / NULLIF(raw_data:unit_price::DECIMAL(10,2), 0) * 100, 2) AS gross_margin_pct,
    CASE
        WHEN raw_data:stock_qty::INTEGER = 0 THEN 'Out of Stock'
        WHEN raw_data:stock_qty::INTEGER < 20 THEN 'Low Stock'
        ELSE 'In Stock'
    END AS stock_status,
    _loaded_at,
    CURRENT_TIMESTAMP() AS _stg_updated_at
FROM RAW_DB.RAW.raw_products
WHERE raw_data:product_id::VARCHAR IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY raw_data:product_id::VARCHAR ORDER BY _loaded_at DESC) = 1;

CREATE OR REPLACE TABLE STAGING_DB.STG.stg_web_events AS
SELECT
    raw_data:event_id::VARCHAR                       AS event_id,
    raw_data:session_id::VARCHAR                     AS session_id,
    raw_data:customer_id::VARCHAR                    AS customer_id,
    raw_data:event_type::VARCHAR                     AS event_type,
    TRY_TO_TIMESTAMP(raw_data:event_ts::VARCHAR)     AS event_ts,
    raw_data:page::VARCHAR                           AS page,
    raw_data:product_id::VARCHAR                     AS product_id,
    raw_data:device::VARCHAR                         AS device,
    raw_data:browser::VARCHAR                        AS browser,
    raw_data:country::VARCHAR                        AS country,
    CASE WHEN raw_data:customer_id::VARCHAR IS NULL THEN TRUE ELSE FALSE END AS is_anonymous,
    DATE_TRUNC('hour', TRY_TO_TIMESTAMP(raw_data:event_ts::VARCHAR))        AS event_hour,
    DATE_TRUNC('day',  TRY_TO_TIMESTAMP(raw_data:event_ts::VARCHAR))        AS event_date,
    _loaded_at,
    CURRENT_TIMESTAMP() AS _stg_updated_at
FROM RAW_DB.RAW.raw_web_events
WHERE raw_data:event_id::VARCHAR IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY raw_data:event_id::VARCHAR ORDER BY _loaded_at DESC) = 1;

SELECT 'stg_orders'     AS tbl, COUNT(*) AS row_count FROM STAGING_DB.STG.stg_orders     UNION ALL
SELECT 'stg_customers'  AS tbl, COUNT(*) AS row_count FROM STAGING_DB.STG.stg_customers  UNION ALL
SELECT 'stg_products'   AS tbl, COUNT(*) AS row_count FROM STAGING_DB.STG.stg_products   UNION ALL
SELECT 'stg_web_events' AS tbl, COUNT(*) AS row_count FROM STAGING_DB.STG.stg_web_events;