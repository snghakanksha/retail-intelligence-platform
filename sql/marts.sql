USE WAREHOUSE RETAIL_WH;

-- ═══════════════════════════════════════════════════════════
-- MART 1: SALES MART
-- Daily revenue, AOV, order counts, discount analysis
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE MARTS_DB.MARTS.mart_sales AS
SELECT
    o.order_date,
    o.order_month,
    o.order_day_of_week,
    p.category,
    p.sub_category,
    o.shipping_country,
    o.order_status,

    COUNT(DISTINCT o.order_id)                        AS total_orders,
    COUNT(DISTINCT o.customer_id)                     AS unique_customers,
    SUM(o.line_revenue)                               AS total_revenue,
    ROUND(AVG(o.line_revenue), 2)                     AS avg_order_value,
    SUM(o.quantity)                                   AS total_units_sold,
    SUM(CASE WHEN o.is_discounted THEN 1 ELSE 0 END)  AS discounted_orders,
    ROUND(
        SUM(CASE WHEN o.is_discounted THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                                 AS discount_rate_pct,
    SUM(o.line_revenue - (p.unit_cost * o.quantity))  AS total_gross_profit,
    ROUND(
        SUM(o.line_revenue - (p.unit_cost * o.quantity)) * 100.0
        / NULLIF(SUM(o.line_revenue), 0), 2
    )                                                 AS gross_profit_margin_pct

FROM STAGING_DB.STG.stg_orders o
LEFT JOIN STAGING_DB.STG.stg_products p ON o.product_id = p.product_id
WHERE o.order_status = 'completed'
GROUP BY 1,2,3,4,5,6,7;


-- ═══════════════════════════════════════════════════════════
-- MART 2: CUSTOMER RFM MART
-- Recency, Frequency, Monetary scoring — MNCs love this
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE MARTS_DB.MARTS.mart_customer_rfm AS
WITH order_stats AS (
    SELECT
        o.customer_id,
        MAX(o.order_date)                             AS last_order_date,
        COUNT(DISTINCT o.order_id)                    AS order_count,
        SUM(o.line_revenue)                           AS total_spent,
        ROUND(AVG(o.line_revenue), 2)                 AS avg_order_value,
        MIN(o.order_date)                             AS first_order_date
    FROM STAGING_DB.STG.stg_orders o
    WHERE o.order_status = 'completed'
    GROUP BY 1
),
rfm_raw AS (
    SELECT
        os.customer_id,
        c.customer_name,
        c.email,
        c.segment,
        c.city,
        c.country,
        c.signup_date,
        os.last_order_date,
        os.first_order_date,
        os.order_count,
        os.total_spent,
        os.avg_order_value,
        DATEDIFF('day', os.last_order_date, CURRENT_DATE()) AS recency_days,

        -- RFM Scores 1-5 using NTILE
        NTILE(5) OVER (ORDER BY DATEDIFF('day', os.last_order_date, CURRENT_DATE()) ASC)  AS recency_score,
        NTILE(5) OVER (ORDER BY os.order_count DESC)                                       AS frequency_score,
        NTILE(5) OVER (ORDER BY os.total_spent DESC)                                       AS monetary_score

    FROM order_stats os
    JOIN STAGING_DB.STG.stg_customers c ON os.customer_id = c.customer_id
),
rfm_scored AS (
    SELECT *,
        ROUND((recency_score + frequency_score + monetary_score) / 3.0, 2) AS rfm_avg_score,
        CONCAT(recency_score::VARCHAR, frequency_score::VARCHAR, monetary_score::VARCHAR) AS rfm_cell
    FROM rfm_raw
)
SELECT *,
    CASE
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3                          THEN 'Loyal Customers'
        WHEN recency_score >= 4 AND frequency_score <= 2                          THEN 'New Customers'
        WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3  THEN 'Potential Loyalists'
        WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3  THEN 'At Risk'
        WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4  THEN 'Cant Lose Them'
        WHEN recency_score <= 2 AND frequency_score <= 2                          THEN 'Lost'
        ELSE 'Needs Attention'
    END AS customer_segment_rfm
FROM rfm_scored;


-- ═══════════════════════════════════════════════════════════
-- MART 3: PRODUCT PERFORMANCE MART
-- Best/worst sellers, margin analysis, return rates
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE MARTS_DB.MARTS.mart_product AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.supplier,
    p.unit_cost,
    p.unit_price,
    p.gross_margin_pct,
    p.stock_status,
    p.stock_qty,

    COUNT(DISTINCT o.order_id)                        AS total_orders,
    SUM(o.quantity)                                   AS total_units_sold,
    SUM(o.line_revenue)                               AS total_revenue,
    ROUND(AVG(o.line_revenue), 2)                     AS avg_order_value,

    -- Return rate
    COUNT(DISTINCT CASE WHEN o.order_status = 'returned' THEN o.order_id END) AS returned_orders,
    ROUND(
        COUNT(DISTINCT CASE WHEN o.order_status = 'returned' THEN o.order_id END) * 100.0
        / NULLIF(COUNT(DISTINCT o.order_id), 0), 2
    )                                                 AS return_rate_pct,

    -- Ranking within category
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(o.line_revenue) DESC) AS revenue_rank_in_category

FROM STAGING_DB.STG.stg_products p
LEFT JOIN STAGING_DB.STG.stg_orders o ON p.product_id = o.product_id
GROUP BY 1,2,3,4,5,6,7,8,9,10;


-- ═══════════════════════════════════════════════════════════
-- MART 4: ANOMALY FLAGS TABLE
-- Revenue anomalies using statistical detection
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE MARTS_DB.MARTS.mart_anomalies AS
WITH daily_revenue AS (
    SELECT
        order_date,
        SUM(line_revenue) AS daily_revenue,
        COUNT(DISTINCT order_id) AS daily_orders
    FROM STAGING_DB.STG.stg_orders
    WHERE order_status = 'completed'
    GROUP BY 1
),
stats AS (
    SELECT
        AVG(daily_revenue)    AS mean_rev,
        STDDEV(daily_revenue) AS stddev_rev,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY daily_revenue) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY daily_revenue) AS q3
    FROM daily_revenue
)
SELECT
    d.order_date,
    d.daily_revenue,
    d.daily_orders,
    s.mean_rev,
    s.stddev_rev,
    ROUND((d.daily_revenue - s.mean_rev) / NULLIF(s.stddev_rev, 0), 2) AS z_score,
    s.q1 - 1.5 * (s.q3 - s.q1)  AS iqr_lower_bound,
    s.q3 + 1.5 * (s.q3 - s.q1)  AS iqr_upper_bound,
    CASE
        WHEN ABS((d.daily_revenue - s.mean_rev) / NULLIF(s.stddev_rev, 0)) > 2 THEN TRUE
        WHEN d.daily_revenue < s.q1 - 1.5 * (s.q3 - s.q1)                     THEN TRUE
        WHEN d.daily_revenue > s.q3 + 1.5 * (s.q3 - s.q1)                     THEN TRUE
        ELSE FALSE
    END AS is_anomaly,
    CASE
        WHEN d.daily_revenue > s.mean_rev + 2 * s.stddev_rev THEN 'Revenue Spike'
        WHEN d.daily_revenue < s.mean_rev - 2 * s.stddev_rev THEN 'Revenue Drop'
        ELSE 'Normal'
    END AS anomaly_type
FROM daily_revenue d
CROSS JOIN stats s
ORDER BY d.order_date;


-- Verify all marts
SELECT 'mart_sales'         AS tbl, COUNT(*) AS row_count FROM MARTS_DB.MARTS.mart_sales         UNION ALL
SELECT 'mart_customer_rfm'  AS tbl, COUNT(*) AS row_count FROM MARTS_DB.MARTS.mart_customer_rfm  UNION ALL
SELECT 'mart_product'       AS tbl, COUNT(*) AS row_count FROM MARTS_DB.MARTS.mart_product        UNION ALL
SELECT 'mart_anomalies'     AS tbl, COUNT(*) AS row_count FROM MARTS_DB.MARTS.mart_anomalies;