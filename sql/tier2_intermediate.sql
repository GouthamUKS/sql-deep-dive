-- Tier 2: Intermediate queries — multi-table joins, aggregations, subqueries, time-series


-- 1. What is the total revenue by customer state and product category for delivered orders?
EXPLAIN ANALYZE
SELECT
    c.state,
    p.category,
    COUNT(DISTINCT o.order_id)                          AS order_count,
    ROUND(SUM(oi.price)::NUMERIC, 2)                    AS product_revenue,
    ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) AS total_revenue
FROM orders o
JOIN customers   c  ON c.customer_id = o.customer_id
JOIN order_items oi ON oi.order_id   = o.order_id
JOIN products    p  ON p.product_id  = oi.product_id
WHERE o.status = 'delivered'
GROUP BY c.state, p.category
ORDER BY c.state, total_revenue DESC;


-- 2. What is the monthly revenue and year-over-year growth for 2017 vs 2018?
EXPLAIN ANALYZE
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', o.purchase_timestamp)       AS order_month,
        EXTRACT(YEAR  FROM o.purchase_timestamp)::INT   AS order_year,
        EXTRACT(MONTH FROM o.purchase_timestamp)::INT   AS order_month_num,
        SUM(oi.price + oi.freight_value)                AS revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY DATE_TRUNC('month', o.purchase_timestamp),
             EXTRACT(YEAR  FROM o.purchase_timestamp),
             EXTRACT(MONTH FROM o.purchase_timestamp)
)
SELECT
    cur.order_month,
    ROUND(cur.revenue::NUMERIC, 2)  AS revenue,
    ROUND(prev.revenue::NUMERIC, 2) AS prev_year_revenue,
    ROUND(
        ((cur.revenue - prev.revenue) / NULLIF(prev.revenue, 0) * 100)::NUMERIC,
        2
    )                               AS yoy_growth_pct
FROM monthly_revenue cur
LEFT JOIN monthly_revenue prev
    ON  prev.order_year      = cur.order_year - 1
    AND prev.order_month_num = cur.order_month_num
ORDER BY cur.order_month;


-- 3. Which customers placed orders in both 2017 and 2018?
EXPLAIN ANALYZE
SELECT
    c.customer_id,
    c.state,
    c.city
FROM customers c
WHERE c.customer_id IN (
    SELECT customer_id
    FROM orders
    WHERE EXTRACT(YEAR FROM purchase_timestamp) = 2017
      AND status = 'delivered'
)
AND c.customer_id IN (
    SELECT customer_id
    FROM orders
    WHERE EXTRACT(YEAR FROM purchase_timestamp) = 2018
      AND status = 'delivered'
)
ORDER BY c.state, c.customer_id;


-- 4. What is the daily order count and the 7-day rolling average?
EXPLAIN ANALYZE
WITH daily_orders AS (
    SELECT
        purchase_timestamp::DATE        AS order_date,
        COUNT(order_id)                 AS daily_count
    FROM orders
    GROUP BY purchase_timestamp::DATE
)
SELECT
    order_date,
    daily_count,
    ROUND(
        AVG(daily_count) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC,
        2
    ) AS rolling_7d_avg
FROM daily_orders
ORDER BY order_date;


-- 5. What is the monthly cohort retention: how many customers from each signup cohort
--    placed a repeat order in subsequent months?
EXPLAIN ANALYZE
WITH customer_first_order AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(purchase_timestamp)) AS cohort_month
    FROM orders
    WHERE status = 'delivered'
    GROUP BY customer_id
),
customer_orders AS (
    SELECT
        o.customer_id,
        DATE_TRUNC('month', o.purchase_timestamp) AS order_month
    FROM orders o
    WHERE o.status = 'delivered'
    GROUP BY o.customer_id, DATE_TRUNC('month', o.purchase_timestamp)
)
SELECT
    cfo.cohort_month,
    EXTRACT(
        MONTH FROM AGE(co.order_month, cfo.cohort_month)
    )::INT                              AS months_since_first_order,
    COUNT(DISTINCT co.customer_id)      AS retained_customers
FROM customer_first_order cfo
JOIN customer_orders co
    ON  co.customer_id = cfo.customer_id
    AND co.order_month >= cfo.cohort_month
GROUP BY cfo.cohort_month,
         EXTRACT(MONTH FROM AGE(co.order_month, cfo.cohort_month))
ORDER BY cfo.cohort_month, months_since_first_order;
