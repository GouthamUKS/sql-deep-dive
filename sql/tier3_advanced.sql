-- Tier 3: Advanced queries — window functions, CTEs, and complex joins


-- 1. Rank each customer within their state by lifetime value using ROW_NUMBER.
--    Who are the top 3 customers per state?
EXPLAIN ANALYZE
WITH customer_ltv AS (
    SELECT
        c.customer_id,
        c.state,
        c.city,
        ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) AS lifetime_value
    FROM customers c
    JOIN orders o       ON o.customer_id = c.customer_id
    JOIN order_items oi ON oi.order_id   = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY c.customer_id, c.state, c.city
),
ranked AS (
    SELECT
        customer_id,
        state,
        city,
        lifetime_value,
        ROW_NUMBER() OVER (
            PARTITION BY state
            ORDER BY lifetime_value DESC
        ) AS state_rank
    FROM customer_ltv
)
SELECT *
FROM ranked
WHERE state_rank <= 3
ORDER BY state, state_rank;


-- 2. Find the product with the 2nd highest total revenue per category
--    using RANK and DENSE_RANK to handle ties.
EXPLAIN ANALYZE
WITH product_revenue AS (
    SELECT
        p.product_id,
        p.category,
        ROUND(SUM(oi.price)::NUMERIC, 2) AS total_revenue
    FROM products p
    JOIN order_items oi ON oi.product_id = p.product_id
    JOIN orders o       ON o.order_id    = oi.order_id
    WHERE o.status = 'delivered'
    GROUP BY p.product_id, p.category
),
ranked AS (
    SELECT
        product_id,
        category,
        total_revenue,
        RANK()       OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rnk,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS dense_rnk
    FROM product_revenue
)
SELECT *
FROM ranked
WHERE dense_rnk = 2
ORDER BY category, total_revenue DESC;


-- 3. What is the day-over-day revenue change and percentage change using LAG/LEAD?
EXPLAIN ANALYZE
WITH daily_revenue AS (
    SELECT
        purchase_timestamp::DATE        AS order_date,
        SUM(oi.price + oi.freight_value) AS daily_revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY purchase_timestamp::DATE
)
SELECT
    order_date,
    ROUND(daily_revenue::NUMERIC, 2)    AS daily_revenue,
    ROUND(
        LAG(daily_revenue) OVER (ORDER BY order_date)::NUMERIC,
        2
    )                                   AS prev_day_revenue,
    ROUND(
        LEAD(daily_revenue) OVER (ORDER BY order_date)::NUMERIC,
        2
    )                                   AS next_day_revenue,
    ROUND(
        (
            (daily_revenue - LAG(daily_revenue) OVER (ORDER BY order_date))
            / NULLIF(LAG(daily_revenue) OVER (ORDER BY order_date), 0)
            * 100
        )::NUMERIC,
        2
    )                                   AS dod_change_pct
FROM daily_revenue
ORDER BY order_date;


-- 4. What is the running total revenue per customer across their orders
--    using SUM OVER with ROWS BETWEEN?
EXPLAIN ANALYZE
WITH customer_orders AS (
    SELECT
        o.customer_id,
        o.order_id,
        o.purchase_timestamp,
        SUM(oi.price + oi.freight_value) AS order_value
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY o.customer_id, o.order_id, o.purchase_timestamp
)
SELECT
    customer_id,
    order_id,
    purchase_timestamp,
    ROUND(order_value::NUMERIC, 2)      AS order_value,
    ROUND(
        SUM(order_value) OVER (
            PARTITION BY customer_id
            ORDER BY purchase_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )::NUMERIC,
        2
    )                                   AS running_total
FROM customer_orders
ORDER BY customer_id, purchase_timestamp;


-- 5. Build an RFM (Recency, Frequency, Monetary) score for each customer
--    using a multi-step CTE pipeline.
EXPLAIN ANALYZE
WITH reference_date AS (
    SELECT MAX(purchase_timestamp)::DATE AS max_date
    FROM orders
    WHERE status = 'delivered'
),
customer_rfm AS (
    SELECT
        o.customer_id,
        (SELECT max_date FROM reference_date)
            - MAX(o.purchase_timestamp)::DATE       AS recency_days,
        COUNT(DISTINCT o.order_id)                  AS frequency,
        SUM(oi.price + oi.freight_value)            AS monetary
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY o.customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        ROUND(monetary::NUMERIC, 2)     AS monetary,
        NTILE(5) OVER (ORDER BY recency_days ASC)   AS r_score,
        NTILE(5) OVER (ORDER BY frequency    DESC)  AS f_score,
        NTILE(5) OVER (ORDER BY monetary     DESC)  AS m_score
    FROM customer_rfm
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score             AS rfm_total,
    CASE
        WHEN r_score >= 4 AND f_score >= 4  THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3  THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score < 2   THEN 'New Customers'
        WHEN r_score < 2 AND f_score >= 3   THEN 'At Risk'
        WHEN r_score < 2 AND f_score < 2    THEN 'Lost'
        ELSE 'Potential Loyalists'
    END                                     AS rfm_segment
FROM rfm_scores
ORDER BY rfm_total DESC;


-- 6. For each combination of state, category, and year, what is the total revenue
--    and the number of unique customers? (Complex star schema join across 4 tables.)
EXPLAIN ANALYZE
SELECT
    c.state,
    p.category,
    EXTRACT(YEAR FROM o.purchase_timestamp)::INT    AS order_year,
    COUNT(DISTINCT o.customer_id)                   AS unique_customers,
    COUNT(DISTINCT o.order_id)                      AS total_orders,
    ROUND(SUM(oi.price)::NUMERIC, 2)                AS product_revenue,
    ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) AS total_revenue
FROM orders o
JOIN customers   c  ON c.customer_id = o.customer_id
JOIN order_items oi ON oi.order_id   = o.order_id
JOIN products    p  ON p.product_id  = oi.product_id
WHERE o.status = 'delivered'
  AND c.state    IN ('SP', 'RJ', 'MG', 'RS', 'PR')
  AND p.category IN ('electronics', 'fashion', 'home_garden', 'sports', 'beauty')
  AND EXTRACT(YEAR FROM o.purchase_timestamp) IN (2017, 2018)
GROUP BY c.state, p.category, EXTRACT(YEAR FROM o.purchase_timestamp)
ORDER BY c.state, order_year, total_revenue DESC;
