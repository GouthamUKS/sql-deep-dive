-- Tier 1: Basic queries — aggregation, filtering, and simple joins


-- 1. What is the total revenue and total number of delivered orders in the dataset?
SELECT
    COUNT(DISTINCT o.order_id)          AS total_orders,
    SUM(oi.price + oi.freight_value)    AS total_revenue,
    AVG(oi.price + oi.freight_value)    AS avg_order_revenue
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
WHERE o.status = 'delivered';


-- 2. What is the average order value broken down by customer state?
SELECT
    c.state,
    COUNT(DISTINCT o.order_id)                          AS order_count,
    ROUND(AVG(oi.price + oi.freight_value)::NUMERIC, 2) AS avg_order_value,
    ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) AS total_revenue
FROM customers c
JOIN orders o     ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
WHERE o.status = 'delivered'
GROUP BY c.state
ORDER BY total_revenue DESC;


-- 3. Which are the top 10 product categories by total revenue?
SELECT
    p.category,
    COUNT(DISTINCT oi.order_id)                         AS order_count,
    SUM(oi.price)                                       AS product_revenue,
    SUM(oi.freight_value)                               AS freight_revenue,
    ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) AS total_revenue
FROM products p
JOIN order_items oi ON oi.product_id = p.product_id
JOIN orders o       ON o.order_id    = oi.order_id
WHERE o.status = 'delivered'
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 10;


-- 4. How many orders were placed each month across the full date range?
SELECT
    DATE_TRUNC('month', purchase_timestamp) AS order_month,
    COUNT(order_id)                         AS order_count,
    COUNT(DISTINCT customer_id)             AS unique_customers
FROM orders
GROUP BY DATE_TRUNC('month', purchase_timestamp)
ORDER BY order_month;


-- 5. Which customers are above the 90th percentile in total lifetime spend?
SELECT
    c.customer_id,
    c.state,
    c.city,
    ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) AS lifetime_spend
FROM customers c
JOIN orders o       ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id   = o.order_id
WHERE o.status = 'delivered'
GROUP BY c.customer_id, c.state, c.city
HAVING SUM(oi.price + oi.freight_value) > (
    SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (
        ORDER BY customer_total
    )
    FROM (
        SELECT SUM(oi2.price + oi2.freight_value) AS customer_total
        FROM orders o2
        JOIN order_items oi2 ON oi2.order_id = o2.order_id
        WHERE o2.status = 'delivered'
        GROUP BY o2.customer_id
    ) customer_totals
)
ORDER BY lifetime_spend DESC;
