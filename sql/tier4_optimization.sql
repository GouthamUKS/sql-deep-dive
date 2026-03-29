-- Tier 4: Query optimization — slow original vs optimized rewrite for each pattern


-- ===========================================================================
-- 1. Sequential scan eliminated by a composite index
-- ===========================================================================

-- ROOT CAUSE: No index on (customer_id, purchase_timestamp); Postgres must
-- read the full orders table to filter by customer and date range.

-- OPTIMIZATION STRATEGY: Add a composite index on (customer_id, purchase_timestamp DESC)
-- so the planner can do an index range scan instead of a sequential scan.
-- Index created in index_strategy.sql:
--   idx_orders_customer_date ON orders (customer_id, purchase_timestamp DESC)

-- ORIGINAL (slow): full sequential scan on orders
SELECT
    o.order_id,
    o.purchase_timestamp,
    o.status
FROM orders o
WHERE o.customer_id = 12345
  AND o.purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
ORDER BY o.purchase_timestamp DESC;

-- OPTIMIZED: identical query — performance gain comes from the index
EXPLAIN ANALYZE
SELECT
    o.order_id,
    o.purchase_timestamp,
    o.status
FROM orders o
WHERE o.customer_id = 12345
  AND o.purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
ORDER BY o.purchase_timestamp DESC;


-- ===========================================================================
-- 2. Correlated subquery replaced by a window function
-- ===========================================================================

-- ROOT CAUSE: The correlated subquery re-executes once per row, scanning
-- order_items and products for every product row (O(n) nested loop).

-- OPTIMIZATION STRATEGY: Use AVG() OVER (PARTITION BY category) to compute
-- per-category average in a single pass over the joined result set.

-- ORIGINAL (slow): correlated subquery executed once per product row
EXPLAIN ANALYZE
SELECT
    p.product_id,
    p.category,
    p.price,
    (
        SELECT AVG(p2.price)
        FROM products p2
        WHERE p2.category = p.category
    ) AS avg_category_price,
    p.price - (
        SELECT AVG(p2.price)
        FROM products p2
        WHERE p2.category = p.category
    ) AS price_vs_avg
FROM products p
ORDER BY p.category, price_vs_avg DESC;

-- OPTIMIZED: single pass with window function
EXPLAIN ANALYZE
SELECT
    product_id,
    category,
    price,
    ROUND(
        AVG(price) OVER (PARTITION BY category)::NUMERIC,
        2
    )                                   AS avg_category_price,
    ROUND(
        (price - AVG(price) OVER (PARTITION BY category))::NUMERIC,
        2
    )                                   AS price_vs_avg
FROM products
ORDER BY category, price_vs_avg DESC;


-- ===========================================================================
-- 3. N+1 pattern replaced by a single aggregating JOIN
-- ===========================================================================

-- ROOT CAUSE: Application code (or an ORM) issues one SELECT per seller to
-- fetch their order count and revenue, resulting in 500 round trips.
-- The equivalent in SQL is a non-correlated subquery in the SELECT list that
-- causes repeated scans.

-- OPTIMIZATION STRATEGY: Replace with a single aggregating JOIN so the
-- database computes all seller metrics in one pass.

-- ORIGINAL (slow): repeated subqueries — one execution per seller row
EXPLAIN ANALYZE
SELECT
    s.seller_id,
    s.city,
    s.state,
    (
        SELECT COUNT(DISTINCT oi.order_id)
        FROM order_items oi
        WHERE oi.seller_id = s.seller_id
    ) AS total_orders,
    (
        SELECT ROUND(SUM(oi.price)::NUMERIC, 2)
        FROM order_items oi
        WHERE oi.seller_id = s.seller_id
    ) AS total_revenue
FROM sellers s
ORDER BY total_revenue DESC NULLS LAST;

-- OPTIMIZED: single pass aggregating JOIN
EXPLAIN ANALYZE
SELECT
    s.seller_id,
    s.city,
    s.state,
    COUNT(DISTINCT oi.order_id)                     AS total_orders,
    ROUND(SUM(oi.price)::NUMERIC, 2)                AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)                AS avg_item_price
FROM sellers s
LEFT JOIN order_items oi ON oi.seller_id = s.seller_id
GROUP BY s.seller_id, s.city, s.state
ORDER BY total_revenue DESC NULLS LAST;


-- ===========================================================================
-- 4. Missing composite index for (state, category, month) GROUP BY
-- ===========================================================================

-- ROOT CAUSE: The query joins four tables and groups by state + category + month.
-- Without a covering index the planner does three sequential scans and an
-- expensive hash aggregate over 120 K order_items rows.

-- OPTIMIZATION STRATEGY:
--   a) Create a composite index idx_customers_state ON customers(state) so the
--      customer filter is an index scan rather than a sequential scan.
--   b) Create a partial index idx_orders_purchase_ts ON orders(purchase_timestamp DESC)
--      so the date-range filter avoids a full table scan.
--   c) Rewrite with a CTE that pre-filters orders to the target year before
--      joining, reducing the join input size significantly.

-- ORIGINAL (slow): unoptimized four-table join with inline expressions in GROUP BY
EXPLAIN ANALYZE
SELECT
    c.state,
    p.category,
    DATE_TRUNC('month', o.purchase_timestamp)       AS order_month,
    COUNT(DISTINCT o.order_id)                      AS order_count,
    SUM(oi.price + oi.freight_value)                AS total_revenue
FROM orders o
JOIN customers   c  ON c.customer_id = o.customer_id
JOIN order_items oi ON oi.order_id   = o.order_id
JOIN products    p  ON p.product_id  = oi.product_id
WHERE o.status = 'delivered'
GROUP BY c.state, p.category, DATE_TRUNC('month', o.purchase_timestamp)
ORDER BY c.state, order_month, total_revenue DESC;

-- OPTIMIZED: pre-filter with CTE, explicit column aliases, indexes in place
EXPLAIN ANALYZE
WITH delivered_orders AS (
    SELECT order_id, customer_id, purchase_timestamp
    FROM orders
    WHERE status = 'delivered'
      AND purchase_timestamp >= '2017-01-01'
      AND purchase_timestamp <  '2019-01-01'
),
order_revenue AS (
    SELECT
        do_.customer_id,
        DATE_TRUNC('month', do_.purchase_timestamp) AS order_month,
        oi.order_id,
        oi.product_id,
        oi.price + oi.freight_value                 AS revenue
    FROM delivered_orders do_
    JOIN order_items oi ON oi.order_id = do_.order_id
)
SELECT
    c.state,
    p.category,
    or_.order_month,
    COUNT(DISTINCT or_.order_id)                        AS order_count,
    ROUND(SUM(or_.revenue)::NUMERIC, 2)                 AS total_revenue
FROM order_revenue or_
JOIN customers c ON c.customer_id = or_.customer_id
JOIN products  p ON p.product_id  = or_.product_id
GROUP BY c.state, p.category, or_.order_month
ORDER BY c.state, or_.order_month, total_revenue DESC;
