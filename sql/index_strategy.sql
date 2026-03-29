-- Index strategy for the Brazilian E-Commerce schema.
-- Each index targets a specific access pattern identified in the query workload.

-- Supports lookups by customer with date range scans (cohort queries, customer LTV).
-- The DESC ordering matches ORDER BY purchase_timestamp DESC patterns.
CREATE INDEX IF NOT EXISTS idx_orders_customer_date
    ON orders (customer_id, purchase_timestamp DESC);

-- Partial index on non-delivered orders only.
-- The WHERE clause eliminates the majority of rows (78% delivered), keeping
-- the index small and cache-resident for operational status dashboards.
CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders (status)
    WHERE status != 'delivered';

-- Supports joins from order_items to products in category-revenue queries.
CREATE INDEX IF NOT EXISTS idx_order_items_product
    ON order_items (product_id);

-- Supports seller performance aggregations and joins from order_items to sellers.
CREATE INDEX IF NOT EXISTS idx_order_items_seller
    ON order_items (seller_id);

-- Supports joins from order_payments, order_reviews back to order_items
-- and aggregations that group by order_id.
CREATE INDEX IF NOT EXISTS idx_order_items_order
    ON order_items (order_id);

-- Supports joins from order_payments to orders; one-to-many relationship
-- makes this the hot path for payment-type breakdowns.
CREATE INDEX IF NOT EXISTS idx_payments_order
    ON order_payments (order_id);

-- Supports joins from order_reviews to orders used in review-score queries.
CREATE INDEX IF NOT EXISTS idx_reviews_order
    ON order_reviews (order_id);

-- Supports category filter and GROUP BY category in product revenue queries.
CREATE INDEX IF NOT EXISTS idx_products_category
    ON products (category);

-- Supports customer segmentation and aggregation by state.
CREATE INDEX IF NOT EXISTS idx_customers_state
    ON customers (state);

-- Covers time-series queries (monthly revenue, daily rolling averages) that
-- scan orders ordered or filtered by purchase_timestamp.
CREATE INDEX IF NOT EXISTS idx_orders_purchase_ts
    ON orders (purchase_timestamp DESC);

-- Composite index for the high-frequency (state, category, month) GROUP BY
-- pattern that appears in Tier 4 optimization queries.  Covers the join path
-- customers → orders → order_items → products in a single index range scan.
CREATE INDEX IF NOT EXISTS idx_orders_state_ts
    ON orders (customer_id, purchase_timestamp DESC);
