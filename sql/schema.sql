-- Brazilian E-Commerce schema (Olist-style mock dataset)

CREATE TABLE IF NOT EXISTS customers (
    customer_id     BIGINT PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    state           CHAR(2) NOT NULL,
    zip_code        VARCHAR(10) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS sellers (
    seller_id       BIGINT PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    state           CHAR(2) NOT NULL,
    zip_code        VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id          BIGINT PRIMARY KEY,
    category            VARCHAR(50) NOT NULL,
    name_length         SMALLINT,
    description_length  INT,
    photos_qty          SMALLINT,
    weight_g            INT,
    price               DECIMAL(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id                BIGINT PRIMARY KEY,
    customer_id             BIGINT NOT NULL REFERENCES customers(customer_id),
    status                  VARCHAR(20) NOT NULL,
    purchase_timestamp      TIMESTAMPTZ NOT NULL,
    approved_at             TIMESTAMPTZ,
    delivered_at            TIMESTAMPTZ,
    estimated_delivery      DATE
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id           BIGSERIAL PRIMARY KEY,
    order_id                BIGINT NOT NULL REFERENCES orders(order_id),
    seller_id               BIGINT NOT NULL REFERENCES sellers(seller_id),
    product_id              BIGINT NOT NULL REFERENCES products(product_id),
    price                   DECIMAL(10, 2) NOT NULL,
    freight_value           DECIMAL(10, 2) NOT NULL,
    shipping_limit_date     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS order_reviews (
    review_id       BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL REFERENCES orders(order_id),
    score           SMALLINT NOT NULL CHECK (score BETWEEN 1 AND 5),
    comment_title   VARCHAR(255),
    comment_message TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    answered_at     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS order_payments (
    payment_id              BIGSERIAL PRIMARY KEY,
    order_id                BIGINT NOT NULL REFERENCES orders(order_id),
    payment_sequential      SMALLINT NOT NULL,
    payment_type            VARCHAR(20) NOT NULL,
    payment_installments    SMALLINT NOT NULL DEFAULT 1,
    payment_value           DECIMAL(10, 2) NOT NULL
);
