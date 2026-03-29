import os
import random
import string
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5433)),
    "dbname": os.getenv("DB_NAME", "ecommerce"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

RANDOM_SEED = 42
rng = np.random.default_rng(RANDOM_SEED)
random.seed(RANDOM_SEED)

START_DATE = datetime(2016, 9, 1, tzinfo=timezone.utc)
END_DATE   = datetime(2018, 8, 31, tzinfo=timezone.utc)
DATE_RANGE_SECONDS = int((END_DATE - START_DATE).total_seconds())

BRAZILIAN_STATES = [
    "SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "ES", "PE",
    "CE", "PA", "MT", "MS", "RN", "PB", "AL", "PI", "SE", "AM",
]
STATE_WEIGHTS = [
    0.30, 0.12, 0.10, 0.08, 0.07, 0.05, 0.04, 0.03, 0.03, 0.03,
    0.02, 0.02, 0.02, 0.02, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01,
]

CITIES_BY_STATE = {
    "SP": ["São Paulo", "Campinas", "Santos", "Guarulhos", "Osasco"],
    "RJ": ["Rio de Janeiro", "Niterói", "São Gonçalo", "Duque de Caxias"],
    "MG": ["Belo Horizonte", "Uberlândia", "Contagem", "Juiz de Fora"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas", "Santa Maria"],
    "PR": ["Curitiba", "Londrina", "Maringá", "Foz do Iguaçu"],
    "SC": ["Florianópolis", "Joinville", "Blumenau", "Chapecó"],
    "BA": ["Salvador", "Feira de Santana", "Vitória da Conquista"],
    "GO": ["Goiânia", "Aparecida de Goiânia", "Anápolis"],
    "ES": ["Vitória", "Vila Velha", "Serra"],
    "PE": ["Recife", "Caruaru", "Olinda"],
    "CE": ["Fortaleza", "Caucaia", "Juazeiro do Norte"],
    "PA": ["Belém", "Ananindeua", "Santarém"],
    "MT": ["Cuiabá", "Várzea Grande", "Rondonópolis"],
    "MS": ["Campo Grande", "Dourados", "Três Lagoas"],
    "RN": ["Natal", "Mossoró", "Parnamirim"],
    "PB": ["João Pessoa", "Campina Grande", "Santa Rita"],
    "AL": ["Maceió", "Arapiraca", "Rio Largo"],
    "PI": ["Teresina", "Parnaíba", "Picos"],
    "SE": ["Aracaju", "Nossa Senhora do Socorro", "Lagarto"],
    "AM": ["Manaus", "Parintins", "Itacoatiara"],
}

PRODUCT_CATEGORIES = [
    "electronics", "fashion", "home_garden", "sports", "beauty",
    "health", "books", "toys", "furniture", "auto",
]
CATEGORY_WEIGHTS = [0.18, 0.17, 0.13, 0.10, 0.10, 0.08, 0.07, 0.07, 0.06, 0.04]

CATEGORY_PRICE_PARAMS = {
    "electronics":  (250, 300),
    "fashion":      (80,  70),
    "home_garden":  (110, 90),
    "sports":       (120, 100),
    "beauty":       (60,  50),
    "health":       (75,  60),
    "books":        (40,  30),
    "toys":         (90,  80),
    "furniture":    (350, 250),
    "auto":         (200, 180),
}

ORDER_STATUSES  = ["delivered", "shipped", "canceled", "invoiced", "processing", "unavailable"]
ORDER_STATUS_W  = [0.78, 0.08, 0.04, 0.05, 0.03, 0.02]

PAYMENT_TYPES   = ["credit_card", "boleto", "voucher", "debit_card"]
PAYMENT_TYPE_W  = [0.74, 0.19, 0.05, 0.02]

REVIEW_SCORES   = [1, 2, 3, 4, 5]
REVIEW_SCORE_W  = [0.11, 0.03, 0.08, 0.19, 0.59]


def random_zip():
    return f"{rng.integers(10000, 99999)}-{rng.integers(100, 999)}"


def random_datetime(after: datetime = None, within_days: int = None) -> datetime:
    if after and within_days:
        delta = int(rng.integers(1, within_days * 86400))
        return after + timedelta(seconds=delta)
    offset = int(rng.integers(0, DATE_RANGE_SECONDS))
    return START_DATE + timedelta(seconds=offset)


def pick(population, weights):
    return random.choices(population, weights=weights, k=1)[0]


def generate_customers(n: int = 10_000) -> pd.DataFrame:
    states = random.choices(BRAZILIAN_STATES, weights=STATE_WEIGHTS, k=n)
    rows = []
    for i, state in enumerate(states):
        city = random.choice(CITIES_BY_STATE[state])
        rows.append({
            "customer_id": i + 1,
            "city":        city,
            "state":       state,
            "zip_code":    random_zip(),
            "created_at":  random_datetime(),
        })
    return pd.DataFrame(rows)


def generate_sellers(n: int = 500) -> pd.DataFrame:
    states = random.choices(BRAZILIAN_STATES, weights=STATE_WEIGHTS, k=n)
    rows = []
    for i, state in enumerate(states):
        city = random.choice(CITIES_BY_STATE[state])
        rows.append({
            "seller_id": i + 1,
            "city":      city,
            "state":     state,
            "zip_code":  random_zip(),
        })
    return pd.DataFrame(rows)


def generate_products(n: int = 3_000) -> pd.DataFrame:
    categories = random.choices(PRODUCT_CATEGORIES, weights=CATEGORY_WEIGHTS, k=n)
    rows = []
    for i, cat in enumerate(categories):
        mean, std = CATEGORY_PRICE_PARAMS[cat]
        price = max(5.0, round(float(rng.normal(mean, std)), 2))
        rows.append({
            "product_id":          i + 1,
            "category":            cat,
            "name_length":         int(rng.integers(10, 60)),
            "description_length":  int(rng.integers(50, 1000)),
            "photos_qty":          int(rng.integers(1, 8)),
            "weight_g":            int(rng.integers(100, 30000)),
            "price":               price,
        })
    return pd.DataFrame(rows)


def generate_orders(n: int = 100_000, customer_ids: list = None) -> pd.DataFrame:
    statuses    = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_W, k=n)
    customer_sample = random.choices(customer_ids, k=n)
    rows = []
    for i in range(n):
        status    = statuses[i]
        purchased = random_datetime()
        approved  = None
        delivered = None
        estimated = None
        if status != "canceled":
            approved  = purchased + timedelta(hours=int(rng.integers(1, 48)))
            estimated = (purchased + timedelta(days=int(rng.integers(7, 30)))).date()
        if status == "delivered":
            delivered = approved + timedelta(days=int(rng.integers(3, 20)))
        rows.append({
            "order_id":             i + 1,
            "customer_id":          customer_sample[i],
            "status":               status,
            "purchase_timestamp":   purchased,
            "approved_at":          approved,
            "delivered_at":         delivered,
            "estimated_delivery":   estimated,
        })
    return pd.DataFrame(rows)


def generate_order_items(
    orders_df: pd.DataFrame,
    seller_ids: list,
    product_ids: list,
    products_df: pd.DataFrame,
) -> pd.DataFrame:
    order_ids  = orders_df["order_id"].tolist()
    purchase_ts = dict(zip(orders_df["order_id"], orders_df["purchase_timestamp"]))
    product_price = dict(zip(products_df["product_id"], products_df["price"]))

    rows = []
    for oid in order_ids:
        n_items = int(rng.choice([1, 1, 1, 2, 2, 3], p=[0.50, 0.20, 0.10, 0.10, 0.06, 0.04]))
        for _ in range(n_items):
            pid          = random.choice(product_ids)
            base_price   = product_price[pid]
            price        = round(base_price * float(rng.uniform(0.85, 1.15)), 2)
            freight      = round(float(rng.uniform(5, 50)), 2)
            purchased    = purchase_ts[oid]
            ship_limit   = purchased + timedelta(days=int(rng.integers(3, 10)))
            rows.append({
                "order_id":           oid,
                "seller_id":          random.choice(seller_ids),
                "product_id":         pid,
                "price":              price,
                "freight_value":      freight,
                "shipping_limit_date": ship_limit,
            })
    return pd.DataFrame(rows)


def generate_order_reviews(order_ids: list, fraction: float = 0.95) -> pd.DataFrame:
    sampled = random.sample(order_ids, int(len(order_ids) * fraction))
    scores  = random.choices(REVIEW_SCORES, weights=REVIEW_SCORE_W, k=len(sampled))
    rows = []
    for i, (oid, score) in enumerate(zip(sampled, scores)):
        created = random_datetime()
        answered = created + timedelta(hours=int(rng.integers(1, 72))) if rng.random() < 0.8 else None
        rows.append({
            "review_id":       i + 1,
            "order_id":        oid,
            "score":           score,
            "comment_title":   None,
            "comment_message": None,
            "created_at":      created,
            "answered_at":     answered,
        })
    return pd.DataFrame(rows)


def generate_order_payments(order_ids: list) -> pd.DataFrame:
    rows = []
    for oid in order_ids:
        n_payments = 1 if rng.random() < 0.85 else 2
        for seq in range(1, n_payments + 1):
            ptype        = pick(PAYMENT_TYPES, PAYMENT_TYPE_W)
            installments = int(rng.integers(1, 13)) if ptype == "credit_card" else 1
            value        = round(float(rng.uniform(20, 800)), 2)
            rows.append({
                "order_id":              oid,
                "payment_sequential":    seq,
                "payment_type":          ptype,
                "payment_installments":  installments,
                "payment_value":         value,
            })
    return pd.DataFrame(rows)


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_schema(conn):
    schema_path = os.path.join(os.path.dirname(__file__), "..", "sql", "schema.sql")
    with open(os.path.abspath(schema_path)) as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print("Schema created.")


def bulk_insert(conn, table: str, df: pd.DataFrame, columns: list):
    records = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]
    placeholders = "(" + ",".join(["%s"] * len(columns)) + ")"
    col_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_str}) VALUES %s ON CONFLICT DO NOTHING"
    with conn.cursor() as cur:
        execute_values(cur, sql, records, template=placeholders, page_size=5000)
    conn.commit()
    print(f"  Loaded {len(records):,} rows into {table}.")


def main():
    print("Generating mock dataset...")

    customers_df = generate_customers(10_000)
    sellers_df   = generate_sellers(500)
    products_df  = generate_products(3_000)

    customer_ids = customers_df["customer_id"].tolist()
    seller_ids   = sellers_df["seller_id"].tolist()
    product_ids  = products_df["product_id"].tolist()

    orders_df       = generate_orders(100_000, customer_ids)
    order_ids       = orders_df["order_id"].tolist()
    order_items_df  = generate_order_items(orders_df, seller_ids, product_ids, products_df)
    order_reviews_df = generate_order_reviews(order_ids)
    order_payments_df = generate_order_payments(order_ids)

    print(f"  customers:      {len(customers_df):>8,}")
    print(f"  sellers:        {len(sellers_df):>8,}")
    print(f"  products:       {len(products_df):>8,}")
    print(f"  orders:         {len(orders_df):>8,}")
    print(f"  order_items:    {len(order_items_df):>8,}")
    print(f"  order_reviews:  {len(order_reviews_df):>8,}")
    print(f"  order_payments: {len(order_payments_df):>8,}")

    print("\nConnecting to PostgreSQL...")
    conn = get_connection()

    print("Loading schema...")
    load_schema(conn)

    print("Inserting data...")
    bulk_insert(conn, "customers", customers_df,
                ["customer_id", "city", "state", "zip_code", "created_at"])
    bulk_insert(conn, "sellers", sellers_df,
                ["seller_id", "city", "state", "zip_code"])
    bulk_insert(conn, "products", products_df,
                ["product_id", "category", "name_length", "description_length",
                 "photos_qty", "weight_g", "price"])
    bulk_insert(conn, "orders", orders_df,
                ["order_id", "customer_id", "status", "purchase_timestamp",
                 "approved_at", "delivered_at", "estimated_delivery"])
    bulk_insert(conn, "order_items", order_items_df,
                ["order_id", "seller_id", "product_id", "price",
                 "freight_value", "shipping_limit_date"])
    bulk_insert(conn, "order_reviews", order_reviews_df,
                ["review_id", "order_id", "score", "comment_title",
                 "comment_message", "created_at", "answered_at"])
    bulk_insert(conn, "order_payments", order_payments_df,
                ["order_id", "payment_sequential", "payment_type",
                 "payment_installments", "payment_value"])

    print("\nCreating indexes...")
    index_path = os.path.join(os.path.dirname(__file__), "..", "sql", "index_strategy.sql")
    with open(os.path.abspath(index_path)) as f:
        index_sql = f.read()
    with conn.cursor() as cur:
        cur.execute(index_sql)
    conn.commit()
    print("  Indexes created.")

    print("\nRunning ANALYZE to update planner statistics...")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("ANALYZE;")
    print("  Done.")

    conn.close()
    print("\nData load complete.")


if __name__ == "__main__":
    main()
