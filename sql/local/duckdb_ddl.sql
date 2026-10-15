-- Local DuckDB mirror of the Snowflake star schema (demo sink).

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk BIGINT PRIMARY KEY,
    product_id VARCHAR UNIQUE,
    sku VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    brand VARCHAR,
    unit_price DOUBLE,
    is_active BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk BIGINT PRIMARY KEY,
    customer_id VARCHAR UNIQUE,
    email VARCHAR,
    country VARCHAR,
    segment VARCHAR,
    is_active BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_clicks (
    click_sk BIGINT,
    event_id VARCHAR,
    event_type VARCHAR,
    event_ts TIMESTAMP,
    session_id VARCHAR,
    customer_sk BIGINT,
    customer_id VARCHAR,
    product_sk BIGINT,
    product_id VARCHAR,
    page_url VARCHAR,
    search_query VARCHAR,
    quantity INTEGER,
    device VARCHAR,
    country VARCHAR,
    referrer VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_sk BIGINT,
    order_id VARCHAR,
    event_id VARCHAR,
    order_ts TIMESTAMP,
    customer_sk BIGINT,
    customer_id VARCHAR,
    product_sk BIGINT,
    product_id VARCHAR,
    quantity INTEGER,
    unit_price DOUBLE,
    line_total DOUBLE,
    currency VARCHAR,
    status VARCHAR,
    channel VARCHAR
);
