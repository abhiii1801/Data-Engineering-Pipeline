CREATE TABLE dbo.fact_sales (
    order_id        INT NOT NULL,
    product_id      INT,
    customer_id     INT,
    quantity        INT,
    price           DECIMAL(10,2),
    total_amount    DECIMAL(12,2),
    price_bucket    VARCHAR(20),
    order_date      DATE,
    order_year      INT,
    order_month     INT,
    processing_date DATE
);