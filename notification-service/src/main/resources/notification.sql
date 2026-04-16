CREATE TABLE processed_order_id
(
    order_id     VARCHAR(36) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT NOW()
);