CREATE TABLE IF NOT EXISTS inventory
(
    id           BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(256) NOT NULL UNIQUE,
    quantity     INT          NOT NULL CHECK (quantity >= 0)
);

INSERT INTO inventory (product_name, quantity)
VALUES ('Smartphone', 5),
       ('Tablet', 10),
       ('Desktop', 6);

CREATE TABLE IF NOT EXISTS processed_order_id
(
    order_id varchar(36) primary key,
    processed_at timestamp default now()
);

CREATE TABLE IF NOT EXISTS outbox_event
(
    key varchar(36) primary key,
    topic varchar(100) not null,
    payload TEXT not null,
    created_at timestamp default now(),
    sent BOOLEAN DEFAULT false
);

CREATE INDEX idx_outbox_event_sent ON outbox_event(sent, created_at);