CREATE TABLE inventory
(
    id           BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(256) NOT NULL UNIQUE,
    quantity     INT          NOT NULL CHECK (quantity >= 0)
);

INSERT INTO inventory (product_name, quantity)
VALUES ('Smartphone', 5),
       ('Tablet', 10),
       ('Desktop', 6);