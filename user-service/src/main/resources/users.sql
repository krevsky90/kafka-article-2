CREATE TABLE IF NOT EXISTS users
(
    email VARCHAR(128) PRIMARY KEY,
    name  VARCHAR(256) NOT NULL,
    age   INT CHECK (age >= 0),
    address VARCHAR(256)
);

INSERT INTO users (email, name, age, address)
VALUES ('krevskiigrigorii@gmail.com', 'Grigorii', 36, 'Moscow'),
       ('ivan@gmail.com', 'Ivan', 26, 'Ivanovo'),
       ('petr@gmail.com', 'Petr', 16, 'Petrovo');
