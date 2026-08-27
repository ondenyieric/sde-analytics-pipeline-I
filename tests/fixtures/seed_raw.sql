-- Seeds raw.products / raw.orders directly, bypassing the Kafka/Debezium
-- path (not present in the CI compose stack). Lets `dbt build`/`dbt test`
-- run against realistic data without needing the full CDC pipeline up.

INSERT INTO raw.products (id, title, category, price, stock, _cdc_op, _cdc_ts) VALUES
    (1, 'Wireless Mouse', 'electronics', 19.99, 120, 'c', now64(3)),
    (2, 'Mechanical Keyboard', 'electronics', 79.99, 45, 'c', now64(3)),
    (3, 'Standing Desk', 'furniture', 349.00, 12, 'c', now64(3)),
    (4, 'Desk Lamp', 'furniture', 24.50, 80, 'c', now64(3));

INSERT INTO raw.orders (id, user_id, product_id, quantity, order_ts, _cdc_op, _cdc_ts) VALUES
    ('1:1', 1, 1, 2, now64(3), 'c', now64(3)),
    ('1:2', 1, 2, 1, now64(3), 'c', now64(3)),
    ('2:3', 2, 3, 1, now64(3), 'c', now64(3)),
    ('2:4', 3, 4, 3, now64(3), 'c', now64(3)),
    ('2:1', 2, 1, 1, now64(3), 'c', now64(3));
