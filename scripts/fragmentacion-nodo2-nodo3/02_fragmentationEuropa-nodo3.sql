-- Fragmentación Horizontal de Customers
-- Nodo 3: Clientes de Europa
CREATE TABLE customers_europe AS
SELECT * FROM customers
WHERE country IN ('UK', 'Germany', 'France', 'Spain', 'Italy', 'Sweden');

SELECT COUNT(*) FROM customers_europe;

-- Crear fragmentación horizontal DERIVADA de Orders
CREATE TABLE orders_europe AS
SELECT o.*
FROM orders o
JOIN customers_europe c ON o.customer_id = c.customer_id;

SELECT * FROM orders_europe
