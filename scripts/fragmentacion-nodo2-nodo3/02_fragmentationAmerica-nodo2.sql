-- Fragmentación Horizontal de Customers
-- Nodo 2: Clientes de América
CREATE TABLE customers_america AS
SELECT * FROM customers
WHERE country IN ('USA', 'Canada', 'Mexico', 'Brazil', 'Argentina');

SELECT COUNT(*) FROM customers_america;

-- Crear fragmentación horizontal DERIVADA de Orders
CREATE TABLE orders_america AS
SELECT o.*
FROM orders o
JOIN customers_america c ON o.customer_id = c.customer_id;

SELECT * FROM orders_america

