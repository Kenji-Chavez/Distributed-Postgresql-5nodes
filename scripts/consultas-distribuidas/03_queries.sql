-- B. Consultas Distribuidas a Implementar
-- Consulta 1: Reporte de Ventas por Región (JOIN entre 3 nodos)
SELECT
  t.country AS pais,
  p.product_name AS producto,
  SUM(d2.quantity) AS cantidad_total,
  SUM(d2.quantity * d1.unit_price) AS monto_total
FROM tmp_clientes_ordenes t
JOIN order_details_parte1_ft d1 ON d1.order_id = t.order_id
JOIN order_details_parte2_ft d2 ON d2.order_id = d1.order_id AND d2.product_id = d1.product_id
JOIN products_ft p ON p.product_id = d1.product_id
GROUP BY t.country, p.product_name
ORDER BY monto_total DESC;

-- Consulta 2: Top 10 Clientes con Mayor Facturación (Agregación distribuida)
-- Crear tabla temporal con TODOS los clientes (América + Europa)
CREATE TEMP TABLE tmp_clientes AS
SELECT customer_id, company_name, country
FROM customers_america_ft
UNION ALL
SELECT customer_id, company_name, country
FROM customers_europe_ft;

-- Verificamos
SELECT COUNT(*) FROM tmp_clientes;

-- Crear tabla temporal con TODOS los pedidos (América + Europa)
CREATE TEMP TABLE tmp_pedidos AS
SELECT order_id, customer_id
FROM orders_america_ft
UNION ALL
SELECT order_id, customer_id
FROM orders_europe_ft;

-- Verificamos
SELECT COUNT(*) FROM tmp_pedidos;

-- Unir los detalles (fragmentación vertical)
CREATE TEMP TABLE tmp_detalles AS
SELECT
    d1.order_id,
    d1.product_id,
    d1.unit_price,
    d2.quantity,
    d2.discount
FROM order_details_parte1_ft d1
JOIN order_details_parte2_ft d2
    ON d1.order_id = d2.order_id
    AND d1.product_id = d2.product_id;

-- Verificamos
SELECT * FROM tmp_detalles LIMIT 10;

-- Calcular facturación por pedido
CREATE TEMP TABLE tmp_facturacion_pedido AS
SELECT
    order_id,
    SUM(unit_price * quantity * (1 - discount)) AS monto_pedido
FROM tmp_detalles
GROUP BY order_id;

-- Verificamos
SELECT * FROM tmp_facturacion_pedido LIMIT 10;

-- Unir clientes + pedidos + facturación
SELECT
    c.customer_id AS id_cliente,
    c.company_name AS nombre_cliente,
    c.country AS pais,
    COUNT(p.order_id) AS total_pedidos,
    SUM(f.monto_pedido) AS monto_total_gastado,
    AVG(f.monto_pedido) AS promedio_por_pedido
FROM tmp_clientes c
JOIN tmp_pedidos p
    ON p.customer_id = c.customer_id
JOIN tmp_facturacion_pedido f
    ON f.order_id = p.order_id
GROUP BY c.customer_id, c.company_name, c.country
ORDER BY monto_total_gastado DESC
LIMIT 10;

-- Consulta 5: Análisis Temporal de Productos Discontinuados (Consulta histórica)
-- Crear las Foreign Tables apuntando a las tablas de Nodo 5
DROP FOREIGN TABLE IF EXISTS detalle_pedidos_ft;
CREATE FOREIGN TABLE IF NOT EXISTS detalle_pedidos_ft (
    order_id smallint,
    quantity smallint,
    discount real,
	product_id integer
)
SERVER nodo5_server
OPTIONS (schema_name 'public', table_name 'order_details_parte2');

DROP FOREIGN TABLE IF EXISTS productos_ft;
CREATE FOREIGN TABLE IF NOT EXISTS productos_ft (
    product_id smallint,
    product_name varchar,
    category_id smallint,
    discontinued integer
)
SERVER nodo5_server
OPTIONS (schema_name 'public', table_name 'products');

CREATE FOREIGN TABLE IF NOT EXISTS orders_ft (
    order_id smallint,
    customer_id varchar,
    order_date date
)
SERVER nodo5_server
OPTIONS (schema_name 'public', table_name 'orders');


DROP FOREIGN TABLE IF EXISTS customers_ft;
CREATE FOREIGN TABLE IF NOT EXISTS customers_ft (
    customer_id varchar,
    company_name varchar,
    country varchar
)
SERVER nodo5_server
OPTIONS (schema_name 'public', table_name 'customers');


-- Consulta final: análisis de productos discontinuados
SELECT 
    p.product_name AS "Nombre Producto",
    p.category_id AS "Categoría",
    MAX(o.order_date) AS "Última Venta",
    SUM(od.quantity) AS "Total Vendido",
    c.company_name AS "Cliente Última Compra",
    CURRENT_DATE - MAX(o.order_date) AS "Días Desde Última Venta"
FROM productos_ft p
JOIN detalle_pedidos_ft od ON od.product_id = p.product_id
JOIN orders_ft o ON o.order_id = od.order_id
JOIN customers_ft c ON c.customer_id = o.customer_id
WHERE p.discontinued = 1
GROUP BY p.product_name, p.category_id, c.company_name
ORDER BY "Última Venta" DESC;