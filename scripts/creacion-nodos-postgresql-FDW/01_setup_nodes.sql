CREATE EXTENSION postgres_fdw;

CREATE SERVER nodo2_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', port '5433', dbname 'northwind');

CREATE SERVER nodo3_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', port '5434', dbname 'northwind');

CREATE SERVER nodo4_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', port '5435', dbname 'northwind');

CREATE SERVER nodo5_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', port '5436', dbname 'northwind');
  
-- Crear los USUARIOS MAPEADOS:
CREATE USER MAPPING FOR postgres
SERVER nodo2_server
OPTIONS (user 'postgres', password '123');

CREATE USER MAPPING FOR postgres
SERVER nodo3_server
OPTIONS (user 'postgres', password '123');

CREATE USER MAPPING FOR postgres
SERVER nodo4_server
OPTIONS (user 'postgres', password '123');

CREATE USER MAPPING FOR postgres
SERVER nodo5_server
OPTIONS (user 'postgres', password '123');

-----------------------------------------------------------------
-- Crear foreign tables
-- Customers / Orders en Nodo 2 (América)
DROP FOREIGN TABLE IF EXISTS customers_america_ft;
CREATE FOREIGN TABLE customers_america_ft (
  customer_id varchar(10),
  company_name varchar,
  contact_name varchar,
  country varchar
) SERVER nodo2_server
OPTIONS (schema_name 'public', table_name 'customers_america');

DROP FOREIGN TABLE IF EXISTS orders_america_ft;
CREATE FOREIGN TABLE orders_america_ft (
  order_id integer,
  customer_id varchar(10),
  order_date date
) SERVER nodo2_server
OPTIONS (schema_name 'public', table_name 'orders');

-- Customers / Orders en Nodo 3 (Europa)
DROP FOREIGN TABLE IF EXISTS customers_europe_ft;
CREATE FOREIGN TABLE customers_europe_ft (
  customer_id varchar(10),
  company_name varchar,
  contact_name varchar,
  country varchar
) SERVER nodo3_server
OPTIONS (schema_name 'public', table_name 'customers_europe');

DROP FOREIGN TABLE IF EXISTS orders_europe_ft;
CREATE FOREIGN TABLE orders_europe_ft (
  order_id integer,
  customer_id varchar(10),
  order_date date
) SERVER nodo3_server
OPTIONS (schema_name 'public', table_name 'orders');

-- Detalles en Nodo 4 y Nodo 5 
DROP FOREIGN TABLE IF EXISTS order_details_parte1_ft;
CREATE FOREIGN TABLE order_details_parte1_ft (
  order_id integer,
  product_id integer,
  unit_price double precision
) SERVER nodo4_server
OPTIONS (schema_name 'public', table_name 'order_details_parte1');

DROP FOREIGN TABLE IF EXISTS order_details_parte2_ft;
CREATE FOREIGN TABLE order_details_parte2_ft (
  order_id integer,
  product_id integer,
  quantity smallint,
  discount double precision
) SERVER nodo5_server
OPTIONS (schema_name 'public', table_name 'order_details_parte2');

-- Productos (si products está en nodo4)
DROP FOREIGN TABLE IF EXISTS products_ft;
CREATE FOREIGN TABLE products_ft (
  product_id integer,
  product_name varchar
) SERVER nodo4_server
OPTIONS (schema_name 'public', table_name 'products');

-----------------------------------------------------------
-- Crear tabla local temporal para juntar clientes+órdenes
CREATE TEMP TABLE tmp_clientes_ordenes (
  order_id integer,
  customer_id varchar(10),
  country varchar
);

-- Insertar los resultados desde los foreign tables (Nodo 2 y 3)
INSERT INTO tmp_clientes_ordenes (order_id, customer_id, country)
SELECT o.order_id, c.customer_id, c.country
FROM customers_america_ft c
JOIN orders_america_ft o ON o.customer_id = c.customer_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1997;

INSERT INTO tmp_clientes_ordenes (order_id, customer_id, country)
SELECT o.order_id, c.customer_id, c.country
FROM customers_europe_ft c
JOIN orders_europe_ft o ON o.customer_id = c.customer_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1997;

-- Verifica contenido temporal
SELECT COUNT(*) FROM tmp_clientes_ordenes;
SELECT * FROM tmp_clientes_ordenes LIMIT 10;





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


-- Consulta 3: Análisis de Inventario Crítico (Subconsulta distribuida)
-- Cargar productos del Nodo 4
CREATE TEMP TABLE tmp_productos AS
SELECT 
    product_id,
    product_name,
    supplier_id,
    category_id,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    discontinued
FROM products_ft;

select * from products_ft;

-- Reconstruir detalle de pedidos (nodo 4 + nodo 5)
CREATE TEMP TABLE tmp_detalles AS
SELECT
    d1.order_id,
    d1.product_id,
    d2.quantity,
    d2.discount
FROM order_details_parte1_ft d1
JOIN order_details_parte2_ft d2
    ON d1.order_id = d2.order_id
    AND d1.product_id = d2.product_id;

-- Traer pedidos de América y Europa (nodo 2 y 3)
CREATE TEMP TABLE tmp_pedidos AS
SELECT order_id, customer_id, order_date
FROM orders_america_ft
UNION ALL
SELECT order_id, customer_id, order_date
FROM orders_europe_ft;

-- Traer clientes (para países donde se vendió)
CREATE TEMP TABLE tmp_clientes AS
SELECT customer_id, country
FROM customers_america_ft
UNION ALL
SELECT customer_id, country
FROM customers_europe_ft;

-- Unidades vendidas por producto en 1997
CREATE TEMP TABLE tmp_ventas_1997 AS
SELECT
    d.product_id,
    SUM(d.quantity) AS unidades_vendidas
FROM tmp_detalles d
JOIN tmp_pedidos p ON p.order_id = d.order_id
WHERE EXTRACT(YEAR FROM p.order_date) = 1997
GROUP BY d.product_id;

-- Países donde se vendió cada producto
CREATE TEMP TABLE tmp_paises AS
SELECT
    d.product_id,
    STRING_AGG(DISTINCT c.country, ', ') AS paises
FROM tmp_detalles d
JOIN tmp_pedidos p ON p.order_id = d.order_id
JOIN tmp_clientes c ON c.customer_id = p.customer_id
WHERE EXTRACT(YEAR FROM p.order_date) = 1997
GROUP BY d.product_id;

-- Consulta final: Inventario crítico
SELECT
    pr.product_name AS nombre_producto,
    cat.category_name AS categoria,
    sup.company_name AS proveedor,
    pr.units_in_stock AS stock_actual,
    v.unidades_vendidas,
    pa.paises
FROM tmp_productos pr
JOIN categories_ft cat ON cat.category_id = pr.category_id
JOIN suppliers_ft sup ON sup.supplier_id = pr.supplier_id
JOIN tmp_ventas_1997 v ON v.product_id = pr.product_id
JOIN tmp_paises pa ON pa.product_id = pr.product_id
WHERE pr.units_in_stock < pr.reorder_level
  AND v.unidades_vendidas > 50
ORDER BY v.unidades_vendidas DESC;

-- Otro intento
CREATE TEMP TABLE ventas_america AS
SELECT 
    o.order_id,
    od2.quantity,
    od1.product_id,
    c.country
FROM customers_america c
JOIN orders o ON o.customer_id = c.customer_id
JOIN detalle_pedidos_parte1 od1 ON od1.order_id = o.order_id
JOIN detalle_pedidos_parte2 od2 ON od2.order_id = o.order_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1997;

----------------------------------------------------
-- Consulta 4: Comparativa de Rendimiento Regional (CTE y Window Functions)
-- Ventas por empleado por región



----------------------------------------------------
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




----------------------------------------------------------------------
-- C. Procedimientos Almacenados (20 puntos)
-- Transferir un pedido a otro cliente dentro del nodo 1
-- Crear tabla de auditoría 
CREATE TABLE IF NOT EXISTS auditoria_transferencias (
    id SERIAL PRIMARY KEY,
    order_id INT,
    customer_origen VARCHAR(5),
    customer_destino VARCHAR(5),
    fecha TIMESTAMP DEFAULT NOW()
);

drop table  auditoria_transferencias

-- Procedimiento
CREATE OR REPLACE PROCEDURE sp_transferir_pedido_local(
    p_order_id INT,
    p_nuevo_cliente_id VARCHAR(5)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_antiguo_cliente VARCHAR(5);
BEGIN
    -- 1. Verificar que el pedido exista y guardar cliente original
    SELECT customer_id INTO v_antiguo_cliente
    FROM orders
    WHERE order_id = p_order_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'El pedido % no existe', p_order_id;
    END IF;

    -- 2. Actualizar el cliente en orders
    UPDATE orders
    SET customer_id = p_nuevo_cliente_id
    WHERE order_id = p_order_id;

    -- 3. Registrar la transferencia en auditoría
    INSERT INTO auditoria_transferencias (order_id, customer_origen, customer_destino)
    VALUES (p_order_id, v_antiguo_cliente, p_nuevo_cliente_id);

    RAISE NOTICE 'Pedido % transferido de % a %', p_order_id, v_antiguo_cliente, p_nuevo_cliente_id;
END;
$$;

CALL sp_transferir_pedido_local(10249, 'ALFKI');

select * from auditoria_transferencias

select * from orders


