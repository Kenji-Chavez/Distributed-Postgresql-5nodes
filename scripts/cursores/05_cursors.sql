-- Customers / Orders en Nodo 2 (América)
DROP FOREIGN TABLE IF EXISTS orders_america_ft;
CREATE FOREIGN TABLE orders_america_ft (
  order_id integer DEFAULT NULL, -- Usar DEFAULT NULL para hinting
  customer_id varchar(10),
  order_date date
) SERVER nodo2_server
OPTIONS (schema_name 'public', table_name 'orders'); -- La tabla remota se llama 'orders'

-- Customers / Orders en Nodo 3 (Europa)
DROP FOREIGN TABLE IF EXISTS orders_europe_ft;
CREATE FOREIGN TABLE orders_europe_ft (
  order_id integer DEFAULT NULL,
  customer_id varchar(10),
  order_date date
) SERVER nodo3_server
OPTIONS (schema_name 'public', table_name 'orders');
---------------------------------------------------------

-- Customers / Orders en Nodo 2 (América)
DROP FOREIGN TABLE IF EXISTS orders_america_ft;
CREATE FOREIGN TABLE orders_america_ft (
    -- **DEBEMOS INCLUIR order_id** para las referencias de clave foránea y otras consultas.
    order_id integer,
    customer_id varchar(10),
    order_date date
) SERVER nodo2_server
OPTIONS (schema_name 'public', table_name 'orders'); 

-- Customers / Orders en Nodo 3 (Europa)
DROP FOREIGN TABLE IF EXISTS orders_europe_ft;
CREATE FOREIGN TABLE orders_europe_ft (
    -- **DEBEMOS INCLUIR order_id**
    order_id integer,
    customer_id varchar(10),
    order_date date
) SERVER nodo3_server
OPTIONS (schema_name 'public', table_name 'orders');
----------------------------------------------------------
-- Ejecutar en el NODO COORDINADOR
DROP FOREIGN TABLE IF EXISTS products_ft;

CREATE FOREIGN TABLE products_ft (
    product_id integer,
    product_name varchar,
    units_in_stock integer,  -- ¡Añadida!
    unit_price double precision -- ¡Añadida!
) SERVER nodo4_server
OPTIONS (schema_name 'public', table_name 'products');

---------------------------------------------------------
-- Ejecutar en el NODO COORDINADOR
DROP FOREIGN TABLE IF EXISTS orders_america_ft;
CREATE FOREIGN TABLE orders_america_ft (
    order_id integer,
    customer_id varchar(10),
    order_date date
) SERVER nodo2_server
OPTIONS (schema_name 'public', table_name 'orders'); 

DROP FOREIGN TABLE IF EXISTS orders_europe_ft;
CREATE FOREIGN TABLE orders_europe_ft (
    order_id integer,
    customer_id varchar(10),
    order_date date
) SERVER nodo3_server
OPTIONS (schema_name 'public', table_name 'orders');


---------------------------------------------------------
-- Ejecutar en el NODO COORDINADOR
CREATE OR REPLACE PROCEDURE sp_process_distributed_order(
    p_customer_id VARCHAR(10), 
    p_product_ids INT[],
    p_quantities INT[],
    OUT p_order_id INT,
    OUT p_total_amount DECIMAL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_region VARCHAR(20);
    v_dblink_conn_name TEXT;    -- Conexión dblink (e.g., nodo2_server)
    v_stock INT;
    v_unit_price DOUBLE PRECISION;
    v_total DECIMAL := 0;
    i INT;
    -- v_order_ft_name ya no es necesario para el insert, pero lo mantenemos para la lógica de región
BEGIN
    -- 1. IDENTIFICAR REGIÓN DEL CLIENTE
    SELECT country INTO v_region FROM customers_america_ft WHERE customer_id = p_customer_id;
    IF FOUND THEN
        v_dblink_conn_name := 'nodo2_server';
    ELSE
        SELECT country INTO v_region FROM customers_europe_ft WHERE customer_id = p_customer_id;
        IF FOUND THEN
            v_dblink_conn_name := 'nodo3_server';
        ELSE
            RAISE EXCEPTION 'Cliente % no encontrado...', p_customer_id;
        END IF;
    END IF;

    -- 2. CREAR PEDIDO (INSERCIÓN GARANTIZADA VÍA DBLINK)
    SELECT order_id INTO p_order_id
    FROM dblink(
        v_dblink_conn_name, 
        -- Llamada a la función wrapper creada en los nodos remotos
        format('SELECT fn_insert_order_remote(%L)', p_customer_id)
    ) AS t(order_id INT); -- Capturamos el resultado de la función remota
    
    -- 3. PROCESAR PRODUCTOS Y ACTUALIZAR INVENTARIO
    FOR i IN array_lower(p_product_ids,1)..array_upper(p_product_ids,1) LOOP
        
        -- 3a. Verificar Stock y obtener precio (products_ft en Nodo 4)
        SELECT units_in_stock, unit_price INTO v_stock, v_unit_price
        FROM products_ft
        WHERE product_id = p_product_ids[i];
        
        IF NOT FOUND THEN RAISE EXCEPTION 'Producto % no encontrado...', p_product_ids[i]; END IF;
        IF v_stock < p_quantities[i] THEN RAISE EXCEPTION 'Producto % sin stock suficiente...', p_product_ids[i]; END IF;

        -- 3b. Insertar detalles del pedido (Fragmentado en Nodo 4 y Nodo 5, usando FDW)
        INSERT INTO order_details_parte1_ft(order_id, product_id, unit_price)
        VALUES (p_order_id, p_product_ids[i], v_unit_price);
        
        INSERT INTO order_details_parte2_ft(order_id, product_id, quantity, discount)
        VALUES (p_order_id, p_product_ids[i], p_quantities[i], 0.0);

        -- 3c. Actualizar inventario (UPDATE en products_ft en Nodo 4)
        UPDATE products_ft
        SET units_in_stock = units_in_stock - p_quantities[i]
        WHERE product_id = p_product_ids[i];

        v_total := v_total + (v_unit_price * p_quantities[i]);
    END LOOP;

    p_total_amount := v_total;
    
    RAISE NOTICE 'Transacción distribuida % completada. ID de Pedido: %', p_customer_id, p_order_id;

END;
$$;
------------------------------------------------------------
DO $$
DECLARE
    -- Ajusta estos valores para probar tu transacción
    v_customer_id CONSTANT VARCHAR(10) := 'ALFKI'; 
    v_product_list INT[] := ARRAY[1, 2, 3];        
    v_quantity_list INT[] := ARRAY[5, 10, 2];      
    
    v_order_id INT;
    v_total_amount DECIMAL;
BEGIN
    CALL sp_process_distributed_order(
        v_customer_id,
        v_product_list,     
        v_quantity_list,     
        v_order_id,          
        v_total_amount       
    );
    
    RAISE NOTICE '--- TRANSACCIÓN MULTI-NODO EXITOSA ---';
    RAISE NOTICE 'Nuevo ID de Pedido: %', v_order_id;
    RAISE NOTICE 'Monto Total del Pedido: %', v_total_amount;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '¡ERROR EN LA TRANSACCIÓN DISTRIBUIDA!: %', SQLERRM;
        RAISE; 
END;
$$;



--------------------------------------------------------------------------------
drop function fn_customer_sales_summary
-- Cursor 1: Recorrido de Clientes Multi-Región
-- La función fn_customer_sales_summary() es una función de reporte distribuido que unifica la información de clientes 
-- almacenados en distintos servidores. Su propósito es consolidar los datos y devolver un resumen completo del número de pedidos y 
-- el gasto total de cada cliente.
CREATE OR REPLACE FUNCTION fn_customer_sales_summary()
RETURNS TABLE (
    -- Renombramos las variables de retorno para evitar ambigüedad:
    ret_customer_id VARCHAR(5),
    ret_company_name VARCHAR(40),
    ret_region VARCHAR(20),
    ret_total_orders INT,
    ret_total_spent DECIMAL
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Variables para almacenar el resultado del FETCH 
    v_customer_id_fetch VARCHAR(5);
    v_company_name_fetch VARCHAR(40);
    v_region_fetch VARCHAR(20);

    -- Cursor 1: Recorre clientes de América
    cur_america CURSOR FOR
        SELECT customer_id, company_name, country
        FROM customers_america_ft;

    -- Cursor 2: Recorre clientes de Europa
    cur_europe CURSOR FOR
        SELECT customer_id, company_name, country
        FROM customers_europe_ft;

    v_total_orders INT;
    v_total_spent DECIMAL;
    
BEGIN
    -- 1. Procesar clientes de América
    OPEN cur_america; 
    LOOP
        -- FETCH a variables explícitas no ambiguas
        FETCH cur_america INTO v_customer_id_fetch, v_company_name_fetch, v_region_fetch;
        EXIT WHEN NOT FOUND;

        -- Calcular pedidos y gasto total
        SELECT COUNT(*),
               COALESCE(SUM(od.unit_price * od.quantity * (1 - od.discount)), 0)
        INTO v_total_orders, v_total_spent
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        WHERE o.customer_id = v_customer_id_fetch; 

        -- ASIGNAR A LAS NUEVAS VARIABLES DE RETORNO IMPLÍCITAS:
        ret_customer_id := v_customer_id_fetch;   
        ret_company_name := v_company_name_fetch;
        ret_region := v_region_fetch;
        ret_total_orders := v_total_orders;
        ret_total_spent := v_total_spent;
        
        RETURN NEXT; 
    END LOOP;
    CLOSE cur_america;


    -- 2. Procesar clientes de Europa
    OPEN cur_europe;
    LOOP
        FETCH cur_europe INTO v_customer_id_fetch, v_company_name_fetch, v_region_fetch;
        EXIT WHEN NOT FOUND;

        SELECT COUNT(*),
               COALESCE(SUM(od.unit_price * od.quantity * (1 - od.discount)), 0)
        INTO v_total_orders, v_total_spent
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        WHERE o.customer_id = v_customer_id_fetch;

        -- Variables de retorno implicitas
        ret_customer_id := v_customer_id_fetch;
        ret_company_name := v_company_name_fetch;
        ret_region := v_region_fetch;
        ret_total_orders := v_total_orders;
        ret_total_spent := v_total_spent;
        
        RETURN NEXT;
    END LOOP;
    CLOSE cur_europe;

    RETURN; 
END;
$$;

SELECT * FROM fn_customer_sales_summary();



-----------------------------------------------------------------------------
DROP FOREIGN TABLE IF EXISTS categories_ft;

DROP FOREIGN TABLE IF EXISTS products_ft;

CREATE FOREIGN TABLE products_ft (
    product_id integer,
    product_name varchar(40),
    category_id integer,         
    units_in_stock integer,      
    unit_price double precision  
) 
SERVER nodo4_server
OPTIONS (schema_name 'public', table_name 'products');
-----------------------------------------------------------------------------
-- Cursor 2: Análisis de Productos con Cursor Anidado
-- La función fn_analisis_productos_categoria() es una herramienta de BI distribuida que analiza ventas por producto recorriendo categorías 
-- y productos mediante cursores anidados. 
-- Su objetivo es consolidar la actividad de ventas obteniendo datos desde múltiples nodos remotos mediante FDW.
CREATE OR REPLACE FUNCTION fn_analisis_productos_categoria()
RETURNS TABLE (
    res_nombre_categoria VARCHAR(15), 
    res_nombre_producto VARCHAR(40),  
    res_unidades_vendidas BIGINT      
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Cursor Externo: Recorre todas las categorías 
    cursor_categorias CURSOR FOR
        SELECT category_id, category_name
        FROM categories_ft
        ORDER BY category_name;

    -- Cursor Interno: Recorre productos de una categoría específica (Nodo 4)
    cursor_productos CURSOR (p_id_categoria INT) FOR
        SELECT product_id, product_name
        FROM products_ft
        WHERE category_id = p_id_categoria
        ORDER BY product_name;

    -- Variables para almacenar el resultado del FETCH 
    var_id_categoria INT;
    var_nombre_categoria VARCHAR(15);
    var_id_producto INT;
    var_nombre_producto VARCHAR(40);
    var_total_unidades_vendidas BIGINT;
    
BEGIN
    
    
    -- 1. Bucle Externo: Recorrer Categorías
    OPEN cursor_categorias;
    LOOP
        FETCH cursor_categorias INTO var_id_categoria, var_nombre_categoria;
        EXIT WHEN NOT FOUND;

        
        -- 2. Bucle Interno: Recorrer Productos por Categoría
        -- Se abre el cursor interno, pasando el ID de la categoría actual
        OPEN cursor_productos(var_id_categoria);
        LOOP
            FETCH cursor_productos INTO var_id_producto, var_nombre_producto;
            EXIT WHEN NOT FOUND;
            
            -- 3. CÁLCULO DISTRIBUIDO: Contar las unidades vendidas para este producto
            SELECT COALESCE(SUM(od.quantity), 0)
            INTO var_total_unidades_vendidas
            FROM order_details od
            WHERE od.product_id = var_id_producto;

            -- 4. ASIGNAR VALORES AL RETORNO IMPLÍCITO 
            res_nombre_categoria := var_nombre_categoria;
            res_nombre_producto := var_nombre_producto;
            res_unidades_vendidas := var_total_unidades_vendidas;
            
            RETURN NEXT;
            
        END LOOP;
        CLOSE cursor_productos;
        
    END LOOP;
    CLOSE cursor_categorias;

    RETURN; 
END;
$$;

SELECT * FROM fn_analisis_productos_categoria();