-- C. Procedimientos Almacenados (20 puntos)
-- Procedimiento Almacenado 1
-- Transferencia de Pedido entre Regiones (PostgreSQL FDW)
-- Primero creamos la tabla de auditoría:
CREATE TABLE auditoria_transferencias (
    id SERIAL PRIMARY KEY,               
    order_id INT NOT NULL,               
    customer_id_origen VARCHAR(5) NOT NULL, 
    customer_id_destino VARCHAR(5) NOT NULL, 
    fecha TIMESTAMP NOT NULL DEFAULT NOW(),  
    region_origen VARCHAR(20),           
    region_destino VARCHAR(20)           
);

drop table auditoria_transferencias

-- Procedimiento
CREATE OR REPLACE PROCEDURE sp_transfer_order_region(
    p_order_id INT,
    p_new_customer_id VARCHAR(5)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_old_customer_id VARCHAR(5);
    v_region_origen TEXT;
    v_region_destino TEXT;
BEGIN
    -- 1. Verificar existencia del pedido
    SELECT customer_id INTO v_old_customer_id
    FROM orders_ft
    WHERE order_id = p_order_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Pedido % no existe', p_order_id;
    END IF;

    -- 2. Identificar región origen y destino
    IF EXISTS (SELECT 1 FROM customers_america_ft WHERE customer_id = v_old_customer_id) THEN
        v_region_origen := 'America';
    ELSIF EXISTS (SELECT 1 FROM customers_europe_ft WHERE customer_id = v_old_customer_id) THEN
        v_region_origen := 'Europa';
    ELSE
        RAISE EXCEPTION 'Cliente origen % no encontrado en ningún nodo', v_old_customer_id;
    END IF;

    IF EXISTS (SELECT 1 FROM customers_america_ft WHERE customer_id = p_new_customer_id) THEN
        v_region_destino := 'America';
    ELSIF EXISTS (SELECT 1 FROM customers_europe_ft WHERE customer_id = p_new_customer_id) THEN
        v_region_destino := 'Europa';
    ELSE
        RAISE EXCEPTION 'Cliente destino % no encontrado en ningún nodo', p_new_customer_id;
    END IF;

    -- 3. Copiar datos al nodo destino (orders)
    -- Solo actualizamos customer_id para transferir pedido
    UPDATE orders_ft
    SET customer_id = p_new_customer_id
    WHERE order_id = p_order_id;

    -- 4. Registrar en auditoría
    INSERT INTO auditoria_transferencias(order_id, customer_id_origen, customer_id_destino, fecha)
    VALUES (p_order_id, v_old_customer_id, p_new_customer_id, NOW());

    RAISE NOTICE 'Pedido % transferido de % a %', p_order_id, v_old_customer_id, p_new_customer_id;
END;
$$;


CALL sp_transfer_order_region(10253, 'ALFKI');

-- Revisar la actualizacion en orders
SELECT * FROM orders_ft WHERE order_id = 10253;

-- Revisar auditoria
SELECT * FROM auditoria_transferencias ORDER BY fecha DESC;



-- Procedimiento Almacenado 2
-- Consolidación de Inventario Multi-Nodo
drop procedure sp_consolidar_inventario_multi_nodo

CREATE OR REPLACE PROCEDURE sp_consolidar_inventario_multi_nodo(
    p_categoria_id INT 
)
LANGUAGE plpgsql
AS $$
DECLARE
    cur_product RECORD;
    total_productos INT := 0;
    valor_total NUMERIC := 0;
    productos_bajo_stock INT := 0;
BEGIN
    -- Nodo 2 
    FOR cur_product IN
        SELECT *
        FROM products 
        WHERE p_categoria_id IS NULL OR category_id = p_categoria_id
    LOOP
        total_productos := total_productos + 1;
        valor_total := valor_total + (cur_product.units_in_stock * cur_product.unit_price);
        IF cur_product.units_in_stock < cur_product.reorder_level THEN
            productos_bajo_stock := productos_bajo_stock + 1;
        END IF;
    END LOOP;

    -- Nodo 3 
    FOR cur_product IN
        SELECT *
        FROM products
        WHERE p_categoria_id IS NULL OR category_id = p_categoria_id
    LOOP
        total_productos := total_productos + 1;
        valor_total := valor_total + (cur_product.units_in_stock * cur_product.unit_price);
        IF cur_product.units_in_stock < cur_product.reorder_level THEN
            productos_bajo_stock := productos_bajo_stock + 1;
        END IF;
    END LOOP;

    -- Nodo 4 aqui estan los productos
    FOR cur_product IN
        SELECT *
        FROM products
        WHERE p_categoria_id IS NULL OR category_id = p_categoria_id
    LOOP
        total_productos := total_productos + 1;
        valor_total := valor_total + (cur_product.units_in_stock * cur_product.unit_price);
        IF cur_product.units_in_stock < cur_product.reorder_level THEN
            productos_bajo_stock := productos_bajo_stock + 1;
        END IF;
    END LOOP;

    -- Nodo 5 
    FOR cur_product IN
        SELECT *
        FROM products
        WHERE p_categoria_id IS NULL OR category_id = p_categoria_id
    LOOP
        total_productos := total_productos + 1;
        valor_total := valor_total + (cur_product.units_in_stock * cur_product.unit_price);
        IF cur_product.units_in_stock < cur_product.reorder_level THEN
            productos_bajo_stock := productos_bajo_stock + 1;
        END IF;
    END LOOP;

    -- Mostrar resultados
    RAISE NOTICE 'Total Productos: %, Valor Total: %, Productos Bajo Stock: %',
        total_productos, valor_total, productos_bajo_stock;

END;
$$;

CALL sp_consolidar_inventario_multi_nodo(2);




-- Procedimiento Almacenado 3 
-- Procedimiento: Procesamiento de Pedido (Nodo Local)

DROP PROCEDURE IF EXISTS sp_generar_reporte_stock(INT);


CREATE OR REPLACE PROCEDURE sp_generar_reporte_stock(
    p_product_id INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Variables para la información básica del producto 
    v_product_name VARCHAR(40);
    v_units_in_stock INT;
    v_reorder_level INT;

    -- Nuevas variables para la información del bucle de pedidos
    v_order_id_loop INT;      
    v_order_date_loop DATE;   
    v_quantity_loop INT;      
    
BEGIN
    -- Obtener información básica del producto 
    SELECT product_name, units_in_stock, reorder_level
    INTO v_product_name, v_units_in_stock, v_reorder_level
    FROM products
    WHERE product_id = p_product_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Producto con ID % no encontrado.', p_product_id;
    END IF;

    RAISE NOTICE '--- REPORTE DE INVENTARIO PARA PRODUCTO % (ID: %) ---', v_product_name, p_product_id;
    RAISE NOTICE 'Stock Actual: %', v_units_in_stock;
    RAISE NOTICE 'Nivel de Reorden: %', v_reorder_level;

    IF v_units_in_stock < v_reorder_level THEN
        RAISE WARNING '¡ALERTA!: El stock está por debajo del nivel de reorden.';
    ELSE
        RAISE NOTICE 'Stock en nivel seguro.';
    END IF;
    
    RAISE NOTICE '-------------------------------------------------------';
    RAISE NOTICE '--- ÚLTIMOS 5 PEDIDOS QUE INCLUYEN ESTE PRODUCTO ---';

    -- Reporte de los últimos 5 pedidos
    FOR v_order_id_loop, v_order_date_loop, v_quantity_loop IN 
        SELECT
            o.order_id,
            o.order_date,
            od.quantity
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        WHERE od.product_id = p_product_id
        ORDER BY o.order_date DESC
        LIMIT 5
    LOOP
        RAISE NOTICE '  Pedido ID: %, Fecha: %, Cantidad: %', v_order_id_loop, v_order_date_loop, v_quantity_loop;
    END LOOP;

    RAISE NOTICE '--- FIN DEL REPORTE ---';

END;
$$;

CALL sp_generar_reporte_stock(11);

