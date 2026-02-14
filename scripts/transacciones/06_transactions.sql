CREATE FOREIGN TABLE auditoria_precios_ft (
    product_id INT,
    precio_anterior DECIMAL,
    precio_nuevo DECIMAL,
    fecha_cambio TIMESTAMP
) SERVER nodo5_server
OPTIONS (schema_name 'public', table_name 'auditoria_precios');



--------------------------------------------------------------------------------------------
-- Transacción 1: Actualización Atómica Multi-Nodo
-- La transacción sp_actualizar_precio_distribuido es un proceso crítico que actualiza precios en varios nodos y garantiza
-- consistencia mediante una ejecución atómica: o todos los cambios se aplican, o ninguno.
CREATE OR REPLACE PROCEDURE sp_actualizar_precio_distribuido(
    p_product_id INT,
    p_nuevo_precio DECIMAL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_precio_anterior DECIMAL;
    v_ordenes_america INT := 0;
    v_ordenes_europa INT := 0;
    
BEGIN
    -- El BEGIN de la transacción es implícito por el bloque DO
    -- 1. ACTUALIZAR EL PRECIO DEL PRODUCTO (NODO 4)
    -- Código para obtener precio anterior y actualizar products_ft
    SELECT unit_price INTO v_precio_anterior FROM products_ft WHERE product_id = p_product_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'Producto % no encontrado.', p_product_id; END IF;

    UPDATE products_ft SET unit_price = p_nuevo_precio WHERE product_id = p_product_id;
    RAISE NOTICE 'Paso 1 completado: Precio de Producto % actualizado.', p_product_id;
                 
    -- 2. RECALCULAR TOTALES DE ÓRDENES ABIERTAS (NODOS 2 y 3)
    -- Recálculo en Nodo 2 (América)
    WITH ordenes_abiertas AS (
        SELECT order_id
        FROM orders_america_ft 
    )
    UPDATE order_details od
    SET unit_price = p_nuevo_precio
    FROM ordenes_abiertas oa
    WHERE od.order_id = oa.order_id 
      AND od.product_id = p_product_id;
    GET DIAGNOSTICS v_ordenes_america = ROW_COUNT;
    
    IF v_ordenes_america > 0 THEN
         RAISE NOTICE 'Paso 2a completado: % órdenes abiertas en América recalculadas.', v_ordenes_america;
    END IF;

    -- Recálculo en Nodo 3 (Europa)
    WITH ordenes_abiertas AS (
        SELECT order_id
        FROM orders_europe_ft 
    )
    UPDATE order_details od
    SET unit_price = p_nuevo_precio
    FROM ordenes_abiertas oa
    WHERE od.order_id = oa.order_id 
      AND od.product_id = p_product_id; 
    GET DIAGNOSTICS v_ordenes_europa = ROW_COUNT;

    IF v_ordenes_europa > 0 THEN
         RAISE NOTICE 'Paso 2b completado: % órdenes abiertas en Europa recalculadas.', v_ordenes_europa;
    END IF;
	
    -- 3. REGISTRAR EL CAMBIO EN TABLA DE AUDITORÍA (NODO 5)    
	INSERT INTO auditoria_precios_ft (
	    product_id, 
	    precio_anterior, 
	    precio_nuevo
	)
	VALUES (p_product_id, v_precio_anterior, p_nuevo_precio);
    
    RAISE NOTICE 'Paso 3 completado: Registro de auditoría guardado en Nodo 5.';
    
END;
$$;


-- Ejecutamos la transacción
DO $$
DECLARE
    p_id_producto_a_actualizar CONSTANT INT := 1;  
    p_nuevo_precio_deseado CONSTANT DECIMAL := 30.00;
BEGIN
    CALL sp_actualizar_precio_distribuido(p_id_producto_a_actualizar, p_nuevo_precio_deseado); 
    
    RAISE NOTICE 'TRANSACCIÓN DISTRIBUIDA FINALIZADA CON ÉXITO.';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'TRANSACCIÓN FALLIDA: Reversión Completa.';
        RAISE NOTICE 'Detalle del Error: SQLSTATE % / Mensaje: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;




-----------------------------------------------------------------------------------------------------
CREATE TABLE traslados_stock (
    id_traslado SERIAL PRIMARY KEY,
    id_producto INT,
    cantidad INT,
    nodo_origen VARCHAR(50),
    nodo_destino VARCHAR(50),
    fecha_traslado TIMESTAMP DEFAULT NOW()
);
-- FT para el Inventario del NODO 2 (América - Origen)
CREATE FOREIGN TABLE inventario_america_ft (
    id_producto INT,
    nivel_stock INT
) 
SERVER nodo2_server 
OPTIONS (table_name 'inventario');

-- FT para el Inventario del NODO 3 (Europa - Destino)
CREATE FOREIGN TABLE inventario_europa_ft (
    id_producto INT,
    nivel_stock INT
) 
SERVER nodo3_server 
OPTIONS (table_name 'inventario');
-----------------------------------------------------------------------------------------------------
-- Transacción 2: Transferencia de Stock entre Proveedores
-- La transacción sp_trasladar_stock_distribuido mueve inventario entre dos nodos (América y Europa) garantizando atomicidad y seguridad en escenarios de fallos 
-- o concurrencia. Usa el nivel de aislamiento SERIALIZABLE para evitar conflictos cuando varios usuarios intentan transferir el mismo stock. 
-- Si ocurre una inconsistencia, PostgreSQL realiza un rollback automático, asegurando que el inventario total siempre se mantenga correcto.
CREATE OR REPLACE PROCEDURE sp_trasladar_stock_distribuido(
    p_id_producto INT,
    p_cantidad INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_stock_actual INT;
BEGIN
    -- El SET TRANSACTION se hace fuera del procedimiento (BEGIN/DO block)
    -- VALIDACIÓN INICIAL
    IF p_cantidad <= 0 THEN
        RAISE EXCEPTION 'La cantidad a trasladar debe ser positiva.';
    END IF;

    -- 2. BLOQUEO DE REGISTROS Y OBTENCIÓN DE STOCK (NODO 2 - ORIGEN)
    SELECT nivel_stock
    INTO v_stock_actual
    FROM inventario_america_ft
    WHERE id_producto = p_id_producto
    FOR UPDATE; -- El bloqueo se da en el Nodo 2

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Producto % no encontrado en el inventario de América.', p_id_producto;
    END IF;
    
    RAISE NOTICE 'Paso 1: Stock actual bloqueado en América: %.', v_stock_actual;
    
    -- 3. VALIDACIÓN DE INTEGRIDAD (Stock Suficiente)
    IF v_stock_actual < p_cantidad THEN
        RAISE EXCEPTION 'Stock insuficiente en América. Disponible: %, Solicitado: %', 
                        v_stock_actual, p_cantidad;
    END IF;
    
    RAISE NOTICE 'Paso 2: Validación de stock completada. Se procede al traslado.';
    
    -- 4a. DECREMENTAR STOCK (Nodo 2 - Origen)
    UPDATE inventario_america_ft
    SET nivel_stock = nivel_stock - p_cantidad
    WHERE id_producto = p_id_producto;
    
    RAISE NOTICE 'Paso 3a: Stock disminuido en América (Nodo 2).';
    
    -- 4b. INCREMENTAR STOCK (Nodo 3 - Destino)
    UPDATE inventario_europa_ft
    SET nivel_stock = nivel_stock + p_cantidad
    WHERE id_producto = p_id_producto;
    
    RAISE NOTICE 'Paso 3b: Stock incrementado en Europa (Nodo 3).';

    -- 5. REGISTRAR EL CAMBIO (Auditoría - Nodo Coordinador)
    INSERT INTO traslados_stock (id_producto, cantidad, nodo_origen, nodo_destino)
    VALUES (p_id_producto, p_cantidad, 'Nodo 2 (América)', 'Nodo 3 (Europa)');
    
    RAISE NOTICE 'Paso 4: Traslado registrado localmente.';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'ERROR CRÍTICO: Transacción ABORTADA. Error: %', SQLERRM;
        RAISE; 
END;
$$;

-- Ejecutamos la transacción
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Se va trasladar 20 unidades del Producto ID 10
CALL sp_trasladar_stock_distribuido(
    p_id_producto := 10,
    p_cantidad := 5
);

COMMIT;

-- Verificamos en la tabla de auditoria de traslados
select * from traslados_stock
