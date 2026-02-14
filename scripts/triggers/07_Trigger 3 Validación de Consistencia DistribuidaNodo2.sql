-- Apunta a la tabla 'products' del NODO 4 (Inventario)
DROP FOREIGN TABLE IF EXISTS products_ft;
CREATE FOREIGN TABLE products_ft (
    product_id INT,
    units_in_stock INT,
    discontinued INT 
) 
SERVER nodo4_server 
OPTIONS (table_name 'products');
------------------------------------------------------
-- Trigger 3: Validación de Consistencia Distribuida
-- El objetivo de esta implementación es doble: garantizar la integridad de datos (no vender productos discontinuados o sin stock) 
-- y la consistencia transaccional entre nodos, asegurando que el inventario del Nodo 4 
-- se actualice correctamente después de una venta en el Nodo 2/3.
-- Función de Validación (BEFORE INSERT)
-- Verifica stock y producto discontinuado en el Nodo 4
CREATE OR REPLACE FUNCTION fn_validar_consistencia_simple()
RETURNS TRIGGER AS $$
DECLARE
    v_stock_info RECORD;
BEGIN
    
    -- Consulta Distribuida al Nodo 4
    SELECT units_in_stock, discontinued
    INTO v_stock_info
    FROM products_ft
    WHERE product_id = NEW.product_id;

    -- 1. VALIDACIÓN: Existencia del Producto
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ERROR: Product ID (%) no encontrado en el inventario (Nodo 4).', NEW.product_id;
    END IF;

    -- 2. VALIDACIÓN: Producto Discontinuado
    IF v_stock_info.discontinued <> 0 THEN
        RAISE EXCEPTION 'ERROR: El Producto ID (%) está discontinuado.', NEW.product_id;
    END IF;
    
    -- 3. VALIDACIÓN: Stock Disponible
    IF v_stock_info.units_in_stock < NEW.quantity THEN
        RAISE EXCEPTION 'ERROR: Stock insuficiente. Disponible: %, Solicitado: % para Producto ID %.', 
            v_stock_info.units_in_stock, NEW.quantity, NEW.product_id;
    END IF;

    -- Permite la inserción si todo es válido
    RETURN NEW; 
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validar_orden_simple ON "order_details";

-- Creamos el trigger
CREATE TRIGGER trg_validar_orden_simple
BEFORE INSERT ON "order_details" 
FOR EACH ROW EXECUTE FUNCTION fn_validar_consistencia_simple();
--------------------------------------------------------
-- Trigger 4: Función de Decremento (AFTER INSERT)
-- Actualiza el inventario en el Nodo 4 después de una venta exitosa
CREATE OR REPLACE FUNCTION fn_decrementar_stock_distribuido()
RETURNS TRIGGER AS $$
BEGIN
    -- Ejecuta el UPDATE remoto en el Nodo 4 a través de products_ft
    UPDATE products_ft 
    SET units_in_stock = units_in_stock - NEW.quantity
    WHERE product_id = NEW.product_id;

    RETURN NULL; 
END;
$$ LANGUAGE plpgsql;

-- Creamos el trigger
CREATE TRIGGER trg_decrementar_stock
AFTER INSERT ON "order_details" 
FOR EACH ROW EXECUTE FUNCTION fn_decrementar_stock_distribuido();
--------------------------------------------------------
-- Ejecutamos el trigger que nos dará el fallo de stock insuficiente 
INSERT INTO "order_details" (order_id, product_id, unit_price, quantity, discount) VALUES (10260, 65, 15.00, 100, 0);
--------------------------------------------------------
-- Ejecutamos el trigger que realizará la operación con éxito
INSERT INTO "order_details" (order_id, product_id, unit_price, quantity, discount) VALUES (10260, 65, 15.00, 6, 0);





--------------------------------------------------------
-- 1. Eliminar los triggers existentes
DROP TRIGGER IF EXISTS trg_validar_orden_simple ON "order_details";
DROP TRIGGER IF EXISTS trg_decrementar_stock ON "order_details";

-- 2. Volver a crear el Trigger 3 (Validación) para que reaccione a INSERT O UPDATE
CREATE TRIGGER trg_validar_orden_simple
BEFORE INSERT OR UPDATE ON "order_details"
FOR EACH ROW EXECUTE FUNCTION fn_validar_consistencia_simple();

-- 3. Volver a crear el Trigger 4 (Decremento) para que reaccione a INSERT O UPDATE
CREATE TRIGGER trg_decrementar_stock
AFTER INSERT OR UPDATE ON "order_details"
FOR EACH ROW EXECUTE FUNCTION fn_decrementar_stock_distribuido();