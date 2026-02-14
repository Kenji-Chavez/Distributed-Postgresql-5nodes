-- 1. Eliminar la Foreign Table existente
DROP FOREIGN TABLE IF EXISTS auditoria_ft;
-- 2. Crear la Foreign Table CORRECTA
CREATE FOREIGN TABLE auditoria_ft (
    tabla_afectada VARCHAR(50),
    id_registro_afectado INT,
    columna_modificada VARCHAR(50),
    valor_anterior TEXT,
    valor_nuevo TEXT,
    usuario_cambio TEXT,
    timestamp_cambio TIMESTAMP WITHOUT TIME ZONE
) 
SERVER nodo5_server 
OPTIONS (table_name 'auditoria_cambios');
-----------------------------------------------------------
-- Trigger 2: Auditoría de Cambios Críticos
-- El propósito de este mecanismo es garantizar la trazabilidad y la integridad de los datos críticos (precios y descuentos) 
-- en nuestra arquitectura distribuida. En lugar de registrar los cambios localmente en cada nodo, los centralizamos 
-- en un único punto (el Nodo 5) lo que simplifica la auditoría y el análisis de cumplimiento
CREATE OR REPLACE FUNCTION fn_auditar_cambio_critico_simple()
RETURNS TRIGGER AS $$
DECLARE
    v_columna_critica VARCHAR(50);
    v_id_afectado INT;
    v_valor_antiguo TEXT;
    v_valor_nuevo TEXT;
BEGIN
    
    IF TG_TABLE_NAME = 'products' THEN
        v_columna_critica := 'unit_price';
        v_id_afectado := NEW.product_id;
        v_valor_antiguo := OLD.unit_price::TEXT;
        v_valor_nuevo := NEW.unit_price::TEXT;
        
        -- Condición: Si el valor no cambió, salir
        IF OLD.unit_price IS NOT DISTINCT FROM NEW.unit_price THEN
            RETURN NEW; 
        END IF;

    ELSIF TG_TABLE_NAME = 'order_details' THEN
        -- Auditoría de Descuentos de Detalles de Órdenes
        v_columna_critica := 'discount';
        v_id_afectado := NEW.order_id;
        v_valor_antiguo := OLD.discount::TEXT;
        v_valor_nuevo := NEW.discount::TEXT;
        
        -- Condición: Si el valor no cambió, salir
        IF OLD.discount IS NOT DISTINCT FROM NEW.discount THEN
            RETURN NEW; 
        END IF;

    ELSE
        -- No se audita si la tabla no está definida
        RETURN NEW;
    END IF;

    -- Registrar el cambio en la FT (se escribe en el Nodo 5)
    INSERT INTO auditoria_ft (
        tabla_afectada,
        id_registro_afectado,
        columna_modificada,
        valor_anterior,
        valor_nuevo,
        usuario_cambio,
        timestamp_cambio
    ) VALUES (
        TG_TABLE_NAME, 
        v_id_afectado,
        v_columna_critica,
        v_valor_antiguo,
        v_valor_nuevo,
        current_user,
        NOW()
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
------------------------------------------------------
-- Creamos el trigger
CREATE TRIGGER trg_auditar_descuento_simple
AFTER UPDATE ON "order_details"
FOR EACH ROW EXECUTE FUNCTION fn_auditar_cambio_critico_simple();