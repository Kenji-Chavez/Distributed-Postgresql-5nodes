-- 1. CREACIÓN DE LA FOREIGN TABLE en el Nodo 3
-- Esto permite que el Nodo 3 pueda escribir en la tabla 'pedidos_replica' del Nodo 5.
CREATE FOREIGN TABLE pedidos_replica_ft (
    id_pedido INT,
    id_cliente VARCHAR(5),
    fecha_pedido DATE,
    fecha_envio TIMESTAMP,
    timestamp_sincronizacion TIMESTAMP WITHOUT TIME ZONE
) 
SERVER nodo5_server 
OPTIONS (table_name 'pedidos_replica');
---------------------------------------------------------------------------
-- Trigger 1: Sincronización de Réplica
-- Creación del trigger en el nodo 3
CREATE OR REPLACE FUNCTION fn_sincronizar_a_replica()
RETURNS TRIGGER AS $$
DECLARE
    v_pedido_id_existente INT;
BEGIN
    -- 1. Verificar si el pedido ya existe en la réplica 
    SELECT id_pedido
    INTO v_pedido_id_existente
    FROM pedidos_replica_ft
    WHERE id_pedido = NEW.order_id; -- Usamos el ID del nuevo registro

    IF v_pedido_id_existente IS NOT NULL THEN
        -- Si existe, hacemos UPDATE 
        UPDATE pedidos_replica_ft
        SET 
            id_cliente = NEW.customer_id,
            fecha_pedido = NEW.order_date,
            fecha_envio = NEW.shipped_date,
            timestamp_sincronizacion = NOW()
        WHERE id_pedido = NEW.order_id;
    ELSE
        -- Si NO existe, hacemos INSERT 
        INSERT INTO pedidos_replica_ft (
            id_pedido, 
            id_cliente, 
            fecha_pedido, 
            fecha_envio, 
            timestamp_sincronizacion
        )
        VALUES (
            NEW.order_id,    
            NEW.customer_id, 
            NEW.order_date,  
            NEW.shipped_date, 
            NOW() 
        );
    END IF;

    RETURN NEW; 
END;
$$ LANGUAGE plpgsql;


-- Creamos el trigger 
CREATE TRIGGER pedidos_replica_ft
AFTER INSERT OR UPDATE ON Orders
FOR EACH ROW EXECUTE FUNCTION fn_sincronizar_a_replica();
---------------------------------------------------------------
-- Ejecutamos
INSERT INTO Orders (order_id, customer_id, employee_id, order_date, required_date)
VALUES (
    11079, 
    'ALFKI', 
    1, 
    CURRENT_DATE, 
    CURRENT_DATE + interval '7 days'
);
select * from orders