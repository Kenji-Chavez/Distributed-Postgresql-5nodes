-- EJECUTAR en el NODO 5 (Réplica Analítica)
CREATE TABLE pedidos_replica (
    id_pedido INT PRIMARY KEY,
    id_cliente VARCHAR(5),
    fecha_pedido DATE,
    fecha_envio TIMESTAMP,
    timestamp_sincronizacion TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

select * from pedidos_replica