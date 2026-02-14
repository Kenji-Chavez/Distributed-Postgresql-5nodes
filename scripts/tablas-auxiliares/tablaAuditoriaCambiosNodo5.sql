drop table auditoria_cambios
-- Se crea la tabla auditoria_cambios para guardar los cambios de los precios y descuentos
CREATE TABLE auditoria_cambios (
    id_auditoria BIGSERIAL PRIMARY KEY,
    tabla_afectada VARCHAR(50) NOT NULL,
    id_registro_afectado INT NOT NULL,
    columna_modificada VARCHAR(50) NOT NULL,
    valor_anterior TEXT,
    valor_nuevo TEXT,
    usuario_cambio TEXT NOT NULL,
    timestamp_cambio TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- -- Consultamos los resultados del trigger del nodo 2
SELECT id_auditoria, tabla_afectada, valor_anterior, valor_nuevo 
FROM auditoria_cambios 
WHERE tabla_afectada LIKE '%Order Details%' OR tabla_afectada LIKE '%order details%'
ORDER BY id_auditoria DESC;
