-- Tabla de auditoria de precios en el nodo 5 
CREATE TABLE auditoria_precios (
    auditoria_id SERIAL PRIMARY KEY,
    product_id INT,
    precio_anterior DECIMAL,
    precio_nuevo DECIMAL,
    fecha_cambio TIMESTAMP DEFAULT NOW()
);

select * from auditoria_precios
