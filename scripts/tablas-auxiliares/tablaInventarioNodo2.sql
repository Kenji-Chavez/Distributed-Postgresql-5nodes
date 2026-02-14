-- EJECUTAR en el NODO 2 y en el NODO 3
drop table inventario
CREATE TABLE inventario (
    id_producto INT PRIMARY KEY,
    nivel_stock INT NOT NULL DEFAULT 0
);

-- Datos de prueba iniciales (ejecutar en NODO 2 - Origen)
INSERT INTO inventario (id_producto, nivel_stock) VALUES (10, 150); 
INSERT INTO inventario (id_producto, nivel_stock) VALUES (11, 80);

select * from inventario

