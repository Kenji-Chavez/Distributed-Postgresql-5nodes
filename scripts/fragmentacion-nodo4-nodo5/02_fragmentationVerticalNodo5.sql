-- Fragmentación vertical de Order_Details
DROP TABLE IF EXISTS order_details_parte2 ;

CREATE TABLE order_details_parte2 AS
SELECT order_id, product_id, quantity, discount
FROM order_details;

SELECT * FROM order_details_parte2

ALTER TABLE order_details_parte2 
    ALTER COLUMN order_id TYPE integer,
    ALTER COLUMN product_id TYPE integer,
    ALTER COLUMN discount TYPE double precision;
