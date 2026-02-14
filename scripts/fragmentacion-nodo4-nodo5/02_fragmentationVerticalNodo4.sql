-- Fragmentación vertical de Order_Details
CREATE TABLE order_details_parte1 AS
SELECT order_id, product_id, unit_price
FROM order_details;

SELECT * FROM order_details_parte1