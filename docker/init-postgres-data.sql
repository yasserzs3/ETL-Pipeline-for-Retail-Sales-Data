-- Create online_sales table
CREATE TABLE IF NOT EXISTS online_sales (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

-- Insert sample data
INSERT INTO online_sales (product_id, date, quantity, price) VALUES 
(1001, '2023-01-01', 10, 10.99),
(1002, '2023-01-01', 5, 25.50),
(1003, '2023-01-01', 3, 15.75),
(1004, '2023-01-02', 8, 8.25),
(1005, '2023-01-02', 4, 12.99),
(1001, '2023-01-02', 7, 10.99),
(1002, '2023-01-03', 3, 25.50),
(1003, '2023-01-03', 5, 15.75),
(1004, '2023-01-04', 12, 8.25),
(1005, '2023-01-04', 9, 12.99);

-- Create a user with appropriate permissions
CREATE USER etl_user WITH PASSWORD 'etl_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO etl_user; 