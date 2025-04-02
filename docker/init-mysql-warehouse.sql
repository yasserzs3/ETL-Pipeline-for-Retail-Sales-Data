-- Create tables for data warehouse

-- Product aggregation table
CREATE TABLE IF NOT EXISTS product_aggregation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    total_quantity INT NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    avg_price DECIMAL(10, 2) NOT NULL,
    transaction_count INT NOT NULL,
    INDEX idx_product_id (product_id)
);

-- Product source aggregation table
CREATE TABLE IF NOT EXISTS product_source_aggregation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    source VARCHAR(50) NOT NULL,
    total_quantity INT NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    INDEX idx_product_id (product_id),
    INDEX idx_source (source)
);

-- Date aggregation table
CREATE TABLE IF NOT EXISTS date_aggregation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    total_quantity INT NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    product_count INT NOT NULL,
    INDEX idx_date (date)
);

-- Product date aggregation table
CREATE TABLE IF NOT EXISTS product_date_aggregation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    date DATE NOT NULL,
    total_quantity INT NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    INDEX idx_product_id (product_id),
    INDEX idx_date (date)
);

-- Store aggregation table
CREATE TABLE IF NOT EXISTS store_aggregation (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    total_quantity INT NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    product_count INT NOT NULL,
    INDEX idx_store_id (store_id)
);

-- Create a user with appropriate permissions
CREATE USER 'etl_user'@'%' IDENTIFIED BY 'etl_password';
GRANT ALL PRIVILEGES ON sales_warehouse.* TO 'etl_user'@'%';
FLUSH PRIVILEGES; 