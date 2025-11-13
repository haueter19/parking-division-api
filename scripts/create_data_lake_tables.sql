-- SQL Server Migration Script for Data Lake Tables
-- Run this to create all staging tables and the normalized transactions table

-- Drop existing tables if needed (be careful with this in production!)
-- DROP TABLE IF EXISTS transactions;
-- DROP TABLE IF EXISTS windcave_staging;
-- DROP TABLE IF EXISTS payments_insider_staging;
-- DROP TABLE IF EXISTS ips_cc_staging;
-- DROP TABLE IF EXISTS ips_mobile_staging;
-- DROP TABLE IF EXISTS ips_cash_staging;
-- DROP TABLE IF EXISTS sql_cash_staging;
-- DROP TABLE IF EXISTS etl_processing_log;

-- ============= Staging Tables =============

-- Windcave Credit Card Staging
CREATE TABLE windcave_staging (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_file_id INT NOT NULL,
    transaction_date DATETIME NULL,
    card_number_masked NVARCHAR(20) NULL,
    amount DECIMAL(10,2) NULL,
    settlement_date DATETIME NULL,
    settlement_amount DECIMAL(10,2) NULL,
    terminal_id NVARCHAR(100) NULL,
    reference NVARCHAR(255) NULL,
    card_type NVARCHAR(50) NULL,
    merchant_id NVARCHAR(100) NULL,
    loaded_at DATETIME DEFAULT GETDATE(),
    processed_to_final BIT DEFAULT 0,
    transaction_id INT NULL,
    FOREIGN KEY (source_file_id) REFERENCES uploaded_files(id)
);

CREATE INDEX IX_windcave_staging_source_file ON windcave_staging(source_file_id);
CREATE INDEX IX_windcave_staging_processed ON windcave_staging(processed_to_final);
CREATE INDEX IX_windcave_staging_terminal ON windcave_staging(terminal_id);

-- Payments Insider Staging
CREATE TABLE payments_insider_staging (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_file_id INT NOT NULL,
    report_type NVARCHAR(20) NULL, -- 'sales' or 'payments'
    transaction_date DATETIME NULL,
    payment_date DATETIME NULL,
    amount DECIMAL(10,2) NULL,
    card_type NVARCHAR(50) NULL,
    terminal_id NVARCHAR(100) NULL,
    location NVARCHAR(255) NULL,
    reference_number NVARCHAR(255) NULL,
    batch_number NVARCHAR(100) NULL,
    loaded_at DATETIME DEFAULT GETDATE(),
    processed_to_final BIT DEFAULT 0,
    transaction_id INT NULL,
    matching_report_id INT NULL, -- Links sales to payments reports
    FOREIGN KEY (source_file_id) REFERENCES uploaded_files(id)
);

CREATE INDEX IX_pi_staging_source_file ON payments_insider_staging(source_file_id);
CREATE INDEX IX_pi_staging_processed ON payments_insider_staging(processed_to_final);
CREATE INDEX IX_pi_staging_reference ON payments_insider_staging(reference_number);
CREATE INDEX IX_pi_staging_matching ON payments_insider_staging(matching_report_id);

-- IPS Credit Card Staging
CREATE TABLE ips_cc_staging (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_file_id INT NOT NULL,
    transaction_date DATETIME NULL,
    amount DECIMAL(10,2) NULL,
    terminal_id NVARCHAR(100) NULL,
    location NVARCHAR(255) NULL,
    card_type NVARCHAR(50) NULL,
    reference NVARCHAR(255) NULL,
    loaded_at DATETIME DEFAULT GETDATE(),
    processed_to_final BIT DEFAULT 0,
    transaction_id INT NULL,
    FOREIGN KEY (source_file_id) REFERENCES uploaded_files(id)
);

CREATE INDEX IX_ips_cc_staging_source_file ON ips_cc_staging(source_file_id);
CREATE INDEX IX_ips_cc_staging_processed ON ips_cc_staging(processed_to_final);

-- IPS Mobile Payments Staging
CREATE TABLE ips_mobile_staging (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_file_id INT NOT NULL,
    transaction_date DATETIME NULL,
    amount DECIMAL(10,2) NULL,
    phone_number NVARCHAR(20) NULL, -- Masked/partial
    location NVARCHAR(255) NULL,
    meter_id NVARCHAR(100) NULL,
    payment_method NVARCHAR(50) NULL, -- 'SMS' or 'App'
    loaded_at DATETIME DEFAULT GETDATE(),
    processed_to_final BIT DEFAULT 0,
    transaction_id INT NULL,
    FOREIGN KEY (source_file_id) REFERENCES uploaded_files(id)
);

CREATE INDEX IX_ips_mobile_staging_source_file ON ips_mobile_staging(source_file_id);
CREATE INDEX IX_ips_mobile_staging_processed ON ips_mobile_staging(processed_to_final);

-- IPS Cash Staging (coins in meters)
CREATE TABLE ips_cash_staging (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_file_id INT NOT NULL,
    collection_date DATETIME NULL,
    amount DECIMAL(10,2) NULL,
    meter_id NVARCHAR(100) NULL,
    location NVARCHAR(255) NULL,
    collector_id NVARCHAR(100) NULL,
    loaded_at DATETIME DEFAULT GETDATE(),
    processed_to_final BIT DEFAULT 0,
    transaction_id INT NULL,
    FOREIGN KEY (source_file_id) REFERENCES uploaded_files(id)
);

CREATE INDEX IX_ips_cash_staging_source_file ON ips_cash_staging(source_file_id);
CREATE INDEX IX_ips_cash_staging_processed ON ips_cash_staging(processed_to_final);

-- SQL Cash Query Staging
CREATE TABLE sql_cash_staging (
    id INT IDENTITY(1,1) PRIMARY KEY,
    transaction_date DATETIME NULL,
    amount DECIMAL(10,2) NULL,
    location NVARCHAR(255) NULL,
    terminal_id NVARCHAR(100) NULL,
    reference NVARCHAR(255) NULL,
    loaded_at DATETIME DEFAULT GETDATE(),
    processed_to_final BIT DEFAULT 0,
    transaction_id INT NULL
);

CREATE INDEX IX_sql_cash_staging_processed ON sql_cash_staging(processed_to_final);

-- ============= Normalized Transactions Table =============
CREATE TABLE transactions (
    id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Core transaction fields
    transaction_date DATETIME NOT NULL,
    transaction_amount DECIMAL(10,2) NOT NULL,
    
    -- Settlement fields
    settle_date DATETIME NULL,
    settle_amount DECIMAL(10,2) NULL,
    
    -- Source and location
    source NVARCHAR(50) NOT NULL,
    location_type NVARCHAR(50) NOT NULL,
    location_name NVARCHAR(255) NULL,
    device_terminal_id NVARCHAR(100) NULL,
    
    -- Payment information
    payment_type NVARCHAR(50) NOT NULL,
    
    -- Additional tracking
    reference_number NVARCHAR(255) NULL,
    org_code NVARCHAR(50) NULL,
    
    -- Audit trail
    staging_table NVARCHAR(50) NULL,
    staging_record_id INT NULL,
    
    -- Timestamps
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME NULL
);

-- Create indexes for common queries
CREATE INDEX IX_transactions_date ON transactions(transaction_date);
CREATE INDEX IX_transactions_settle_date ON transactions(settle_date);
CREATE INDEX IX_transactions_source ON transactions(source);
CREATE INDEX IX_transactions_terminal ON transactions(device_terminal_id);
CREATE INDEX IX_transactions_org_code ON transactions(org_code);
CREATE INDEX IX_transactions_location_type ON transactions(location_type);
CREATE INDEX IX_transactions_payment_type ON transactions(payment_type);

-- ============= ETL Processing Log =============
CREATE TABLE etl_processing_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_table NVARCHAR(50) NOT NULL,
    source_file_id INT NULL,
    start_time DATETIME DEFAULT GETDATE(),
    end_time DATETIME NULL,
    records_processed INT NULL,
    records_created INT NULL,
    records_updated INT NULL,
    records_failed INT NULL,
    status NVARCHAR(20) NULL, -- 'running', 'completed', 'failed'
    error_message NVARCHAR(MAX) NULL,
    FOREIGN KEY (source_file_id) REFERENCES uploaded_files(id)
);

CREATE INDEX IX_etl_log_status ON etl_processing_log(status);
CREATE INDEX IX_etl_log_source_table ON etl_processing_log(source_table);

-- ============= Update uploaded_files table if needed =============
-- Add columns for tracking processing if they don't exist
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('uploaded_files') AND name = 'records_processed')
BEGIN
    ALTER TABLE uploaded_files ADD records_processed INT NULL;
END

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('uploaded_files') AND name = 'processing_errors')
BEGIN
    ALTER TABLE uploaded_files ADD processing_errors NVARCHAR(MAX) NULL;
END

-- ============= Sample Views for Reporting =============

-- Daily transaction summary
CREATE VIEW vw_daily_transaction_summary AS
SELECT 
    CAST(transaction_date AS DATE) as transaction_date,
    source,
    location_type,
    payment_type,
    COUNT(*) as transaction_count,
    SUM(transaction_amount) as total_amount,
    SUM(settle_amount) as total_settled,
    AVG(transaction_amount) as avg_amount
FROM transactions
GROUP BY 
    CAST(transaction_date AS DATE),
    source,
    location_type,
    payment_type;

-- Pending staging records
CREATE VIEW vw_pending_staging_records AS
SELECT 'windcave' as staging_table, COUNT(*) as pending_count
FROM windcave_staging WHERE processed_to_final = 0
UNION ALL
SELECT 'payments_insider', COUNT(*)
FROM payments_insider_staging WHERE processed_to_final = 0
UNION ALL
SELECT 'ips_cc', COUNT(*)
FROM ips_cc_staging WHERE processed_to_final = 0
UNION ALL
SELECT 'ips_mobile', COUNT(*)
FROM ips_mobile_staging WHERE processed_to_final = 0
UNION ALL
SELECT 'ips_cash', COUNT(*)
FROM ips_cash_staging WHERE processed_to_final = 0
UNION ALL
SELECT 'sql_cash', COUNT(*)
FROM sql_cash_staging WHERE processed_to_final = 0;

PRINT 'Data Lake tables created successfully!';
