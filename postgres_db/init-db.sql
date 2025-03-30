
BEGIN;

DO $$ 
BEGIN
    -- Create roles
    IF NOT EXISTS (SELECT * FROM pg_roles WHERE rolname = 'db_user') THEN
        CREATE ROLE db_user;
        GRANT CONNECT ON DATABASE my_database TO db_user;
        --SET ROLE db_user;  -- test this
    END IF;

    IF NOT EXISTS (SELECT * FROM pg_roles WHERE rolname = 'db_reader') THEN
        CREATE ROLE db_reader;
        GRANT CONNECT ON DATABASE my_database TO db_reader;
    END IF;


    -- Create users 
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'user1') THEN
        CREATE USER User1 WITH PASSWORD 'User123';
        GRANT db_user TO user1;
        ALTER ROLE user1 INHERIT;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst') THEN
        CREATE USER Analyst WITH PASSWORD 'Analyst456';
        GRANT db_reader TO analyst;
        ALTER ROLE analyst INHERIT;
    END IF;
    
END $$;


-- Create table transformed_fraud_data
CREATE TABLE IF NOT EXISTS transformed_data (
    transactionID SERIAL PRIMARY KEY,  -- Auto-incrementing unique ID
    transType VARCHAR(50) NOT NULL,      -- Transaction type (PAYMENT, TRANSFER, etc.)
    amount DECIMAL(15,2) NOT NULL,     -- Amount of the transaction
    nameOrig VARCHAR(50) NOT NULL,     -- Sender’s account ID
    oldbalanceOrg DECIMAL(15,2),       -- Sender’s balance before transaction
    newbalanceOrig DECIMAL(15,2),      -- Sender’s balance after transaction
    nameDest VARCHAR(50) NOT NULL,     -- Receiver’s account ID
    oldbalanceDest DECIMAL(15,2),      -- Receiver’s balance before transaction
    newbalanceDest DECIMAL(15,2),      -- Receiver’s balance after transaction
    isFraud BOOLEAN NOT NULL,          -- 1 for fraud, 0 otherwise
    isFlaggedFraud BOOLEAN NOT NULL,   -- 1 if flagged, 0 otherwise
    timestamp TIMESTAMP NOT NULL       -- Date & time of transaction
);

-- Create table for aggregated data fraud by type
CREATE TABLE fraud_by_type (
    type VARCHAR(20),
    total_transactions INT,
    fraudulent_transactions INT,
    fraud_rate FLOAT
);


-- Create table for aggregated data flagged fraud accurancy
CREATE TABLE flagged_fraud_accuracy (
    total_flagged INT,
    true_positive_flags INT,
    flag_accuracy FLOAT
);

-- Create table for aggregated data fraud Average amount and Maximum amount
CREATE TABLE fraud_stats (
    avg_fraud_amount FLOAT,
    max_fraud_amount FLOAT
);


-- Grant permissions to user roles for Tables
GRANT USAGE ON SCHEMA public TO db_user; 
GRANT SELECT, INSERT, UPDATE ON TABLE transformed_data TO db_user;

GRANT USAGE ON SCHEMA public TO db_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_reader;


-- Set permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO db_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO db_reader;


-- Set the Search Path for users
-- This will ensure that when users run queries,
-- PostgreSQL will look for tables in the public schema.

ALTER ROLE user1 SET search_path TO public;
ALTER ROLE analyst SET search_path TO public;

-- Revoke public access to prevent accidental data exposure
REVOKE ALL ON SCHEMA public FROM PUBLIC;



COMMIT;
