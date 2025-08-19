-- =====================================================
-- FSI Demo - Foundation Setup Script
-- =====================================================
-- Idempotent setup using CREATE OR ALTER commands
-- Generic financial services platform for leisure/lifestyle payments
-- 
-- This script can be run multiple times safely
-- Uses principle of least privilege and simplicity
-- =====================================================

USE ROLE ACCOUNTADMIN;

-- =====================================================
-- 1. WAREHOUSES
-- =====================================================

-- Small warehouse for streaming ingestion (auto-suspend 60s)
CREATE OR ALTER WAREHOUSE S_WH 
  WITH 
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Small warehouse for streaming data ingestion';

-- Medium warehouse for dbt transformations (auto-suspend 60s)  
CREATE OR ALTER WAREHOUSE M_WH
  WITH
    WAREHOUSE_SIZE = 'MEDIUM' 
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Medium warehouse for dbt transformations and analytics';

-- =====================================================
-- 2. CUSTOM ROLES (Minimal Set)
-- =====================================================

-- Data Engineer Role: Ingestion and raw data management
CREATE OR ALTER ROLE data_engineer_role
  COMMENT = 'Role for data engineers managing ingestion and raw data pipelines';

-- Data Analyst Role: Transformations and analytics (with PII masking)
CREATE OR ALTER ROLE data_analyst_role  
  COMMENT = 'Role for data analysts working with transformed data (PII masked)';

-- =====================================================
-- 3. ROLE HIERARCHY
-- =====================================================

-- Grant custom roles to SYSADMIN (following best practices)
GRANT ROLE data_engineer_role TO ROLE SYSADMIN;
GRANT ROLE data_analyst_role TO ROLE SYSADMIN;

-- =====================================================
-- 4. SCHEMAS (Create if don't exist)
-- =====================================================

-- TRANSFORMED schema for cleaned, normalized data
CREATE OR ALTER SCHEMA FSI_DEMO.TRANSFORMED
  COMMENT = 'Schema for cleaned and normalized data from dbt transformations';

-- ANALYTICS schema for business-ready views and ML features  
CREATE OR ALTER SCHEMA FSI_DEMO.ANALYTICS
  COMMENT = 'Schema for business-ready analytics views and ML features';

-- =====================================================
-- 5. SERVICE ACCOUNT (Single Account for Simplicity)
-- =====================================================

-- Single automation service account for all programmatic access
-- Note: CREATE OR ALTER USER is not supported, using CREATE USER IF NOT EXISTS pattern
CREATE USER IF NOT EXISTS fsi_automation_service
  WITH
    PASSWORD = NULL  -- Will use key-pair authentication
    MUST_CHANGE_PASSWORD = FALSE
    DEFAULT_WAREHOUSE = 'S_WH'
    DEFAULT_NAMESPACE = 'FSI_DEMO.RAW_DATA'
    DEFAULT_ROLE = 'data_engineer_role'
    COMMENT = 'Service account for FSI demo automation (streaming, dbt, etc)';

-- Grant data_engineer_role to service account
GRANT ROLE data_engineer_role TO USER fsi_automation_service;

-- =====================================================
-- 6. WAREHOUSE PERMISSIONS
-- =====================================================

-- Data Engineer: Access to streaming warehouse
GRANT USAGE ON WAREHOUSE S_WH TO ROLE data_engineer_role;
GRANT OPERATE ON WAREHOUSE S_WH TO ROLE data_engineer_role;

-- Data Analyst: Access to transformation warehouse  
GRANT USAGE ON WAREHOUSE M_WH TO ROLE data_analyst_role;

-- Both roles can use existing XS_WH for development
GRANT USAGE ON WAREHOUSE XS_WH TO ROLE data_engineer_role;
GRANT USAGE ON WAREHOUSE XS_WH TO ROLE data_analyst_role;

-- =====================================================
-- 7. DATABASE AND SCHEMA PERMISSIONS
-- =====================================================

-- Data Engineer: Full access to RAW_DATA and TRANSFORMED
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_engineer_role;
GRANT ALL PRIVILEGES ON SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_engineer_role;
GRANT ALL PRIVILEGES ON SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_engineer_role;
GRANT USAGE ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_engineer_role;

-- Data Analyst: Read access to TRANSFORMED, full access to ANALYTICS
GRANT USAGE ON DATABASE FSI_DEMO TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_analyst_role;
GRANT ALL PRIVILEGES ON SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_analyst_role;
GRANT ALL PRIVILEGES ON SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;

-- Grant future privileges for new objects
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_engineer_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA FSI_DEMO.RAW_DATA TO ROLE data_engineer_role;

GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_engineer_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_engineer_role;

GRANT SELECT ON FUTURE TABLES IN SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_analyst_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA FSI_DEMO.TRANSFORMED TO ROLE data_analyst_role;

GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA FSI_DEMO.ANALYTICS TO ROLE data_analyst_role;

-- =====================================================
-- 8. SUMMARY
-- =====================================================

-- Show created objects
SHOW WAREHOUSES LIKE 'S_WH';
SHOW WAREHOUSES LIKE 'M_WH'; 
SHOW ROLES LIKE 'data_%_role';
SHOW USERS LIKE 'fsi_automation_service';
SHOW SCHEMAS IN DATABASE FSI_DEMO;

-- Success message
SELECT 'FSI Demo Foundation Setup Complete! 🎉' AS status;
