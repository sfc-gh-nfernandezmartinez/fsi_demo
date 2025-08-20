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
-- 1. FINOPS-OPTIMIZED WAREHOUSES
-- =====================================================
-- Workload-based warehouse strategy for fine-grained cost control
-- Naming convention: [WORKLOAD]_WH_[SIZE] for immediate cost visibility


-- 1. Ingestion Warehouse: Fast, frequent, small jobs
CREATE OR ALTER WAREHOUSE INGESTION_WH_XS
  WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 30              -- Fast suspend for cost optimization
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1          -- No scaling needed for ingestion
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'FinOps: X-Small warehouse for streaming and batch data ingestion';

-- 2. Transformation Warehouse: dbt and data processing
CREATE OR ALTER WAREHOUSE TRANSFORMATION_WH_S  
  WITH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60              -- Moderate suspend for transformation workloads
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2          -- Can scale for heavy transformations
    SCALING_POLICY = 'ECONOMY'     -- Cost-optimized scaling
    COMMENT = 'FinOps: Small warehouse for dbt transformations and data processing';

-- 3. Analytics Warehouse: BI, dashboards, concurrent users
CREATE OR ALTER WAREHOUSE ANALYTICS_WH_S
  WITH
    WAREHOUSE_SIZE = 'SMALL'  
    AUTO_SUSPEND = 300             -- 5 minutes for better user experience
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3          -- Multi-cluster for concurrent BI users
    SCALING_POLICY = 'STANDARD'    -- Responsive scaling for user experience
    COMMENT = 'FinOps: Small warehouse for analytics and BI with multi-cluster support';

-- 4. ML/Data Science Warehouse: On-demand for compute-intensive work
CREATE OR ALTER WAREHOUSE ML_WH_M
  WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60              -- Quick suspend when not actively used  
    AUTO_RESUME = FALSE            -- Manual control for cost management
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1          -- Single cluster, manually scaled when needed
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'FinOps: Medium warehouse for ML/data science - manual control for cost optimization';

-- 5. Development Warehouse: Aggressive cost controls for development
CREATE OR ALTER WAREHOUSE DEV_WH_XS
  WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60              -- Quick suspend for development cost control
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1  
    MAX_CLUSTER_COUNT = 1          -- No scaling for development
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'FinOps: X-Small warehouse for development with aggressive cost controls';

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
    DEFAULT_WAREHOUSE = 'INGESTION_WH_XS'  -- FinOps: Default to ingestion warehouse
    DEFAULT_NAMESPACE = 'FSI_DEMO.RAW_DATA'
    DEFAULT_ROLE = 'data_engineer_role'
    COMMENT = 'Service account for FSI demo automation (streaming, dbt, etc)';

-- Grant data_engineer_role to service account
GRANT ROLE data_engineer_role TO USER fsi_automation_service;

-- =====================================================
-- 6. FINOPS WAREHOUSE PERMISSIONS
-- =====================================================

-- Data Engineer: Ingestion, Transformation, and Development access
GRANT USAGE ON WAREHOUSE INGESTION_WH_XS TO ROLE data_engineer_role;
GRANT OPERATE ON WAREHOUSE INGESTION_WH_XS TO ROLE data_engineer_role;
GRANT USAGE ON WAREHOUSE TRANSFORMATION_WH_S TO ROLE data_engineer_role;
GRANT OPERATE ON WAREHOUSE TRANSFORMATION_WH_S TO ROLE data_engineer_role;
GRANT USAGE ON WAREHOUSE DEV_WH_XS TO ROLE data_engineer_role;
GRANT OPERATE ON WAREHOUSE DEV_WH_XS TO ROLE data_engineer_role;

-- Data Analyst: Analytics, Transformation (usage only), and Development
GRANT USAGE ON WAREHOUSE ANALYTICS_WH_S TO ROLE data_analyst_role;
GRANT USAGE ON WAREHOUSE TRANSFORMATION_WH_S TO ROLE data_analyst_role;
GRANT USAGE ON WAREHOUSE DEV_WH_XS TO ROLE data_analyst_role;

-- Both roles retain access to existing XS_WH for compatibility
GRANT USAGE ON WAREHOUSE XS_WH TO ROLE data_engineer_role;
GRANT USAGE ON WAREHOUSE XS_WH TO ROLE data_analyst_role;

-- Note: ML_WH_M permissions will be granted to data_scientist_role when created

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
SHOW WAREHOUSES LIKE '%_WH_%';
SHOW ROLES LIKE 'data_%_role';
SHOW USERS LIKE 'fsi_automation_service';
SHOW SCHEMAS IN DATABASE FSI_DEMO;

-- Success message
SELECT 'FSI Demo FinOps Foundation Setup Complete! 🎯🎉' AS status,
       'Cost-optimized workload-based warehouse strategy deployed' AS details;
