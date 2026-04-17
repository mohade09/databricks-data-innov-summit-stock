-- ============================================================
-- Phase 1: Unity Catalog Setup
-- Catalog: debadm (pre-existing)
-- Workspace: fe-vm-vdm-serverless-iwnbow.cloud.databricks.com
-- ============================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS debadm.ecom_bronze
  COMMENT 'E-commerce chatbot - raw ingested tables (Bronze layer)';

CREATE SCHEMA IF NOT EXISTS debadm.ecom_silver
  COMMENT 'E-commerce chatbot - cleaned and conformed tables (Silver layer)';

CREATE SCHEMA IF NOT EXISTS debadm.ecom_gold
  COMMENT 'E-commerce chatbot - business aggregates (Gold layer)';

CREATE SCHEMA IF NOT EXISTS debadm.ecom_metrics
  COMMENT 'E-commerce chatbot - UC Metric Views (Semantic layer)';

CREATE SCHEMA IF NOT EXISTS debadm.ecom_raw_data
  COMMENT 'E-commerce chatbot - UC Volume for source files';

-- Volume for landing raw files
CREATE VOLUME IF NOT EXISTS debadm.ecom_raw_data.landing
  COMMENT 'Landing zone for raw CSV/JSON source files';
