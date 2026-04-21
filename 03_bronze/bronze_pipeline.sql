-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer — Raw Ingestion via Auto Loader
-- MAGIC
-- MAGIC SDP Streaming Tables that ingest CSV/JSON files from UC Volume
-- MAGIC `debadm.ecom_raw_data.landing` into `debadm.ecom_bronze`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Customers (CSV)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.customers
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/customers/',
  format => 'csv',
  header => true,
  inferColumnTypes => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Products (CSV)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.products
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/products/',
  format => 'csv',
  header => true,
  inferColumnTypes => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Orders (JSON)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.orders
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/orders/',
  format => 'json',
  inferColumnTypes => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Order Items (JSON)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.order_items
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/order_items/',
  format => 'json',
  inferColumnTypes => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Invoices (JSON)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.invoices
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/invoices/',
  format => 'json',
  inferColumnTypes => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Payments (JSON)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.payments
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/payments/',
  format => 'json',
  inferColumnTypes => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Returns (JSON)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.returns
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/returns/',
  format => 'json',
  inferColumnTypes => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Return Policy (CSV)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.return_policy
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/return_policy/',
  format => 'csv',
  header => true,
  inferColumnTypes => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Shipping Tracking (JSON)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE debadm.ecom_bronze.shipping_tracking
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/debadm/ecom_raw_data/landing/shipping_tracking/',
  format => 'json',
  inferColumnTypes => true
);
