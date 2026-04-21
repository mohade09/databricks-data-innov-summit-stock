-- Bronze Layer: Raw ingestion via Auto Loader
-- Ingests CSV/JSON from UC Volume into streaming tables

-- 1. Customers (CSV)
CREATE OR REFRESH STREAMING TABLE customers
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

-- 2. Products (CSV)
CREATE OR REFRESH STREAMING TABLE products
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

-- 3. Orders (JSON)
CREATE OR REFRESH STREAMING TABLE orders
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

-- 4. Order Items (JSON)
CREATE OR REFRESH STREAMING TABLE order_items
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

-- 5. Invoices (JSON)
CREATE OR REFRESH STREAMING TABLE invoices
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

-- 6. Payments (JSON)
CREATE OR REFRESH STREAMING TABLE payments
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

-- 7. Returns (JSON)
CREATE OR REFRESH STREAMING TABLE returns
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

-- 8. Return Policy (CSV)
CREATE OR REFRESH STREAMING TABLE return_policy
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

-- 9. Shipping Tracking (JSON)
CREATE OR REFRESH STREAMING TABLE shipping_tracking
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
