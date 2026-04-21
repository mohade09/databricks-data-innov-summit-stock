-- Bronze Layer: Raw ingestion via Auto Loader
-- Ingests CSV/JSON from UC Volume into streaming tables
-- All columns carry comments for catalog discoverability

-- 1. Customers (CSV)
CREATE OR REFRESH STREAMING TABLE customers (
  customer_id STRING COMMENT 'Unique customer identifier (CUST-XXXXX)',
  customer_name STRING COMMENT 'Full name of the customer',
  email STRING COMMENT 'Customer email address',
  phone STRING COMMENT 'Customer phone number',
  shipping_address STRING COMMENT 'Default shipping address',
  billing_address STRING COMMENT 'Billing address on file',
  signup_date DATE COMMENT 'Date the customer created their account',
  loyalty_tier STRING COMMENT 'Loyalty program tier: bronze, silver, gold, platinum',
  region STRING COMMENT 'Geographic region: Northeast, West, Midwest, South, Southwest',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Raw customer profiles ingested from CSV'
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
CREATE OR REFRESH STREAMING TABLE products (
  product_id STRING COMMENT 'Unique product identifier (PROD-XXXX)',
  product_name STRING COMMENT 'Product display name including brand',
  category STRING COMMENT 'Top-level product category (Electronics, Clothing, etc.)',
  subcategory STRING COMMENT 'Product subcategory',
  brand STRING COMMENT 'Brand or manufacturer name',
  price DOUBLE COMMENT 'Listed retail price in USD',
  sku STRING COMMENT 'Stock Keeping Unit code',
  return_window_days INT COMMENT 'Number of days the product can be returned after delivery',
  is_returnable BOOLEAN COMMENT 'Whether this product is eligible for returns',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Raw product catalog ingested from CSV'
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
CREATE OR REFRESH STREAMING TABLE orders (
  channel STRING COMMENT 'Order channel: web, app, or phone',
  customer_id STRING COMMENT 'FK to customers table',
  order_date STRING COMMENT 'ISO timestamp when the order was placed',
  order_id STRING COMMENT 'Unique order identifier (ORD-XXXXX)',
  shipping_cost DOUBLE COMMENT 'Shipping charge in USD',
  status STRING COMMENT 'Order status: placed, shipped, delivered, cancelled',
  tax_amount DOUBLE COMMENT 'Tax charged in USD',
  total_amount DOUBLE COMMENT 'Total order amount including tax and shipping',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Raw order transactions ingested from JSON'
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
CREATE OR REFRESH STREAMING TABLE order_items (
  discount_amount DOUBLE COMMENT 'Discount applied to this line item in USD',
  item_status STRING COMMENT 'Status of this line item (mirrors order status)',
  order_id STRING COMMENT 'FK to orders table',
  order_item_id STRING COMMENT 'Unique line item identifier (OI-XXXXXX)',
  product_id STRING COMMENT 'FK to products table',
  quantity BIGINT COMMENT 'Quantity of this product ordered',
  unit_price DOUBLE COMMENT 'Price per unit at time of purchase in USD',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Raw order line items ingested from JSON'
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
CREATE OR REFRESH STREAMING TABLE invoices (
  customer_id STRING COMMENT 'FK to customers table',
  due_date STRING COMMENT 'Payment due date (YYYY-MM-DD)',
  invoice_date STRING COMMENT 'ISO timestamp when invoice was generated',
  invoice_id STRING COMMENT 'Unique invoice identifier (INV-XXXXX)',
  invoice_number STRING COMMENT 'Human-readable invoice number (INV-2025-XXXXX)',
  order_id STRING COMMENT 'FK to orders table',
  pdf_url STRING COMMENT 'Path to the invoice PDF document',
  shipping DOUBLE COMMENT 'Shipping amount on invoice in USD',
  subtotal DOUBLE COMMENT 'Subtotal before tax and shipping in USD',
  tax DOUBLE COMMENT 'Tax amount on invoice in USD',
  total DOUBLE COMMENT 'Total invoice amount in USD',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Raw invoices ingested from JSON'
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
CREATE OR REFRESH STREAMING TABLE payments (
  amount DOUBLE COMMENT 'Payment amount in USD',
  invoice_id STRING COMMENT 'FK to invoices table',
  order_id STRING COMMENT 'FK to orders table',
  paid_at STRING COMMENT 'ISO timestamp when payment was completed (null if pending/failed)',
  payment_id STRING COMMENT 'Unique payment identifier (PAY-XXXXXX)',
  payment_method STRING COMMENT 'Payment method: credit_card, debit, upi, wallet, cod',
  payment_status STRING COMMENT 'Payment status: pending, completed, failed, refunded',
  transaction_ref STRING COMMENT 'External payment gateway transaction reference',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Raw payment transactions ingested from JSON'
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
CREATE OR REFRESH STREAMING TABLE returns (
  completed_at STRING COMMENT 'ISO timestamp when return was completed or rejected (null if in progress)',
  customer_id STRING COMMENT 'FK to customers table',
  order_id STRING COMMENT 'FK to orders table',
  order_item_id STRING COMMENT 'FK to order_items table - specific item being returned',
  product_id STRING COMMENT 'FK to products table',
  refund_amount DOUBLE COMMENT 'Refund amount in USD (0 if rejected)',
  refund_method STRING COMMENT 'Refund method: credit_card, store_credit, original_payment, wallet',
  requested_at STRING COMMENT 'ISO timestamp when return was requested',
  return_id STRING COMMENT 'Unique return identifier (RET-XXXXX)',
  return_reason STRING COMMENT 'Return reason code: DEFECTIVE, WRONG_ITEM, NOT_AS_DESCRIBED, CHANGED_MIND, SIZE_FIT, ARRIVED_LATE',
  return_status STRING COMMENT 'Return status: requested, approved, shipped_back, received, refund_processed, rejected',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Raw return requests ingested from JSON'
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
CREATE OR REFRESH STREAMING TABLE return_policy (
  product_category STRING COMMENT 'Product category this policy applies to (or ALL for default)',
  return_window_days INT COMMENT 'Number of days allowed for returns',
  restocking_fee_pct INT COMMENT 'Restocking fee percentage (0-100)',
  conditions_text STRING COMMENT 'Human-readable return conditions and requirements',
  is_final_sale BOOLEAN COMMENT 'Whether items in this category are final sale (no returns)',
  reason_code STRING COMMENT 'Return reason code this policy row applies to',
  reason_label STRING COMMENT 'Human-readable label for the return reason',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Return policy rules by product category ingested from CSV'
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
CREATE OR REFRESH STREAMING TABLE shipping_tracking (
  carrier STRING COMMENT 'Shipping carrier: FedEx, UPS, USPS, DHL, Amazon Logistics',
  estimated_delivery STRING COMMENT 'Estimated delivery date (YYYY-MM-DD)',
  last_update STRING COMMENT 'ISO timestamp of the last tracking status update',
  order_id STRING COMMENT 'FK to orders table',
  return_id STRING COMMENT 'FK to returns table (null for outbound shipments)',
  status STRING COMMENT 'Shipment status: label_created, picked_up, in_transit, out_for_delivery, delivered',
  tracking_id STRING COMMENT 'Unique tracking identifier (TRK-XXXXXX)',
  tracking_number STRING COMMENT 'Carrier tracking number for customer lookup',
  _rescued_data STRING COMMENT 'Auto Loader rescued data column for schema mismatches',
  source_file STRING COMMENT 'Source file path in UC Volume',
  file_mod_time TIMESTAMP COMMENT 'Last modification time of the source file',
  ingested_at TIMESTAMP COMMENT 'Timestamp when this row was ingested into bronze'
)
COMMENT 'Raw shipment tracking events ingested from JSON'
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
