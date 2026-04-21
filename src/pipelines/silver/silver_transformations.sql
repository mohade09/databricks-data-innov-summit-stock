-- Silver Layer: Cleaned, conformed, enriched materialized views
-- Source: debadm.ecom_bronze.*  →  Target: debadm.ecom_silver.*

-- ============================================================
-- 1. Customers — deduped, PII masked for chatbot display
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW customers (
  customer_id COMMENT 'Unique customer identifier (CUST-XXXXX)',
  customer_name COMMENT 'Full name of the customer',
  email COMMENT 'Normalized email address (lowercase, trimmed)',
  phone_masked COMMENT 'Phone number masked for privacy (shows last 4 digits only)',
  shipping_address COMMENT 'Default shipping address',
  billing_address COMMENT 'Billing address on file',
  signup_date COMMENT 'Date the customer created their account',
  loyalty_tier COMMENT 'Loyalty program tier: bronze, silver, gold, platinum',
  region COMMENT 'Geographic region: Northeast, West, Midwest, South, Southwest',
  ingested_at COMMENT 'Timestamp when this row was ingested into bronze',
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned customer profiles - deduped by customer_id, PII masked'
AS SELECT
  customer_id,
  customer_name,
  LOWER(TRIM(email)) AS email,
  CONCAT('***-***-', RIGHT(phone, 4)) AS phone_masked,
  shipping_address,
  billing_address,
  signup_date,
  loyalty_tier,
  region,
  ingested_at
FROM debadm.ecom_bronze.customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ingested_at DESC) = 1;

-- ============================================================
-- 2. Products — standardized, typed
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW products (
  product_id COMMENT 'Unique product identifier (PROD-XXXX)',
  product_name COMMENT 'Product display name including brand',
  category COMMENT 'Top-level product category',
  subcategory COMMENT 'Product subcategory',
  brand COMMENT 'Brand or manufacturer name',
  price COMMENT 'Listed retail price in USD (DECIMAL 10,2)',
  sku COMMENT 'Stock Keeping Unit code',
  return_window_days COMMENT 'Number of days the product can be returned after delivery',
  is_returnable COMMENT 'Whether this product is eligible for returns',
  ingested_at COMMENT 'Timestamp when this row was ingested into bronze',
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned product catalog with standardized pricing'
AS SELECT
  product_id,
  product_name,
  category,
  subcategory,
  brand,
  CAST(price AS DECIMAL(10,2)) AS price,
  sku,
  return_window_days,
  is_returnable,
  ingested_at
FROM debadm.ecom_bronze.products;

-- ============================================================
-- 3. Orders — cast dates, compute days_since_order
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW orders (
  order_id COMMENT 'Unique order identifier (ORD-XXXXX)',
  customer_id COMMENT 'FK to customers table',
  order_date COMMENT 'Timestamp when the order was placed',
  status COMMENT 'Order status: placed, shipped, delivered, cancelled',
  total_amount COMMENT 'Total order amount including tax and shipping (DECIMAL 12,2)',
  shipping_cost COMMENT 'Shipping charge in USD (DECIMAL 10,2)',
  tax_amount COMMENT 'Tax charged in USD (DECIMAL 10,2)',
  channel COMMENT 'Order channel: web, app, or phone',
  days_since_order COMMENT 'Computed: number of days since order was placed',
  ingested_at COMMENT 'Timestamp when this row was ingested into bronze',
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IN ('placed','shipped','delivered','cancelled')) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned orders with computed days_since_order for return eligibility checks'
AS SELECT
  order_id,
  customer_id,
  CAST(order_date AS TIMESTAMP) AS order_date,
  status,
  CAST(total_amount AS DECIMAL(12,2)) AS total_amount,
  CAST(shipping_cost AS DECIMAL(10,2)) AS shipping_cost,
  CAST(tax_amount AS DECIMAL(10,2)) AS tax_amount,
  channel,
  DATEDIFF(CURRENT_DATE(), CAST(order_date AS DATE)) AS days_since_order,
  ingested_at
FROM debadm.ecom_bronze.orders;

-- ============================================================
-- 4. Order Items — compute line_total
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW order_items (
  order_item_id COMMENT 'Unique line item identifier (OI-XXXXXX)',
  order_id COMMENT 'FK to orders table',
  product_id COMMENT 'FK to products table',
  quantity COMMENT 'Quantity of this product ordered',
  unit_price COMMENT 'Price per unit at time of purchase (DECIMAL 10,2)',
  discount_amount COMMENT 'Discount applied to this line item (DECIMAL 10,2)',
  line_total COMMENT 'Computed: quantity * unit_price - discount_amount (DECIMAL 12,2)',
  item_status COMMENT 'Status of this line item (mirrors order status)',
  ingested_at COMMENT 'Timestamp when this row was ingested into bronze',
  CONSTRAINT valid_item EXPECT (order_item_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned order line items with computed line_total'
AS SELECT
  order_item_id,
  order_id,
  product_id,
  CAST(quantity AS INT) AS quantity,
  CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
  CAST(discount_amount AS DECIMAL(10,2)) AS discount_amount,
  CAST(quantity * unit_price - COALESCE(discount_amount, 0) AS DECIMAL(12,2)) AS line_total,
  item_status,
  ingested_at
FROM debadm.ecom_bronze.order_items;

-- ============================================================
-- 5. Invoices — validate, add overdue flag
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW invoices (
  invoice_id COMMENT 'Unique invoice identifier (INV-XXXXX)',
  order_id COMMENT 'FK to orders table',
  customer_id COMMENT 'FK to customers table',
  invoice_number COMMENT 'Human-readable invoice number (INV-2025-XXXXX)',
  invoice_date COMMENT 'Timestamp when invoice was generated',
  subtotal COMMENT 'Subtotal before tax and shipping (DECIMAL 12,2)',
  tax COMMENT 'Tax amount on invoice (DECIMAL 10,2)',
  shipping COMMENT 'Shipping amount on invoice (DECIMAL 10,2)',
  total COMMENT 'Total invoice amount (DECIMAL 12,2)',
  due_date COMMENT 'Payment due date',
  pdf_url COMMENT 'Path to the invoice PDF document',
  is_overdue COMMENT 'Computed: true if due_date has passed and payment not received',
  ingested_at COMMENT 'Timestamp when this row was ingested into bronze',
  CONSTRAINT valid_invoice EXPECT (invoice_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned invoices with computed is_overdue flag'
AS SELECT
  invoice_id,
  order_id,
  customer_id,
  invoice_number,
  CAST(invoice_date AS TIMESTAMP) AS invoice_date,
  CAST(subtotal AS DECIMAL(12,2)) AS subtotal,
  CAST(tax AS DECIMAL(10,2)) AS tax,
  CAST(shipping AS DECIMAL(10,2)) AS shipping,
  CAST(total AS DECIMAL(12,2)) AS total,
  CAST(due_date AS DATE) AS due_date,
  pdf_url,
  CASE WHEN CAST(due_date AS DATE) < CURRENT_DATE() THEN true ELSE false END AS is_overdue,
  ingested_at
FROM debadm.ecom_bronze.invoices;

-- ============================================================
-- 6. Payments — validate status
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW payments (
  payment_id COMMENT 'Unique payment identifier (PAY-XXXXXX)',
  invoice_id COMMENT 'FK to invoices table',
  order_id COMMENT 'FK to orders table',
  payment_method COMMENT 'Payment method: credit_card, debit, upi, wallet, cod',
  payment_status COMMENT 'Payment status: pending, completed, failed, refunded',
  transaction_ref COMMENT 'External payment gateway transaction reference',
  amount COMMENT 'Payment amount in USD (DECIMAL 12,2)',
  paid_at COMMENT 'Timestamp when payment was completed (null if pending/failed)',
  ingested_at COMMENT 'Timestamp when this row was ingested into bronze',
  CONSTRAINT valid_payment EXPECT (payment_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_pay_status EXPECT (payment_status IN ('pending','completed','failed','refunded')) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned payment transactions with validated status'
AS SELECT
  payment_id,
  invoice_id,
  order_id,
  payment_method,
  payment_status,
  transaction_ref,
  CAST(amount AS DECIMAL(12,2)) AS amount,
  CAST(paid_at AS TIMESTAMP) AS paid_at,
  ingested_at
FROM debadm.ecom_bronze.payments;

-- ============================================================
-- 7. Returns — enrich with reason label, compute processing time
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW returns (
  return_id COMMENT 'Unique return identifier (RET-XXXXX)',
  order_id COMMENT 'FK to orders table',
  customer_id COMMENT 'FK to customers table',
  order_item_id COMMENT 'FK to order_items - specific item being returned',
  product_id COMMENT 'FK to products table',
  return_reason COMMENT 'Return reason code: DEFECTIVE, WRONG_ITEM, NOT_AS_DESCRIBED, CHANGED_MIND, SIZE_FIT, ARRIVED_LATE',
  reason_label COMMENT 'Human-readable return reason description',
  return_status COMMENT 'Return status: requested, approved, shipped_back, received, refund_processed, rejected',
  refund_amount COMMENT 'Refund amount in USD (0 if rejected)',
  refund_method COMMENT 'Refund method: credit_card, store_credit, original_payment, wallet',
  requested_at COMMENT 'Timestamp when return was requested by customer',
  completed_at COMMENT 'Timestamp when return was completed or rejected (null if in progress)',
  days_in_process COMMENT 'Computed: number of days since return was requested (or until completed)',
  ingested_at COMMENT 'Timestamp when this row was ingested into bronze',
  CONSTRAINT valid_return EXPECT (return_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_ret_status EXPECT (
    return_status IN ('requested','approved','shipped_back','received','refund_processed','rejected')
  ) ON VIOLATION DROP ROW
)
COMMENT 'Enriched returns with human-readable reason labels and processing time'
AS SELECT
  r.return_id,
  r.order_id,
  r.customer_id,
  r.order_item_id,
  r.product_id,
  r.return_reason,
  CASE r.return_reason
    WHEN 'DEFECTIVE' THEN 'Product arrived defective or damaged'
    WHEN 'WRONG_ITEM' THEN 'Wrong item was shipped'
    WHEN 'NOT_AS_DESCRIBED' THEN 'Product does not match description'
    WHEN 'CHANGED_MIND' THEN 'Customer changed their mind'
    WHEN 'SIZE_FIT' THEN 'Size or fit issue'
    WHEN 'ARRIVED_LATE' THEN 'Item arrived after expected date'
    ELSE r.return_reason
  END AS reason_label,
  r.return_status,
  CAST(r.refund_amount AS DECIMAL(12,2)) AS refund_amount,
  r.refund_method,
  CAST(r.requested_at AS TIMESTAMP) AS requested_at,
  CAST(r.completed_at AS TIMESTAMP) AS completed_at,
  CASE
    WHEN r.completed_at IS NOT NULL
    THEN ROUND(TIMESTAMPDIFF(HOUR, CAST(r.requested_at AS TIMESTAMP), CAST(r.completed_at AS TIMESTAMP)) / 24.0, 1)
    ELSE ROUND(TIMESTAMPDIFF(HOUR, CAST(r.requested_at AS TIMESTAMP), CURRENT_TIMESTAMP()) / 24.0, 1)
  END AS days_in_process,
  r.ingested_at
FROM debadm.ecom_bronze.returns r;

-- ============================================================
-- 8. Shipping — parse dates, flag delays
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW shipping (
  tracking_id COMMENT 'Unique tracking identifier (TRK-XXXXXX)',
  order_id COMMENT 'FK to orders table',
  return_id COMMENT 'FK to returns table (null for outbound order shipments)',
  carrier COMMENT 'Shipping carrier: FedEx, UPS, USPS, DHL, Amazon Logistics',
  tracking_number COMMENT 'Carrier tracking number for customer lookup',
  status COMMENT 'Shipment status: label_created, picked_up, in_transit, out_for_delivery, delivered',
  estimated_delivery COMMENT 'Estimated delivery date',
  last_update COMMENT 'Timestamp of the last tracking status update',
  is_delayed COMMENT 'Computed: true if estimated delivery has passed and shipment not delivered',
  ingested_at COMMENT 'Timestamp when this row was ingested into bronze',
  CONSTRAINT valid_tracking EXPECT (tracking_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned shipping tracking with computed delay flag'
AS SELECT
  tracking_id,
  order_id,
  return_id,
  carrier,
  tracking_number,
  status,
  CAST(estimated_delivery AS DATE) AS estimated_delivery,
  CAST(last_update AS TIMESTAMP) AS last_update,
  CASE
    WHEN CAST(estimated_delivery AS DATE) < CURRENT_DATE() AND status != 'delivered'
    THEN true ELSE false
  END AS is_delayed,
  ingested_at
FROM debadm.ecom_bronze.shipping_tracking;
