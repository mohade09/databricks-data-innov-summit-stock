-- Silver Layer: Cleaned, conformed, enriched materialized views
-- Source: debadm.ecom_bronze.*  →  Target: debadm.ecom_silver.*

-- ============================================================
-- 1. Customers — deduped, PII masked for chatbot display
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW customers (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
)
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
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW
)
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
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IN ('placed','shipped','delivered','cancelled')) ON VIOLATION DROP ROW
)
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
  CONSTRAINT valid_item EXPECT (order_item_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW
)
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
  CONSTRAINT valid_invoice EXPECT (invoice_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW
)
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
-- 6. Payments — validate status, mask card info
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW payments (
  CONSTRAINT valid_payment EXPECT (payment_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_pay_status EXPECT (payment_status IN ('pending','completed','failed','refunded')) ON VIOLATION DROP ROW
)
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
  CONSTRAINT valid_return EXPECT (return_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_ret_status EXPECT (
    return_status IN ('requested','approved','shipped_back','received','refund_processed','rejected')
  ) ON VIOLATION DROP ROW
)
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
  CONSTRAINT valid_tracking EXPECT (tracking_id IS NOT NULL) ON VIOLATION DROP ROW
)
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
