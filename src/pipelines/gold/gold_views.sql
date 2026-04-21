-- Gold Layer: Customer-facing query-ready views
-- Source: debadm.ecom_silver.*  →  Target: debadm.ecom_gold.*
-- These tables power the Genie Space chatbot for returns & invoices.

-- ============================================================
-- 1. Customer Orders — order history + invoice + payment lookup
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW customer_orders
AS SELECT
  c.customer_id,
  c.customer_name,
  c.email,
  c.loyalty_tier,
  -- Order
  o.order_id,
  o.order_date,
  o.status AS order_status,
  o.total_amount AS order_total,
  o.shipping_cost,
  o.tax_amount,
  o.channel AS order_channel,
  o.days_since_order,
  -- Invoice
  i.invoice_id,
  i.invoice_number,
  i.invoice_date,
  i.subtotal AS invoice_subtotal,
  i.tax AS invoice_tax,
  i.shipping AS invoice_shipping,
  i.total AS invoice_total,
  i.due_date AS invoice_due_date,
  i.is_overdue,
  i.pdf_url AS invoice_pdf_url,
  -- Payment
  p.payment_id,
  p.payment_method,
  p.payment_status,
  p.transaction_ref,
  p.amount AS payment_amount,
  p.paid_at,
  -- Item count
  item_agg.item_count,
  item_agg.items_total
FROM debadm.ecom_silver.customers c
INNER JOIN debadm.ecom_silver.orders o ON c.customer_id = o.customer_id
LEFT JOIN debadm.ecom_silver.invoices i ON o.order_id = i.order_id
LEFT JOIN (
  SELECT invoice_id, payment_id, payment_method, payment_status,
         transaction_ref, amount, paid_at,
         ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY paid_at DESC NULLS LAST) AS rn
  FROM debadm.ecom_silver.payments
  WHERE payment_status = 'completed' OR payment_status = 'pending'
) p ON i.invoice_id = p.invoice_id AND p.rn = 1
LEFT JOIN (
  SELECT order_id,
         COUNT(*) AS item_count,
         SUM(line_total) AS items_total
  FROM debadm.ecom_silver.order_items
  GROUP BY order_id
) item_agg ON o.order_id = item_agg.order_id;

-- ============================================================
-- 2. Customer Returns — return status + tracking
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW customer_returns
AS SELECT
  c.customer_id,
  c.customer_name,
  c.email,
  r.return_id,
  r.order_id,
  r.product_id,
  pr.product_name,
  pr.category AS product_category,
  pr.sku,
  r.return_reason,
  r.reason_label,
  r.return_status,
  r.refund_amount,
  r.refund_method,
  r.requested_at,
  r.completed_at,
  r.days_in_process,
  -- Original order context
  o.order_date,
  o.status AS order_status,
  o.days_since_order,
  oi.quantity AS returned_quantity,
  oi.unit_price AS original_price,
  oi.line_total AS original_line_total,
  -- Return shipment tracking
  s.carrier AS return_carrier,
  s.tracking_number AS return_tracking_number,
  s.status AS return_shipment_status,
  s.estimated_delivery AS return_estimated_arrival,
  s.is_delayed AS return_shipment_delayed,
  -- Friendly status
  CASE
    WHEN r.return_status = 'refund_processed' THEN 'Refund completed'
    WHEN r.return_status = 'received' THEN 'Item received, refund processing (2-5 business days)'
    WHEN r.return_status = 'shipped_back' THEN 'Return in transit'
    WHEN r.return_status = 'approved' THEN 'Return approved, awaiting shipment'
    WHEN r.return_status = 'requested' THEN 'Return request under review'
    WHEN r.return_status = 'rejected' THEN 'Return request rejected'
    ELSE r.return_status
  END AS return_status_description
FROM debadm.ecom_silver.returns r
INNER JOIN debadm.ecom_silver.customers c ON r.customer_id = c.customer_id
LEFT JOIN debadm.ecom_silver.products pr ON r.product_id = pr.product_id
LEFT JOIN debadm.ecom_silver.orders o ON r.order_id = o.order_id
LEFT JOIN debadm.ecom_silver.order_items oi ON r.order_item_id = oi.order_item_id
LEFT JOIN debadm.ecom_silver.shipping s ON r.return_id = s.return_id;

-- ============================================================
-- 3. Return Eligibility — can this item be returned?
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW return_eligibility
AS SELECT
  c.customer_id,
  c.customer_name,
  o.order_id,
  o.order_date,
  o.days_since_order,
  oi.order_item_id,
  pr.product_id,
  pr.product_name,
  pr.category,
  pr.sku,
  oi.quantity,
  oi.line_total,
  -- Policy rules
  pr.return_window_days,
  COALESCE(pr.is_returnable, true) AS is_product_returnable,
  -- Eligibility
  CASE
    WHEN COALESCE(pr.is_returnable, true) = false THEN false
    WHEN o.status = 'cancelled' THEN false
    WHEN o.status = 'placed' THEN false
    WHEN o.days_since_order > pr.return_window_days THEN false
    ELSE true
  END AS is_eligible_for_return,
  -- Reason if not eligible
  CASE
    WHEN COALESCE(pr.is_returnable, true) = false THEN 'This product category is non-returnable'
    WHEN o.status = 'cancelled' THEN 'Order was cancelled'
    WHEN o.status = 'placed' THEN 'Order has not been shipped yet'
    WHEN o.days_since_order > pr.return_window_days
      THEN CONCAT('Return window of ', pr.return_window_days, ' days has expired')
    ELSE 'Eligible for return'
  END AS eligibility_reason,
  -- Days remaining
  GREATEST(0, pr.return_window_days - o.days_since_order) AS days_remaining_to_return,
  -- Estimated refund
  oi.line_total AS estimated_refund_amount
FROM debadm.ecom_silver.customers c
INNER JOIN debadm.ecom_silver.orders o ON c.customer_id = o.customer_id
INNER JOIN debadm.ecom_silver.order_items oi ON o.order_id = oi.order_id
INNER JOIN debadm.ecom_silver.products pr ON oi.product_id = pr.product_id
WHERE o.status IN ('delivered', 'shipped');

-- ============================================================
-- 4. Invoice Details — flat denormalized for chatbot display
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW invoice_details
AS SELECT
  i.invoice_id,
  i.invoice_number,
  i.invoice_date,
  i.customer_id,
  c.customer_name,
  c.email,
  c.billing_address,
  c.shipping_address,
  i.order_id,
  o.order_date,
  o.channel AS order_channel,
  -- Line items
  oi.order_item_id,
  pr.product_name,
  pr.sku,
  pr.category AS product_category,
  pr.brand,
  oi.quantity,
  oi.unit_price,
  oi.discount_amount,
  oi.line_total,
  -- Invoice totals
  i.subtotal,
  i.tax,
  i.shipping,
  i.total AS invoice_total,
  i.due_date,
  i.is_overdue,
  i.pdf_url,
  -- Payment
  p.payment_method,
  p.payment_status,
  p.transaction_ref,
  p.paid_at
FROM debadm.ecom_silver.invoices i
INNER JOIN debadm.ecom_silver.customers c ON i.customer_id = c.customer_id
INNER JOIN debadm.ecom_silver.orders o ON i.order_id = o.order_id
LEFT JOIN debadm.ecom_silver.order_items oi ON o.order_id = oi.order_id
LEFT JOIN debadm.ecom_silver.products pr ON oi.product_id = pr.product_id
LEFT JOIN (
  SELECT invoice_id, payment_method, payment_status, transaction_ref, paid_at,
         ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY paid_at DESC NULLS LAST) AS rn
  FROM debadm.ecom_silver.payments
  WHERE payment_status = 'completed' OR payment_status = 'pending'
) p ON i.invoice_id = p.invoice_id AND p.rn = 1;

-- ============================================================
-- 5. Refund Status — dedicated refund tracking view
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW refund_status
AS SELECT
  r.return_id,
  r.customer_id,
  c.customer_name,
  c.email,
  r.order_id,
  pr.product_name,
  r.refund_amount,
  r.refund_method,
  r.return_status,
  r.requested_at,
  r.completed_at,
  r.days_in_process,
  -- Refund payment details
  p_refund.payment_status AS refund_payment_status,
  p_refund.paid_at AS refund_processed_at,
  p_refund.transaction_ref AS refund_transaction_ref,
  -- Friendly timeline message
  CASE
    WHEN r.return_status = 'refund_processed' THEN 'Refund complete'
    WHEN r.return_status = 'received' AND r.refund_method = 'credit_card'
      THEN 'Refund will appear on your card in 5-10 business days'
    WHEN r.return_status = 'received' AND r.refund_method = 'store_credit'
      THEN 'Store credit will be applied within 24 hours'
    WHEN r.return_status = 'received' AND r.refund_method IN ('wallet', 'original_payment')
      THEN 'Refund will appear in your account in 2-3 business days'
    WHEN r.return_status = 'shipped_back'
      THEN 'Waiting to receive your return - refund will process after inspection'
    WHEN r.return_status = 'approved'
      THEN 'Please ship the item back using the prepaid label'
    WHEN r.return_status = 'requested'
      THEN 'Your return request is being reviewed (1-2 business days)'
    ELSE 'Contact support for details'
  END AS refund_timeline_message,
  -- Original payment for context
  p_orig.payment_method AS original_payment_method,
  p_orig.amount AS original_payment_amount
FROM debadm.ecom_silver.returns r
INNER JOIN debadm.ecom_silver.customers c ON r.customer_id = c.customer_id
LEFT JOIN debadm.ecom_silver.products pr ON r.product_id = pr.product_id
LEFT JOIN (
  SELECT order_id, payment_status, paid_at, transaction_ref,
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC NULLS LAST) AS rn
  FROM debadm.ecom_silver.payments
  WHERE payment_status = 'refunded'
) p_refund ON r.order_id = p_refund.order_id AND p_refund.rn = 1
LEFT JOIN (
  SELECT order_id, payment_method, amount,
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC NULLS LAST) AS rn
  FROM debadm.ecom_silver.payments
  WHERE payment_status = 'completed'
) p_orig ON r.order_id = p_orig.order_id AND p_orig.rn = 1;

-- ============================================================
-- 6. Return Analysis — aggregated patterns for Supervisor Agent
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW return_analysis
AS SELECT
  pr.category AS product_category,
  pr.brand,
  pr.return_window_days AS current_return_window,
  -- Volume
  COUNT(DISTINCT r.return_id) AS total_returns,
  COUNT(DISTINCT o.order_id) AS total_orders_in_category,
  ROUND(COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS return_rate_pct,
  -- Reason breakdown
  SUM(CASE WHEN r.return_reason = 'DEFECTIVE' THEN 1 ELSE 0 END) AS defective_returns,
  SUM(CASE WHEN r.return_reason = 'WRONG_ITEM' THEN 1 ELSE 0 END) AS wrong_item_returns,
  SUM(CASE WHEN r.return_reason = 'NOT_AS_DESCRIBED' THEN 1 ELSE 0 END) AS not_as_described_returns,
  SUM(CASE WHEN r.return_reason = 'CHANGED_MIND' THEN 1 ELSE 0 END) AS changed_mind_returns,
  SUM(CASE WHEN r.return_reason = 'SIZE_FIT' THEN 1 ELSE 0 END) AS size_fit_returns,
  SUM(CASE WHEN r.return_reason = 'ARRIVED_LATE' THEN 1 ELSE 0 END) AS late_arrival_returns,
  -- Financial impact
  SUM(r.refund_amount) AS total_refund_cost,
  ROUND(AVG(r.refund_amount), 2) AS avg_refund_amount,
  ROUND(AVG(oi.line_total), 2) AS avg_item_value,
  -- Timing
  ROUND(AVG(r.days_in_process), 1) AS avg_processing_days,
  -- Rejection rate
  ROUND(SUM(CASE WHEN r.return_status = 'rejected' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rejection_rate_pct,
  -- Month
  DATE_TRUNC('month', r.requested_at) AS return_month
FROM debadm.ecom_silver.returns r
INNER JOIN debadm.ecom_silver.orders o ON r.order_id = o.order_id
INNER JOIN debadm.ecom_silver.order_items oi ON r.order_item_id = oi.order_item_id
INNER JOIN debadm.ecom_silver.products pr ON r.product_id = pr.product_id
GROUP BY ALL;

-- ============================================================
-- 7. Abuse Signals — return abuse detection for Supervisor Agent
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW abuse_signals
AS SELECT
  c.customer_id,
  c.customer_name,
  c.loyalty_tier,
  c.signup_date,
  COUNT(DISTINCT r.return_id) AS total_returns,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS personal_return_rate_pct,
  SUM(r.refund_amount) AS total_refund_value,
  -- Abuse indicators
  SUM(CASE WHEN r.return_reason = 'CHANGED_MIND' THEN 1 ELSE 0 END) AS changed_mind_count,
  -- Abuse risk level
  CASE
    WHEN COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0) > 50 THEN 'high_risk'
    WHEN COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0) > 30 THEN 'medium_risk'
    ELSE 'low_risk'
  END AS abuse_risk_level
FROM debadm.ecom_silver.customers c
INNER JOIN debadm.ecom_silver.orders o ON c.customer_id = o.customer_id
LEFT JOIN debadm.ecom_silver.returns r ON o.order_id = r.order_id
GROUP BY c.customer_id, c.customer_name, c.loyalty_tier, c.signup_date
HAVING COUNT(DISTINCT r.return_id) >= 3;
