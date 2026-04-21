-- Gold Layer: Customer-facing query-ready views
-- Source: debadm.ecom_silver.*  →  Target: debadm.ecom_gold.*
-- These tables power the Genie Space chatbot for returns & invoices.

-- ============================================================
-- 1. Customer Orders — order history + invoice + payment lookup
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW customer_orders (
  customer_id COMMENT 'Unique customer identifier',
  customer_name COMMENT 'Full name of the customer',
  email COMMENT 'Customer email address (normalized)',
  loyalty_tier COMMENT 'Customer loyalty tier: bronze, silver, gold, platinum',
  order_id COMMENT 'Unique order identifier',
  order_date COMMENT 'Timestamp when the order was placed',
  order_status COMMENT 'Current order status: placed, shipped, delivered, cancelled',
  order_total COMMENT 'Total order amount including tax and shipping in USD',
  shipping_cost COMMENT 'Shipping charge in USD',
  tax_amount COMMENT 'Tax charged in USD',
  order_channel COMMENT 'Channel where order was placed: web, app, phone',
  days_since_order COMMENT 'Number of days since order was placed',
  invoice_id COMMENT 'Invoice identifier linked to this order',
  invoice_number COMMENT 'Human-readable invoice number for customer display',
  invoice_date COMMENT 'Timestamp when the invoice was generated',
  invoice_subtotal COMMENT 'Invoice subtotal before tax and shipping',
  invoice_tax COMMENT 'Tax amount on invoice',
  invoice_shipping COMMENT 'Shipping amount on invoice',
  invoice_total COMMENT 'Total invoice amount',
  invoice_due_date COMMENT 'Payment due date for the invoice',
  is_overdue COMMENT 'True if invoice payment is past due',
  invoice_pdf_url COMMENT 'URL to download the invoice PDF',
  payment_id COMMENT 'Payment identifier for the most recent successful payment',
  payment_method COMMENT 'Payment method used: credit_card, debit, upi, wallet, cod',
  payment_status COMMENT 'Payment status: pending, completed, failed, refunded',
  transaction_ref COMMENT 'External payment gateway transaction reference',
  payment_amount COMMENT 'Amount paid in USD',
  paid_at COMMENT 'Timestamp when payment was completed',
  item_count COMMENT 'Number of line items in the order',
  items_total COMMENT 'Sum of all line item totals in USD'
)
COMMENT 'Denormalized order view joining customer, order, invoice, and payment for chatbot order history and invoice lookup'
AS SELECT
  c.customer_id,
  c.customer_name,
  c.email,
  c.loyalty_tier,
  o.order_id,
  o.order_date,
  o.status AS order_status,
  o.total_amount AS order_total,
  o.shipping_cost,
  o.tax_amount,
  o.channel AS order_channel,
  o.days_since_order,
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
  p.payment_id,
  p.payment_method,
  p.payment_status,
  p.transaction_ref,
  p.amount AS payment_amount,
  p.paid_at,
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
CREATE OR REFRESH MATERIALIZED VIEW customer_returns (
  customer_id COMMENT 'Customer who initiated the return',
  customer_name COMMENT 'Full name of the customer',
  email COMMENT 'Customer email address',
  return_id COMMENT 'Unique return identifier',
  order_id COMMENT 'Original order being returned',
  product_id COMMENT 'Product being returned',
  product_name COMMENT 'Display name of the returned product',
  product_category COMMENT 'Category of the returned product',
  sku COMMENT 'SKU of the returned product',
  return_reason COMMENT 'Return reason code',
  reason_label COMMENT 'Human-readable return reason',
  return_status COMMENT 'Current return status',
  refund_amount COMMENT 'Refund amount in USD',
  refund_method COMMENT 'How the refund will be issued',
  requested_at COMMENT 'When the customer requested the return',
  completed_at COMMENT 'When the return was completed or rejected',
  days_in_process COMMENT 'Days since return was requested',
  order_date COMMENT 'When the original order was placed',
  order_status COMMENT 'Current status of the original order',
  days_since_order COMMENT 'Days since the original order was placed',
  returned_quantity COMMENT 'Quantity of items being returned',
  original_price COMMENT 'Original unit price paid',
  original_line_total COMMENT 'Original line total for the returned item',
  return_carrier COMMENT 'Carrier handling the return shipment',
  return_tracking_number COMMENT 'Tracking number for the return shipment',
  return_shipment_status COMMENT 'Current status of the return shipment',
  return_estimated_arrival COMMENT 'Estimated arrival date for the return shipment',
  return_shipment_delayed COMMENT 'True if return shipment is delayed',
  return_status_description COMMENT 'Customer-friendly description of the return status'
)
COMMENT 'Complete return view with tracking and friendly status messages for chatbot return status queries'
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
  o.order_date,
  o.status AS order_status,
  o.days_since_order,
  oi.quantity AS returned_quantity,
  oi.unit_price AS original_price,
  oi.line_total AS original_line_total,
  s.carrier AS return_carrier,
  s.tracking_number AS return_tracking_number,
  s.status AS return_shipment_status,
  s.estimated_delivery AS return_estimated_arrival,
  s.is_delayed AS return_shipment_delayed,
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
CREATE OR REFRESH MATERIALIZED VIEW return_eligibility (
  customer_id COMMENT 'Customer who placed the order',
  customer_name COMMENT 'Full name of the customer',
  order_id COMMENT 'Order containing the item',
  order_date COMMENT 'When the order was placed',
  days_since_order COMMENT 'Days since the order was placed',
  order_item_id COMMENT 'Specific line item to evaluate',
  product_id COMMENT 'Product identifier',
  product_name COMMENT 'Display name of the product',
  category COMMENT 'Product category',
  sku COMMENT 'Product SKU',
  quantity COMMENT 'Quantity purchased',
  line_total COMMENT 'Total paid for this line item',
  return_window_days COMMENT 'Number of days allowed for return per product policy',
  is_product_returnable COMMENT 'Whether this product type allows returns',
  is_eligible_for_return COMMENT 'Computed: true if the item can be returned right now',
  eligibility_reason COMMENT 'Human-readable explanation of eligibility status',
  days_remaining_to_return COMMENT 'Days left in the return window (0 if expired)',
  estimated_refund_amount COMMENT 'Estimated refund amount in USD'
)
COMMENT 'Return eligibility check for each delivered/shipped order item - powers the can-I-return-this chatbot flow'
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
  pr.return_window_days,
  COALESCE(pr.is_returnable, true) AS is_product_returnable,
  CASE
    WHEN COALESCE(pr.is_returnable, true) = false THEN false
    WHEN o.status = 'cancelled' THEN false
    WHEN o.status = 'placed' THEN false
    WHEN o.days_since_order > pr.return_window_days THEN false
    ELSE true
  END AS is_eligible_for_return,
  CASE
    WHEN COALESCE(pr.is_returnable, true) = false THEN 'This product category is non-returnable'
    WHEN o.status = 'cancelled' THEN 'Order was cancelled'
    WHEN o.status = 'placed' THEN 'Order has not been shipped yet'
    WHEN o.days_since_order > pr.return_window_days
      THEN CONCAT('Return window of ', pr.return_window_days, ' days has expired')
    ELSE 'Eligible for return'
  END AS eligibility_reason,
  GREATEST(0, pr.return_window_days - o.days_since_order) AS days_remaining_to_return,
  oi.line_total AS estimated_refund_amount
FROM debadm.ecom_silver.customers c
INNER JOIN debadm.ecom_silver.orders o ON c.customer_id = o.customer_id
INNER JOIN debadm.ecom_silver.order_items oi ON o.order_id = oi.order_id
INNER JOIN debadm.ecom_silver.products pr ON oi.product_id = pr.product_id
WHERE o.status IN ('delivered', 'shipped');

-- ============================================================
-- 4. Invoice Details — flat denormalized for chatbot display
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW invoice_details (
  invoice_id COMMENT 'Unique invoice identifier',
  invoice_number COMMENT 'Human-readable invoice number for customer display',
  invoice_date COMMENT 'When the invoice was generated',
  customer_id COMMENT 'Customer who was invoiced',
  customer_name COMMENT 'Full name of the customer',
  email COMMENT 'Customer email address',
  billing_address COMMENT 'Billing address on the invoice',
  shipping_address COMMENT 'Shipping address for the order',
  order_id COMMENT 'Order associated with this invoice',
  order_date COMMENT 'When the order was placed',
  order_channel COMMENT 'Channel where order was placed: web, app, phone',
  order_item_id COMMENT 'Line item identifier',
  product_name COMMENT 'Product name for this line item',
  sku COMMENT 'Product SKU',
  product_category COMMENT 'Product category',
  brand COMMENT 'Product brand',
  quantity COMMENT 'Quantity of this item',
  unit_price COMMENT 'Price per unit at time of purchase',
  discount_amount COMMENT 'Discount applied to this line item',
  line_total COMMENT 'Line item total (quantity * unit_price - discount)',
  subtotal COMMENT 'Invoice subtotal before tax and shipping',
  tax COMMENT 'Tax amount on invoice',
  shipping COMMENT 'Shipping amount on invoice',
  invoice_total COMMENT 'Total invoice amount',
  due_date COMMENT 'Payment due date',
  is_overdue COMMENT 'True if invoice payment is past due',
  pdf_url COMMENT 'URL to download the invoice PDF',
  payment_method COMMENT 'Payment method used',
  payment_status COMMENT 'Current payment status',
  transaction_ref COMMENT 'Payment gateway transaction reference',
  paid_at COMMENT 'When payment was completed'
)
COMMENT 'Denormalized invoice with line items and payment status - powers the invoice lookup chatbot flow'
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
  oi.order_item_id,
  pr.product_name,
  pr.sku,
  pr.category AS product_category,
  pr.brand,
  oi.quantity,
  oi.unit_price,
  oi.discount_amount,
  oi.line_total,
  i.subtotal,
  i.tax,
  i.shipping,
  i.total AS invoice_total,
  i.due_date,
  i.is_overdue,
  i.pdf_url,
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
CREATE OR REFRESH MATERIALIZED VIEW refund_status (
  return_id COMMENT 'Unique return identifier',
  customer_id COMMENT 'Customer who requested the return',
  customer_name COMMENT 'Full name of the customer',
  email COMMENT 'Customer email address',
  order_id COMMENT 'Original order being returned',
  product_name COMMENT 'Name of the returned product',
  refund_amount COMMENT 'Refund amount in USD',
  refund_method COMMENT 'How the refund is being issued',
  return_status COMMENT 'Current return/refund status',
  requested_at COMMENT 'When the return was requested',
  completed_at COMMENT 'When the return was completed',
  days_in_process COMMENT 'Days since return was requested',
  refund_payment_status COMMENT 'Payment status of the refund transaction',
  refund_processed_at COMMENT 'When the refund payment was processed',
  refund_transaction_ref COMMENT 'Transaction reference for the refund',
  refund_timeline_message COMMENT 'Customer-friendly message about refund timing and next steps',
  original_payment_method COMMENT 'Original payment method used for the order',
  original_payment_amount COMMENT 'Original payment amount for the order'
)
COMMENT 'Refund tracking view with customer-friendly timeline messages - powers the where-is-my-refund chatbot flow'
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
  p_refund.payment_status AS refund_payment_status,
  p_refund.paid_at AS refund_processed_at,
  p_refund.transaction_ref AS refund_transaction_ref,
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
CREATE OR REFRESH MATERIALIZED VIEW return_analysis (
  product_category COMMENT 'Product category being analyzed',
  brand COMMENT 'Brand within the category',
  current_return_window COMMENT 'Current return window in days for this product',
  total_returns COMMENT 'Total number of returns in this category/brand/month',
  total_orders_in_category COMMENT 'Total orders containing products in this category',
  return_rate_pct COMMENT 'Return rate as percentage of orders',
  defective_returns COMMENT 'Count of returns due to defective products',
  wrong_item_returns COMMENT 'Count of returns due to wrong item shipped',
  not_as_described_returns COMMENT 'Count of returns due to product not matching description',
  changed_mind_returns COMMENT 'Count of returns where customer changed their mind',
  size_fit_returns COMMENT 'Count of returns due to size or fit issues',
  late_arrival_returns COMMENT 'Count of returns due to late delivery',
  total_refund_cost COMMENT 'Total refund cost in USD for this segment',
  avg_refund_amount COMMENT 'Average refund amount per return in USD',
  avg_item_value COMMENT 'Average item value for returned products in USD',
  avg_processing_days COMMENT 'Average number of days to process returns',
  rejection_rate_pct COMMENT 'Percentage of return requests that were rejected',
  return_month COMMENT 'Month when returns were requested (truncated to first of month)'
)
COMMENT 'Aggregated return patterns by category, brand, and month - input for Supervisor Agent policy analysis'
AS SELECT
  pr.category AS product_category,
  pr.brand,
  pr.return_window_days AS current_return_window,
  COUNT(DISTINCT r.return_id) AS total_returns,
  COUNT(DISTINCT o.order_id) AS total_orders_in_category,
  ROUND(COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS return_rate_pct,
  SUM(CASE WHEN r.return_reason = 'DEFECTIVE' THEN 1 ELSE 0 END) AS defective_returns,
  SUM(CASE WHEN r.return_reason = 'WRONG_ITEM' THEN 1 ELSE 0 END) AS wrong_item_returns,
  SUM(CASE WHEN r.return_reason = 'NOT_AS_DESCRIBED' THEN 1 ELSE 0 END) AS not_as_described_returns,
  SUM(CASE WHEN r.return_reason = 'CHANGED_MIND' THEN 1 ELSE 0 END) AS changed_mind_returns,
  SUM(CASE WHEN r.return_reason = 'SIZE_FIT' THEN 1 ELSE 0 END) AS size_fit_returns,
  SUM(CASE WHEN r.return_reason = 'ARRIVED_LATE' THEN 1 ELSE 0 END) AS late_arrival_returns,
  SUM(r.refund_amount) AS total_refund_cost,
  ROUND(AVG(r.refund_amount), 2) AS avg_refund_amount,
  ROUND(AVG(oi.line_total), 2) AS avg_item_value,
  ROUND(AVG(r.days_in_process), 1) AS avg_processing_days,
  ROUND(SUM(CASE WHEN r.return_status = 'rejected' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rejection_rate_pct,
  DATE_TRUNC('month', r.requested_at) AS return_month
FROM debadm.ecom_silver.returns r
INNER JOIN debadm.ecom_silver.orders o ON r.order_id = o.order_id
INNER JOIN debadm.ecom_silver.order_items oi ON r.order_item_id = oi.order_item_id
INNER JOIN debadm.ecom_silver.products pr ON r.product_id = pr.product_id
GROUP BY ALL;

-- ============================================================
-- 7. Abuse Signals — return abuse detection for Supervisor Agent
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW abuse_signals (
  customer_id COMMENT 'Customer being evaluated for return abuse',
  customer_name COMMENT 'Full name of the customer',
  loyalty_tier COMMENT 'Customer loyalty tier',
  signup_date COMMENT 'When the customer signed up',
  total_returns COMMENT 'Total number of returns by this customer',
  total_orders COMMENT 'Total number of orders by this customer',
  personal_return_rate_pct COMMENT 'Customer return rate as percentage of their orders',
  total_refund_value COMMENT 'Total refund value received by this customer in USD',
  changed_mind_count COMMENT 'Number of returns with reason changed_mind (abuse indicator)',
  abuse_risk_level COMMENT 'Risk classification: high_risk (>50%), medium_risk (>30%), low_risk'
)
COMMENT 'Return abuse detection signals by customer - input for Supervisor Agent policy recommendations'
AS SELECT
  c.customer_id,
  c.customer_name,
  c.loyalty_tier,
  c.signup_date,
  COUNT(DISTINCT r.return_id) AS total_returns,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS personal_return_rate_pct,
  SUM(r.refund_amount) AS total_refund_value,
  SUM(CASE WHEN r.return_reason = 'CHANGED_MIND' THEN 1 ELSE 0 END) AS changed_mind_count,
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
