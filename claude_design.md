# E-Commerce Customer-Facing Support Chatbot — Databricks Design

## Overview

Build a **customer-facing chatbot** for an E-commerce platform that helps customers with two primary use cases:

1. **Product Returns** — Check return eligibility, initiate returns, track return status, view return policy
2. **Past Invoices** — Look up order history, view invoices, download invoice details, check payment status
3. **Return Policy Advisor (Supervisor Agent)** — An AI agent that analyzes return patterns, costs, and abuse signals to **propose optimized return policy documents** per product category

The platform uses Databricks Lakehouse (Bronze → Silver → Gold), Unity Catalog Metric Views as a semantic layer, a **Genie Space** as the natural language interface for the chatbot, and a **Databricks Supervisor Agent (MAS)** that orchestrates sub-agents to generate return policy recommendations.

---

## 1. Use Case Flows

### 1.1 Product Returns — Customer Journey

```
Customer: "I want to return the headphones I ordered last week"
    │
    ▼
Genie looks up customer's recent orders with product details
    │
    ▼
Checks return eligibility (within return window? item condition? policy?)
    │
    ▼
Shows return options: refund to original payment, store credit, exchange
    │
    ▼
Customer: "What's the status of my return RET-20251234?"
    │
    ▼
Genie queries return tracking with current status and refund ETA
```

### 1.2 Past Invoices — Customer Journey

```
Customer: "Show me my invoices from last 3 months"
    │
    ▼
Genie retrieves all orders with invoice numbers, dates, amounts
    │
    ▼
Customer: "What did I order in invoice INV-2025-0456?"
    │
    ▼
Genie returns line-item details: products, quantities, prices, tax, shipping
    │
    ▼
Customer: "Has my payment cleared for order ORD-78901?"
    │
    ▼
Genie shows payment status, method, and transaction reference
```

---

## 2. Datasets

### 2.1 Core Datasets

| Dataset | Description | Source Format | Volume (Synthetic) |
|---------|-------------|---------------|-------------------|
| `customers` | Customer profiles — id, name, email, phone, shipping_address, billing_address, signup_date, loyalty_tier | CSV | ~10,000 rows |
| `products` | Product catalog — id, name, category, subcategory, brand, price, sku, return_window_days, is_returnable | CSV | ~500 rows |
| `orders` | Orders — id, customer_id, order_date, status (placed/shipped/delivered/cancelled), total_amount, shipping_cost, tax_amount, channel (web/app/phone) | JSON | ~50,000 rows |
| `order_items` | Line items — id, order_id, product_id, quantity, unit_price, discount_amount, item_status | JSON | ~120,000 rows |
| `invoices` | Invoices — invoice_id, order_id, customer_id, invoice_number, invoice_date, subtotal, tax, shipping, total, due_date, pdf_url | JSON | ~50,000 rows |
| `payments` | Payments — id, invoice_id, order_id, payment_method (credit_card/debit/upi/wallet/cod), payment_status (pending/completed/failed/refunded), transaction_ref, paid_at | JSON | ~55,000 rows |
| `returns` | Returns — id, order_id, customer_id, order_item_id, product_id, return_reason, return_status (requested/approved/shipped_back/received/refund_processed/rejected), refund_amount, refund_method, requested_at, completed_at | JSON | ~8,000 rows |
| `return_policy` | Policy rules — product_category, return_window_days, restocking_fee_pct, conditions_text, is_final_sale | CSV | ~30 rows |
| `shipping_tracking` | Shipment tracking — id, order_id, return_id, carrier, tracking_number, status, estimated_delivery, last_update | JSON | ~60,000 rows |

### 2.2 Reference Data

| Dataset | Description |
|---------|-------------|
| `return_reasons` | Lookup: reason codes → labels (DEFECTIVE, WRONG_ITEM, NOT_AS_DESCRIBED, CHANGED_MIND, SIZE_FIT, ARRIVED_LATE) |
| `payment_methods` | Lookup: method codes → display names, refund processing days |
| `order_statuses` | Lookup: status codes → labels, customer-friendly descriptions |

---

## 3. Architecture

```
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                      CUSTOMER-FACING LAYER                                         │
  │                                                                                                     │
  │   ┌──────────────────────┐          ┌────────────────────────────────────────────────────────────┐  │
  │   │   CUSTOMER CHATBOT   │          │         SUPERVISOR AGENT (MAS)                             │  │
  │   │   (Genie Space)      │          │         "Return Policy Advisor"                            │  │
  │   │                      │          │                                                            │  │
  │   │  "Show my invoices"  │          │  ┌─────────────┐ ┌──────────────┐ ┌─────────────────────┐ │  │
  │   │  "Return my order"   │          │  │ Return Data │ │ Policy Draft │ │ Policy Review &     │ │  │
  │   │  "Refund status?"    │          │  │ Analyst     │ │ Generator    │ │ Compliance Checker  │ │  │
  │   │                      │          │  │ (Genie)     │ │ (FMAPI LLM) │ │ (FMAPI LLM)        │ │  │
  │   └──────────┬───────────┘          │  └──────┬──────┘ └──────┬───────┘ └──────────┬──────────┘ │  │
  │              │                       │         │               │                    │            │  │
  │              │                       │         └───────────────┼────────────────────┘            │  │
  │              │                       │                         ▼                                 │  │
  │              │                       │              ┌──────────────────┐                         │  │
  │              │                       │              │ Return Policy    │                         │  │
  │              │                       │              │ Document (PDF)   │                         │  │
  │              │                       │              │ → UC Volume      │                         │  │
  │              │                       │              └──────────────────┘                         │  │
  │              │                       └────────────────────────────────────────────────────────────┘  │
  └──────────────┼──────────────────────────────────────────────┼───────────────────────────────────────┘
                 │                                              │
                 ▼                                              ▼
  Raw Files             Bronze                    Silver                    Gold + Metrics
  (UC Volume)           (Streaming Tables)        (Materialized Views)     (Customer-Ready)
  ┌───────────┐       ┌──────────────────┐      ┌──────────────────┐     ┌──────────────────────┐
  │ customers │──AL──▶│ bronze.customers │──MV─▶│ silver.customers │──┐  │ gold.customer_orders │
  │ products  │──AL──▶│ bronze.products  │──MV─▶│ silver.products  │  │  │ gold.customer_returns│
  │ orders    │──AL──▶│ bronze.orders    │──MV─▶│ silver.orders    │  ├─▶│ gold.return_eligiblty│
  │ order_item│──AL──▶│ bronze.order_item│──MV─▶│ silver.order_item│  │  │ gold.invoice_details │
  │ invoices  │──AL──▶│ bronze.invoices  │──MV─▶│ silver.invoices  │  │  │ gold.refund_status   │
  │ payments  │──AL──▶│ bronze.payments  │──MV─▶│ silver.payments  │  │  │                      │
  │ returns   │──AL──▶│ bronze.returns   │──MV─▶│ silver.returns   │  │  │ gold.return_analysis │◄── Supervisor
  │ ret_policy│──AL──▶│ bronze.ret_policy│      │                  │  │  │ gold.abuse_signals   │◄── Agent reads
  │ shipping  │──AL──▶│ bronze.shipping  │──MV─▶│ silver.shipping  │──┘  │ gold.policy_history  │◄── these
  └───────────┘       └──────────────────┘      └──────────────────┘     └──────────┬───────────┘
                                                                                    │
                                                                                    ▼
                                                                         ┌──────────────────────┐
                                                                         │  UC Metric Views     │
                                                                         │  (Semantic Layer)    │
                                                                         │                      │
                                                                         │  - order_metrics     │
                                                                         │  - return_metrics    │
                                                                         │  - invoice_metrics   │
                                                                         │  - policy_metrics    │
                                                                         └──────────────────────┘
```

---

## 4. Unity Catalog Structure

```
catalog:   ecom_chatbot
schemas:
  ├── bronze       — raw ingested tables (streaming tables via Auto Loader)
  ├── silver       — cleaned, conformed, enriched (materialized views)
  ├── gold         — customer-facing query-ready tables (materialized views)
  ├── metrics      — UC Metric Views (semantic layer)
  └── raw_data     — UC Volume for source files
```

---

## 5. Bronze Layer — Raw Ingestion

**Technology:** Spark Declarative Pipelines (SDP) with Auto Loader streaming tables.

Each source file lands in a UC Volume (`/Volumes/ecom_chatbot/raw_data/landing/`) and is ingested as-is.

### Bronze Tables

| Table | Source Path | Format |
|-------|------------|--------|
| `bronze.customers` | `.../landing/customers/` | CSV |
| `bronze.products` | `.../landing/products/` | CSV |
| `bronze.orders` | `.../landing/orders/` | JSON |
| `bronze.order_items` | `.../landing/order_items/` | JSON |
| `bronze.invoices` | `.../landing/invoices/` | JSON |
| `bronze.payments` | `.../landing/payments/` | JSON |
| `bronze.returns` | `.../landing/returns/` | JSON |
| `bronze.return_policy` | `.../landing/return_policy/` | CSV |
| `bronze.shipping_tracking` | `.../landing/shipping_tracking/` | JSON |

### SDP Definition (Bronze)

```sql
-- All bronze tables follow this pattern
CREATE OR REFRESH STREAMING TABLE bronze.orders
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/orders/',
  format => 'json',
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE bronze.invoices
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/invoices/',
  format => 'json',
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE bronze.returns
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/returns/',
  format => 'json',
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE bronze.payments
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/payments/',
  format => 'json',
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE bronze.shipping_tracking
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/shipping_tracking/',
  format => 'json',
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE bronze.customers
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/customers/',
  format => 'csv',
  header => true,
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE bronze.products
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/products/',
  format => 'csv',
  header => true,
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE bronze.order_items
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/order_items/',
  format => 'json',
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE bronze.return_policy
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_chatbot/raw_data/landing/return_policy/',
  format => 'csv',
  header => true,
  inferColumnTypes => true
);
```

---

## 6. Silver Layer — Cleaned & Conformed

**Technology:** SDP Materialized Views with data quality constraints.

### Transformations

| Silver Table | Source | Key Transformations |
|-------------|--------|-------------------|
| `silver.customers` | `bronze.customers` | Trim whitespace, lowercase email, mask phone (show last 4), dedup on customer_id |
| `silver.products` | `bronze.products` | Standardize category, cast price to decimal(10,2), flag non-returnable items |
| `silver.orders` | `bronze.orders` | Cast dates, validate status enum, compute days_since_order |
| `silver.order_items` | `bronze.order_items` | Cast types, compute line_total = quantity * unit_price - discount_amount |
| `silver.invoices` | `bronze.invoices` | Validate invoice_number format, cast amounts, compute is_overdue flag |
| `silver.payments` | `bronze.payments` | Validate payment_status, mask card numbers, compute days_to_payment |
| `silver.returns` | `bronze.returns` | Validate return_status flow, compute days_in_return_process, enrich with reason label |
| `silver.shipping` | `bronze.shipping_tracking` | Parse timestamps, compute days_in_transit, flag delayed shipments |

### SDP Definitions (Silver)

```sql
-- Customers: cleaned with masked PII for chatbot display
CREATE OR REFRESH MATERIALIZED VIEW silver.customers (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email RLIKE '^[^@]+@[^@]+\\.[^@]+$') ON VIOLATION DROP ROW
)
AS SELECT
  customer_id,
  customer_name,
  LOWER(TRIM(email)) AS email,
  CONCAT('***-***-', RIGHT(phone, 4)) AS phone_masked,
  shipping_address,
  billing_address,
  CAST(signup_date AS DATE) AS signup_date,
  loyalty_tier,
  ingested_at
FROM bronze.customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ingested_at DESC) = 1;

-- Orders: enriched with time-based fields
CREATE OR REFRESH MATERIALIZED VIEW silver.orders (
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
FROM bronze.orders;

-- Order Items: with computed line totals
CREATE OR REFRESH MATERIALIZED VIEW silver.order_items (
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
FROM bronze.order_items;

-- Invoices: with overdue flag
CREATE OR REFRESH MATERIALIZED VIEW silver.invoices (
  CONSTRAINT valid_invoice EXPECT (invoice_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  invoice_id,
  order_id,
  customer_id,
  invoice_number,
  CAST(invoice_date AS DATE) AS invoice_date,
  CAST(subtotal AS DECIMAL(12,2)) AS subtotal,
  CAST(tax AS DECIMAL(10,2)) AS tax,
  CAST(shipping AS DECIMAL(10,2)) AS shipping,
  CAST(total AS DECIMAL(12,2)) AS total,
  CAST(due_date AS DATE) AS due_date,
  pdf_url,
  CASE WHEN CAST(due_date AS DATE) < CURRENT_DATE() THEN true ELSE false END AS is_overdue,
  ingested_at
FROM bronze.invoices;

-- Payments: with masked card info
CREATE OR REFRESH MATERIALIZED VIEW silver.payments (
  CONSTRAINT valid_payment EXPECT (payment_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (payment_status IN ('pending','completed','failed','refunded')) ON VIOLATION DROP ROW
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
  DATEDIFF(CAST(paid_at AS DATE), CURRENT_DATE()) AS days_since_payment,
  ingested_at
FROM bronze.payments;

-- Returns: enriched with processing time and reason labels
CREATE OR REFRESH MATERIALIZED VIEW silver.returns (
  CONSTRAINT valid_return EXPECT (return_id IS NOT NULL AND order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (
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
  rr.reason_label,
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
FROM bronze.returns r
LEFT JOIN bronze.return_policy rr ON r.return_reason = rr.reason_code;

-- Shipping: with transit tracking
CREATE OR REFRESH MATERIALIZED VIEW silver.shipping (
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
FROM bronze.shipping_tracking;
```

---

## 7. Gold Layer — Customer-Facing Query-Ready Tables

These are the tables the Genie Space queries directly. They are designed to answer customer questions in natural language.

### 7.1 `gold.customer_orders` — Order History & Invoice Lookup

The primary table for "Show me my past orders" and "What's in invoice X?"

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.customer_orders
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
  -- Invoice details
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
  -- Payment details
  p.payment_id,
  p.payment_method,
  p.payment_status,
  p.transaction_ref,
  p.amount AS payment_amount,
  p.paid_at,
  -- Line items (aggregated)
  COUNT(oi.order_item_id) AS item_count,
  SUM(oi.line_total) AS items_total,
  COLLECT_LIST(
    NAMED_STRUCT(
      'product_name', pr.product_name,
      'category', pr.category,
      'quantity', oi.quantity,
      'unit_price', oi.unit_price,
      'discount', oi.discount_amount,
      'line_total', oi.line_total,
      'sku', pr.sku
    )
  ) AS line_items
FROM silver.customers c
INNER JOIN silver.orders o ON c.customer_id = o.customer_id
LEFT JOIN silver.invoices i ON o.order_id = i.order_id
LEFT JOIN silver.payments p ON i.invoice_id = p.invoice_id
LEFT JOIN silver.order_items oi ON o.order_id = oi.order_id
LEFT JOIN bronze.products pr ON oi.product_id = pr.product_id
GROUP BY
  c.customer_id, c.customer_name, c.email, c.loyalty_tier,
  o.order_id, o.order_date, o.status, o.total_amount, o.shipping_cost,
  o.tax_amount, o.channel, o.days_since_order,
  i.invoice_id, i.invoice_number, i.invoice_date, i.subtotal,
  i.tax, i.shipping, i.total, i.due_date, i.is_overdue, i.pdf_url,
  p.payment_id, p.payment_method, p.payment_status, p.transaction_ref,
  p.amount, p.paid_at;
```

### 7.2 `gold.customer_returns` — Return Status & Tracking

The primary table for "What's the status of my return?" and "Where is my refund?"

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.customer_returns
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
  o.order_status,
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
  -- Refund status
  CASE
    WHEN r.return_status = 'refund_processed' THEN 'Refund completed'
    WHEN r.return_status = 'received' THEN 'Item received, refund processing (2-5 business days)'
    WHEN r.return_status = 'shipped_back' THEN 'Return in transit'
    WHEN r.return_status = 'approved' THEN 'Return approved, awaiting shipment'
    WHEN r.return_status = 'requested' THEN 'Return request under review'
    WHEN r.return_status = 'rejected' THEN 'Return request rejected'
    ELSE r.return_status
  END AS return_status_description
FROM silver.returns r
INNER JOIN silver.customers c ON r.customer_id = c.customer_id
LEFT JOIN bronze.products pr ON r.product_id = pr.product_id
LEFT JOIN silver.orders o ON r.order_id = o.order_id
LEFT JOIN silver.order_items oi ON r.order_item_id = oi.order_item_id
LEFT JOIN silver.shipping s ON r.return_id = s.return_id;
```

### 7.3 `gold.return_eligibility` — Can This Item Be Returned?

Answers "Can I return this product?" by checking policy rules against order age.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.return_eligibility
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
  COALESCE(pr.return_window_days, rp.return_window_days, 30) AS return_window_days,
  COALESCE(rp.restocking_fee_pct, 0) AS restocking_fee_pct,
  COALESCE(pr.is_returnable, true) AS is_product_returnable,
  COALESCE(rp.is_final_sale, false) AS is_final_sale,
  rp.conditions_text AS return_conditions,
  -- Eligibility calculation
  CASE
    WHEN COALESCE(rp.is_final_sale, false) = true THEN false
    WHEN COALESCE(pr.is_returnable, true) = false THEN false
    WHEN o.status = 'cancelled' THEN false
    WHEN o.days_since_order > COALESCE(pr.return_window_days, rp.return_window_days, 30) THEN false
    WHEN EXISTS (
      SELECT 1 FROM silver.returns ret
      WHERE ret.order_item_id = oi.order_item_id AND ret.return_status != 'rejected'
    ) THEN false
    ELSE true
  END AS is_eligible_for_return,
  -- Reason if not eligible
  CASE
    WHEN COALESCE(rp.is_final_sale, false) = true THEN 'Item is a final sale and cannot be returned'
    WHEN COALESCE(pr.is_returnable, true) = false THEN 'This product category is non-returnable'
    WHEN o.status = 'cancelled' THEN 'Order was cancelled'
    WHEN o.days_since_order > COALESCE(pr.return_window_days, rp.return_window_days, 30)
      THEN CONCAT('Return window of ', COALESCE(pr.return_window_days, rp.return_window_days, 30), ' days has expired')
    WHEN EXISTS (
      SELECT 1 FROM silver.returns ret
      WHERE ret.order_item_id = oi.order_item_id AND ret.return_status != 'rejected'
    ) THEN 'A return has already been submitted for this item'
    ELSE 'Eligible for return'
  END AS eligibility_reason,
  -- Days remaining in return window
  GREATEST(0, COALESCE(pr.return_window_days, rp.return_window_days, 30) - o.days_since_order) AS days_remaining_to_return,
  -- Estimated refund
  ROUND(oi.line_total * (1 - COALESCE(rp.restocking_fee_pct, 0) / 100.0), 2) AS estimated_refund_amount
FROM silver.customers c
INNER JOIN silver.orders o ON c.customer_id = o.customer_id
INNER JOIN silver.order_items oi ON o.order_id = oi.order_id
INNER JOIN bronze.products pr ON oi.product_id = pr.product_id
LEFT JOIN bronze.return_policy rp ON pr.category = rp.product_category
WHERE o.status IN ('delivered', 'shipped');
```

### 7.4 `gold.invoice_details` — Detailed Invoice View

Flat, denormalized invoice with line items for easy chatbot display.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.invoice_details
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
  -- Line item details
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
  -- Payment info
  p.payment_method,
  p.payment_status,
  p.transaction_ref,
  p.paid_at
FROM silver.invoices i
INNER JOIN silver.customers c ON i.customer_id = c.customer_id
INNER JOIN silver.orders o ON i.order_id = o.order_id
LEFT JOIN silver.order_items oi ON o.order_id = oi.order_id
LEFT JOIN bronze.products pr ON oi.product_id = pr.product_id
LEFT JOIN silver.payments p ON i.invoice_id = p.invoice_id;
```

### 7.5 `gold.refund_status` — Refund Tracking

Dedicated view for "Where is my refund?" queries.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.refund_status
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
  -- Payment reversal details
  p_refund.payment_status AS refund_payment_status,
  p_refund.paid_at AS refund_processed_at,
  p_refund.transaction_ref AS refund_transaction_ref,
  -- Estimated timelines
  CASE
    WHEN r.return_status = 'refund_processed' THEN 'Refund complete'
    WHEN r.return_status = 'received' AND r.refund_method = 'credit_card'
      THEN 'Refund will appear on your card in 5-10 business days'
    WHEN r.return_status = 'received' AND r.refund_method = 'store_credit'
      THEN 'Store credit will be applied within 24 hours'
    WHEN r.return_status = 'received' AND r.refund_method IN ('upi', 'wallet')
      THEN 'Refund will appear in your account in 2-3 business days'
    WHEN r.return_status = 'shipped_back'
      THEN 'Waiting to receive your return — refund will process after inspection'
    WHEN r.return_status = 'approved'
      THEN 'Please ship the item back using the prepaid label'
    WHEN r.return_status = 'requested'
      THEN 'Your return request is being reviewed (1-2 business days)'
    ELSE 'Contact support for details'
  END AS refund_timeline_message,
  -- Original payment for context
  p_orig.payment_method AS original_payment_method,
  p_orig.amount AS original_payment_amount
FROM silver.returns r
INNER JOIN silver.customers c ON r.customer_id = c.customer_id
LEFT JOIN bronze.products pr ON r.product_id = pr.product_id
LEFT JOIN silver.payments p_refund
  ON r.order_id = p_refund.order_id AND p_refund.payment_status = 'refunded'
LEFT JOIN silver.payments p_orig
  ON r.order_id = p_orig.order_id AND p_orig.payment_status = 'completed';
```

### 7.6 `gold.return_analysis` — Return Patterns by Category (Supervisor Agent Input)

Aggregated return trends that the Supervisor Agent uses to identify policy optimization opportunities.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.return_analysis
AS SELECT
  pr.category AS product_category,
  pr.brand,
  rp.return_window_days AS current_return_window,
  rp.restocking_fee_pct AS current_restocking_fee,
  rp.is_final_sale,
  -- Volume metrics
  COUNT(DISTINCT r.return_id) AS total_returns,
  COUNT(DISTINCT o.order_id) AS total_orders_in_category,
  ROUND(COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS return_rate_pct,
  -- Reason breakdown
  COUNT(CASE WHEN r.return_reason = 'DEFECTIVE' THEN 1 END) AS defective_returns,
  COUNT(CASE WHEN r.return_reason = 'WRONG_ITEM' THEN 1 END) AS wrong_item_returns,
  COUNT(CASE WHEN r.return_reason = 'NOT_AS_DESCRIBED' THEN 1 END) AS not_as_described_returns,
  COUNT(CASE WHEN r.return_reason = 'CHANGED_MIND' THEN 1 END) AS changed_mind_returns,
  COUNT(CASE WHEN r.return_reason = 'SIZE_FIT' THEN 1 END) AS size_fit_returns,
  COUNT(CASE WHEN r.return_reason = 'ARRIVED_LATE' THEN 1 END) AS late_arrival_returns,
  -- Financial impact
  SUM(r.refund_amount) AS total_refund_cost,
  ROUND(AVG(r.refund_amount), 2) AS avg_refund_amount,
  ROUND(AVG(oi.line_total), 2) AS avg_item_value,
  -- Timing patterns
  ROUND(AVG(r.days_in_process), 1) AS avg_processing_days,
  ROUND(AVG(o.days_since_order - DATEDIFF(CURRENT_DATE(), CAST(r.requested_at AS DATE))), 1) AS avg_days_to_return_request,
  -- Rejection & approval rates
  ROUND(SUM(CASE WHEN r.return_status = 'rejected' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rejection_rate_pct,
  -- Month-over-month trend
  DATE_TRUNC('month', r.requested_at) AS return_month
FROM silver.returns r
INNER JOIN silver.orders o ON r.order_id = o.order_id
INNER JOIN silver.order_items oi ON r.order_item_id = oi.order_item_id
INNER JOIN bronze.products pr ON r.product_id = pr.product_id
LEFT JOIN bronze.return_policy rp ON pr.category = rp.product_category
GROUP BY ALL;
```

### 7.7 `gold.abuse_signals` — Return Abuse Detection (Supervisor Agent Input)

Flags customers and patterns that suggest return abuse, which the agent factors into policy recommendations.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.abuse_signals
AS SELECT
  c.customer_id,
  c.customer_name,
  c.loyalty_tier,
  c.signup_date,
  -- Return behavior
  COUNT(DISTINCT r.return_id) AS total_returns,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS personal_return_rate_pct,
  SUM(r.refund_amount) AS total_refund_value,
  -- Abuse indicators
  SUM(CASE WHEN r.return_reason = 'CHANGED_MIND' THEN 1 ELSE 0 END) AS changed_mind_count,
  SUM(CASE WHEN r.days_in_process < 1 AND r.return_status = 'requested' THEN 1 ELSE 0 END) AS same_day_return_requests,
  SUM(CASE WHEN o.days_since_order <= 2 AND r.return_id IS NOT NULL THEN 1 ELSE 0 END) AS returns_within_2_days,
  -- Abuse score (higher = more suspicious)
  CASE
    WHEN COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0) > 50 THEN 'high_risk'
    WHEN COUNT(DISTINCT r.return_id) * 100.0 / NULLIF(COUNT(DISTINCT o.order_id), 0) > 30 THEN 'medium_risk'
    ELSE 'low_risk'
  END AS abuse_risk_level,
  -- Category concentration
  COLLECT_LIST(DISTINCT pr.category) AS returned_categories,
  MODE(pr.category) AS most_returned_category
FROM silver.customers c
INNER JOIN silver.orders o ON c.customer_id = o.customer_id
LEFT JOIN silver.returns r ON o.order_id = r.order_id
LEFT JOIN bronze.products pr ON r.product_id = pr.product_id
GROUP BY c.customer_id, c.customer_name, c.loyalty_tier, c.signup_date
HAVING COUNT(DISTINCT r.return_id) >= 3;
```

### 7.8 `gold.policy_history` — Policy Change Log (Supervisor Agent Output Tracking)

Tracks policy proposals generated by the Supervisor Agent over time.

```sql
-- This table is written to by the Supervisor Agent, not by SDP
-- Schema definition for reference:
CREATE TABLE IF NOT EXISTS gold.policy_history (
  policy_id STRING,
  product_category STRING,
  proposed_at TIMESTAMP,
  proposed_by STRING DEFAULT 'supervisor_agent',
  -- Current policy
  current_return_window_days INT,
  current_restocking_fee_pct DECIMAL(5,2),
  current_is_final_sale BOOLEAN,
  -- Proposed changes
  proposed_return_window_days INT,
  proposed_restocking_fee_pct DECIMAL(5,2),
  proposed_is_final_sale BOOLEAN,
  -- Rationale
  rationale STRING,
  supporting_data STRING,  -- JSON blob of key metrics
  -- Impact estimates
  estimated_return_reduction_pct DECIMAL(5,2),
  estimated_cost_savings DECIMAL(12,2),
  estimated_customer_impact STRING,  -- positive / neutral / negative
  -- Status
  status STRING DEFAULT 'proposed',  -- proposed / approved / rejected / implemented
  reviewed_by STRING,
  reviewed_at TIMESTAMP,
  -- Document
  policy_document_url STRING  -- UC Volume path to generated PDF
);
```

---

## 8. Supervisor Agent — Return Policy Advisor

### 8.1 Overview

The **Return Policy Advisor** is a Databricks Supervisor Agent (Multi-Agent System) that analyzes return data and proposes optimized return policy documents per product category. It runs on-demand or on a scheduled basis (e.g., monthly) and produces PDF policy documents stored in a UC Volume.

### 8.2 Agent Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                     SUPERVISOR AGENT (Orchestrator)                       │
│                     "Return Policy Advisor"                               │
│                                                                           │
│  Responsibilities:                                                        │
│  - Coordinates sub-agents in sequence                                     │
│  - Passes context between agents                                          │
│  - Writes final policy to gold.policy_history                             │
│  - Uploads PDF to UC Volume                                               │
│                                                                           │
│  ┌───────────────────┐   ┌───────────────────┐   ┌─────────────────────┐ │
│  │  SUB-AGENT 1      │   │  SUB-AGENT 2      │   │  SUB-AGENT 3       │ │
│  │  Return Data      │──▶│  Policy Draft      │──▶│  Policy Review &   │ │
│  │  Analyst          │   │  Generator         │   │  Compliance Check  │ │
│  │                   │   │                    │   │                    │ │
│  │  Type: Genie      │   │  Type: FMAPI LLM  │   │  Type: FMAPI LLM  │ │
│  │  Tool: SQL query  │   │  Tool: ai_gen      │   │  Tool: ai_gen     │ │
│  │                   │   │                    │   │                    │ │
│  │  Reads:           │   │  Input:            │   │  Input:            │ │
│  │  - return_analysis│   │  - Analyst report  │   │  - Draft policy    │ │
│  │  - abuse_signals  │   │  - Current policy  │   │  - Analyst data    │ │
│  │  - return_metrics │   │  - Best practices  │   │  - Legal rules     │ │
│  │                   │   │                    │   │                    │ │
│  │  Output:          │   │  Output:           │   │  Output:           │ │
│  │  - Category stats │   │  - Draft policy    │   │  - Final policy    │ │
│  │  - Anomalies      │   │    document        │   │  - Compliance flag │ │
│  │  - Recommendations│   │  - Change summary  │   │  - Risk assessment │ │
│  └───────────────────┘   └───────────────────┘   └─────────────────────┘ │
│                                                            │              │
│                                                            ▼              │
│                                                  ┌──────────────────┐    │
│                                                  │  PDF Generation  │    │
│                                                  │  & Upload to     │    │
│                                                  │  UC Volume       │    │
│                                                  └──────────────────┘    │
└───────────────────────────────────────────────────────────────────────────┘
```

### 8.3 Sub-Agent 1: Return Data Analyst (Genie Agent)

**Type:** Genie Space sub-agent
**Purpose:** Query gold tables to produce a data-driven analysis of return patterns for a given product category.

**Genie Space:** Internal analytics Genie (separate from customer-facing one) attached to:
- `gold.return_analysis`
- `gold.abuse_signals`
- `metrics.return_metrics`

**Prompted Questions (run for each product category):**
1. "What is the return rate for {category} over the last 6 months? Show monthly trend."
2. "What are the top 3 return reasons for {category}? Break down by percentage."
3. "What is the total refund cost for {category} this quarter vs last quarter?"
4. "How many high-risk abuse customers are returning {category} products?"
5. "What is the average days-to-return-request for {category}? Is it near the return window deadline?"
6. "Compare {category} return rate to the overall average across all categories."

**Output Schema:**
```json
{
  "category": "Electronics",
  "analysis_period": "2025-10-01 to 2026-03-31",
  "return_rate_pct": 18.5,
  "return_rate_trend": "increasing",
  "total_returns": 1240,
  "total_refund_cost": 89500.00,
  "top_reasons": [
    {"reason": "DEFECTIVE", "pct": 35.2},
    {"reason": "NOT_AS_DESCRIBED", "pct": 28.1},
    {"reason": "CHANGED_MIND", "pct": 22.4}
  ],
  "abuse_customers_count": 45,
  "abuse_refund_total": 12300.00,
  "avg_days_to_request": 12.3,
  "current_window_days": 30,
  "deadline_clustering": "28% of returns filed in last 3 days of window",
  "vs_overall_avg": "+6.2pp above average",
  "anomalies": [
    "Defective returns spiked 40% in February — possible batch quality issue",
    "High-risk customers account for 14% of total refund cost"
  ],
  "data_driven_recommendations": [
    "Consider reducing return window from 30 to 21 days for this category",
    "Add mandatory photo upload for 'NOT_AS_DESCRIBED' returns",
    "Implement tiered restocking fee: 0% for defective, 15% for changed_mind"
  ]
}
```

### 8.4 Sub-Agent 2: Policy Draft Generator (FMAPI LLM Agent)

**Type:** Foundation Model API (Claude/DBRX) via Model Serving endpoint
**Purpose:** Generate a structured return policy document based on the analyst's findings and current policy.

**System Prompt:**
```text
You are a retail policy expert. Given return data analysis and the current return policy
for a product category, draft an updated return policy document.

The policy document must include:
1. **Policy Title & Effective Date**
2. **Scope** — which products/categories this applies to
3. **Return Window** — number of days, with justification for any change
4. **Eligibility Conditions** — what condition the item must be in
5. **Return Process** — step-by-step for the customer
6. **Refund Options** — original payment, store credit, exchange
7. **Restocking Fees** — if any, with clear schedule
8. **Exceptions** — final sale items, opened software, hygiene products, etc.
9. **Abuse Prevention** — measures to deter serial returners
10. **Customer Communication** — friendly language for policy page
11. **Change Summary** — what changed vs current policy and why

Use clear, customer-friendly language. Avoid legal jargon.
Format as a structured document with headers and bullet points.
Include specific numbers from the data analysis to justify changes.
```

**Input:**
- Analyst report JSON (from Sub-Agent 1)
- Current policy from `bronze.return_policy`
- Product category details

**Output:** Structured policy document text (Markdown format)

### 8.5 Sub-Agent 3: Policy Review & Compliance Checker (FMAPI LLM Agent)

**Type:** Foundation Model API (Claude/DBRX) via Model Serving endpoint
**Purpose:** Review the draft policy for compliance, fairness, and business impact before finalizing.

**System Prompt:**
```text
You are a retail compliance and customer experience reviewer. Review the proposed
return policy document and check for:

COMPLIANCE:
- Consumer protection law alignment (FTC guidelines, state-specific rules)
- Clear and conspicuous disclosure of all terms
- No deceptive or unfair practices
- Reasonable return windows (not shorter than industry minimum)

CUSTOMER IMPACT:
- Will this policy negatively impact customer satisfaction?
- Is the language clear and friendly?
- Are the steps reasonable and not burdensome?
- Does it maintain competitive parity with industry norms?

BUSINESS RISK:
- Could this change trigger negative PR or social media backlash?
- Does the abuse prevention go too far (penalizing legitimate customers)?
- Are restocking fees reasonable and clearly communicated?

Output a structured review with:
1. compliance_status: "pass" | "flag" | "fail"
2. customer_impact: "positive" | "neutral" | "negative"
3. issues: list of specific concerns
4. suggestions: improvements
5. final_recommendation: "approve" | "revise" | "reject"
6. risk_score: 1-10 (10 = highest risk)
```

**Output Schema:**
```json
{
  "compliance_status": "pass",
  "customer_impact": "neutral",
  "issues": [
    "Restocking fee of 20% for 'changed_mind' is above industry average of 15%"
  ],
  "suggestions": [
    "Reduce restocking fee to 15% to match competitors",
    "Add explicit exception for defective items — no fee regardless"
  ],
  "final_recommendation": "revise",
  "risk_score": 3,
  "revised_sections": {
    "restocking_fees": "Updated recommendation with 15% fee"
  }
}
```

### 8.6 Supervisor Orchestration Flow

```python
# Pseudocode for the Supervisor Agent orchestration

def run_policy_advisor(category: str):
    """
    Supervisor Agent: orchestrates 3 sub-agents to propose a return policy.
    Deployed as a Databricks Model Serving endpoint using ChatAgent.
    """

    # Step 1: Data Analyst (Genie sub-agent)
    analyst_report = genie_agent.query(
        genie_space_id="internal_return_analytics",
        questions=ANALYST_QUESTIONS.format(category=category)
    )

    # Step 2: Get current policy
    current_policy = spark.sql(f"""
        SELECT * FROM bronze.return_policy
        WHERE product_category = '{category}'
    """).first()

    # Step 3: Policy Draft Generator (LLM sub-agent)
    draft_policy = llm_agent.generate(
        system_prompt=POLICY_GENERATOR_PROMPT,
        user_prompt=f"""
        Category: {category}
        Current Policy: {current_policy}
        Data Analysis: {analyst_report}
        Generate an updated return policy document.
        """
    )

    # Step 4: Compliance Review (LLM sub-agent)
    review = compliance_agent.generate(
        system_prompt=COMPLIANCE_REVIEWER_PROMPT,
        user_prompt=f"""
        Draft Policy: {draft_policy}
        Data Analysis: {analyst_report}
        Review this policy for compliance and customer impact.
        """
    )

    # Step 5: If review says "revise", send back to draft generator
    if review["final_recommendation"] == "revise":
        draft_policy = llm_agent.generate(
            system_prompt=POLICY_GENERATOR_PROMPT,
            user_prompt=f"""
            Original draft: {draft_policy}
            Review feedback: {review}
            Revise the policy addressing the feedback.
            """
        )

    # Step 6: Generate PDF and upload to UC Volume
    pdf_path = generate_pdf(draft_policy, category)
    volume_path = f"/Volumes/ecom_chatbot/raw_data/policy_documents/{category}/"
    upload_to_volume(pdf_path, volume_path)

    # Step 7: Log to policy_history table
    spark.sql(f"""
        INSERT INTO gold.policy_history VALUES (
            '{uuid4()}', '{category}', current_timestamp(), 'supervisor_agent',
            {current_policy.return_window_days}, {current_policy.restocking_fee_pct},
            {current_policy.is_final_sale},
            {draft_policy.proposed_window}, {draft_policy.proposed_fee},
            {draft_policy.proposed_final_sale},
            '{review.rationale}', '{json.dumps(analyst_report)}',
            {review.estimated_reduction}, {review.estimated_savings},
            '{review.customer_impact}',
            'proposed', NULL, NULL,
            '{volume_path}/{category}_policy.pdf'
        )
    """)

    return {
        "category": category,
        "policy_document_url": volume_path,
        "review_status": review["final_recommendation"],
        "risk_score": review["risk_score"],
        "key_changes": draft_policy["change_summary"]
    }
```

### 8.7 Agent Deployment

| Component | Deployment |
|-----------|-----------|
| **Supervisor Agent** | Databricks Model Serving endpoint (ChatAgent) |
| **Sub-Agent 1 (Analyst)** | Genie Space (internal analytics) via Genie Conversation API |
| **Sub-Agent 2 (Drafter)** | Foundation Model API (Claude/DBRX) via `ai_query()` or Model Serving |
| **Sub-Agent 3 (Reviewer)** | Foundation Model API (Claude/DBRX) via `ai_query()` or Model Serving |
| **PDF Generator** | Python function using HTML→PDF conversion, uploaded to UC Volume |
| **Scheduling** | Databricks Job — monthly run per product category, or on-demand via endpoint |

### 8.8 UC Functions as Agent Tools

The Supervisor Agent uses UC Functions registered in Unity Catalog:

```sql
-- Tool 1: Get return analysis for a category
CREATE OR REPLACE FUNCTION ecom_chatbot.tools.get_return_analysis(category STRING)
RETURNS TABLE
RETURN
  SELECT * FROM gold.return_analysis
  WHERE product_category = category
  ORDER BY return_month DESC;

-- Tool 2: Get abuse signals for a category
CREATE OR REPLACE FUNCTION ecom_chatbot.tools.get_abuse_signals(category STRING)
RETURNS TABLE
RETURN
  SELECT * FROM gold.abuse_signals
  WHERE most_returned_category = category
  ORDER BY personal_return_rate_pct DESC
  LIMIT 20;

-- Tool 3: Get current policy for a category
CREATE OR REPLACE FUNCTION ecom_chatbot.tools.get_current_policy(category STRING)
RETURNS TABLE
RETURN
  SELECT * FROM bronze.return_policy
  WHERE product_category = category;

-- Tool 4: Log a policy proposal
CREATE OR REPLACE FUNCTION ecom_chatbot.tools.log_policy_proposal(
  category STRING,
  proposed_window INT,
  proposed_fee DECIMAL(5,2),
  rationale STRING,
  document_url STRING
)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  import uuid
  from datetime import datetime
  # Insert into policy_history via spark
  return f"Policy proposal {uuid.uuid4()} logged for {category}"
$$;

-- Tool 5: Generate policy PDF
CREATE OR REPLACE FUNCTION ecom_chatbot.tools.generate_policy_pdf(
  category STRING,
  policy_content STRING
)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  # Convert markdown to HTML to PDF
  # Upload to /Volumes/ecom_chatbot/raw_data/policy_documents/{category}/
  return f"/Volumes/ecom_chatbot/raw_data/policy_documents/{category}/policy.pdf"
$$;
```

### 8.9 Sample Supervisor Agent Interaction

```
User: "Generate a new return policy proposal for Electronics"

Supervisor: Starting policy analysis for Electronics...

[Sub-Agent 1 - Analyst]
📊 Analysis complete for Electronics:
- Return rate: 18.5% (↑ 3.2% vs last quarter)
- Top reason: DEFECTIVE (35.2%)
- Refund cost this quarter: $89,500
- 45 high-risk abuse customers ($12,300 in refunds)
- 28% of returns filed in last 3 days of window

[Sub-Agent 2 - Drafter]
📝 Draft policy generated with proposed changes:
- Return window: 30 days → 21 days (most returns happen within 14 days)
- Restocking fee: 0% → 15% for "changed mind" returns only
- New: mandatory photo upload for "not as described" claims
- New: loyalty tier benefits — Gold/Platinum keep 30-day window

[Sub-Agent 3 - Reviewer]
✅ Compliance: PASS
👤 Customer Impact: NEUTRAL
⚠️  1 suggestion: reduce restocking fee to 10% for first-time returners
📊 Risk Score: 3/10

[Final]
📄 Policy document generated and uploaded:
   /Volumes/ecom_chatbot/raw_data/policy_documents/Electronics/policy_2026_04.pdf
📋 Logged to gold.policy_history (status: proposed)
```

---

## 9. Unity Catalog Metric Views (Semantic Layer)

### 8.1 `metrics.order_metrics`

```yaml
name: order_metrics
description: Customer order and invoice metrics for chatbot queries
source: ecom_chatbot.gold.customer_orders

dimensions:
  - name: customer_id
    type: string
    description: Unique customer identifier
  - name: customer_name
    type: string
    description: Customer full name
  - name: order_date
    type: date
    description: Date the order was placed
  - name: order_status
    type: string
    description: Current order status (placed, shipped, delivered, cancelled)
  - name: payment_status
    type: string
    description: Payment status (pending, completed, failed, refunded)
  - name: payment_method
    type: string
    description: How the customer paid
  - name: order_channel
    type: string
    description: Channel where order was placed (web, app, phone)

measures:
  - name: total_orders
    type: count_distinct
    column: order_id
    description: Total number of orders
  - name: total_spent
    type: sum
    column: order_total
    description: Total amount spent across all orders
  - name: avg_order_value
    type: average
    column: order_total
    description: Average order value
  - name: total_items
    type: sum
    column: item_count
    description: Total items purchased
  - name: overdue_invoices
    type: sum
    column: is_overdue
    description: Number of overdue invoices
```

### 8.2 `metrics.return_metrics`

```yaml
name: return_metrics
description: Return and refund metrics for customer support chatbot
source: ecom_chatbot.gold.customer_returns

dimensions:
  - name: customer_id
    type: string
    description: Customer identifier
  - name: return_status
    type: string
    description: Current return status
  - name: return_reason
    type: string
    description: Why the customer is returning
  - name: product_category
    type: string
    description: Category of returned product
  - name: refund_method
    type: string
    description: How the refund is being processed

measures:
  - name: total_returns
    type: count_distinct
    column: return_id
    description: Total number of returns
  - name: total_refund_amount
    type: sum
    column: refund_amount
    description: Total refund amount
  - name: avg_days_to_process
    type: average
    column: days_in_process
    description: Average days to process a return
  - name: returns_in_transit
    type: count_distinct
    column: return_id
    filter: return_status = 'shipped_back'
    description: Returns currently in transit
  - name: pending_refunds
    type: count_distinct
    column: return_id
    filter: return_status IN ('received', 'shipped_back', 'approved')
    description: Returns awaiting refund
```

### 8.3 `metrics.invoice_metrics`

```yaml
name: invoice_metrics
description: Invoice and payment tracking metrics
source: ecom_chatbot.gold.invoice_details

dimensions:
  - name: customer_id
    type: string
  - name: invoice_date
    type: date
    description: Date the invoice was issued
  - name: payment_status
    type: string
    description: Payment status
  - name: payment_method
    type: string
    description: Payment method used
  - name: product_category
    type: string
    description: Product category

measures:
  - name: total_invoices
    type: count_distinct
    column: invoice_id
    description: Total number of invoices
  - name: total_invoiced
    type: sum
    column: invoice_total
    description: Total invoiced amount
  - name: total_tax
    type: sum
    column: tax
    description: Total tax charged
  - name: overdue_count
    type: sum
    column: is_overdue
    description: Number of overdue invoices
  - name: avg_invoice_value
    type: average
    column: invoice_total
    description: Average invoice value
```

---

## 9. Genie Space — Customer Support Chatbot

### 9.1 Configuration

| Property | Value |
|----------|-------|
| **Name** | ShopEase Customer Support |
| **Description** | Hi! I can help you with product returns and past invoices. Ask me about your orders, returns, refunds, or invoices. |
| **Tables** | `gold.customer_orders`, `gold.customer_returns`, `gold.return_eligibility`, `gold.invoice_details`, `gold.refund_status` |
| **Metric Views** | `metrics.order_metrics`, `metrics.return_metrics`, `metrics.invoice_metrics` |
| **SQL Warehouse** | Serverless SQL Warehouse |

### 9.2 Genie Instructions

```text
You are a friendly E-commerce customer support chatbot for ShopEase.
You help customers with two things: product returns and past invoices.

IMPORTANT RULES:
- Always ask for the customer's email or customer ID to look them up first.
- Never expose internal IDs — use order numbers, invoice numbers, and product names instead.
- Use friendly, concise language. Customers are not technical.
- When showing monetary values, always format with currency symbol and 2 decimal places.
- Dates should be shown in a readable format like "March 15, 2025".

RETURNS:
- To check return eligibility, query gold.return_eligibility filtered by customer_id.
- "is_eligible_for_return = true" means the item can be returned.
- Always show "days_remaining_to_return" so the customer knows urgency.
- Show "estimated_refund_amount" which accounts for any restocking fee.
- If not eligible, explain why using the "eligibility_reason" field.
- For return status, query gold.customer_returns — show return_status_description.
- For refund status, query gold.refund_status — show refund_timeline_message.

INVOICES:
- To show order history, query gold.customer_orders filtered by customer_id.
- Default to showing last 6 months of orders unless customer specifies otherwise.
- For invoice details, query gold.invoice_details filtered by invoice_number.
- Show line items with product name, quantity, price, and any discounts.
- Show payment status prominently — customers often ask "has my payment gone through?"
- If invoice is overdue (is_overdue = true), flag it and show due_date.

HELPFUL PHRASES:
- "Your return is on its way back to us" (shipped_back status)
- "We've received your item and your refund is being processed" (received status)
- "Your refund of $X has been processed and will appear in Y days" (refund_processed)
- "Great news — this item is eligible for return! You have X days left" (eligible)
- "I'm sorry, this item is past the return window" (not eligible)
```

### 9.3 Sample Questions & Expected Behavior

#### Returns Flow
| Customer Question | Genie Action |
|---|---|
| "I want to return my order" | Ask for email/order number → query `return_eligibility` → show eligible items |
| "Can I return the blue headphones from my last order?" | Lookup customer → find order with product → check eligibility → show window + refund estimate |
| "What's the status of my return?" | Query `customer_returns` → show status description + tracking |
| "Where is my refund?" | Query `refund_status` → show refund_timeline_message |
| "Why was my return rejected?" | Query `customer_returns` where status = rejected → show reason |
| "How long do I have to return this?" | Query `return_eligibility` → show days_remaining_to_return |

#### Invoice Flow
| Customer Question | Genie Action |
|---|---|
| "Show me my recent orders" | Query `customer_orders` last 6 months → show order list with status |
| "What's in invoice INV-2025-0456?" | Query `invoice_details` by invoice_number → show line items |
| "Has my payment for order ORD-789 been processed?" | Query `customer_orders` by order_id → show payment_status |
| "Show me all my invoices from January" | Query `customer_orders` filtered by invoice_date range |
| "I need a copy of my invoice" | Query `invoice_details` → return pdf_url |
| "How much did I spend last month?" | Query `order_metrics` aggregated by month for customer |

---

## 10. Implementation Plan

### Phase 1: Setup & Data Generation
- [ ] Create catalog `ecom_chatbot` with schemas: `bronze`, `silver`, `gold`, `metrics`, `raw_data`
- [ ] Create UC Volume at `raw_data.landing` with subdirectories for each dataset
- [ ] Generate synthetic datasets using Faker + PySpark (realistic order/invoice/return data)
- [ ] Upload synthetic data to UC Volume

### Phase 2: Bronze Layer (SDP Pipeline)
- [ ] Create SDP pipeline `ecom_chatbot_ingestion`
- [ ] Define 9 streaming tables with Auto Loader (customers, products, orders, order_items, invoices, payments, returns, return_policy, shipping_tracking)
- [ ] Run initial ingestion and validate row counts

### Phase 3: Silver Layer (SDP Pipeline)
- [ ] Add 8 materialized views to the pipeline with data quality constraints
- [ ] Validate: dedup logic, type casting, computed fields (days_since_order, is_overdue, etc.)
- [ ] Verify data quality expectation drop counts

### Phase 4: Gold Layer (SDP Pipeline)
- [ ] Add 5 gold materialized views: customer_orders, customer_returns, return_eligibility, invoice_details, refund_status
- [ ] Validate: join correctness, eligibility logic, refund timeline messages
- [ ] Test with sample customer queries

### Phase 5: Semantic Layer
- [ ] Create 3 UC Metric Views: order_metrics, return_metrics, invoice_metrics
- [ ] Validate metric definitions return correct aggregations

### Phase 6: Genie Space
- [ ] Create Genie Space "ShopEase Customer Support"
- [ ] Attach gold tables and metric views
- [ ] Add chatbot instructions and key definitions
- [ ] Test all 12 sample questions from section 9.3
- [ ] Iterate on instructions based on Genie response quality

---

## 11. Folder Structure

```
databricks-data-innov-summit-stock/
├── claude_design.md                   # This design document
├── main.py                            # Orchestrator / entry point
├── 01_setup/
│   ├── create_catalog_schema.sql      # UC catalog + schema DDL
│   └── create_volumes.sql             # Volume + subdirectory creation
├── 02_data_gen/
│   └── generate_synthetic_data.py     # Faker-based: customers, orders, invoices, returns
├── 03_bronze/
│   └── bronze_pipeline.sql            # SDP streaming tables (9 tables)
├── 04_silver/
│   └── silver_pipeline.sql            # SDP materialized views (8 views)
├── 05_gold/
│   └── gold_pipeline.sql              # SDP gold views (5 views)
├── 06_metrics/
│   └── metric_views.sql               # UC Metric View YAML definitions
└── 07_genie/
    └── genie_space_config.json        # Genie Space setup + instructions
```
