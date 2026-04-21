-- UC Metric Views — Semantic Layer for E-Commerce Customer Support Chatbot
-- These metric views power Genie Space, dashboards, and the Supervisor Agent.
-- Run after gold layer pipeline completes.

-- ============================================================
-- 1. Order Metrics — order history and invoice KPIs
-- ============================================================
CREATE OR REPLACE VIEW debadm.ecom_metrics.order_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Customer order and invoice KPIs for chatbot and dashboards"
  source: debadm.ecom_gold.customer_orders
  dimensions:
    - name: Customer ID
      expr: customer_id
      comment: "Unique customer identifier"
    - name: Customer Name
      expr: customer_name
      comment: "Full name of the customer"
    - name: Loyalty Tier
      expr: loyalty_tier
      comment: "Customer loyalty tier: bronze, silver, gold, platinum"
    - name: Order Month
      expr: DATE_TRUNC('MONTH', order_date)
      comment: "Month when order was placed"
    - name: Order Status
      expr: order_status
      comment: "Current order status"
    - name: Payment Status
      expr: payment_status
      comment: "Payment status: pending, completed, failed, refunded"
    - name: Payment Method
      expr: payment_method
      comment: "How the customer paid"
    - name: Order Channel
      expr: order_channel
      comment: "Channel: web, app, phone"
  measures:
    - name: Total Orders
      expr: COUNT(DISTINCT order_id)
      comment: "Total number of orders"
    - name: Total Revenue
      expr: SUM(order_total)
      comment: "Total revenue across all orders in USD"
    - name: Avg Order Value
      expr: AVG(order_total)
      comment: "Average order value in USD"
    - name: Total Items Sold
      expr: SUM(item_count)
      comment: "Total items purchased across all orders"
    - name: Unique Customers
      expr: COUNT(DISTINCT customer_id)
      comment: "Unique customer count"
    - name: Revenue Per Customer
      expr: SUM(order_total) / COUNT(DISTINCT customer_id)
      comment: "Average revenue per unique customer"
    - name: Overdue Invoices
      expr: COUNT(DISTINCT order_id) FILTER (WHERE is_overdue = true)
      comment: "Number of orders with overdue invoice payments"
$$;

-- ============================================================
-- 2. Return Metrics — return and refund KPIs
-- ============================================================
CREATE OR REPLACE VIEW debadm.ecom_metrics.return_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Return and refund KPIs for customer support chatbot"
  source: debadm.ecom_gold.customer_returns
  dimensions:
    - name: Customer ID
      expr: customer_id
      comment: "Customer who initiated the return"
    - name: Customer Name
      expr: customer_name
      comment: "Full name of the customer"
    - name: Return Status
      expr: return_status
      comment: "Current return status"
    - name: Return Reason
      expr: return_reason
      comment: "Why the customer is returning the product"
    - name: Reason Label
      expr: reason_label
      comment: "Human-readable return reason"
    - name: Product Category
      expr: product_category
      comment: "Category of the returned product"
    - name: Product Name
      expr: product_name
      comment: "Name of the returned product"
    - name: Refund Method
      expr: refund_method
      comment: "How the refund is being processed"
    - name: Return Month
      expr: DATE_TRUNC('MONTH', requested_at)
      comment: "Month when return was requested"
    - name: Return Carrier
      expr: return_carrier
      comment: "Carrier handling the return shipment"
  measures:
    - name: Total Returns
      expr: COUNT(DISTINCT return_id)
      comment: "Total number of returns"
    - name: Total Refund Amount
      expr: SUM(refund_amount)
      comment: "Total refund amount in USD"
    - name: Avg Refund Amount
      expr: AVG(refund_amount)
      comment: "Average refund amount per return"
    - name: Avg Days to Process
      expr: AVG(days_in_process)
      comment: "Average days to process a return"
    - name: Returns In Transit
      expr: COUNT(DISTINCT return_id) FILTER (WHERE return_status = 'shipped_back')
      comment: "Returns currently in transit back to warehouse"
    - name: Pending Refunds
      expr: COUNT(DISTINCT return_id) FILTER (WHERE return_status IN ('received', 'shipped_back', 'approved'))
      comment: "Returns awaiting refund processing"
    - name: Completed Refunds
      expr: COUNT(DISTINCT return_id) FILTER (WHERE return_status = 'refund_processed')
      comment: "Returns with completed refunds"
    - name: Rejected Returns
      expr: COUNT(DISTINCT return_id) FILTER (WHERE return_status = 'rejected')
      comment: "Returns that were rejected"
    - name: Rejection Rate
      expr: COUNT(DISTINCT return_id) FILTER (WHERE return_status = 'rejected') * 100.0 / COUNT(DISTINCT return_id)
      comment: "Percentage of returns rejected"
$$;

-- ============================================================
-- 3. Invoice Metrics — invoice and payment tracking KPIs
-- ============================================================
CREATE OR REPLACE VIEW debadm.ecom_metrics.invoice_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Invoice and payment tracking KPIs"
  source: debadm.ecom_gold.invoice_details
  dimensions:
    - name: Customer ID
      expr: customer_id
      comment: "Customer who was invoiced"
    - name: Customer Name
      expr: customer_name
      comment: "Full name of the customer"
    - name: Invoice Month
      expr: DATE_TRUNC('MONTH', invoice_date)
      comment: "Month when invoice was issued"
    - name: Payment Status
      expr: payment_status
      comment: "Payment status of the invoice"
    - name: Payment Method
      expr: payment_method
      comment: "Payment method used"
    - name: Product Category
      expr: product_category
      comment: "Product category on the invoice"
    - name: Brand
      expr: brand
      comment: "Product brand"
    - name: Order Channel
      expr: order_channel
      comment: "Channel where order was placed"
    - name: Is Overdue
      expr: CAST(is_overdue AS STRING)
      comment: "Whether invoice payment is overdue"
  measures:
    - name: Total Invoices
      expr: COUNT(DISTINCT invoice_id)
      comment: "Total number of invoices"
    - name: Total Invoiced Amount
      expr: SUM(invoice_total)
      comment: "Total invoiced amount in USD"
    - name: Total Tax Collected
      expr: SUM(tax)
      comment: "Total tax charged across invoices"
    - name: Total Shipping Revenue
      expr: SUM(shipping)
      comment: "Total shipping charges collected"
    - name: Avg Invoice Value
      expr: AVG(invoice_total)
      comment: "Average invoice value in USD"
    - name: Overdue Invoice Count
      expr: COUNT(DISTINCT invoice_id) FILTER (WHERE is_overdue = true)
      comment: "Number of invoices past due date"
    - name: Total Line Items
      expr: COUNT(DISTINCT order_item_id)
      comment: "Total line items across all invoices"
$$;

-- ============================================================
-- 4. Policy Metrics — return policy effectiveness for Supervisor Agent
-- ============================================================
CREATE OR REPLACE VIEW debadm.ecom_metrics.policy_metrics
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Return policy effectiveness metrics for Supervisor Agent analysis"
  source: debadm.ecom_gold.return_analysis
  dimensions:
    - name: Product Category
      expr: product_category
      comment: "Product category the policy applies to"
    - name: Brand
      expr: brand
      comment: "Brand within the category"
    - name: Return Month
      expr: return_month
      comment: "Month when returns were requested"
    - name: Current Return Window
      expr: current_return_window
      comment: "Current return window in days for this product"
  measures:
    - name: Total Returns
      expr: SUM(total_returns)
      comment: "Total number of returns"
    - name: Total Refund Cost
      expr: SUM(total_refund_cost)
      comment: "Total cost of refunds in USD"
    - name: Avg Return Rate
      expr: AVG(return_rate_pct)
      comment: "Average return rate as percentage of orders"
    - name: Avg Processing Days
      expr: AVG(avg_processing_days)
      comment: "Average days to process a return"
    - name: Defective Returns
      expr: SUM(defective_returns)
      comment: "Total returns due to defective products"
    - name: Changed Mind Returns
      expr: SUM(changed_mind_returns)
      comment: "Total returns where customer changed mind"
    - name: Wrong Item Returns
      expr: SUM(wrong_item_returns)
      comment: "Total returns due to wrong item shipped"
    - name: Size Fit Returns
      expr: SUM(size_fit_returns)
      comment: "Total returns due to size or fit issues"
    - name: Avg Rejection Rate
      expr: AVG(rejection_rate_pct)
      comment: "Average rejection rate across categories"
    - name: Defective Rate
      expr: SUM(defective_returns) * 100.0 / SUM(total_returns)
      comment: "Percentage of returns due to defects"
$$;
