# E-Commerce Customer Support Chatbot — Databricks Design (Cursor)

## Document purpose

This file is the **authoritative design spec** for building the e-commerce customer support intelligence stack in Cursor: medallion **Bronze → Silver → Gold**, **Unity Catalog Metric Views** (semantic layer), and a **Genie Space** for natural-language analytics.

A sibling copy of the core technical content also exists in [claude_design.md](claude_design.md). Prefer **this file (`cursor.md`)** when aligning agents and tasks in this repo.

**Scope note:** “Chatbot” here means **Genie-driven Q&A over governed tables and metrics** (analyst-style). A separate **RAG / conversational agent** over ticket text (Vector Search + Agent Framework) is optional and not detailed below unless you extend the project.

---

## Overview

Build an end-to-end E-commerce Customer Support analytics platform using Databricks Lakehouse architecture (Bronze → Silver → Gold), Unity Catalog Metric Views as a semantic layer, and a Genie Space for natural language querying.

---

## 1. Datasets

### 1.1 Core Datasets

| Dataset | Description | Source Format | Volume (Synthetic) |
|---------|-------------|---------------|-------------------|
| `customers` | Customer profiles (id, name, email, phone, segment, region, signup_date) | CSV/JSON | ~10,000 rows |
| `products` | Product catalog (id, name, category, subcategory, price, brand) | CSV | ~500 rows |
| `orders` | Order transactions (id, customer_id, order_date, status, total_amount, channel) | JSON | ~50,000 rows |
| `order_items` | Line items per order (order_id, product_id, quantity, unit_price, discount) | JSON | ~120,000 rows |
| `support_tickets` | Support tickets (id, customer_id, order_id, category, priority, status, created_at, resolved_at) | JSON | ~15,000 rows |
| `support_interactions` | Chat/email/call logs (id, ticket_id, agent_id, channel, message, sentiment, created_at) | JSON | ~40,000 rows |
| `agents` | Support agent profiles (id, name, team, shift, hire_date) | CSV | ~50 rows |
| `returns` | Return requests (id, order_id, reason, status, refund_amount, created_at) | JSON | ~8,000 rows |
| `customer_feedback` | CSAT/NPS survey responses (id, ticket_id, customer_id, rating, comment, survey_date) | JSON | ~5,000 rows |

### 1.2 Reference Data

| Dataset | Description |
|---------|-------------|
| `ticket_categories` | Lookup: category codes → labels (e.g., SHIP_DELAY, WRONG_ITEM, REFUND, BILLING) |
| `sentiment_labels` | Lookup: sentiment scores → labels (positive, neutral, negative) |
| `regions` | Lookup: region codes → names, timezones |

---

## 2. Architecture

```
  Raw Files (Volume)        Bronze (Streaming Tables)       Silver (MV/ST)            Gold (MV)              Genie
  ┌──────────────┐         ┌───────────────────────┐      ┌──────────────────┐     ┌──────────────────┐    ┌─────────┐
  │ CSV / JSON   │──Auto──▶│ bronze_customers      │──MV─▶│ silver_customers │──MV▶│ gold_ticket_     │───▶│         │
  │ files in     │  Loader │ bronze_products        │      │ silver_orders    │     │   summary        │    │  Genie  │
  │ UC Volume    │         │ bronze_orders          │      │ silver_tickets   │     │ gold_agent_      │    │  Space  │
  │              │         │ bronze_order_items     │      │ silver_returns   │     │   performance    │    │         │
  │              │         │ bronze_support_tickets │      │ silver_feedback  │     │ gold_customer_   │    │         │
  │              │         │ bronze_support_interact│      │ silver_interact  │     │   health         │    │         │
  │              │         │ bronze_agents          │      │                  │     │ gold_product_    │    │         │
  │              │         │ bronze_returns         │      │                  │     │   issues         │    │         │
  │              │         │ bronze_feedback        │      │                  │     │ gold_resolution_ │    │         │
  │              │         │                        │      │                  │     │   metrics        │    │         │
  └──────────────┘         └───────────────────────┘      └──────────────────┘     └──────────────────┘    └─────────┘
                                                                                          │
                                                                                          ▼
                                                                                  ┌──────────────────┐
                                                                                  │  UC Metric Views │
                                                                                  │  (Semantic Layer)│
                                                                                  └──────────────────┘
```

---

## 3. Unity Catalog Structure

```
catalog:   ecom_support
schemas:
  ├── bronze       — raw ingested tables (streaming tables via Auto Loader)
  ├── silver       — cleaned, conformed, joined (materialized views)
  ├── gold         — aggregated business-level tables (materialized views)
  ├── metrics      — UC Metric Views (semantic layer)
  └── raw_data     — UC Volume for source files
```

---

## 4. Bronze Layer — Raw Ingestion

**Technology:** Spark Declarative Pipelines (SDP) with Auto Loader streaming tables.

Each source file lands in a UC Volume (`/Volumes/ecom_support/raw_data/landing/`) and is ingested as-is with metadata columns.

### Bronze Tables

| Table | Source Path | Format |
|-------|------------|--------|
| `bronze.customers` | `.../landing/customers/` | CSV |
| `bronze.products` | `.../landing/products/` | CSV |
| `bronze.orders` | `.../landing/orders/` | JSON |
| `bronze.order_items` | `.../landing/order_items/` | JSON |
| `bronze.support_tickets` | `.../landing/support_tickets/` | JSON |
| `bronze.support_interactions` | `.../landing/support_interactions/` | JSON |
| `bronze.agents` | `.../landing/agents/` | CSV |
| `bronze.returns` | `.../landing/returns/` | JSON |
| `bronze.customer_feedback` | `.../landing/customer_feedback/` | JSON |

### Example SDP Definition (Bronze)

```sql
CREATE OR REFRESH STREAMING TABLE bronze.support_tickets
AS SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/ecom_support/raw_data/landing/support_tickets/',
  format => 'json',
  inferColumnTypes => true
);
```

---

## 5. Silver Layer — Cleaned & Conformed

**Technology:** SDP Materialized Views with data quality expectations.

### Transformations

| Silver Table | Source | Key Transformations |
|-------------|--------|-------------------|
| `silver.customers` | `bronze.customers` | Trim whitespace, normalize email to lowercase, validate phone format, dedup on customer_id |
| `silver.products` | `bronze.products` | Standardize category names, cast price to decimal(10,2) |
| `silver.orders` | `bronze.orders` + `bronze.order_items` | Cast dates, validate status enum, join line items, compute order_total |
| `silver.support_tickets` | `bronze.support_tickets` | Parse timestamps, compute `resolution_time_hours`, validate category against lookup, enrich with customer segment |
| `silver.support_interactions` | `bronze.support_interactions` | Parse timestamps, normalize sentiment scores to [-1, 1], flag escalations |
| `silver.returns` | `bronze.returns` | Validate refund amounts, join with order for product context |
| `silver.customer_feedback` | `bronze.customer_feedback` | Normalize rating to 1-5 scale, classify NPS (promoter/passive/detractor) |

### Example SDP Definition (Silver)

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver.support_tickets
AS SELECT
  t.ticket_id,
  t.customer_id,
  c.customer_name,
  c.segment AS customer_segment,
  c.region,
  t.order_id,
  t.category,
  t.priority,
  t.status,
  CAST(t.created_at AS TIMESTAMP) AS created_at,
  CAST(t.resolved_at AS TIMESTAMP) AS resolved_at,
  ROUND(
    TIMESTAMPDIFF(MINUTE, CAST(t.created_at AS TIMESTAMP), CAST(t.resolved_at AS TIMESTAMP)) / 60.0, 2
  ) AS resolution_time_hours,
  t.ingested_at
FROM bronze.support_tickets t
LEFT JOIN silver.customers c ON t.customer_id = c.customer_id
WHERE t.ticket_id IS NOT NULL;
```

### Data Quality Expectations

```sql
CONSTRAINT valid_ticket_id EXPECT (ticket_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_priority EXPECT (priority IN ('low', 'medium', 'high', 'critical')) ON VIOLATION DROP ROW
CONSTRAINT valid_resolution_time EXPECT (resolution_time_hours >= 0 OR resolution_time_hours IS NULL) ON VIOLATION DROP ROW
```

---

## 6. Gold Layer — Business Aggregates

### Gold Tables

#### 6.1 `gold.ticket_summary`
Daily/weekly ticket volume, resolution rates, and average resolution time by category, priority, and region.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.ticket_summary
AS SELECT
  DATE_TRUNC('day', created_at) AS ticket_date,
  category,
  priority,
  region,
  customer_segment,
  COUNT(*) AS total_tickets,
  SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved_tickets,
  ROUND(AVG(resolution_time_hours), 2) AS avg_resolution_hours,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resolution_time_hours), 2) AS median_resolution_hours,
  ROUND(
    SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
  ) AS resolution_rate_pct
FROM silver.support_tickets
GROUP BY ALL;
```

#### 6.2 `gold.agent_performance`
Per-agent metrics: tickets handled, avg resolution time, CSAT score, escalation rate.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.agent_performance
AS SELECT
  a.agent_id,
  a.agent_name,
  a.team,
  DATE_TRUNC('week', i.created_at) AS week_start,
  COUNT(DISTINCT i.ticket_id) AS tickets_handled,
  ROUND(AVG(t.resolution_time_hours), 2) AS avg_resolution_hours,
  ROUND(AVG(f.rating), 2) AS avg_csat_score,
  SUM(CASE WHEN i.is_escalation THEN 1 ELSE 0 END) AS escalations,
  ROUND(
    SUM(CASE WHEN i.is_escalation THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT i.ticket_id), 0), 1
  ) AS escalation_rate_pct
FROM silver.support_interactions i
JOIN bronze.agents a ON i.agent_id = a.agent_id
LEFT JOIN silver.support_tickets t ON i.ticket_id = t.ticket_id
LEFT JOIN silver.customer_feedback f ON t.ticket_id = f.ticket_id
GROUP BY ALL;
```

#### 6.3 `gold.customer_health`
Customer-level support health: ticket frequency, avg sentiment, CSAT, return rate, lifetime value.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.customer_health
AS SELECT
  c.customer_id,
  c.customer_name,
  c.segment,
  c.region,
  COUNT(DISTINCT t.ticket_id) AS total_tickets,
  COUNT(DISTINCT r.return_id) AS total_returns,
  ROUND(AVG(f.rating), 2) AS avg_csat,
  ROUND(AVG(i.sentiment_score), 2) AS avg_sentiment,
  SUM(o.total_amount) AS lifetime_order_value,
  MAX(t.created_at) AS last_ticket_date,
  CASE
    WHEN AVG(f.rating) >= 4 AND COUNT(DISTINCT t.ticket_id) <= 2 THEN 'healthy'
    WHEN AVG(f.rating) BETWEEN 2.5 AND 4 THEN 'at_risk'
    ELSE 'critical'
  END AS health_status
FROM silver.customers c
LEFT JOIN silver.support_tickets t ON c.customer_id = t.customer_id
LEFT JOIN silver.returns r ON c.customer_id = r.customer_id
LEFT JOIN silver.customer_feedback f ON c.customer_id = f.customer_id
LEFT JOIN silver.support_interactions i ON t.ticket_id = i.ticket_id
LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
GROUP BY ALL;
```

#### 6.4 `gold.product_issues`
Products with highest return/complaint rates.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.product_issues
AS SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.brand,
  COUNT(DISTINCT t.ticket_id) AS ticket_count,
  COUNT(DISTINCT r.return_id) AS return_count,
  ROUND(AVG(f.rating), 2) AS avg_rating,
  COLLECT_SET(t.category) AS issue_categories,
  SUM(r.refund_amount) AS total_refund_amount
FROM silver.products p
LEFT JOIN silver.orders o ON TRUE
LEFT JOIN bronze.order_items oi ON o.order_id = oi.order_id AND p.product_id = oi.product_id
LEFT JOIN silver.support_tickets t ON o.order_id = t.order_id
LEFT JOIN silver.returns r ON o.order_id = r.order_id
LEFT JOIN silver.customer_feedback f ON t.ticket_id = f.ticket_id
WHERE oi.product_id IS NOT NULL
GROUP BY ALL;
```

#### 6.5 `gold.resolution_metrics`
Overall operational KPIs over time.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold.resolution_metrics
AS SELECT
  DATE_TRUNC('day', t.created_at) AS metric_date,
  COUNT(*) AS tickets_opened,
  SUM(CASE WHEN t.status = 'resolved' THEN 1 ELSE 0 END) AS tickets_resolved,
  SUM(CASE WHEN t.priority = 'critical' THEN 1 ELSE 0 END) AS critical_tickets,
  ROUND(AVG(t.resolution_time_hours), 2) AS avg_resolution_hours,
  ROUND(AVG(f.rating), 2) AS avg_csat,
  ROUND(AVG(i.sentiment_score), 2) AS avg_sentiment,
  SUM(CASE WHEN t.status = 'open' THEN 1 ELSE 0 END) AS backlog_count
FROM silver.support_tickets t
LEFT JOIN silver.customer_feedback f ON t.ticket_id = f.ticket_id
LEFT JOIN silver.support_interactions i ON t.ticket_id = i.ticket_id
GROUP BY ALL;
```

---

## 7. Unity Catalog Metric Views (Semantic Layer)

Metric Views provide governed, reusable business metric definitions that Genie and dashboards can consume.

### 7.1 `metrics.support_metrics`

```yaml
# Metric View: support_metrics
name: support_metrics
description: Core customer support KPIs
source: ecom_support.gold.ticket_summary

dimensions:
  - name: ticket_date
    type: date
    description: Date the ticket was created
  - name: category
    type: string
    description: Support ticket category (e.g., SHIP_DELAY, REFUND, BILLING)
  - name: priority
    type: string
    description: Ticket priority level
  - name: region
    type: string
    description: Customer region
  - name: customer_segment
    type: string
    description: Customer segment (enterprise, mid-market, consumer)

measures:
  - name: total_tickets
    type: sum
    column: total_tickets
    description: Total number of support tickets
  - name: resolved_tickets
    type: sum
    column: resolved_tickets
    description: Number of resolved tickets
  - name: avg_resolution_hours
    type: average
    column: avg_resolution_hours
    description: Average ticket resolution time in hours
  - name: resolution_rate
    type: derived
    expression: resolved_tickets / NULLIF(total_tickets, 0) * 100
    description: Percentage of tickets resolved
    format: percentage
```

### 7.2 `metrics.agent_metrics`

```yaml
name: agent_metrics
description: Agent performance and productivity KPIs
source: ecom_support.gold.agent_performance

dimensions:
  - name: agent_name
    type: string
  - name: team
    type: string
  - name: week_start
    type: date

measures:
  - name: tickets_handled
    type: sum
    column: tickets_handled
    description: Total tickets handled by agent
  - name: avg_resolution_hours
    type: average
    column: avg_resolution_hours
    description: Average hours to resolve per agent
  - name: avg_csat_score
    type: average
    column: avg_csat_score
    description: Average customer satisfaction score
  - name: escalation_rate
    type: derived
    expression: escalations / NULLIF(tickets_handled, 0) * 100
    description: Percentage of tickets escalated
    format: percentage
```

### 7.3 `metrics.customer_health_metrics`

```yaml
name: customer_health_metrics
description: Customer support health and lifetime value metrics
source: ecom_support.gold.customer_health

dimensions:
  - name: segment
    type: string
  - name: region
    type: string
  - name: health_status
    type: string

measures:
  - name: total_customers
    type: count
    description: Number of customers
  - name: avg_csat
    type: average
    column: avg_csat
    description: Average customer satisfaction score
  - name: avg_lifetime_value
    type: average
    column: lifetime_order_value
    description: Average customer lifetime order value
  - name: total_tickets
    type: sum
    column: total_tickets
    description: Total support tickets across customers
  - name: total_returns
    type: sum
    column: total_returns
    description: Total returns across customers
```

---

## 8. Genie Space

### 8.1 Configuration

| Property | Value |
|----------|-------|
| **Name** | E-Commerce Customer Support Intelligence |
| **Description** | Ask questions about customer support performance, agent productivity, ticket trends, product issues, and customer health |
| **Tables** | `gold.ticket_summary`, `gold.agent_performance`, `gold.customer_health`, `gold.product_issues`, `gold.resolution_metrics` |
| **Metric Views** | `metrics.support_metrics`, `metrics.agent_metrics`, `metrics.customer_health_metrics` |
| **SQL Warehouse** | Serverless SQL Warehouse |

### 8.2 Sample Instructions for Genie

```text
You are an E-commerce Customer Support analyst. Help users understand support operations.

Key definitions:
- Resolution Rate = resolved_tickets / total_tickets * 100
- CSAT = Customer Satisfaction score on a 1-5 scale (target >= 4.0)
- Escalation Rate = escalations / tickets_handled * 100 (target < 10%)
- Customer Health: "healthy" (CSAT >= 4, tickets <= 2), "at_risk" (CSAT 2.5-4), "critical" (CSAT < 2.5)
- SLA Breach: resolution_time_hours > 24 for high/critical priority tickets

When asked about trends, default to the last 30 days unless specified.
When asked about "top" or "worst", default to top/bottom 10.
Always include the time period in your response.
```

### 8.3 Sample Questions the Genie Space Should Answer

1. "What is the average resolution time this month?"
2. "Which ticket category has the most volume?"
3. "Show me agents with the highest CSAT scores"
4. "How many critical tickets are in the backlog?"
5. "Which products have the most complaints?"
6. "What is the escalation rate by team?"
7. "Show customer health distribution by segment"
8. "What regions have the slowest resolution times?"
9. "Trend of ticket volume over the last 90 days"
10. "Which customers are in critical health status?"

---

## 9. Implementation Plan

### Phase 1: Setup & Data Generation
- [ ] Create catalog `ecom_support` with schemas: `bronze`, `silver`, `gold`, `metrics`, `raw_data`
- [ ] Create UC Volume at `raw_data.landing`
- [ ] Generate synthetic datasets using Faker + PySpark
- [ ] Upload synthetic data to UC Volume

### Phase 2: Bronze Layer
- [ ] Create SDP pipeline `ecom_support_bronze`
- [ ] Define 9 streaming tables with Auto Loader
- [ ] Run initial ingestion and validate row counts

### Phase 3: Silver Layer
- [ ] Create SDP pipeline `ecom_support_silver`
- [ ] Define materialized views with joins and quality constraints
- [ ] Validate data quality expectations

### Phase 4: Gold Layer
- [ ] Create SDP pipeline `ecom_support_gold`
- [ ] Define 5 gold materialized views
- [ ] Validate aggregation correctness

### Phase 5: Semantic Layer
- [ ] Create UC Metric Views for support, agent, and customer metrics
- [ ] Validate metric definitions against gold tables

### Phase 6: Genie Space
- [ ] Create Genie Space with gold tables and metric views
- [ ] Add instructions and sample questions
- [ ] Test with 10+ sample questions
- [ ] Share with stakeholders

---

## 10. Folder Structure

```
databricks-data-innov-summit-stock/
├── cursor.md                          # Cursor design spec (this file)
├── claude_design.md                   # Parallel design reference
├── main.py                            # Orchestrator / entry point
├── 01_setup/
│   ├── create_catalog_schema.sql      # UC setup DDL
│   └── create_volumes.sql             # Volume creation
├── 02_data_gen/
│   └── generate_synthetic_data.py     # Faker-based data generation
├── 03_bronze/
│   └── bronze_pipeline.sql            # SDP streaming tables
├── 04_silver/
│   └── silver_pipeline.sql            # SDP materialized views
├── 05_gold/
│   └── gold_pipeline.sql              # SDP gold aggregates
├── 06_metrics/
│   └── metric_views.sql               # UC Metric View definitions
└── 07_genie/
    └── genie_space_config.json        # Genie Space setup config
```

---

## 11. Governance and operations (checklist)

- **Identity:** Use groups for `DATA_ENGINEER`, `SUPPORT_ANALYST`, `GENIE_USER`; grant `USAGE` on catalog/schema and `SELECT` on gold + metrics as appropriate. Pipeline service principal owns bronze/silver/gold write paths.
- **PII:** Keep raw message bodies in bronze/silver only if required; for demos, hash or truncate `support_interactions.message` in silver before exposing to broad Genie users.
- **Warehouse:** Serverless SQL warehouse sized for Genie concurrency; pin Genie Space to one warehouse for predictable cost.
- **Lineage:** Rely on Unity Catalog lineage from SDP; document external volume paths in `01_setup`.
- **Quality:** Surface DLT/SDP expectations in the job UI; alert on failed expectations if you enable notifications.

---

## 12. Optional extension: RAG “chatbot”

If you need conversational answers over unstructured ticket text: chunk `silver.support_interactions`, embed into a **Vector Search** index, and build a **Databricks Agent** or App that retrieves context then calls an LLM—with stricter access control than Genie on raw text.
