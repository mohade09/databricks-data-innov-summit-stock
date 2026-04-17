# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Customer Support Chatbot — Synthetic Data Generation
# MAGIC
# MAGIC Generates 9 datasets and uploads them to UC Volume:
# MAGIC - **customers** (10K) — CSV
# MAGIC - **products** (500) — CSV
# MAGIC - **orders** (50K) — JSON
# MAGIC - **order_items** (120K) — JSON
# MAGIC - **invoices** (50K) — JSON
# MAGIC - **payments** (55K) — JSON
# MAGIC - **returns** (8K) — JSON
# MAGIC - **return_policy** (15) — CSV
# MAGIC - **shipping_tracking** (60K) — JSON

# COMMAND ----------

import json
import csv
import random
import io
from datetime import datetime, timedelta

random.seed(42)

VOLUME_BASE = "/Volumes/debadm/ecom_raw_data/landing"

# ============================================================
# HELPERS
# ============================================================

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))


def random_ts(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def write_csv(data, path):
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=data[0].keys())
    w.writeheader()
    w.writerows(data)
    with open(path, "w") as f:
        f.write(buf.getvalue())


def write_jsonl(data, path):
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    with open(path, "w") as f:
        for row in data:
            f.write(json.dumps(row) + "\n")


# ============================================================
# REFERENCE DATA
# ============================================================

NOW = datetime(2026, 4, 17)
ONE_YEAR_AGO = NOW - timedelta(days=365)

FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "James", "Mia", "Logan", "Charlotte", "Lucas", "Amelia",
    "Alexander", "Harper", "Benjamin", "Evelyn", "Daniel", "Abigail", "Henry",
    "Emily", "Sebastian", "Elizabeth", "Jack", "Sofia", "Aiden", "Avery",
    "Owen", "Ella", "Samuel", "Scarlett", "Ryan", "Grace", "Nathan", "Chloe",
    "Carter", "Victoria", "Luke",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
    "Hernandez", "Moore", "Martin", "Jackson", "Thompson", "White", "Lopez",
    "Lee", "Harris", "Clark", "Lewis", "Robinson", "Walker", "Young", "Allen",
    "King", "Wright",
]

STREETS = [
    "Oak St", "Maple Ave", "Cedar Ln", "Pine Rd", "Elm Blvd", "Main St",
    "1st Ave", "Park Ave", "Lake Dr", "Sunset Blvd",
]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Diego", "Dallas", "Austin", "Seattle", "Denver",
    "Boston", "Portland", "Miami", "Atlanta",
]
STATES = ["NY", "CA", "IL", "TX", "AZ", "PA", "CA", "TX", "TX", "WA", "CO", "MA", "OR", "FL", "GA"]
REGIONS = [
    "Northeast", "West", "Midwest", "South", "Southwest", "Northeast",
    "West", "South", "South", "West", "West", "Northeast", "West", "South", "South",
]

CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Sports", "Books",
    "Beauty", "Toys", "Automotive", "Garden",
]

BRANDS = {
    "Electronics": ["TechPro", "SoundWave", "PixelMax", "NovaTech", "ByteGear"],
    "Clothing": ["UrbanFit", "StyleCraft", "ComfortWear", "TrendSetters", "EcoThread"],
    "Home & Kitchen": ["HomeEssence", "ChefMaster", "CozyLiving", "CleanPro", "KitchenAid"],
    "Sports": ["ActiveEdge", "FitGear", "ProSport", "TrailBlaze", "PowerMove"],
    "Books": ["PageTurner", "ReadMore", "BookHaven", "LitPress", "NovelWorld"],
    "Beauty": ["GlowUp", "PureSkin", "LuxBeauty", "NaturalBliss", "VelvetTouch"],
    "Toys": ["FunFactory", "PlayTime", "KidJoy", "ToyWorld", "SmartPlay"],
    "Automotive": ["AutoParts", "DriveMax", "MotorCraft", "SpeedTech", "CarCare"],
    "Garden": ["GreenThumb", "BloomCraft", "GardenPro", "PlantLife", "EcoGarden"],
}

PRODUCT_NAMES = {
    "Electronics": ["Wireless Headphones", "Bluetooth Speaker", "Smart Watch", "USB-C Hub", "Laptop Stand", "Webcam HD", "Mechanical Keyboard", "Gaming Mouse", "Portable Charger", "HDMI Cable"],
    "Clothing": ["Cotton T-Shirt", "Denim Jeans", "Running Shoes", "Winter Jacket", "Casual Hoodie", "Dress Shirt", "Yoga Pants", "Baseball Cap", "Wool Sweater", "Cargo Shorts"],
    "Home & Kitchen": ["Coffee Maker", "Air Fryer", "Knife Set", "Throw Pillow", "Bath Towel Set", "Cutting Board", "Blender", "Dinner Plate Set", "Storage Containers", "Scented Candle"],
    "Sports": ["Yoga Mat", "Resistance Bands", "Water Bottle", "Running Belt", "Jump Rope", "Foam Roller", "Gym Bag", "Fitness Tracker", "Pull-Up Bar", "Tennis Racket"],
    "Books": ["Python Programming", "Data Science Handbook", "Mystery Novel", "Cookbook Basics", "History Atlas", "Self-Help Guide", "Sci-Fi Collection", "Art of Design", "Travel Journal", "Business Strategy"],
    "Beauty": ["Moisturizer Cream", "Shampoo Set", "Lip Balm Pack", "Face Mask Set", "Sunscreen SPF50", "Hair Serum", "Nail Polish Kit", "Eye Cream", "Body Lotion", "Perfume Spray"],
    "Toys": ["Building Blocks", "Board Game", "Plush Bear", "RC Car", "Puzzle Set", "Art Supply Kit", "Action Figure", "Dollhouse", "Science Kit", "Card Game"],
    "Automotive": ["Car Phone Mount", "Dash Cam", "Tire Inflator", "Seat Covers", "Car Vacuum", "LED Headlights", "Floor Mats", "Air Freshener", "Jump Starter", "Wiper Blades"],
    "Garden": ["Garden Hose", "Plant Pots Set", "Pruning Shears", "Seed Starter Kit", "Solar Lights", "Compost Bin", "Raised Bed", "Watering Can", "Bird Feeder", "Herb Garden Kit"],
}

CHANNELS = ["web", "app", "phone"]
PAY_METHODS = ["credit_card", "debit", "upi", "wallet", "cod"]
RET_REASONS = ["DEFECTIVE", "WRONG_ITEM", "NOT_AS_DESCRIBED", "CHANGED_MIND", "SIZE_FIT", "ARRIVED_LATE"]
RET_STATUSES = ["requested", "approved", "shipped_back", "received", "refund_processed", "rejected"]
CARRIERS = ["FedEx", "UPS", "USPS", "DHL", "Amazon Logistics"]
SHIP_STAT = ["label_created", "picked_up", "in_transit", "out_for_delivery", "delivered"]
LOYALTY = ["bronze", "silver", "gold", "platinum"]

REASON_LABELS = {
    "DEFECTIVE": "Product arrived defective or damaged",
    "WRONG_ITEM": "Wrong item was shipped",
    "NOT_AS_DESCRIBED": "Product does not match description",
    "CHANGED_MIND": "Customer changed their mind",
    "SIZE_FIT": "Size or fit issue",
    "ARRIVED_LATE": "Item arrived after expected date",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customers (10,000 rows)

# COMMAND ----------

customers = []
for i in range(1, 10001):
    ci = random.randint(0, len(CITIES) - 1)
    fn, ln = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
    addr = f"{random.randint(100, 9999)} {random.choice(STREETS)}, {CITIES[ci]}, {STATES[ci]} {random.randint(10000, 99999)}"
    customers.append({
        "customer_id": f"CUST-{i:05d}",
        "customer_name": f"{fn} {ln}",
        "email": f"{fn.lower()}.{ln.lower()}{i}@email.com",
        "phone": f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
        "shipping_address": addr,
        "billing_address": addr,
        "signup_date": random_date(datetime(2020, 1, 1), NOW).strftime("%Y-%m-%d"),
        "loyalty_tier": random.choice(LOYALTY),
        "region": REGIONS[ci],
    })

write_csv(customers, f"{VOLUME_BASE}/customers/customers.csv")
print(f"✓ Customers: {len(customers)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Products (500 rows)

# COMMAND ----------

products = []
pid = 0
for cat in CATEGORIES:
    for pn in PRODUCT_NAMES[cat]:
        for br in BRANDS[cat]:
            pid += 1
            is_returnable = False if cat == "Grocery" else random.random() > 0.05
            products.append({
                "product_id": f"PROD-{pid:04d}",
                "product_name": f"{br} {pn}",
                "category": cat,
                "subcategory": pn.split()[0],
                "brand": br,
                "price": round(random.uniform(5.99, 499.99), 2),
                "sku": f"SKU-{cat[:3].upper()}-{pid:04d}",
                "return_window_days": 30 if cat in ["Electronics", "Clothing"] else 14 if cat in ["Beauty"] else 21,
                "is_returnable": is_returnable,
            })
            if pid >= 500:
                break
        if pid >= 500:
            break
    if pid >= 500:
        break
products = products[:500]

write_csv(products, f"{VOLUME_BASE}/products/products.csv")
print(f"✓ Products: {len(products)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Orders (50,000 rows) & Order Items (~120,000 rows)

# COMMAND ----------

orders = []
order_items = []
oi_id = 0
for i in range(1, 50001):
    cust = random.choice(customers)
    odate = random_ts(ONE_YEAR_AGO, NOW)
    days_ago = (NOW - odate).days
    status = "delivered" if days_ago > 7 else random.choice(["placed", "shipped", "delivered"])
    if random.random() < 0.03:
        status = "cancelled"

    num_items = random.randint(1, 5)
    items_total = 0
    for j in range(num_items):
        oi_id += 1
        prod = random.choice(products)
        qty = random.randint(1, 3)
        disc = round(random.uniform(0, prod["price"] * 0.2), 2) if random.random() < 0.3 else 0
        line = round(qty * prod["price"] - disc, 2)
        items_total += line
        order_items.append({
            "order_item_id": f"OI-{oi_id:06d}",
            "order_id": f"ORD-{i:05d}",
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": prod["price"],
            "discount_amount": disc,
            "item_status": status,
        })

    tax = round(items_total * 0.08, 2)
    ship = round(random.choice([0, 5.99, 9.99, 12.99]), 2) if items_total < 50 else 0
    orders.append({
        "order_id": f"ORD-{i:05d}",
        "customer_id": cust["customer_id"],
        "order_date": odate.strftime("%Y-%m-%dT%H:%M:%S"),
        "status": status,
        "total_amount": round(items_total + tax + ship, 2),
        "shipping_cost": ship,
        "tax_amount": tax,
        "channel": random.choice(CHANNELS),
    })

write_jsonl(orders, f"{VOLUME_BASE}/orders/orders.json")
print(f"✓ Orders: {len(orders)} rows")

write_jsonl(order_items, f"{VOLUME_BASE}/order_items/order_items.json")
print(f"✓ Order Items: {len(order_items)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Invoices (50,000 rows)

# COMMAND ----------

invoices = []
for o in orders:
    odate = datetime.strptime(o["order_date"], "%Y-%m-%dT%H:%M:%S")
    inv_date = odate + timedelta(hours=random.randint(0, 2))
    due_date = inv_date + timedelta(days=30)
    subtotal = round(o["total_amount"] - o["tax_amount"] - o["shipping_cost"], 2)
    invoices.append({
        "invoice_id": f"INV-{o['order_id'].split('-')[1]}",
        "order_id": o["order_id"],
        "customer_id": o["customer_id"],
        "invoice_number": f"INV-2025-{o['order_id'].split('-')[1]}",
        "invoice_date": inv_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "subtotal": subtotal,
        "tax": o["tax_amount"],
        "shipping": o["shipping_cost"],
        "total": o["total_amount"],
        "due_date": due_date.strftime("%Y-%m-%d"),
        "pdf_url": f"/invoices/{o['order_id']}/invoice.pdf",
    })

write_jsonl(invoices, f"{VOLUME_BASE}/invoices/invoices.json")
print(f"✓ Invoices: {len(invoices)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Payments (~55,000 rows)

# COMMAND ----------

random.seed(99)
payments = []
pay_id = 0
for o in orders:
    pay_id += 1
    odate = datetime.strptime(o["order_date"], "%Y-%m-%dT%H:%M:%S")
    method = random.choice(PAY_METHODS)
    if o["status"] == "cancelled":
        pstatus, paid_at = "failed", None
    elif method == "cod":
        pstatus = "completed" if o["status"] == "delivered" else "pending"
        paid_at = (odate + timedelta(days=random.randint(3, 10))).strftime("%Y-%m-%dT%H:%M:%S") if pstatus == "completed" else None
    else:
        pstatus = "completed"
        paid_at = (odate + timedelta(minutes=random.randint(1, 30))).strftime("%Y-%m-%dT%H:%M:%S")

    payments.append({
        "payment_id": f"PAY-{pay_id:06d}",
        "invoice_id": f"INV-{o['order_id'].split('-')[1]}",
        "order_id": o["order_id"],
        "payment_method": method,
        "payment_status": pstatus,
        "transaction_ref": f"TXN-{random.randint(100000000, 999999999)}",
        "amount": o["total_amount"],
        "paid_at": paid_at,
    })
    # 10% chance of a prior failed attempt
    if random.random() < 0.1 and pstatus == "completed":
        pay_id += 1
        payments.append({
            "payment_id": f"PAY-{pay_id:06d}",
            "invoice_id": f"INV-{o['order_id'].split('-')[1]}",
            "order_id": o["order_id"],
            "payment_method": method,
            "payment_status": "failed",
            "transaction_ref": f"TXN-{random.randint(100000000, 999999999)}",
            "amount": o["total_amount"],
            "paid_at": None,
        })

write_jsonl(payments, f"{VOLUME_BASE}/payments/payments.json")
print(f"✓ Payments: {len(payments)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Returns (8,000 rows)

# COMMAND ----------

# Build order_items lookup
oi_by_order = {}
for oi in order_items:
    oi_by_order.setdefault(oi["order_id"], []).append(oi)

prod_map = {p["product_id"]: p for p in products}

delivered_orders = [o for o in orders if o["status"] == "delivered"]
return_candidates = random.sample(delivered_orders, min(8000, len(delivered_orders)))

returns = []
for i, o in enumerate(return_candidates):
    odate = datetime.strptime(o["order_date"], "%Y-%m-%dT%H:%M:%S")
    items = oi_by_order.get(o["order_id"], [])
    if not items:
        continue
    item = random.choice(items)

    req_at = odate + timedelta(days=random.randint(1, 28))
    reason = random.choice(RET_REASONS)
    days_since_req = (NOW - req_at).days

    if days_since_req > 20:
        status = random.choices(["refund_processed", "rejected"], weights=[85, 15])[0]
    elif days_since_req > 10:
        status = random.choice(["shipped_back", "received", "refund_processed"])
    elif days_since_req > 3:
        status = random.choice(["approved", "shipped_back"])
    else:
        status = "requested"

    completed = (req_at + timedelta(days=random.randint(5, 20))).strftime("%Y-%m-%dT%H:%M:%S") if status in ["refund_processed", "rejected"] else None
    line_total = round(item["quantity"] * item["unit_price"] - item["discount_amount"], 2)
    refund_amt = round(line_total * random.uniform(0.8, 1.0), 2) if status != "rejected" else 0

    returns.append({
        "return_id": f"RET-{i + 1:05d}",
        "order_id": o["order_id"],
        "customer_id": o["customer_id"],
        "order_item_id": item["order_item_id"],
        "product_id": item["product_id"],
        "return_reason": reason,
        "return_status": status,
        "refund_amount": refund_amt,
        "refund_method": random.choice(["credit_card", "store_credit", "original_payment", "wallet"]),
        "requested_at": req_at.strftime("%Y-%m-%dT%H:%M:%S"),
        "completed_at": completed,
    })

write_jsonl(returns, f"{VOLUME_BASE}/returns/returns.json")
print(f"✓ Returns: {len(returns)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Return Policy (reference data)

# COMMAND ----------

policy = []
for cat in CATEGORIES:
    window = 30 if cat in ["Electronics", "Clothing"] else 14 if cat in ["Beauty"] else 21
    fee = 0 if cat in ["Electronics"] else 15 if cat in ["Clothing", "Sports"] else 10
    final = cat in ["Beauty"]
    policy.append({
        "product_category": cat,
        "return_window_days": window,
        "restocking_fee_pct": fee,
        "conditions_text": f"Item must be unused and in original packaging. {cat}-specific conditions apply.",
        "is_final_sale": final,
        "reason_code": random.choice(RET_REASONS),
        "reason_label": REASON_LABELS[random.choice(RET_REASONS)],
    })

# Add per-reason reference rows
for reason, label in REASON_LABELS.items():
    policy.append({
        "product_category": "ALL",
        "return_window_days": 30,
        "restocking_fee_pct": 0,
        "conditions_text": "General return conditions apply",
        "is_final_sale": False,
        "reason_code": reason,
        "reason_label": label,
    })

write_csv(policy, f"{VOLUME_BASE}/return_policy/return_policy.csv")
print(f"✓ Return Policy: {len(policy)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Shipping Tracking (~60,000 rows)

# COMMAND ----------

tracking = []
tid = 0

# Order shipments
for o in orders:
    if o["status"] == "placed":
        continue
    tid += 1
    odate = datetime.strptime(o["order_date"], "%Y-%m-%dT%H:%M:%S")
    ship_date = odate + timedelta(days=random.randint(0, 2))
    est_del = ship_date + timedelta(days=random.randint(3, 7))
    if o["status"] == "delivered":
        sstatus = "delivered"
    elif o["status"] == "shipped":
        sstatus = random.choice(["in_transit", "out_for_delivery"])
    else:
        sstatus = random.choice(SHIP_STAT[:3])

    tracking.append({
        "tracking_id": f"TRK-{tid:06d}",
        "order_id": o["order_id"],
        "return_id": None,
        "carrier": random.choice(CARRIERS),
        "tracking_number": f"{random.choice(CARRIERS)[:2].upper()}{random.randint(100000000, 999999999)}",
        "status": sstatus,
        "estimated_delivery": est_del.strftime("%Y-%m-%d"),
        "last_update": (ship_date + timedelta(days=random.randint(0, 5))).strftime("%Y-%m-%dT%H:%M:%S"),
    })

# Return shipments
for r in returns:
    if r["return_status"] in ["shipped_back", "received", "refund_processed"]:
        tid += 1
        req = datetime.strptime(r["requested_at"], "%Y-%m-%dT%H:%M:%S")
        ship_date = req + timedelta(days=random.randint(1, 3))
        est_del = ship_date + timedelta(days=random.randint(3, 7))
        sstatus = "delivered" if r["return_status"] in ["received", "refund_processed"] else random.choice(["in_transit", "out_for_delivery"])

        tracking.append({
            "tracking_id": f"TRK-{tid:06d}",
            "order_id": r["order_id"],
            "return_id": r["return_id"],
            "carrier": random.choice(CARRIERS),
            "tracking_number": f"RT{random.randint(100000000, 999999999)}",
            "status": sstatus,
            "estimated_delivery": est_del.strftime("%Y-%m-%d"),
            "last_update": (ship_date + timedelta(days=random.randint(0, 5))).strftime("%Y-%m-%dT%H:%M:%S"),
        })

write_jsonl(tracking, f"{VOLUME_BASE}/shipping_tracking/shipping_tracking.json")
print(f"✓ Shipping Tracking: {len(tracking)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 50)
print("DATA GENERATION COMPLETE")
print("=" * 50)
print(f"  Customers:         {len(customers):>8,}")
print(f"  Products:          {len(products):>8,}")
print(f"  Orders:            {len(orders):>8,}")
print(f"  Order Items:       {len(order_items):>8,}")
print(f"  Invoices:          {len(invoices):>8,}")
print(f"  Payments:          {len(payments):>8,}")
print(f"  Returns:           {len(returns):>8,}")
print(f"  Return Policy:     {len(policy):>8,}")
print(f"  Shipping Tracking: {len(tracking):>8,}")
print(f"\nVolume: {VOLUME_BASE}")
