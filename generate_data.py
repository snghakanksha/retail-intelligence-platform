"""
Retail Intelligence Platform — Synthetic Data Generator
Generates realistic e-commerce data for Snowflake ingestion.
Run once: python generate_data.py
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Configuration ──────────────────────────────────────────────
N_CUSTOMERS = 10_000
N_PRODUCTS  = 500
N_ORDERS    = 50_000
N_EVENTS = 50_000   # was 200_000 — 50k is plenty for this project

START_DATE  = datetime(2023, 1, 1)
END_DATE    = datetime(2024, 12, 31)

CATEGORIES  = ["Electronics", "Clothing", "Home & Kitchen", "Books",
               "Sports", "Beauty", "Toys", "Automotive", "Garden"]

SEGMENTS    = ["Premium", "Standard", "Budget"]
STATUSES    = ["completed", "completed", "completed", "returned",
               "cancelled", "pending"]   # weighted toward completed
EVENT_TYPES = ["page_view", "page_view", "page_view", "add_to_cart",
               "add_to_cart", "checkout", "purchase"]

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


# ── 1. Generate Customers ──────────────────────────────────────
print("Generating customers...")
customers = []
for _ in range(N_CUSTOMERS):
    signup = random_date(START_DATE, END_DATE)
    customers.append({
        "customer_id":   str(uuid.uuid4()),
        "customer_name": fake.name(),
        "email":         fake.email(),
        "phone":         fake.phone_number(),
        "city":          fake.city(),
        "country":       fake.country(),
        "segment":       random.choice(SEGMENTS),
        "signup_date":   signup.strftime("%Y-%m-%d"),
    })

customer_ids = [c["customer_id"] for c in customers]

with open(OUTPUT_DIR / "customers.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=customers[0].keys())
    writer.writeheader()
    writer.writerows(customers)

print(f"  ✓ {len(customers):,} customers written")


# ── 2. Generate Products ───────────────────────────────────────
print("Generating products...")
products = []
for _ in range(N_PRODUCTS):
    category = random.choice(CATEGORIES)
    cost      = round(random.uniform(5, 400), 2)
    # margin between 20-60%
    price     = round(cost * random.uniform(1.2, 1.6), 2)
    products.append({
        "product_id":    str(uuid.uuid4()),
        "product_name":  f"{fake.word().capitalize()} {category[:-1] if category.endswith('s') else category}",
        "category":      category,
        "sub_category":  fake.word().capitalize(),
        "supplier":      fake.company(),
        "unit_cost":     cost,
        "unit_price":    price,
        "stock_qty":     random.randint(0, 500),
        "is_active":     random.choices([True, False], weights=[95, 5])[0],
    })

product_ids = [p["product_id"] for p in products]

with open(OUTPUT_DIR / "products.json", "w") as f:
    json.dump(products, f, indent=2)

print(f"  ✓ {len(products):,} products written")


# ── 3. Generate Orders ─────────────────────────────────────────
print("Generating orders...")
orders = []
for _ in range(N_ORDERS):
    order_date = random_date(START_DATE, END_DATE)
    unit_price = random.choice(products)["unit_price"]
    orders.append({
        "order_id":        str(uuid.uuid4()),
        "customer_id":     random.choice(customer_ids),
        "product_id":      random.choice(product_ids),
        "order_date":      order_date.strftime("%Y-%m-%d"),
        "order_status":    random.choice(STATUSES),
        "quantity":        random.randint(1, 10),
        "unit_price":      round(unit_price, 2),
        "discount_pct":    round(random.choices(
                               [0, 0, 0, 5, 10, 15, 20],
                               weights=[50, 15, 10, 10, 7, 5, 3]
                           )[0], 2),
        "shipping_city":   fake.city(),
        "shipping_country": fake.country(),
    })

with open(OUTPUT_DIR / "orders.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)

print(f"  ✓ {len(orders):,} orders written")


# ── 4. Generate Web Events ─────────────────────────────────────
print("Generating web events...")
events = []
for _ in range(N_EVENTS):
    event_time = random_date(START_DATE, END_DATE)
    event_type = random.choice(EVENT_TYPES)
    event = {
        "event_id":      str(uuid.uuid4()),
        "session_id":    str(uuid.uuid4()),
        "customer_id":   random.choice(customer_ids + [None] * 2000),  # some anonymous
        "event_type":    event_type,
        "event_ts":      event_time.isoformat(),
        "page":          random.choice(["/home", "/products", "/cart",
                                        "/checkout", "/search", "/account"]),
        "product_id":    random.choice(product_ids) if event_type in
                         ("add_to_cart", "purchase") else None,
        "device":        random.choice(["mobile", "desktop", "tablet"]),
        "browser":       random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
        "country":       fake.country_code(),
    }
    events.append(event)

with open(OUTPUT_DIR / "web_events.json", "w") as f:
    json.dump(events, f, indent=2)

print(f"  ✓ {len(events):,} web events written")

print("\nAll files generated in data/raw/")
print("  customers.csv   — structured, CSV")
print("  products.json   — semi-structured, JSON")
print("  orders.csv      — structured, CSV")
print("  web_events.json — semi-structured, JSON")