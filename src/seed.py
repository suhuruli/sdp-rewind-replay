# Databricks notebook source
# MAGIC %md
# MAGIC # Payment event seeder
# MAGIC
# MAGIC Appends a batch of payment authorizations to the landing table. Scheduled
# MAGIC every few minutes so the pipeline behaves like a live production stream with
# MAGIC data continuously arriving.
# MAGIC
# MAGIC ## Why event time is spread, and why it matters
# MAGIC
# MAGIC Gold aggregates into 5-minute tumbling windows behind a 10-minute watermark.
# MAGIC A window only emits once the watermark passes its end. Two consequences:
# MAGIC
# MAGIC 1. If every row in a batch shares one timestamp, they all land in a single
# MAGIC    still-open window and **gold emits nothing at all**.
# MAGIC 2. Recent windows legitimately stay open, so gold row counts freeze even
# MAGIC    though the pipeline is perfectly healthy. That is expected behavior, not
# MAGIC    a symptom of a failed rewind.
# MAGIC
# MAGIC So each batch spreads its events across `spread_minutes` of event time, and
# MAGIC `lag_minutes` places the batch far enough in the past that its windows close
# MAGIC promptly instead of hovering open.

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

dbutils.widgets.text("catalog", "harsha_rewind_demo")
dbutils.widgets.text("schema", "payments")
dbutils.widgets.text("events_per_run", "400")
dbutils.widgets.text("spread_minutes", "12")
dbutils.widgets.text("lag_minutes", "20")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
EVENTS = int(dbutils.widgets.get("events_per_run"))
SPREAD_MINUTES = float(dbutils.widgets.get("spread_minutes"))
LAG_MINUTES = float(dbutils.widgets.get("lag_minutes"))

# COMMAND ----------

# Fixed merchants so gold has stable grouping keys across runs.
MERCHANTS = [
    ("M001", "Northwind Coffee", "food_beverage"),
    ("M002", "Aurora Cycles", "sporting_goods"),
    ("M003", "Bellwether Books", "retail"),
    ("M004", "Cedar & Pine Hardware", "home_improvement"),
    ("M005", "Halcyon Pharmacy", "health"),
    ("M006", "Ridgeline Fuel", "fuel"),
]

CURRENCIES = ["USD"] * 17 + ["CAD", "GBP", "EUR"]

# Typical ticket size per merchant, in cents. Deliberately spread so the gold
# chart has visible separation: coffee runs ~$14, bikes ~$530.
TICKET_CENTS = {
    "M001": (350, 2400),
    "M002": (4500, 89000),
    "M003": (1200, 6500),
    "M004": (2200, 47000),
    "M005": (800, 12000),
    "M006": (3500, 11000),
}

SCHEMA_STRUCT = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount_minor", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("card_network", StringType(), True),
        StructField("auth_result", StringType(), True),
        StructField("is_test", BooleanType(), True),
        StructField("event_time", StringType(), True),
    ]
)


def make_event(event_time: datetime) -> Row:
    merchant_id, merchant_name, category = random.choice(MERCHANTS)
    low, high = TICKET_CENTS[merchant_id]

    return Row(
        payment_id=str(uuid.uuid4()),
        merchant_id=merchant_id,
        merchant_name=merchant_name,
        merchant_category=category,
        # Integer cents, never a decimal. This is the field that gets misread.
        amount_minor=random.randint(low, high),
        currency=random.choice(CURRENCIES),
        card_network=random.choice(["visa", "mastercard", "amex", "discover"]),
        auth_result=random.choices(
            ["approved", "declined", "referred"], weights=[92, 7, 1]
        )[0],
        is_test=random.random() < 0.02,
        event_time=event_time.isoformat(),
    )


# COMMAND ----------

batch_end = datetime.now(timezone.utc) - timedelta(minutes=LAG_MINUTES)
spread = timedelta(minutes=SPREAD_MINUTES)

rows = [
    make_event(batch_end - spread * (1 - i / max(EVENTS - 1, 1)))
    for i in range(EVENTS)
]

(
    spark.createDataFrame(rows, schema=SCHEMA_STRUCT)
    .write.mode("append")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.landing_payments")
)

print(
    f"appended {len(rows)} events spanning "
    f"{(batch_end - spread).strftime('%H:%M:%S')}..{batch_end.strftime('%H:%M:%S')} UTC"
)
