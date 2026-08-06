# Databricks notebook source
# MAGIC %md
# MAGIC # Payment authorization pipeline
# MAGIC
# MAGIC ```
# MAGIC   landing_payments        Delta table. Amounts in integer cents.
# MAGIC         |
# MAGIC   bronze_payments         ST, append. Ingest only, no logic.
# MAGIC         |
# MAGIC   silver_payments         ST, append. Cleanse and normalize.
# MAGIC         |
# MAGIC   gold_merchant_5min      ST, stateful. 5-minute tumbling window per
# MAGIC                           merchant, 10-minute watermark.
# MAGIC ```
# MAGIC
# MAGIC The only interesting line in this file is the `amount_minor / 100` in
# MAGIC `silver_payments`. Removing it is the bug the demo recovers from.
# MAGIC
# MAGIC A Delta landing table is deliberate, not incidental: Auto Loader sources
# MAGIC currently fail whole-pipeline rewind with
# MAGIC `CF_TIME_TRAVEL_ERROR.OFFSET_NOT_FOUND`.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("demo.catalog")
SCHEMA = spark.conf.get("demo.schema")

# Set to "true" in the pipeline configuration to ship the cents-as-dollars bug.
# Kept as a config flag so the whole scenario is scriptable; a real deploy would
# be a code change, and the blog frames it that way.
BUG_ENABLED = spark.conf.get("demo.bug.enabled", "false").lower() == "true"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze: ingest, unmodified
# MAGIC
# MAGIC No business logic lives here. That is the point: a bug in cleansing must
# MAGIC never require re-reading the source, which is what the scoped rewind shows.


@dp.table(
    name="bronze_payments",
    comment="Raw payment authorizations as delivered. Amounts still in integer cents.",
    table_properties={"quality": "bronze"},
)
def bronze_payments():
    return (
        spark.readStream.table(f"{CATALOG}.{SCHEMA}.landing_payments")
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: cleanse and normalize
# MAGIC
# MAGIC Drops test traffic and non-approved authorizations, parses event time, and
# MAGIC converts cents to dollars.
# MAGIC
# MAGIC Note the buggy branch casts to `double` rather than leaving the column as
# MAGIC `bigint`. A type change would be caught immediately by SDP
# MAGIC (`CANNOT_UPDATE_TABLE_SCHEMA`) and the bad deploy would fail loudly, which
# MAGIC is the opposite of the failure mode this demo is about. Holding the type
# MAGIC steady is what lets the bug ship silently.


@dp.table(
    name="silver_payments",
    comment="Approved, non-test payments with amounts normalized to dollars.",
    table_properties={"quality": "silver"},
)
def silver_payments():
    amount = (
        F.col("amount_minor").cast("double")
        if BUG_ENABLED
        else F.col("amount_minor") / F.lit(100)
    )

    return (
        spark.readStream.table("bronze_payments")
        .where(~F.coalesce(F.col("is_test"), F.lit(False)))
        .where(F.col("auth_result") == "approved")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("amount", amount)
        .select(
            "payment_id",
            "merchant_id",
            "merchant_name",
            "merchant_category",
            "amount",
            "currency",
            "card_network",
            "event_ts",
            "_ingested_at",
        )
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold: stateful 5-minute windowed totals
# MAGIC
# MAGIC A streaming table is an append-only target, so this uses a watermark and a
# MAGIC tumbling window. Each window's row is emitted once, when the watermark
# MAGIC passes the end of that window. The partial windows and the watermark
# MAGIC position live in operator state, not in the table.
# MAGIC
# MAGIC That is the part a table RESTORE cannot put back, and the reason rewinding
# MAGIC data alone is not enough.


@dp.table(
    name="gold_merchant_5min",
    comment="Per-merchant payment totals in 5-minute tumbling windows.",
    table_properties={"quality": "gold"},
)
def gold_merchant_5min():
    return (
        spark.readStream.table("silver_payments")
        .withWatermark("event_ts", "10 minutes")
        .groupBy(F.window("event_ts", "5 minutes"), F.col("merchant_id"))
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("txn_count"),
            F.max("amount").alias("max_amount"),
            F.first("merchant_name").alias("merchant_name"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "merchant_id",
            "merchant_name",
            F.round("total_amount", 2).alias("total_amount"),
            "txn_count",
            F.round("max_amount", 2).alias("max_amount"),
            # The diagnostic column. Volume stays normal while this explodes,
            # which is what identifies a valuation bug rather than a volume spike.
            F.round(F.col("total_amount") / F.col("txn_count"), 2).alias("avg_ticket"),
        )
    )
