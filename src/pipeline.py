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
# MAGIC The only interesting line in this file is the `/ 100` on the `amount`
# MAGIC column in `silver_payments`. Deleting it is the bug the demo recovers from.
# MAGIC
# MAGIC A Delta landing table is deliberate, not incidental: Auto Loader sources
# MAGIC currently fail whole-pipeline rewind with
# MAGIC `CF_TIME_TRAVEL_ERROR.OFFSET_NOT_FOUND`.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("demo.catalog")
SCHEMA = spark.conf.get("demo.schema")

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
# MAGIC Four cleansing steps: drop test traffic, drop non-approved authorizations,
# MAGIC parse event time, and convert the amount from integer cents to dollars.
# MAGIC
# MAGIC The `/ 100` on the `amount` line is what the demo deletes. The cast to
# MAGIC `double` stays, which is what makes the bad deploy survive: `amount` keeps
# MAGIC its declared type, so SDP has nothing to reject and the update reports
# MAGIC COMPLETED. Drop the cast as well and SDP fails the deploy immediately with
# MAGIC `CANNOT_UPDATE_TABLE_SCHEMA`, because `amount` would turn from `double` into
# MAGIC `bigint`.
# MAGIC
# MAGIC That contrast is the argument for the feature. SDP already catches bad
# MAGIC deploys that change a schema. Rewind exists for the ones that do not.


@dp.table(
    name="silver_payments",
    comment="Approved, non-test payments with amounts normalized to dollars.",
    table_properties={"quality": "silver"},
)
def silver_payments():
    return (
        spark.readStream.table("bronze_payments")
        .where(~F.coalesce(F.col("is_test"), F.lit(False)))
        .where(F.col("auth_result") == "approved")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        # Amounts arrive as integer minor units: 7969 means $79.69.
        .withColumn("amount", F.col("amount_minor").cast("double") / 100)
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
