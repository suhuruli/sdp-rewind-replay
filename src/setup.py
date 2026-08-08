# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: catalog, schema, and the landing table
# MAGIC
# MAGIC Run once before deploying the pipeline. Idempotent.

# COMMAND ----------

# Defaults are fallbacks only. The real values arrive as base_parameters from
# resources/jobs.yml, which sources them from the DAB variables.
dbutils.widgets.text("catalog", "harsha_rewind_demo")
dbutils.widgets.text("schema", "payments")
dbutils.widgets.text("landing_table", "landing_payments")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LANDING = dbutils.widgets.get("landing_table")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# The pipeline reads this as a streaming source, so it must exist before the first
# pipeline update. Amounts are integer cents, exactly as a card network reports them.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{LANDING} (
  payment_id        STRING  COMMENT 'Unique authorization id',
  merchant_id       STRING  COMMENT 'Merchant identifier',
  merchant_name     STRING,
  merchant_category STRING,
  amount_minor      BIGINT  COMMENT 'Amount in MINOR UNITS (cents). 7969 means $79.69.',
  currency          STRING,
  card_network      STRING,
  auth_result       STRING  COMMENT 'approved | declined | referred',
  is_test           BOOLEAN COMMENT 'Synthetic test traffic, filtered out in silver',
  event_time        STRING  COMMENT 'Authorization timestamp, ISO-8601'
)
COMMENT 'Raw payment authorizations as delivered by the processor.'
""")

print(f"ready: {CATALOG}.{SCHEMA}.{LANDING}")
