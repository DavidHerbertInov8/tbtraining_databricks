# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError(
        "Please enter your schema name in the widget above before running."
    )

CATALOG = "tesco_bank_training"
SOURCE_SCHEMA = "datasets"
BRONZE_SCHEMA = f"{user_schema}_bronze"

print(f"Source:  {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target:  {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

raw_data = spark.table("tesco_bank_training.datasets.customers").select(
    "*", current_timestamp().alias("ingestion_timestamp"), lit("customers").alias("source_file")
)

# COMMAND ----------

if raw_data.limit(1).count() == 0:
    raise ValueError("Error: Bronze Customer table has zero observations")


raw_data.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_customer")

# COMMAND ----------

raw_data_r = spark.table("tesco_bank_training.datasets.repayments").select(
    "*", current_timestamp().alias("ingestion_timestamp"), lit("repayments").alias("source_file")
)

# COMMAND ----------

if raw_data_r.limit(1).count() == 0:
    raise ValueError("Error: Bronze Repayment table has zero observations")

raw_data_r.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_repayments")

# COMMAND ----------

raw_data_t = spark.table("tesco_bank_training.datasets.transactions").select(
    "*", current_timestamp().alias("ingestion_timestamp"), lit("transactions").alias("source_file")
)

# COMMAND ----------

if raw_data_t.limit(1).count() == 0:
    raise ValueError("Error: Bronze Transaction table has zero observations")

raw_data_t.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_transactions")