# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: Data Ingestion
# MAGIC
# MAGIC Reads all three source datasets from the shared `datasets` schema and
# MAGIC writes them to the trainee's personal bronze schema as managed Delta tables.
# MAGIC
# MAGIC - Preserves raw data exactly as-is
# MAGIC - Adds two audit columns: `_ingested_at` and `_source_file`
# MAGIC - Writes with `overwrite` mode for idempotent re-runs

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError("Please enter your schema name in the widget above before running.")

CATALOG        = "tesco_bank_training"
SOURCE_SCHEMA  = "datasets"
BRONZE_SCHEMA  = f"{user_schema}_bronze"

print(f"Source:  {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target:  {CATALOG}.{BRONZE_SCHEMA}")

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

print(spark.sql("SELECT current_user()").collect())
print(spark.sql("SHOW CATALOGS").collect())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

TABLES = ["customers", "transactions", "repayments"]

for table_name in TABLES:
    source = f"{CATALOG}.{SOURCE_SCHEMA}.{table_name}"
    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    df = (
        spark.table(source)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(table_name))
    )

    df.write.format("delta").mode("overwrite").saveAsTable(target)

    count = spark.table(target).count()
    assert count > 0, f"Bronze write produced an empty table for {table_name}."
    print(f"  {target}: {count} rows")

print("Bronze ingest complete.")
