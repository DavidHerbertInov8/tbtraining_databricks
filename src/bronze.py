# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: Data Ingestion
# MAGIC
# MAGIC **Task:** Read the raw transactions CSV and write it to your personal bronze schema as a managed Delta table.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Preserve the raw data exactly as it landed (no cleaning here)
# MAGIC - Add two audit columns: `_ingested_at` and `_source_file`
# MAGIC - Write as a managed Delta table to your bronze schema
# MAGIC - Add a row count assertion at the end
# MAGIC
# MAGIC **Enter your schema name in the widget below before running.**

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError("Please enter your schema name in the widget above before running.")

CATALOG       = "tesco_bank_training"
BRONZE_SCHEMA = f"{user_schema}_bronze"
RAW_PATH      = "s3://databricks-inov8-training-sandbox/raw-data/transactions.csv"

print(f"Target table: {CATALOG}.{BRONZE_SCHEMA}.transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Read the raw CSV
# MAGIC
# MAGIC Use `spark.read` to load the CSV from `RAW_PATH`.
# MAGIC Use `inferSchema` and `header` options.
# MAGIC Print the row count and display a sample.

# COMMAND ----------

# TODO: Read the raw CSV into a DataFrame
# Hint: spark.read.format("csv").option(...).load(RAW_PATH)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Add audit columns
# MAGIC
# MAGIC Add two new columns to the DataFrame:
# MAGIC - `_ingested_at`: the current timestamp
# MAGIC - `_source_file`: the literal string `"transactions.csv"`
# MAGIC
# MAGIC Hint: you will need to import functions from `pyspark.sql.functions`

# COMMAND ----------

# TODO: Add _ingested_at and _source_file columns


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Write to bronze Delta table
# MAGIC
# MAGIC Write the DataFrame as a managed Delta table.
# MAGIC Use `overwrite` mode so the notebook can be re-run idempotently.

# COMMAND ----------

# TODO: Write to Delta table
# Hint: .write.format("delta").mode("overwrite").saveAsTable(...)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Row count assertion
# MAGIC
# MAGIC Read back the table and assert the row count is greater than zero.
# MAGIC This will cause the job to fail loudly if something went wrong.

# COMMAND ----------

# TODO: Add row count assertion
# Hint: use spark.table(...).count() and assert


