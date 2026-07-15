# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError("Please enter your schema name in the widget above before running.")

CATALOG        = "tesco_bank_training"
SOURCE_SCHEMA  = f"{user_schema}_bronze"
SILVER_SCHEMA  = f"{user_schema}_silver"

print(f"Source:  {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target:  {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

silver_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest").select(
    F.lower(F.col("customer_id")),
    F.upper(F.col("product_type")),
    F.col("credit_limit"),
    F.col("city")
)

# COMMAND ----------

silver_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean")
