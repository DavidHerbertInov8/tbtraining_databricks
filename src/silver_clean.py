# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

customers = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_customers")
display(customers)

# COMMAND ----------

# DBTITLE 1,Cell 5
# a. Perform data cleaning as needed to satisfy business rules.

silver_df = customers.select(
    F.lower(F.col("customer_id")).alias("customer_id"),
    F.initcap(F.trim(F.col("first_name"))).alias("first_name"),
    F.initcap(F.trim(F.col("last_name"))).alias("last_name"),
    F.coalesce(
        F.try_to_date(F.col("date_of_birth"), "dd/MM/yyyy"),
        F.try_to_date(F.col("date_of_birth"), "yyyy-MM-dd"),
        F.try_to_date(F.col("date_of_birth"), "dd MMM yyyy"),
        F.try_to_date(F.col("date_of_birth"), "yyyy/MM/dd"),
        F.try_to_date(F.col("date_of_birth"), "dd-MM-yyyy"),
    ).alias("date_of_birth"),
    F.initcap(F.trim(F.col("city"))).alias("city"),
    F.col("employment_status"),
    F.col("annual_income"),
    F.upper(F.col("product_type")).alias("product_type"),
    F.coalesce(
        F.try_to_date(F.col("account_opened"), "dd/MM/yyyy"),
        F.try_to_date(F.col("account_opened"), "yyyy-MM-dd"),
        F.try_to_date(F.col("account_opened"), "dd MMM yyyy"),
        F.try_to_date(F.col("account_opened"), "yyyy/MM/dd"),
        F.try_to_date(F.col("account_opened"), "dd-MM-yyyy"),
    ).alias("account_opened"),
    F.col("credit_limit"),
    F.to_date(F.col("ingested_at"), "yyyy-MM-dd").alias("ingested_at"),
    F.col("source_file")
)
display(silver_df)

# COMMAND ----------

# DBTITLE 1,Cell 6
# adjust the format for annual_income
silver_v2 = silver_df.withColumn("annual_income", F.regexp_replace("annual_income", "£", ""))
silver_v2 = silver_v2.withColumn("annual_income", F.regexp_replace("annual_income", ",", ""))
silver_v2 = silver_v2.withColumn("annual_income", F.col("annual_income").cast("decimal(10,2)"))

display(silver_v2)

# COMMAND ----------

# b. Store the output as a managed Delta table in your silver schema (This should be an overwrite).
silver_df.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean")