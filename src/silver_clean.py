# Databricks notebook source
from pyspark.sql import functions as F
spark.conf.set("spark.sql.ansi.enabled", "false")

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

from pyspark.sql.functions import col, current_date, months_between, floor, coalesce, to_timestamp, trim, initcap, to_date


# COMMAND ----------

# DBTITLE 1,Cell 5
silver_df_customers = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_cp_customers").select(
    col("customer_id")
    ,initcap(trim(col("first_name"))).alias("first_name")
    ,initcap(trim(col("last_name"))).alias("last_name")
    ,coalesce(
        to_timestamp(trim(col("date_of_birth").cast("string")), "dd-MM-yyyy"),
        to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy-MM-dd"),
        to_timestamp(trim(col("date_of_birth").cast("string")), "dd/MM/yyyy"),
        to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy/MM/dd"),
        to_timestamp(trim(col("date_of_birth").cast("string")), "dd MMM yyyy"),
        to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy MMM dd")
    ).cast("date").alias("date_of_birth")
    ,initcap(trim(col("city"))).alias("city")
    ,col("employment_status")
    ,col("annual_income").cast("decimal(10,2)").alias("annual_income")
    ,col("product_type")
    ,coalesce(
        to_date(trim(col("account_opened").cast("string")), "dd-MM-yyyy"),
        to_date(trim(col("account_opened").cast("string")), "yyyy-MM-dd"),
        to_date(trim(col("account_opened").cast("string")), "dd/MM/yyyy"),
        to_date(trim(col("account_opened").cast("string")), "yyyy/MM/dd"),
        to_date(trim(col("account_opened").cast("string")), "dd MMM yyyy"),
        to_date(trim(col("account_opened").cast("string")), "yyyy MMM dd")
    ).cast("date").alias("account_opened")
    ,col("credit_limit")
    ,col("ingested_at")
    ,col("source_file"))

silver_df_repayments = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_cp_repayments").select(
    col("repayment_id")
    ,col("customer_id")
    ,coalesce(
        to_timestamp(trim(col("due_date").cast("string")), "dd-MM-yyyy"),
        to_timestamp(trim(col("due_date").cast("string")), "yyyy-MM-dd"),
        to_timestamp(trim(col("due_date").cast("string")), "dd/MM/yyyy"),
        to_timestamp(trim(col("due_date").cast("string")), "yyyy/MM/dd"),
        to_timestamp(trim(col("due_date").cast("string")), "dd MMM yyyy"),
        to_timestamp(trim(col("due_date").cast("string")), "yyyy MMM dd")
    ).cast("date").alias("due_date")
    ,col("amount_due")
    ,col("amount_paid")
    ,col("payment_status")
    ,col("ingested_at")
    ,col("source_file"))

silver_df_transactions = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_cp_transactions").select(
    col("transaction_id")
    ,col("customer_id")
    ,coalesce(
        to_timestamp(trim(col("transaction_date").cast("string")), "dd-MM-yyyy"),
        to_timestamp(trim(col("transaction_date").cast("string")), "yyyy-MM-dd"),
        to_timestamp(trim(col("transaction_date").cast("string")), "dd/MM/yyyy"),
        to_timestamp(trim(col("transaction_date").cast("string")), "yyyy/MM/dd"),
        to_timestamp(trim(col("transaction_date").cast("string")), "dd MMM yyyy"),
        to_timestamp(trim(col("transaction_date").cast("string")), "yyyy MMM dd")
    ).cast("date").alias("transaction_date")
    ,col("amount")
    ,initcap(trim(col("transaction_type"))).alias("transaction_type")
    ,col("merchant_category")
    ,col("ingested_at")
    ,col("source_file"))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %sql
# MAGIC drop table tesco_bank_training.gavin_smith_silver.silver_cp_customers;
# MAGIC drop table tesco_bank_training.gavin_smith_silver.silver_cp_repayments;
# MAGIC drop table tesco_bank_training.gavin_smith_silver.silver_cp_transactions;

# COMMAND ----------

silver_df_customers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_cp_customers")

silver_df_repayments.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_cp_repayments")

silver_df_transactions.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_cp_transactions")

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_cp_customers"))
display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_cp_repayments"))
display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_cp_transactions"))