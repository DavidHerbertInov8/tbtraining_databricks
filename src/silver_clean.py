# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# %sql
#drop table tesco_bank_training.samantha_bell_bronze.bronze_ingest

# COMMAND ----------

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

silver_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_customer").select(
    F.trim(F.upper(F.col("customer_id"))).alias("customer_id"),
    F.trim(F.initcap(F.col("first_name"))).alias("first_name"),
    F.trim(F.initcap(F.col("last_name"))).alias("last_name"),
    F.coalesce(F.try_to_date("date_of_birth", 'yyyy-MM-dd'), 
            F.try_to_date("date_of_birth", 'yyyy/MM/dd'),
            F.try_to_date("date_of_birth", 'dd MMM yy'),
            F.try_to_date("date_of_birth", 'dd MMM yyyy'),
            F.try_to_date("date_of_birth", 'dd/MM/yyyy'),
            F.try_to_date("date_of_birth", 'dd-MM-yyyy')
                ).alias("date_of_birth"),
    F.trim(F.initcap(F.col("city"))).alias("city"),
    F.col("employment_status"),
    F.regexp_replace("annual_income", "[£,]", "").cast("decimal(18,2)").alias("income"),
    F.trim(F.initcap(F.col("product_type"))).alias("product_type"),
    F.coalesce(F.try_to_date("account_opened", 'dd/mm/yyyy'), 
               F.try_to_date("account_opened", 'yyyy-MM-dd'),
               F.try_to_date("account_opened", 'dd-MM-yyyy'),
               F.try_to_date("account_opened", 'yyyy/MM/dd'),
               F.try_to_date("account_opened", 'dd MMM yyyy')
               ).alias("account_opened"),
    F.col("credit_limit"),   
    F.col("ingestion_timestamp"),
    F.col("source_file")
    )

# COMMAND ----------

display(silver_df.limit(30))
#display(silver_df.filter("Open_Date is null"))

# COMMAND ----------

if silver_df.limit(1).count() == 0:
    raise ValueError("Error: Silver Customer table has zero observations")

silver_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_cust")

#to write as iceberg
#silver_df.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean")

# COMMAND ----------

silver_dfr = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_repayments").select(
    F.trim(F.upper(F.col("repayment_id"))).alias("repayment_id"), 
    F.trim(F.upper(F.col("customer_id"))).alias("customer_id"),
    F.coalesce(F.try_to_date("due_date", 'dd/mm/yyyy'), 
               F.try_to_date("due_date", 'yyyy-MM-dd'),
               F.try_to_date("due_date", 'dd-MM-yyyy'),
               F.try_to_date("due_date", 'yyyy/MM/dd'),
               F.try_to_date("due_date", 'dd MMM yyyy')
               ).alias("due_date"),
    F.col("amount_due").cast("decimal(18,2)").alias("amount_due"),
    F.col("amount_paid").cast("decimal(18,2)").alias("amount_paid"),
    F.col("payment_status"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
    )


# COMMAND ----------

display(silver_dfr.limit(30))

# COMMAND ----------

if silver_dfr.limit(1).count() == 0:
    raise ValueError("Error: Silver Repayments table has zero observations")

silver_dfr.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_repay")

# COMMAND ----------

silver_dft = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_transactions").select(
    F.trim(F.upper(F.col("transaction_id"))).alias("transaction_id"),
    F.trim(F.upper(F.col("customer_id"))).alias("customer_id"),
    F.coalesce(F.try_to_date("transaction_date", 'dd/mm/yyyy'), 
               F.try_to_date("transaction_date", 'yyyy-MM-dd'),
               F.try_to_date("transaction_date", 'dd-MM-yyyy'),
               F.try_to_date("transaction_date", 'yyyy/MM/dd'),
               F.try_to_date("transaction_date", 'dd MMM yyyy')
               ).alias("transaction_date"),
    F.col("amount").cast("decimal(18,2)").alias("amount"), 
    F.trim(F.initcap(F.col("transaction_type"))).alias("transaction_type"),
    F.trim(F.initcap(F.col("merchant_category"))).alias("merchant_category"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
    )


# COMMAND ----------

display(silver_dft.limit(30))

# COMMAND ----------

if silver_dft.limit(1).count() == 0:
    raise ValueError("Error: Silver Transactions table has zero observations")

silver_dft.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_trans")