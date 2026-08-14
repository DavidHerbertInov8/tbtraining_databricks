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
# display(customers)

# COMMAND ----------

# DBTITLE 1,Cell 5
# a. Perform data cleaning as needed to satisfy business rules. - customer

silver_cust = customers.select(
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

display(silver_cust)

# COMMAND ----------

# DBTITLE 1,Cell 6
# adjust the format for annual_income
silver_cust_v2 = silver_cust.withColumn("annual_income", F.regexp_replace("annual_income", "£", ""))
silver_cust_v2 = silver_cust_v2.withColumn("annual_income", F.regexp_replace("annual_income", ",", ""))
silver_cust_v2 = silver_cust_v2.withColumn("annual_income", F.col("annual_income").cast("decimal(10,2)"))

display(silver_cust_v2)

# COMMAND ----------

# DBTITLE 1,Cell 7
# duplicates
silver_cust_v2 = silver_cust_v2.orderBy("customer_id")

silver_cust_v3 = silver_cust_v2.dropDuplicates()
# cust00161 - double check the record

display(silver_cust_v2.where("customer_id = 'cust00161'"))
display(silver_cust_v3.where("customer_id = 'cust00161'"))


# COMMAND ----------

# cleansing repayments
repayments = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_repayments")

# display(repayments)

# COMMAND ----------

# DBTITLE 1,Cell 9
silver_repayments = repayments.select(
    F.lower(F.col("repayment_id")).alias("repayment_id"),
    F.lower(F.col("customer_id")).alias("customer_id"),
    F.coalesce(
        F.try_to_date(F.col("due_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("due_date"), "yyyy-MM-dd"),
        F.try_to_date(F.col("due_date"), "dd MMM yyyy"),
        F.try_to_date(F.col("due_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("due_date"), "dd-MM-yyyy"),
    ).alias("due_date"),
    F.col("amount_due").cast("decimal(10,2)").alias("amount_due"),
    F.col("amount_paid").cast("decimal(10,2)").alias("amount_paid"),
    F.initcap(F.trim(F.col("payment_status"))).alias("payment_status"),
    F.to_date(F.col("ingested_at"), "yyyy-MM-dd").alias("ingested_at"),
    F.col("source_file").alias("source_file")
)

display(silver_repayments)

# COMMAND ----------

# duplicates
silver_repayments_v2 = silver_repayments.orderBy("customer_id")
# display(silver_repayments_v2)
silver_repayments_v3 = silver_repayments_v2.dropDuplicates()

display(silver_repayments_v2.where("customer_id = 'cust00005'"))
display(silver_repayments_v3.where("customer_id = 'cust00005'"))

# COMMAND ----------

# cleansing transactions
transactions = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_transactions")

# display(transactions)

# COMMAND ----------

silver_transactions = transactions.select(
    F.lower(F.col("transaction_id")).alias("transaction_id"),
    F.lower(F.col("customer_id")).alias("customer_id"),
    F.coalesce(
        F.try_to_date(F.col("transaction_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("transaction_date"), "yyyy-MM-dd"),
        F.try_to_date(F.col("transaction_date"), "dd MMM yyyy"),
        F.try_to_date(F.col("transaction_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("transaction_date"), "dd-MM-yyyy"),
    ).alias("transaction_date"),
    F.col("amount").cast("decimal(10,2)").alias("amount"),
    F.initcap(F.trim(F.col("transaction_type"))).alias("transaction_type"),
    F.initcap(F.trim(F.col("merchant_category"))).alias("merchant_category"),
    F.to_date(F.col("ingested_at"), "yyyy-MM-dd").alias("ingested_at"),
    F.col("source_file").alias("source_file")
)

display(silver_transactions)

# COMMAND ----------

# duplicates
silver_transactions_v2 = silver_transactions.orderBy("customer_id", "transaction_id")
# display(silver_transactions_v2)
silver_transactions_v3 = silver_transactions_v2.dropDuplicates()

dups = silver_transactions_v2.groupBy(silver_transactions_v2.columns).count().filter(F.col("count") > 1)

# display(dups)
display(silver_transactions_v2.where("customer_id = 'cust00500'"))
display(silver_transactions_v3.where("customer_id = 'cust00500'"))

# COMMAND ----------

# b. Store the output as a managed Delta table in your silver schema (This should be an overwrite).
silver_cust_v3.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_customers")

silver_repayments_v3.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_repayments")

silver_transactions_v3.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_transactions")
