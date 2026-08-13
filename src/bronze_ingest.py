# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import current_timestamp, lit, count

# COMMAND ----------

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

input_customers_table = f"{CATALOG}.{SOURCE_SCHEMA}.customers"
input_repayments_table = f"{CATALOG}.{SOURCE_SCHEMA}.repayments"
input_transactions_table = f"{CATALOG}.{SOURCE_SCHEMA}.transactions"

output_customers_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_customers"
output_repayments_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_repayments"
output_transactions_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_transactions"


# COMMAND ----------

# Customers dataset
raw_data_customers = spark.table(input_customers_table).select(
    "*", current_timestamp().alias("ingested_at"), lit("customers").alias("source_file"))

# COMMAND ----------

# Repayments dataset
raw_data_repayments = spark.table(input_repayments_table).select(
    "*", current_timestamp().alias("ingested_at"), lit("repayments").alias("source_file"))

# COMMAND ----------

# Transactions dataset
raw_data_transactions = spark.table(input_transactions_table).select(
    "*", current_timestamp().alias("ingested_at"), lit("transactions").alias("source_file"))

# COMMAND ----------

# Write out customer table to bronze and check if 0 obs
raw_data_customers.write.mode("overwrite").saveAsTable(output_customers_table)

customers_check = spark.table(output_customers_table).select(count("customer_id")).collect()[0][0]
if customers_check == 0:
    raise ValueError("O observations in the bronze_ingest_customers table")

# COMMAND ----------

# Write out repayments table to bronze and check if 0 obs
raw_data_repayments.write.mode("overwrite").saveAsTable(output_repayments_table)

repayments_check = spark.table(output_repayments_table).select(count("repayment_id")).collect()[0][0]
if repayments_check == 0:
    raise ValueError("O observations in the bronze_ingest_repayments table")

# COMMAND ----------

# Write out transactions table to bronze and check if 0 obs
raw_data_transactions.write.mode("overwrite").saveAsTable(output_transactions_table)

transactions_check = spark.table(output_transactions_table).select(count("transaction_id")).collect()[0][0]
if transactions_check == 0:
    raise ValueError("O observations in the bronze_ingest_transations table")