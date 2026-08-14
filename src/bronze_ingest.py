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

input_customers = f"{CATALOG}.{SOURCE_SCHEMA}.customers"
input_repayments = f"{CATALOG}.{SOURCE_SCHEMA}.repayments"
input_transactions = f"{CATALOG}.{SOURCE_SCHEMA}.transactions"

output_customers = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_customers"
output_repayments = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_repayments"
output_transactions = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_transactions"


# COMMAND ----------

# Add new variables to each of the tables and write them out; check if final table has 0 obs
main_tables = {"customers": [input_customers, output_customers], "repayments": [input_repayments, output_repayments], "transactions": [input_transactions, output_transactions]}

for table_type, table_names in main_tables.items():
    spark.table(table_names[0]).select(
        "*", current_timestamp().alias("ingested_at"), lit(f"{table_type}").alias("source_file")).write.mode("overwrite").saveAsTable(table_names[1])
    
    check_obs = spark.table(table_names[1]).select(count("*")).collect()[0][0]
    if check_obs == 0:
        raise ValueError(f"O observations in the {table_names[1]}")
