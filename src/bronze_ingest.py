# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

# COMMAND ----------


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

# dbutils.widgets.text("catalog", "tesco_bank_training", "Catalog")
# dbutils.widgets.text("schema", "datasets", "Schema")
# dbutils.widgets.text("write_schema", "", "write_schema")

# catalog = dbutils.widgets.get("catalog")
# schema = dbutils.widgets.get("schema")
# write_schema = dbutils.widgets.get("schema")

customers_table = f"{CATALOG}.{SOURCE_SCHEMA}.customers"
repayments_table = f"{CATALOG}.{SOURCE_SCHEMA}.repayments"
transactions_table = f"{CATALOG}.{SOURCE_SCHEMA}.transactions"

# write_schema = dbutils.widgets.get("write_schema")

# write_schema = write_schema + "_" + "bronze"

print(customers_table)
print(repayments_table)
print(transactions_table)
# print(write_schema)

# COMMAND ----------

# a. Read and preserve the raw data as-is.
customers  = spark.table(customers_table)
repayments = spark.table(repayments_table)
transactions = spark.table(transactions_table)
# display(customers)
# display(repayments)
# display(transactions)


# COMMAND ----------

# DBTITLE 1,Cell 4
# b. However - add two audit columns: ingested_at (current timestamp) and source_file (filename).
customers = customers.select("*", current_timestamp().alias("ingested_at"), lit("customers").alias("source_file"))

repayments = repayments.select("*", current_timestamp().alias("ingested_at"), lit("repayments").alias("source_file"))

transactions = transactions.select("*", current_timestamp().alias("ingested_at"), lit("transactions").alias("source_file"))

# display(customers)
# display(repayments)
# display(transactions)

# COMMAND ----------

# c. Store the output as a managed Delta table in your bronze schema (This should be done as an overwrite)
customers.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_customers")
repayments.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_repayments")
transactions.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_transactions")