# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

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

# Customers Table
raw_data_custs = spark.table("tesco_bank_training.datasets.customers").select(
    "*",
    current_timestamp().alias("ingested_at"),
    lit("customers").alias("source_file")
)
display(raw_data_custs)

# COMMAND ----------

# Repayments Table
raw_data_repay = spark.table("tesco_bank_training.datasets.repayments").select(
    "*",
    current_timestamp().alias("ingested_at"),
    lit("repayments").alias("source_file")
) 
display(raw_data_repay)

# COMMAND ----------

# Transactions Table
raw_data_trans = spark.table("tesco_bank_training.datasets.transactions").select(
    "*",
    current_timestamp().alias("ingested_at"),
    lit("transactions").alias("source_file")
)
display(raw_data_trans)

# COMMAND ----------

# Validations
cust_count = raw_data_custs.count()

if cust_count > 0:
    raw_data_custs.write.mode("overwrite").saveAsTable(
        f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_custs"
    )
    print(f"bronze_ingest_custs updated successfully. Row count: {cust_count}")
else:
    print("ERROR: raw_data_custs is empty. Table not updated.")


repay_count = raw_data_repay.count()

assert repay_count > 0, "raw_data_repay is empty"


if repay_count > 0:
    raw_data_repay.write.mode("overwrite").saveAsTable(
        f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_repay"
    )
    print(f"bronze_ingest_repay updated successfully. Row count: {repay_count}")
else:
    print("ERROR: raw_data_repay is empty. Table not updated.")


trans_count = raw_data_trans.count()

if trans_count > 0:
    raw_data_trans.write.mode("overwrite").saveAsTable(
        f"{CATALOG}.{BRONZE_SCHEMA}.bronze_ingest_trans"
    )
    print(f"bronze_ingest_trans updated successfully. Row count: {trans_count}")
else:
    print("ERROR: raw_data_trans is empty. Table not updated.")