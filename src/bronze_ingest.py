# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()
display(user_schema)

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

customers_gs = spark.table("tesco_bank_training.datasets.customers").select(
    "*"
    , current_timestamp().alias("ingested_at")
    , lit("customers").alias("source_file")
)

repayments_gs = spark.table("tesco_bank_training.datasets.repayments").select(
    "*"
    , current_timestamp().alias("ingested_at")
    , lit("repayments").alias("source_file")
)

transactions_gs = spark.table("tesco_bank_training.datasets.transactions").select(
    "*"
    , current_timestamp().alias("ingested_at")
    , lit("transactions").alias("source_file")
)

# COMMAND ----------

customers_gs.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_cp_customers")

repayments_gs.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_cp_repayments")

transactions_gs.write.mode("overwrite").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_cp_transactions")

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("""
# MAGIC           alter table tesco_bank_training.gavin_smith_bronze.bronze_capstone
# MAGIC           add columns (
# MAGIC               ingested_at timestamp,
# MAGIC               source_file string
# MAGIC           )""")

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("""
# MAGIC           drop table tesco_bank_training.gavin_smith_bronze.bronze_capstone
# MAGIC           """)

# COMMAND ----------

display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_cp_customers"))
display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_cp_repayments"))
display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_cp_transactions"))