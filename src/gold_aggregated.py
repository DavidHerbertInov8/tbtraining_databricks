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
SOURCE_SCHEMA  = f"{user_schema}_silver"
GOLD_SCHEMA  = f"{user_schema}_gold"

print(f"Source:  {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target:  {CATALOG}.{GOLD_SCHEMA}")

input_customers_table = f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_customers"
input_repayments_table = f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_repayments"
input_transactions_table = f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_transactions"

output_final_table = f"{CATALOG}.{GOLD_SCHEMA}.gold_final"

# COMMAND ----------

# Base table
customers = spark.table(input_customers_table).select("customer_id")

# COMMAND ----------

# New column at_risk with values Low, Medium or High; aggregated per customer
late_repayments = spark.table(input_repayments_table).where("payment_status = 'Late' or payment_status = 'Missed'").groupBy('customer_id').agg(F.count("payment_status").alias("late_payments"))

customer_late_repayments = customers.join(late_repayments, on=["customer_id"], how="left").fillna(0, subset=["late_payments"])

customer_at_risk = customer_late_repayments.withColumn("at_risk", F.when(F.col("late_payments") == 0, "Low").when(F.col("late_payments") == 1, "Medium").otherwise("High")).select("customer_id","at_risk")

# COMMAND ----------

# New age column
customer_age = 

# COMMAND ----------

# New average transaction value column


# COMMAND ----------

display(customer_at_risk)

# COMMAND ----------

display(spark.table(input_repayments_table))

# COMMAND ----------

transactions_check = spark.table(input_repayments_table).select("payment_status")
distinct_column = transactions_check.dropDuplicates(["payment_status"]).select("payment_status")
display(distinct_column)

# COMMAND ----------

gold_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean").groupBy("city", "product_type").count()

# COMMAND ----------

