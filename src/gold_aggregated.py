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

# New column risk_profile with values Low, Medium or High; aggregated per customer
late_repayments = spark.table(input_repayments_table).where("payment_status = 'Late' or payment_status = 'Missed'").groupBy("customer_id").agg(F.count("payment_status").alias("late_payments"))

customer_late_repayments = customers.join(late_repayments, on=["customer_id"], how="left").fillna(0, subset=["late_payments"])

customer_at_risk = customer_late_repayments.withColumn("risk_profile", F.when(F.col("late_payments") == 0, "Low").when(F.col("late_payments") == 1, "Medium").otherwise("High")).select("customer_id","risk_profile")

# COMMAND ----------

# New age column
customer_age = spark.table(input_customers_table).withColumn('age', (F.months_between(F.current_date(),F.col("date_of_birth"))/12).cast("int")).select("customer_id","age")

# COMMAND ----------

# New average transaction value column
customer_avg_tran = spark.table(input_transactions_table).groupBy("customer_id").agg(F.avg("amount").alias("avg_tran").cast("decimal(10,2)"))
 


# COMMAND ----------

# New active customer column
customer_max_tran = spark.table(input_transactions_table).groupBy("customer_id").agg(F.max("transaction_date").alias("last_tran"))

customer_active = customer_max_tran.withColumn("active_customer", F.when(F.year(F.col("last_tran")) >= 2025, "Y").otherwise("N")).select("customer_id","active_customer")

# COMMAND ----------

# Combine all new variables together into one final table
gold_final = customer_at_risk.join(customer_age, on=["customer_id"], how="left").join(customer_avg_tran, on=["customer_id"], how="left").join(customer_active, on=["customer_id"], how="left")

# COMMAND ----------

# Write out final table to gold and check if 0 obs
gold_final.write.mode("overwrite").saveAsTable(output_final_table)

obs_check = spark.table(output_final_table).select(F.count("*")).collect()[0][0]
if obs_check == 0:
    raise ValueError("O observations in the gold_final table")