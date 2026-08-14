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

# COMMAND ----------

# a. Aggregate the silver data to produce one row per custome
customers = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_customers")

repayments = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_repayments")

transactions = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_transactions")

display(customers)
display(repayments)
display(transactions)

# COMMAND ----------

# DBTITLE 1,Cell 5
# b. Derive a risk_profile column with values of Low, Medium or High (Customers with two or more late payments are classified as High Risk. Customers with exactly one late payment are classified as Medium Risk. Customers with no late payments are classified as Low Risk).
late = repayments.where("payment_status == 'Late'")

result = late.groupBy("customer_id").agg(F.count("*").alias("late_count"))

risk = result.withColumn(
    "risk_profile", 
    F.when(F.col("late_count") == 0, "Low")
    .when(F.col("late_count") == 1, "Medium")
    .when(F.col("late_count") >= 2, "High")
    )

# payments_result = repayments.groupBy("customer_id", "payment_status").count("payment_status" == 'Late').alias("risk_profile")


# COMMAND ----------

# age
age = customers.withColumn(
    "age",
    F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365)
    )

# display(age)

# COMMAND ----------

# DBTITLE 1,Cell 7
# average transaction amount

transactions_amount = transactions.select("customer_id", "amount")

trans_group = transactions_amount.groupBy("customer_id").agg(
    F.avg("amount").cast("decimal(10,2)").alias("avg_trans_amount")
)
# display(trans_group)

# COMMAND ----------

# active


# COMMAND ----------

# join

join_customers = (customers
    .join(risk.select("risk_profile"), customers.customer_id == risk.customer_id, "left")
    .join(age.select("age"), customers.customer_id == age.customer_id, "left")
    .join(trans_group.select("avg_trans_amount"), customers.customer_id == trans_group.customer_id, "left")
)

display(join_customers)

# COMMAND ----------

join_customers.write.format("iceberg").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.gold_customers")