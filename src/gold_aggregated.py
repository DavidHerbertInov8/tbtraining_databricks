# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC One row per customer
# MAGIC
# MAGIC Risk profile column - High Med Low
# MAGIC High - 2+ late payments
# MAGIC Med - 1 late payment
# MAGIC Low - no late payment
# MAGIC
# MAGIC Age column - done
# MAGIC
# MAGIC average transaction value column
# MAGIC
# MAGIC active customer column = Active customer has at least one transaction in 2025 or later
# MAGIC
# MAGIC store output as overwrite table. 
# MAGIC ****

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
SOURCE_SCHEMA  = f"{user_schema}_silver"
GOLD_SCHEMA  = f"{user_schema}_gold"

print(f"Source:  {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target:  {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

#Customer variables
gold_dfc = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_cust").select("*",
    F.floor(F.months_between(F.current_date(), F.col("date_of_birth"))/12).alias("age")).orderBy("customer_id")

#Aggregate to one customer id
gold_cust = (gold_dfc.groupBy("customer_id").agg(F.max("age").alias("cust_age")))

display(gold_cust)


# COMMAND ----------

#Repayment variables
gold_dfr = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_repay").select("*")

customer_payment_counts = (gold_dfr.groupBy("customer_id").agg(F.sum(F.when(F.col("payment_status") == "Late", 1).otherwise(0)).alias("Late_count")))

gold_repay = (customer_payment_counts.withColumn(
    "risk_profile",
    F.when(F.col("Late_count") > 1, "High")
     .when(F.col("Late_count") == 1, "Medium")
     .otherwise("Low")
).select("customer_id", "risk_profile"))

# COMMAND ----------

#Transaction variables
gold_dft = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_trans").select("*").orderBy(F.col("customer_id"))

avg_trans = gold_dft.groupBy("customer_id").agg(F.avg("amount").alias("avg_trans"))

active_cust = gold_dft.withColumn(
    "active_flag",
    F.when(F.col("Transaction_Date") >= F.lit("2025-01-01"), 1).otherwise(0))

agg_active = active_cust.groupBy("customer_id").agg(F.max("active_flag").alias("active_flag"))

gold_trans = (avg_trans.select("customer_id", "avg_trans")
            .join(agg_active.select("customer_id", "active_flag"),
                        on="customer_id", 
                        how = 'left'))


# COMMAND ----------

#Aggregated final output

gold_final = (gold_cust
                .join(gold_repay.select("customer_id", "risk_profile"),
                               on="customer_id",
                               how='left')
                .join(gold_trans.select("customer_id", "avg_trans", "active_flag"),
                                on="customer_id",
                                how='left'))
    

# COMMAND ----------

display(gold_final)

# COMMAND ----------

if gold_final.limit(1).count() == 0:
    raise ValueError("Error: Gold Final table has zero observations")

gold_final.write.mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.gold_final")

# COMMAND ----------

