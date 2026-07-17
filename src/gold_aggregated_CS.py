# Databricks notebook source
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

custs = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_custs")
repay = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_repay")
trans = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_clean_trans")

gold_df_combine = (
    custs
    .join(repay, on="customer_id", how="left")
    .join(trans, on="customer_id", how="left")
)

display(gold_df_combine)

# COMMAND ----------

gold_df_final = (
    gold_df_combine
    .groupBy("customer_id")
    .agg(
        F.sum(
            F.when(F.col("payment_status") == "Late", 1)
            .otherwise(0)
        ).alias("late_payment_count"),
        
        F.avg("amount").alias("avg_transaction_value"),
        
        F.max("transaction_date").alias("last_transaction_date"),
        
        F.max(
            F.when(F.year("transaction_date") >= 2025, 1)
            .otherwise(0)
        ).alias("is_active_customer"),
        
        F.first("age", ignorenulls=True).alias("age")
    )
    .withColumn(
        "risk_profile",
        F.when(F.col("late_payment_count") >= 2, F.lit("High"))
         .when(F.col("late_payment_count") == 1, F.lit("Medium"))
         .otherwise(F.lit("Low"))
    )
    .withColumn(
        "active_customer",
        F.when(F.col("is_active_customer") == 1, F.lit("Yes"))
         .otherwise(F.lit("No"))
    )
    .select(
        "customer_id",
        "risk_profile",
        "avg_transaction_value",
        "active_customer",
        "age"
    )
)

display(gold_df_final)

# COMMAND ----------

gold_df_final = (
    gold_df_combine
    .groupBy("customer_id")
    .agg(
        F.sum(
            F.when(
                F.col("payment_status").isin("Late"),
                1
            ).otherwise(0)
        ).alias("late_payment_count"),

        F.avg("amount").alias("avg_transaction_value"),

        F.max(
            F.when(
                F.year("transaction_date") >= 2025,
                1
            ).otherwise(0)
        ).alias("is_active_customer"),

        F.first("age", ignorenulls=True).alias("age")
    )
    .withColumn(
        "risk_profile",
        F.when(F.col("late_payment_count") >= 2, "High")
         .when(F.col("late_payment_count") == 1, "Medium")
         .otherwise("Low")
    )
    .withColumn(
        "active_customer",
        F.when(F.col("is_active_customer") == 1, "Yes")
         .otherwise("No")
    )
    .select(
        "customer_id",
        "risk_profile",
        "avg_transaction_value",
        "active_customer",
        "age"
    )
)

display(gold_df_final)

# COMMAND ----------

# Validations
gold_count = gold_df_final.count()

if gold_count > 0:
    gold_df_final.write.mode("overwrite").saveAsTable(
        f"{CATALOG}.{GOLD_SCHEMA}.gold_final"
    )
    print(f"gold_final updated successfully. Row count: {gold_count}")
else:
    print("ERROR: gold_df_final is empty. Table not updated.")