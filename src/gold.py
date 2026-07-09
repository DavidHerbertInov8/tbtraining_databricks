# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold: Aggregation and Risk Profiling
# MAGIC
# MAGIC Joins all three silver tables, aggregates to one row per customer,
# MAGIC and derives a `risk_profile` column.
# MAGIC
# MAGIC **Risk profile rules:**
# MAGIC | Missed payments (last 12 months) | risk_profile |
# MAGIC |---|---|
# MAGIC | 0 | Low |
# MAGIC | 1–2 | Medium |
# MAGIC | 3+ | High |
# MAGIC
# MAGIC Customers with missing payment_status on all repayments are flagged
# MAGIC as Medium by default (unknown risk is not low risk).

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError("Please enter your schema name in the widget above before running.")

CATALOG       = "tesco_bank_training"
SILVER_SCHEMA = f"{user_schema}_silver"
GOLD_SCHEMA   = f"{user_schema}_gold"

print(f"Reading from: {CATALOG}.{SILVER_SCHEMA}")
print(f"Writing to:   {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as _sum, avg, when,
    countDistinct, round as _round, floor, 
    datediff, current_date, max, year
)

customers    = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
transactions = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.transactions")
repayments   = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.repayments")

# COMMAND ----------

txn_agg = (
    transactions
    .filter(~col("is_invalid_amount"))  # exclude invalid amounts from metrics
    .groupBy("customer_id")
    .agg(
        count("transaction_id").alias("total_transactions"),
        _round(_sum("amount"), 2).alias("total_spend"),
        _round(avg("amount"), 2).alias("avg_transaction_value"),
        countDistinct("merchant_category").alias("distinct_merchant_categories"),
        max(col("transaction_date")).alias("last_transaction_date")
    ).withColumn("is_active", when(year(col("last_transaction_date")) > 2025, True).otherwise(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Aggregate repayments per customer
# MAGIC
# MAGIC Count missed payments to drive the risk profile.
# MAGIC Missing status is treated conservatively (not assumed to be Paid).

# COMMAND ----------

rep_agg = (
    repayments
    .groupBy("customer_id")
    .agg(
        count("repayment_id").alias("total_repayments"),
        _sum(when(col("payment_status") == "Missed", 1).otherwise(0))
            .alias("missed_payment_count"),
        _sum(when(col("payment_status") == "Late", 1).otherwise(0))
            .alias("late_payment_count"),
        _sum(when(col("is_status_missing"), 1).otherwise(0))
            .alias("unknown_status_count"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Derive Customer Age

# COMMAND ----------

Customers_deriv = customers.withColumn(
    "age",
    floor(datediff(current_date(), col("date_of_birth")) / 365))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Join everything to one row per customer

# COMMAND ----------

gold_df = (
    Customers_deriv
    .join(txn_agg, on="customer_id", how="left")
    .join(rep_agg, on="customer_id", how="left")
)

# COMMAND ----------

display(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Derive risk_profile
# MAGIC
# MAGIC Based on missed payment count.
# MAGIC Customers with no repayment records at all default to Medium
# MAGIC (insufficient data to confirm low risk).

# COMMAND ----------

gold_df = gold_df.withColumn(
    "risk_profile",
    when(col("missed_payment_count") >= 3,                                    "High")
    .when((col("missed_payment_count") >= 1) |
          (col("unknown_status_count") > col("total_repayments") * 0.5),      "Medium")
    .when(col("total_repayments").isNull(),                                    "Medium")
    .otherwise("Low")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Write to gold Delta table
# MAGIC
# MAGIC One row per customer, clean, typed, and queryable directly from Databricks SQL.

# COMMAND ----------

gold_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.customer_risk")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer_risk").count()
assert count > 0, "Gold write produced an empty table."
print(f"Gold aggregate complete: {count} customer rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Summary SQL query
# MAGIC
# MAGIC Count of customers per risk_profile category and their average transaction value.

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer_risk")
    .select(
        "customer_id", "first_name", "last_name",
        "total_transactions", "total_spend", "avg_transaction_value",
        "missed_payment_count", "risk_profile", "age", "is_active", "date_of_birth"
    )
    .orderBy("risk_profile", "customer_id")
    .limit(20)
)
