# Databricks notebook source
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Data Cleaning
# MAGIC
# MAGIC Reads from the bronze schema, applies cleaning to all three tables,
# MAGIC and writes conformed Delta tables to the silver schema.
# MAGIC
# MAGIC **Cleaning applied:**
# MAGIC
# MAGIC | Table | Issues addressed |
# MAGIC |---|---|
# MAGIC | customers | Dedupe, type casting, string standardisation, null flags |
# MAGIC | transactions | Dedupe, type casting, invalid amount flags, string standardisation |
# MAGIC | repayments | Dedupe, payment_status standardisation, meaningful null flags |

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError("Please enter your schema name in the widget above before running.")

CATALOG       = "tesco_bank_training"
BRONZE_SCHEMA = f"{user_schema}_bronze"
SILVER_SCHEMA = f"{user_schema}_silver"

print(f"Reading from: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Writing to:   {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, lower, upper, initcap, when,
    to_date, regexp_replace, coalesce, lit
)

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, to_timestamp, trim

def parse_date(column):
    c = trim(col(column).cast("string"))

    return coalesce(
        to_timestamp(c, "dd-MM-yyyy"),
        to_timestamp(c, "yyyy-MM-dd"),
        to_timestamp(c, "dd/MM/yyyy"),
        to_timestamp(c, "yyyy/MM/dd")
    ).cast("date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean: customers

# COMMAND ----------

customers_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")

customers_silver = (
    customers_raw

    # Deduplicate on natural key
    .dropDuplicates(["customer_id"])

    # Standardise name and string fields
    .withColumn("first_name",         trim(initcap(col("first_name"))))
    .withColumn("last_name",          trim(initcap(col("last_name"))))
    .withColumn("city",               trim(initcap(col("city"))))
    .withColumn("employment_status",  trim(initcap(col("employment_status"))))
    .withColumn("product_type",       trim(initcap(col("product_type"))))

    # Strip currency symbols and cast annual_income to double
    .withColumn("annual_income",
        regexp_replace(col("annual_income"), "[£,]", "").cast("double"))

    # Cast credit_limit to double
    .withColumn("credit_limit", col("credit_limit").cast("double"))

    # Parse date columns - handle mixed formats with coalesce
    .withColumn("date_of_birth", parse_date("date_of_birth"))
    .withColumn("account_opened", parse_date("account_opened"))

    # Flag meaningful nulls rather than dropping
    .withColumn("is_city_missing",             col("city").isNull())
    .withColumn("is_annual_income_missing",    col("annual_income").isNull())
    .withColumn("is_employment_status_missing",col("employment_status").isNull())
)

customers_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.customers")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers").count()
assert count > 0, "Silver customers table is empty."
print(f"customers: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean: transactions

# COMMAND ----------

transactions_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions")

transactions_silver = (
    transactions_raw
    .dropDuplicates(["transaction_id"])

    # Type casting
    .withColumn("amount", col("amount").cast("DOUBLE"))
    .withColumn("transaction_date", parse_date("transaction_date"))

    # Standardise string fields
    .withColumn("transaction_type",  trim(initcap(col("transaction_type"))))
    .withColumn("merchant_category", trim(initcap(col("merchant_category"))))

    # Flag invalid amounts — negative or zero
    .withColumn("is_invalid_amount", (col("amount") <= 0) | col("amount").isNull())
)

transactions_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.transactions")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.transactions").count()
assert count > 0, "Silver transactions table is empty."
print(f"transactions: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean: repayments

# COMMAND ----------

repayments_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.repayments")

repayments_silver = (
    repayments_raw
    .dropDuplicates(["repayment_id"])

    # Type casting
    .withColumn("amount_due",  col("amount_due").cast("DOUBLE"))
    .withColumn("amount_paid", col("amount_paid").cast("DOUBLE"))
    .withColumn("due_date", parse_date("due_date"))


    # Standardise payment_status
    .withColumn(
        "payment_status",
        when(lower(trim(col("payment_status"))).isin("paid", "p"),       "Paid")
        .when(lower(trim(col("payment_status"))).isin("missed", "m"),    "Missed")
        .when(lower(trim(col("payment_status"))).isin("late", "l"),      "Late")
        .otherwise(None)
    )

    # Flag missing payment_status as meaningful null
    # (drives the risk profile, must not be silently dropped)
    .withColumn("is_status_missing", col("payment_status").isNull())
)

repayments_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.repayments")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.repayments").count()
assert count > 0, "Silver repayments table is empty."
print(f"repayments: {count} rows")

print("\nSilver cleaning complete.")
