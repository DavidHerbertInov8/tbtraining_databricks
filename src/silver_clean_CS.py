# Databricks notebook source
from pyspark.sql import functions as F
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError("Please enter your schema name in the widget above before running.")

CATALOG        = "tesco_bank_training"
SOURCE_SCHEMA  = f"{user_schema}_bronze"
SILVER_SCHEMA  = f"{user_schema}_silver"

print(f"Source:  {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target:  {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# Customers Clean
from pyspark.sql.functions import current_date, datediff, floor, col, coalesce, to_timestamp, trim

from pyspark.sql import functions as F

silver_df_custs = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_custs").select(
    F.lower(F.col("customer_id")).alias("customer_id"),
    F.upper(F.col("product_type")).alias("product_type"),
    F.coalesce(F.col("credit_limit"), F.lit("0")).alias("credit_limit"),
    F.initcap(F.col("city")).alias("city"),
    coalesce(
        to_timestamp(trim(col("date_of_birth").cast("string")), "dd-MM-yyyy"),
        to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy-MM-dd"),
        to_timestamp(trim(col("date_of_birth").cast("string")), "dd/MM/yyyy"),
        to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy/MM/dd")
    ).cast("date").alias("date_of_birth"),

    floor(
        F.months_between(
            current_date(),
            coalesce(
                to_timestamp(trim(col("date_of_birth").cast("string")), "dd-MM-yyyy"),
                to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy-MM-dd"),
                to_timestamp(trim(col("date_of_birth").cast("string")), "dd/MM/yyyy"),
                to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy/MM/dd")
            ).cast("date")
        ) / 12
    ).alias("age")
)

display(silver_df_custs)

# COMMAND ----------

# Repayments Clean
from pyspark.sql.functions import current_date, datediff, floor, col, coalesce, to_timestamp, trim

from pyspark.sql import functions as F

silver_df_repay = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_repay").select(
    F.lower(F.col("customer_id")).alias("customer_id"),
    F.initcap(F.col("payment_status")).alias("payment_status"),
coalesce(
        to_timestamp(trim(col("due_date").cast("string")), "dd-MM-yyyy"),
        to_timestamp(trim(col("due_date").cast("string")), "yyyy-MM-dd"),
        to_timestamp(trim(col("due_date").cast("string")), "dd/MM/yyyy"),
        to_timestamp(trim(col("due_date").cast("string")), "yyyy/MM/dd")
    ).cast("date").alias("due_date"),
)


display(silver_df_repay)


# COMMAND ----------

# Transactions Clean
from pyspark.sql.functions import current_date, datediff, floor, col, coalesce, to_timestamp, trim

from pyspark.sql import functions as F

silver_df_trans = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_trans").select(
    F.lower(F.col("customer_id")).alias("customer_id"),
    F.col("amount").alias("amount"),
    coalesce(
        to_timestamp(trim(col("transaction_date").cast("string")), "dd-MM-yyyy"),
        to_timestamp(trim(col("transaction_date").cast("string")), "yyyy-MM-dd"),
        to_timestamp(trim(col("transaction_date").cast("string")), "dd/MM/yyyy"),
        to_timestamp(trim(col("transaction_date").cast("string")), "yyyy/MM/dd")
    ).cast("date").alias("transaction_date"),
F.initcap(F.trim(F.col("transaction_type"))).alias("transaction_type")
)

display(silver_df_trans)

# COMMAND ----------

# Validations
cust_count = silver_df_custs.count()

if cust_count > 0:
    silver_df_custs.write.mode("overwrite").saveAsTable(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_custs"
    )
    print(f"silver_clean_custs updated successfully. Row count: {cust_count}")
else:
    print("ERROR: silver_df_custs is empty. Table not updated.")


repay_count = silver_df_repay.count()

if repay_count > 0:
    silver_df_repay.write.mode("overwrite").saveAsTable(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_repay"
    )
    print(f"silver_clean_repay updated successfully. Row count: {repay_count}")
else:
    print("ERROR: silver_df_repay is empty. Table not updated.")


trans_count = silver_df_trans.count()

if trans_count > 0:
    silver_df_trans.write.mode("overwrite").saveAsTable(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_trans"
    )
    print(f"silver_clean_trans updated successfully. Row count: {trans_count}")
else:
    print("ERROR: silver_df_trans is empty. Table not updated.")