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

gold_df_customers = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_cp_customers").select("*")

gold_df_repayments = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_cp_repayments").select("*")

gold_df_transactions = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_cp_transactions").select("*")
   

# COMMAND ----------

# MAGIC %skip
# MAGIC df_2 = df1.withColumn(
# MAGIC     "risk_profile",
# MAGIC     F.when(F.col("count") >= 2, "High")
# MAGIC     .when(F.col("count") == 1, "Medium")
# MAGIC     .otherwise("Low")
# MAGIC )
# MAGIC )

# COMMAND ----------

gold_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.silver_capstone").groupBy("city", "product_type").count()

# COMMAND ----------

# MAGIC %skip
# MAGIC 3.	Gold Notebook
# MAGIC a.	Aggregate the silver data to produce one row per customer. 
# MAGIC b.	Derive a risk_profile column with values of Low, Medium or High (Customers with two or more late payments are classified as High Risk. Customers with exactly one late payment are classified as Medium Risk. Customers with no late payments are classified as Low Risk).
# MAGIC c.	Derive an age column.
# MAGIC d.	Derive an average transaction value column.
# MAGIC e.	Derive an active customer column based on transaction date (Customer must have a transaction in the year 2025 or later).
# MAGIC f.	Store the output as a managed Delta table in your gold schema (This should be an overwrite table).
# MAGIC