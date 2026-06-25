# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold: Aggregation and Risk Profiling
# MAGIC
# MAGIC **Task:** Read from your silver table, aggregate to one row per customer,
# MAGIC and derive a `risk_profile` column.
# MAGIC
# MAGIC **End goal:** a clean, typed, business-ready table queryable directly
# MAGIC from Databricks SQL — no further transformation required.
# MAGIC
# MAGIC **Enter your schema name in the widget below before running.**

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError("Please enter your schema name in the widget above before running.")

CATALOG       = "tesco_bank_training"
SILVER_SCHEMA = f"{user_schema}_silver"
GOLD_SCHEMA   = f"{user_schema}_gold"

print(f"Reading from:  {CATALOG}.{SILVER_SCHEMA}.transactions")
print(f"Writing to:    {CATALOG}.{GOLD_SCHEMA}.transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Aggregate per customer
# MAGIC
# MAGIC Produce one row per customer. Think about which metrics are meaningful
# MAGIC for a credit risk output. At minimum include:
# MAGIC - Total number of transactions
# MAGIC - Total and average transaction value
# MAGIC - Any fields relevant to deriving the risk profile
# MAGIC
# MAGIC Hint: use `groupBy` and `agg`

# COMMAND ----------

# TODO: Read silver table and aggregate to one row per customer


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Derive risk_profile
# MAGIC
# MAGIC Add a `risk_profile` column with values of **Low**, **Medium**, or **High**.
# MAGIC Define your own business rules based on the data available.
# MAGIC Be prepared to explain and justify your rules to the group.
# MAGIC
# MAGIC Hint: use `when` / `otherwise` from `pyspark.sql.functions`

# COMMAND ----------

# TODO: Derive risk_profile column


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Write to gold Delta table
# MAGIC
# MAGIC Write as a managed Delta table. Use `overwrite` mode.

# COMMAND ----------

# TODO: Write to gold Delta table


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Row count assertion

# COMMAND ----------

# TODO: Assert the gold table is not empty


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Summary query
# MAGIC
# MAGIC Write a SQL query that returns the count of customers in each
# MAGIC `risk_profile` category and their average transaction value.
# MAGIC
# MAGIC This is your final deliverable — make sure it runs cleanly.

# COMMAND ----------

# TODO: Write your summary SQL query
# Hint: use spark.sql(...) or %sql magic


