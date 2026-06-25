# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver: Data Cleaning
# MAGIC
# MAGIC **Task:** Read from your bronze table, apply data cleaning, and write a conformed Delta table to your silver schema.
# MAGIC
# MAGIC **The data has been seeded with intentional issues. You need to find and fix them.**
# MAGIC Start by profiling the data before writing any cleaning logic.
# MAGIC
# MAGIC **Enter your schema name in the widget below before running.**

# COMMAND ----------

dbutils.widgets.text("user_schema", "", "Your schema name (e.g. user_david_herbert)")
user_schema = dbutils.widgets.get("user_schema").strip()

if not user_schema:
    raise ValueError("Please enter your schema name in the widget above before running.")

CATALOG       = "tesco_bank_training"
BRONZE_SCHEMA = f"{user_schema}_bronze"
SILVER_SCHEMA = f"{user_schema}_silver"

print(f"Reading from:  {CATALOG}.{BRONZE_SCHEMA}.transactions")
print(f"Writing to:    {CATALOG}.{SILVER_SCHEMA}.transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Profile the data
# MAGIC
# MAGIC Before cleaning anything, understand what you are working with.
# MAGIC - How many rows are there?
# MAGIC - Are there duplicates?
# MAGIC - Which columns have null values, and how many?
# MAGIC - Are the data types correct?
# MAGIC - Are string fields consistent (casing, whitespace)?
# MAGIC - Are numeric fields all valid values?

# COMMAND ----------

# TODO: Read bronze table and profile it
# Hint: spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions")
# Use display(), .dtypes, .describe(), or SQL to explore


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Apply cleaning transformations
# MAGIC
# MAGIC Based on what you found above, apply the necessary cleaning steps.
# MAGIC Think about:
# MAGIC - Deduplication
# MAGIC - Type casting
# MAGIC - Standardising string fields
# MAGIC - Handling nulls (should you drop, impute, or flag them?)
# MAGIC - Invalid values (e.g. negative amounts)
# MAGIC
# MAGIC **Note:** For meaningful nulls, consider flagging with a boolean column
# MAGIC rather than dropping the row — downstream logic may need to know the value was missing.

# COMMAND ----------

# TODO: Apply cleaning transformations


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Write to silver Delta table
# MAGIC
# MAGIC Write the cleaned DataFrame as a managed Delta table in your silver schema.
# MAGIC Use `overwrite` mode.

# COMMAND ----------

# TODO: Write to silver Delta table


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Row count assertion

# COMMAND ----------

# TODO: Assert the silver table is not empty


