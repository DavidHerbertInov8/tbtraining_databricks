# Databricks notebook source
# Module 1: Unity Catalog, DataFrames & Core Operations
# Audience: SQL-familiar, Python beginners
# Format: Each section = concept → SQL way → PySpark way → write → test
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧱 Module 1: Querying Data in Databricks
# MAGIC
# MAGIC This notebook is structured as a series of **exercises**.
# MAGIC Each one follows the same pattern:
# MAGIC
# MAGIC 1. 📖 What are we doing and why?
# MAGIC 2. 🟦 Do it in **Spark SQL** 
# MAGIC 3. 🐍 Do it in **PySpark** 
# MAGIC 4. 💾 Write the result to a table
# MAGIC 5. ✅ Test it worked
# MAGIC
# MAGIC > **Before you start:** Replace the values in the cell below with your own
# MAGIC > catalog, schema, and source table name. Run that cell first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Setup — Run This First
# MAGIC
# MAGIC These variables are used throughout the notebook.
# MAGIC Edit them to match your environment, then run the cell.

# COMMAND ----------

# ── EDIT THESE ──────────────────────────────────────────────
CATALOG    = "tesco_bank_training"      # your Unity Catalog catalog name
SCHEMA     = "my_schema"       # your schema / database name
SRC_TABLE  = "customers"       # source table you want to work with
OUT_SCHEMA = "my_schema"       # schema to write results into (can be same)
# ────────────────────────────────────────────────────────────

# Build the full three-part table name — used throughout the exercises
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{SRC_TABLE}"

print(f"Source table : {FULL_TABLE}")
print(f"Output schema: {CATALOG}.{OUT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 1 — Load a Table
# MAGIC
# MAGIC ## 📖 What are we doing?
# MAGIC
# MAGIC The first thing you'll do with any dataset is load it and have a look.
# MAGIC
# MAGIC Unity Catalog uses a **three-part dot notation** you'll already recognise from SQL:
# MAGIC
# MAGIC ```
# MAGIC catalog . schema . table
# MAGIC   ↑          ↑        ↑
# MAGIC like a     like a   the table
# MAGIC database   schema   itself
# MAGIC ```
# MAGIC
# MAGIC This is identical to how `database.schema.table` works in SQL Server —
# MAGIC the only difference is the extra catalog level at the front.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟦 1A — Load with Spark SQL
# MAGIC
# MAGIC In any notebook cell you can switch to SQL with the `%sql` magic.
# MAGIC This is plain SQL — write it exactly as you would today.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load 10 rows from the source table
# MAGIC SELECT *
# MAGIC FROM tesco_bank_training.bronze.transactions
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐍 1B — Load with PySpark
# MAGIC
# MAGIC `spark` is a built-in object available in every Databricks notebook.
# MAGIC `spark.table()` reads a Unity Catalog table and returns a **DataFrame** —
# MAGIC think of it as a table held in memory with the same rows and columns.

# COMMAND ----------

# Load the table into a DataFrame
df = spark.table(FULL_TABLE)

# .limit() is SELECT ... LIMIT 10
# .display() renders the result as an interactive table — like running %sql
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 1C — Write to a New Table
# MAGIC
# MAGIC Once you have a DataFrame you can persist it back to Unity Catalog.
# MAGIC This is the equivalent of `CREATE TABLE AS SELECT`.

# COMMAND ----------

# Write the DataFrame to a new table within your schema 
# mode="overwrite" = CREATE OR REPLACE TABLE
# mode="append"    = INSERT INTO
df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{OUT_SCHEMA}.ex1_loaded_customers")

print("✅ Table written: ex1_loaded_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 1D — Test It
# MAGIC
# MAGIC Good habit: always verify your output table looks right.
# MAGIC We check row count, column names, and spot-check a few rows.

# COMMAND ----------

# Load the table we just wrote
df_check = spark.table(f"{CATALOG}.{OUT_SCHEMA}.ex1_loaded_customers")

# Test 1: row count should be greater than 0
row_count = df_check.count()
assert row_count > 0, f"❌ Table is empty — expected rows, got {row_count}"
print(f"✅ Row count: {row_count}")

# Test 2: the table should have the same columns as the source
assert df_check.columns == df.columns, "❌ Column mismatch between source and output"
print(f"✅ Columns match: {df_check.columns}")

# Spot check — show 5 rows
df_check.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 2 — Filter Rows (WHERE clause)
# MAGIC
# MAGIC ## 📖 What are we doing?
# MAGIC
# MAGIC Filtering rows is the same idea as a SQL `WHERE` clause.
# MAGIC In PySpark you use `.filter()` or `.where()` — they are identical, use whichever you prefer.
# MAGIC
# MAGIC > ✏️ **Your turn:** Before running 2B, look at the data from Exercise 1 and pick
# MAGIC > a column and value that makes sense to filter on (e.g. a status, a country, a year).
# MAGIC > Update the filter condition in 2B and 2C to match your data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟦 2A — Filter with Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM my_catalog.my_schema.customers
# MAGIC WHERE status = 'active'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐍 2B — Filter with PySpark
# MAGIC
# MAGIC You can write the filter condition as a plain string (exactly like SQL),
# MAGIC or use PySpark's column syntax. Both are shown below.

# COMMAND ----------

# Option A: string condition — reads just like a SQL WHERE clause
df_active = df.filter("status = 'active'")

# Option B: Python column syntax — useful when building conditions in code
# col() turns a column name into a Python object you can compare, combine, etc.
from pyspark.sql.functions import col

df_active = df.filter(col("status") == "active")

print(f"Rows after filter: {df_active.count()}")
df_active.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 2C — Write Filtered Results to a Table

# COMMAND ----------

df_active.write.mode("overwrite").saveAsTable(f"{CATALOG}.{OUT_SCHEMA}.ex2_active_customers")

print("✅ Table written: ex2_active_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 2D — Test It

# COMMAND ----------

df_check = spark.table(f"{CATALOG}.{OUT_SCHEMA}.ex2_active_customers")

# Test 1: there should be fewer rows than the original (filter should have removed some)
original_count = df.count()
filtered_count = df_check.count()

assert filtered_count > 0,              "❌ Filtered table is empty"
assert filtered_count < original_count, "❌ Filter had no effect — same row count as source"
print(f"✅ Source rows: {original_count}  →  Filtered rows: {filtered_count}")

# Test 2: every row in the output should have status = 'active'
# .filter() on the check table should return the same count if all rows match
bad_rows = df_check.filter("status != 'active'").count()
assert bad_rows == 0, f"❌ Found {bad_rows} rows where status != 'active'"
print("✅ All rows have status = 'active'")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 3 — Select & Rename Columns
# MAGIC
# MAGIC ## 📖 What are we doing?
# MAGIC
# MAGIC `.select()` is the PySpark equivalent of the `SELECT` column list in SQL.
# MAGIC You can pick columns, rename them with `.alias()`, and do simple expressions.
# MAGIC
# MAGIC > ✏️ **Your turn:** Update the column names below to ones that exist in your table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟦 3A — Select Columns with Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     id,
# MAGIC     name                   AS customer_name,
# MAGIC     status,
# MAGIC     created_date
# MAGIC FROM my_catalog.my_schema.customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐍 3B — Select Columns with PySpark
# MAGIC
# MAGIC `.alias()` is the Python equivalent of `AS` in SQL.

# COMMAND ----------

df_selected = df.select(
    col("id"),
    col("name").alias("customer_name"),   # same as:  name AS customer_name
    col("status"),
    col("created_date")
)

df_selected.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 3C — Write to a Table

# COMMAND ----------

df_selected.write.mode("overwrite").saveAsTable(f"{CATALOG}.{OUT_SCHEMA}.ex3_customer_cols")

print("✅ Table written: ex3_customer_cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 3D — Test It

# COMMAND ----------

df_check = spark.table(f"{CATALOG}.{OUT_SCHEMA}.ex3_customer_cols")

# Test 1: output should have exactly the columns we selected
expected_cols = ["id", "customer_name", "status", "created_date"]
assert df_check.columns == expected_cols, \
    f"❌ Expected columns {expected_cols}, got {df_check.columns}"
print(f"✅ Columns are correct: {df_check.columns}")

# Test 2: no extra columns sneaked in
assert len(df_check.columns) == 4, \
    f"❌ Expected 4 columns, got {len(df_check.columns)}"
print("✅ Column count correct: 4")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 4 — Aggregate Data (GROUP BY)
# MAGIC
# MAGIC ## 📖 What are we doing?
# MAGIC
# MAGIC Aggregations in PySpark use `.groupBy()` followed by an aggregation function.
# MAGIC It maps directly to `GROUP BY` in SQL.
# MAGIC
# MAGIC | SQL | PySpark |
# MAGIC |-----|---------|
# MAGIC | `GROUP BY status` | `.groupBy("status")` |
# MAGIC | `COUNT(*)` | `.count()` |
# MAGIC | `SUM(col)` | `.agg(sum("col"))` |
# MAGIC | `AVG(col)` | `.agg(avg("col"))` |
# MAGIC
# MAGIC > ✏️ **Your turn:** Change `status` to a column in your table that makes sense to group by.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟦 4A — Aggregate with Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     status,
# MAGIC     COUNT(*)        AS customer_count,
# MAGIC     COUNT(DISTINCT id) AS unique_customers
# MAGIC FROM my_catalog.my_schema.customers
# MAGIC GROUP BY status
# MAGIC ORDER BY customer_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐍 4B — Aggregate with PySpark

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

df_agg = (
    df
    .groupBy("status")                                     # GROUP BY status
    .agg(
        count("*").alias("customer_count"),                # COUNT(*)
        countDistinct("id").alias("unique_customers")      # COUNT(DISTINCT id)
    )
    .orderBy(col("customer_count").desc())                 # ORDER BY customer_count DESC
)

df_agg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 4C — Write Aggregated Results

# COMMAND ----------

df_agg.write.mode("overwrite").saveAsTable(f"{CATALOG}.{OUT_SCHEMA}.ex4_customer_counts")

print("✅ Table written: ex4_customer_counts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 4D — Test It

# COMMAND ----------

df_check = spark.table(f"{CATALOG}.{OUT_SCHEMA}.ex4_customer_counts")

# Test 1: should have fewer rows than the source (aggregated)
assert df_check.count() < df.count(), \
    "❌ Aggregated table has as many rows as the source — groupBy may not have worked"
print(f"✅ Aggregated to {df_check.count()} group(s) from {df.count()} source rows")

# Test 2: customer_count values should all be positive integers
from pyspark.sql.functions import min as spark_min
min_count = df_check.agg(spark_min("customer_count")).collect()[0][0]
assert min_count > 0, f"❌ Found a group with count <= 0: {min_count}"
print(f"✅ Smallest group has {min_count} customer(s)")

df_check.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 5 — Join Two Tables
# MAGIC
# MAGIC ## 📖 What are we doing?
# MAGIC
# MAGIC Joins in PySpark use `.join()` and work the same as SQL joins.
# MAGIC The syntax is: `left_df.join(right_df, on="key_column", how="join_type")`
# MAGIC
# MAGIC | SQL join type | PySpark `how=` value |
# MAGIC |---|---|
# MAGIC | `INNER JOIN` | `"inner"` |
# MAGIC | `LEFT JOIN` | `"left"` |
# MAGIC | `RIGHT JOIN` | `"right"` |
# MAGIC | `FULL OUTER JOIN` | `"outer"` |
# MAGIC
# MAGIC > ✏️ **Your turn:** Replace `orders` and `customer_id` with a second table and
# MAGIC > join key that exists in your environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟦 5A — Join with Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.id,
# MAGIC     c.name,
# MAGIC     o.order_id,
# MAGIC     o.order_date,
# MAGIC     o.total_amount
# MAGIC FROM my_catalog.my_schema.customers c
# MAGIC LEFT JOIN my_catalog.my_schema.orders o
# MAGIC     ON c.id = o.customer_id
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐍 5B — Join with PySpark

# COMMAND ----------

# Load the second table
df_orders = spark.table(f"{CATALOG}.{SCHEMA}.orders")

# Join customers to orders
df_joined = (
    df                                            # left table  (customers)
    .join(df_orders, on="customer_id", how="left") # LEFT JOIN orders ON customer_id
    .select(
        col("id"),
        col("name"),
        col("order_id"),
        col("order_date"),
        col("total_amount")
    )
)

df_joined.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 5C — Write Joined Results

# COMMAND ----------

df_joined.write.mode("overwrite").saveAsTable(f"{CATALOG}.{OUT_SCHEMA}.ex5_customers_with_orders")

print("✅ Table written: ex5_customers_with_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 5D — Test It

# COMMAND ----------

df_check = spark.table(f"{CATALOG}.{OUT_SCHEMA}.ex5_customers_with_orders")

# Test 1: left join should return at least as many rows as the customers table
# (every customer appears, even those with no orders)
cust_count = df.count()
joined_count = df_check.count()
assert joined_count >= cust_count, \
    f"❌ Join returned fewer rows ({joined_count}) than customers ({cust_count})"
print(f"✅ Customers: {cust_count}  →  Joined rows: {joined_count}")

# Test 2: the expected columns should all exist in the output
for col_name in ["id", "name", "order_id", "order_date", "total_amount"]:
    assert col_name in df_check.columns, f"❌ Missing column: {col_name}"
print(f"✅ All expected columns present: {df_check.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 6 — Add a Derived Column
# MAGIC
# MAGIC ## 📖 What are we doing?
# MAGIC
# MAGIC `.withColumn()` adds or replaces a column — the equivalent of adding an expression
# MAGIC to your SELECT list in SQL (or a `CASE WHEN`, computed column, etc.)
# MAGIC
# MAGIC > ✏️ **Your turn:** Adapt the expression to use a numeric or date column from your table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟦 6A — Derived Column with Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     id,
# MAGIC     name,
# MAGIC     total_amount,
# MAGIC     total_amount * 1.2          AS amount_inc_vat,
# MAGIC     CASE
# MAGIC         WHEN total_amount >= 1000 THEN 'high'
# MAGIC         WHEN total_amount >= 100  THEN 'medium'
# MAGIC         ELSE 'low'
# MAGIC     END                         AS spend_tier
# MAGIC FROM my_catalog.my_schema.orders
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐍 6B — Derived Column with PySpark
# MAGIC
# MAGIC `when()` is the PySpark equivalent of `CASE WHEN`.
# MAGIC `.otherwise()` is the `ELSE`.

# COMMAND ----------

from pyspark.sql.functions import when, lit

df_derived = (
    df_orders
    .withColumn("amount_inc_vat", col("total_amount") * 1.2)          # simple expression
    .withColumn(
        "spend_tier",
        when(col("total_amount") >= 1000, lit("high"))                # CASE WHEN >= 1000
        .when(col("total_amount") >= 100,  lit("medium"))             # WHEN >= 100
        .otherwise(lit("low"))                                        # ELSE
    )
)

df_derived.select("id", "total_amount", "amount_inc_vat", "spend_tier").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 6C — Write to a Table

# COMMAND ----------

df_derived.write.mode("overwrite").saveAsTable(f"{CATALOG}.{OUT_SCHEMA}.ex6_orders_enriched")

print("✅ Table written: ex6_orders_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ 6D — Test It

# COMMAND ----------

df_check = spark.table(f"{CATALOG}.{OUT_SCHEMA}.ex6_orders_enriched")

# Test 1: new columns exist
for col_name in ["amount_inc_vat", "spend_tier"]:
    assert col_name in df_check.columns, f"❌ Missing column: {col_name}"
print("✅ Both derived columns present")

# Test 2: spend_tier should only contain the three expected values
valid_tiers = {"high", "medium", "low"}
actual_tiers = {row["spend_tier"] for row in df_check.select("spend_tier").distinct().collect()}
unexpected = actual_tiers - valid_tiers
assert not unexpected, f"❌ Unexpected spend_tier values found: {unexpected}"
print(f"✅ spend_tier values are valid: {actual_tiers}")

# Test 3: amount_inc_vat should equal total_amount * 1.2 (spot check first 100 rows)
from pyspark.sql.functions import round as spark_round, abs as spark_abs
discrepancies = (
    df_check
    .withColumn("expected_vat", spark_round(col("total_amount") * 1.2, 2))
    .withColumn("actual_vat",   spark_round(col("amount_inc_vat"), 2))
    .filter(col("expected_vat") != col("actual_vat"))
    .count()
)
assert discrepancies == 0, f"❌ {discrepancies} rows have incorrect amount_inc_vat"
print("✅ amount_inc_vat calculation is correct")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Module 1 Complete — Exercises Summary
# MAGIC
# MAGIC | Exercise | Concept | SQL equivalent |
# MAGIC |---|---|---|
# MAGIC | 1 | Load a table | `SELECT * FROM catalog.schema.table` |
# MAGIC | 2 | Filter rows | `WHERE col = 'value'` |
# MAGIC | 3 | Select & rename columns | `SELECT col AS alias` |
# MAGIC | 4 | Aggregate | `GROUP BY / COUNT / SUM` |
# MAGIC | 5 | Join two tables | `LEFT JOIN … ON` |
# MAGIC | 6 | Add a derived column | `CASE WHEN / col * 1.2` |
# MAGIC
# MAGIC Each exercise follows the pattern: **SQL way → PySpark way → write → test**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ➡️ Module 2: Scheduling this Notebook as a Databricks Job
# MAGIC
# MAGIC Now that you can transform data in a notebook, the next step is automating it.
# MAGIC Module 2 covers turning these cells into a reusable job with parameters and scheduling.
