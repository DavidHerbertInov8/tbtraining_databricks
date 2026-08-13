# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Programming - Hands-On
# MAGIC Companion notebook for **Spark & Databricks** (Chapters 4 & 5).
# MAGIC
# MAGIC Datasets used:
# MAGIC - `customers`
# MAGIC - `repayments`
# MAGIC - `transactions`
# MAGIC
# MAGIC Update the `catalog` and `schema` widgets below to point at your environment before running.

# COMMAND ----------

dbutils.widgets.text("catalog", "tesco_bank_training", "Catalog")
dbutils.widgets.text("schema", "datasets", "Schema")
dbutils.widgets.text("write_schema", "", "write_schema ( Your name ie. jack_gibb)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
write_schema = dbutils.widgets.get("schema")

customers_table = f"{catalog}.{schema}.customers"
repayments_table = f"{catalog}.{schema}.repayments"
transactions_table = f"{catalog}.{schema}.transactions"

write_schema = dbutils.widgets.get("write_schema")

write_schema = write_schema + "_" + "silver"

print(customers_table)
print(repayments_table)
print(transactions_table)
print(write_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read data into a Spark DataFrame
# MAGIC Two equivalent ways to read a Unity Catalog table: the PySpark API and the Spark SQL API.

# COMMAND ----------

# PySpark API - spark.table() method
customers = spark.table(customers_table)
display(customers)

# COMMAND ----------

# Spark SQL API - spark.sql() method
transactions = spark.sql(f"""SELECT * FROM {transactions_table}""")
display(transactions)

# COMMAND ----------

repayments = spark.table(repayments_table)
display(repayments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Basic Filtering
# MAGIC Same result, shown via both the PySpark API and the Spark SQL API.

# COMMAND ----------

# PySpark API - .where() method
late_repayments_py = repayments.where("payment_status = 'Late'")
display(late_repayments_py)

# COMMAND ----------

spark.sql(f"SELECT * FROM {catalog}.{schema}.repayments WHERE payment_status = 'Late'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Basic Transformations (PySpark API)

# COMMAND ----------

# Importing required functions
# (wildcard - avoid in production code - import only what is needed)
from pyspark.sql.functions import *

# COMMAND ----------

# Selecting - Subset
subset_df = customers.select("customer_id", "product_type", "credit_limit")
display(subset_df)

# COMMAND ----------

# Selecting - All
all_columns_df = customers.select(["*"])
display(all_columns_df)

# COMMAND ----------

# Filtering - Specific Records
# (SAS Context: this is your WHERE clause or IF subsetting in a data step)
low_limit_df = (
    customers.select("customer_id", "product_type", "credit_limit")
    .where(col("credit_limit") < 5000)
)
display(low_limit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Advanced Transformations: Distinct & Ordered Data (PySpark API)

# COMMAND ----------

# Collecting Distinct Records
distinct_products_df = customers.dropDuplicates(["product_type", "city"]).select("product_type", "city")
display(distinct_products_df)

# COMMAND ----------

# Ordering Records
# Sorting records in ascending order, use ascending=False for descending order
sorted_df = transactions.sort("amount", ascending=True)
display(sorted_df)

# The orderBy function can also be used
ordered_df = transactions.orderBy("amount", ascending=False)
display(ordered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Advanced Transformations: GroupBy (PySpark API)

# COMMAND ----------

# Total transaction value by merchant category
category_totals_df = transactions.groupBy("merchant_category").sum("amount")
display(category_totals_df)

# COMMAND ----------

# Volume of transactions per customer
transaction_count_df = transactions.groupBy("customer_id").count()
display(transaction_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Advanced Transformations: Aggregation

# COMMAND ----------

# PySpark API - min amount_due by payment_status
min_due_df = repayments.groupBy("payment_status").min("amount_due")
display(min_due_df)

# COMMAND ----------

# groupBy and aggregate on multiple columns
merchant_totals_df = transactions.groupBy("merchant_category", "transaction_type").sum("amount")
display(merchant_totals_df)

# COMMAND ----------

# agg() - compute multiple aggregations in a single statement
customer_agg_df = (
    transactions.groupBy("customer_id")
    .agg(
        avg("amount").alias("avg_amount"),
        sum("amount").alias("sum_amount"),
        max("amount").alias("max_amount"),
        count("transaction_id").alias("total_transactions"),
    )
)
display(customer_agg_df)

# COMMAND ----------

# Spark SQL API - aggregations with GROUP BY
spark.sql(f""" SELECT
customer_id,
  avg(amount) AS avg_amount,
  sum(amount) AS sum_amount,
  max(amount) AS max_amount,
  count(transaction_id) AS total_transactions
FROM {catalog}.{schema}.transactions
GROUP BY customer_id""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Advanced Transformations: Pivoting (PySpark API)

# COMMAND ----------

# Group by customer, pivot by merchant_category, and get the total sum of amount
pivot_df = transactions.groupBy("customer_id").pivot("merchant_category").sum("amount")
display(pivot_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Advanced Transformations: Joins (PySpark API)

# COMMAND ----------

# Inner Join - default join in PySpark
inner_join_df = transactions.join(customers, transactions.customer_id == customers.customer_id, "inner")
display(inner_join_df)

# COMMAND ----------

# Full Outer Join - returns all rows from both datasets, nulls where there is no match
full_outer_join_df = transactions.join(customers, transactions.customer_id == customers.customer_id, "outer")
display(full_outer_join_df)

# COMMAND ----------

# Left Join - all rows from the left dataset (transactions), nulls where no match on the right
left_outer_join_df = transactions.join(customers, transactions.customer_id == customers.customer_id, "left")
display(left_outer_join_df)

# COMMAND ----------

# Right Join - all rows from the right dataset (customers), nulls where no match on the left
right_outer_join_df = transactions.join(customers, transactions.customer_id == customers.customer_id, "right")
display(right_outer_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Advanced Transformations: Union & Intersect (PySpark API)

# COMMAND ----------

# Split customers into two subsets, then bring them back together
current_product_df = customers.where("product_type = 'Credit Card'")
savings_product_df = customers.where("product_type = 'Savings'")

# Keep duplicates in union
union_df = current_product_df.union(savings_product_df)
display(union_df)

# Use the distinct() method to remove any duplicates from the union
union_df_no_duplicates = current_product_df.union(savings_product_df).distinct()
display(union_df_no_duplicates)

# unionByName matches on column names (column ordering is irrelevant across both DataFrames)
union_by_name_df = current_product_df.unionByName(savings_product_df)
display(union_by_name_df)

# COMMAND ----------

# Intersect - common rows between two DataFrames
late_customer_ids = late_repayments_py.select("customer_id").distinct()
credit_card_customer_ids = current_product_df.select("customer_id").distinct()

intersect_df = late_customer_ids.intersect(credit_card_customer_ids)
display(intersect_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Testing

# COMMAND ----------

from pyspark.testing import assertDataFrameEqual

# Compare the PySpark filter result against a Spark SQL equivalent
late_repayments_sql = spark.sql(f"SELECT * FROM {repayments_table} WHERE payment_status = 'Late'")

assertDataFrameEqual(late_repayments_py, late_repayments_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Writing to External Sources

# COMMAND ----------

print((f"{catalog}.{write_schema}.customer_transaction_summary"))

# COMMAND ----------

# Writing - overwrite mode
customer_agg_df.write.mode("overwrite").saveAsTable(f"{catalog}.{write_schema}.customer_transaction_summary")

# COMMAND ----------

# Writing - append mode
customer_agg_df.write.mode("append").saveAsTable(f"{catalog}.{write_schema}.customer_transaction_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Optimisation

# COMMAND ----------

# Broadcast join - customers is the small reference table, transactions is the large fact table
# No shuffle of the large dataset is needed
broadcast_join_df = transactions.join(customers.hint("broadcast"), on="customer_id", how="left")
broadcast_join_df.show()