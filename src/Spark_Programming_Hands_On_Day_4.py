# Databricks notebook source
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

# COMMAND ----------

# Read table into dataframe using spark.table() 
customers = spark.table(customers_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write data into a Unity table

# COMMAND ----------

# Writing - overwrite mode
customers.write.mode("overwrite").saveAsTable(f"{catalog}.{write_schema}.customer_version_history")

# COMMAND ----------

# Writing - append mode (this step is our "mistake" that we want to reverse)
customers.write.mode("append").saveAsTable(f"{catalog}.{write_schema}.customer_version_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The Describe and Restore Command

# COMMAND ----------

# Running a describe history on the table 
display(spark.sql(f"""DESCRIBE HISTORY {catalog}.{write_schema}.customer_version_history"""))

# COMMAND ----------

# This restore command will restore the table to the first version of its history before the append
spark.sql(f"""RESTORE TABLE {catalog}.{write_schema}.customer_version_history TO VERSION AS OF 0""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. GDPR Delete Request

# COMMAND ----------

# Deleting the customer from the table
spark.sql(f"""DELETE FROM {catalog}.{write_schema}.customer_version_history WHERE customer_id = 'CUST00377' """)

# COMMAND ----------

# Checking the record has been deleted from the table
display(spark.sql(f"""SELECT * FROM {catalog}.{write_schema}.customer_version_history WHERE customer_id = 'CUST00377' """))

# COMMAND ----------

# Running a describe history on the table to see the version we are looking for
display(spark.sql(f"""DESCRIBE HISTORY {catalog}.{write_schema}.customer_version_history"""))

# COMMAND ----------

# this shows us we can still access this customers record so it is not really deleted 
display(spark.sql(f"""SELECT * FROM {catalog}.{write_schema}.customer_version_history version as of 0 WHERE customer_id = 'CUST00377' """))

# COMMAND ----------

# Changing the retention time of the deleted files (this should not be done in a production environment this is purely for this exercise)
spark.sql(
  f"""
  ALTER TABLE {catalog}.{write_schema}.customer_version_history
  SET TBLPROPERTIES (
    delta.deletedFileRetentionDuration = 'interval 0 hours'
  )
  """
)

# COMMAND ----------

# Running the VACUUM Command to remove the file in question
spark.sql(f"""VACUUM {catalog}.{write_schema}.customer_version_history""")

# COMMAND ----------

# Showing that we can no longer access this customers record
display(spark.sql(f"""SELECT * FROM {catalog}.{write_schema}.customer_version_history version as of 0 WHERE customer_id = 'CUST00377' """))