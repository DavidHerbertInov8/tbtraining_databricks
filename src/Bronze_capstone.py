# Databricks notebook source
# Random code stuff, from playing around etc but not used in main code

file_path = "/home/user/documents/report.pdf"
file_path = os.path.pathname
file_name = os.path.basename(file_path)

print(file_name) # Output: report.pdf

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(notebook_path)



#date_format(current_timestamp(), "yyyy/MM/dd HH:mm:ss").alias("ingest_time")#.format("yyyy/mm/dd hh:mm:ss")

# COMMAND ----------

# import stuff

from pyspark.sql.functions import current_timestamp, lit, date_format

#import os

# COMMAND ----------

# set up stuff
dbutils.widgets.text("catalog", "tesco_bank_training", "Catalog")
dbutils.widgets.text("read_schema", "datasets", "read_schema")
dbutils.widgets.text("write_schema", "", "write_schema ( Your name ie. jack_gibb)")

catalog = dbutils.widgets.get("catalog")
read_schema = dbutils.widgets.get("read_schema")
write_schema = dbutils.widgets.get("write_schema")

tablename_customers = "customers"
tablename_repayments = "repayments"
tablename_transactions = "transactions"

customers = f"{catalog}.{read_schema}.customers"
repayments = f"{catalog}.{read_schema}.repayments"
transactions = f"{catalog}.{read_schema}.transactions"

print(write_schema)


# COMMAND ----------

# deal with customers table

customers_imported = spark.table(customers).select(
    "*",
    date_format(current_timestamp(), ("yyyy/mm/dd hh:mm:ss") ).alias("ingested_at"),
    lit(tablename_customers).alias("source_file"),
)

display(customers_imported)

customers_imported.write.mode("overwrite").saveAsTable(f"{catalog}.{write_schema}.customers_imported")


# COMMAND ----------

# deal with repayments table

repayments_imported = spark.table(repayments).select(
    "*",
    date_format(current_timestamp(), ("yyyy/mm/dd hh:mm:ss") ).alias("ingested_at"),
    lit(tablename_repayments).alias("source_file"),
)

display(repayments_imported)

repayments_imported.write.mode("overwrite").saveAsTable(f"{catalog}.{write_schema}.repayments_imported")

# COMMAND ----------

# deal with transactions table

transactions_imported = spark.table(transactions).select(
    "*",
    date_format(current_timestamp(), ("yyyy/mm/dd hh:mm:ss") ).alias("ingested_at"),
    lit(tablename_transactions).alias("source_file"),
)

display(transactions_imported)

transactions_imported.write.mode("overwrite").saveAsTable(f"{catalog}.{write_schema}.transactions_imported")