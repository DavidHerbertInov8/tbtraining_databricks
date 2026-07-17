# Databricks notebook source
# random stuff, not to be run as part of the main job.



    customers_df = spark.table(
    f"{CATALOG}.{read_schema}.customers_imported").select(
        "*",
        initcap(trim(col("first_name"))).alias("first_name_adj"),
        initcap(trim(col("last_name"))).alias("last_name_adj")
   ).limit(500)

# COMMAND ----------

# import and options
spark.conf.set("spark.sql.ansi.enabled", "false")

#from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, initcap, to_timestamp, regexp_replace

#coalesce

# COMMAND ----------

# set up stuff 
dbutils.widgets.text("catalog", "tesco_bank_training", "Catalog")
dbutils.widgets.text("read_schema", "", "read_schema (Your name ie. jack_gibb)")
dbutils.widgets.text("write_schema", "", "write_schema (Your name ie. jack_gibb)")

catalog = dbutils.widgets.get("catalog")
read_schema = dbutils.widgets.get("read_schema")
write_schema = dbutils.widgets.get("write_schema")

if not read_schema:
    raise ValueError("Please enter your read_schema name in the widget above before running.")
if not write_schema:
    raise ValueError("Please enter your write_schema name in the widget above before running.")

print(f"Source:  {CATALOG}.{read_schema}")
print(f"Target:  {CATALOG}.{write_schema}")

# COMMAND ----------

# clean customer table

chars_to_remove = r"(£|,)"

customers_df = spark.table(
    f"{CATALOG}.{read_schema}.customers_imported").select(
        #"*",
        #initcap(trim(col("first_name"))).alias("first_name_adj"),
        #initcap(trim(col("last_name"))).alias("last_name_adj"),
        #coalesce(
        #    to_timestamp(trim(col("date_of_birth").cast("string")), "dd-MM-yyyy"),
        #    to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy-MM-dd"),
        #    to_timestamp(trim(col("date_of_birth").cast("string")), "yyyy/MM/dd"),
        #    to_timestamp(trim(col("date_of_birth").cast("string")), "dd MMM yyyy"),
        #    to_timestamp(trim(col("date_of_birth").cast("string")), "dd/MM/yyyy")
        #).cast("date").alias("date_of_birth_adj"),
        #initcap(trim(col("city"))).alias("city_adj"),
        col("annual_income"),
        regexp_replace(col("annual_income"), chars_to_remove, "").cast("decimal(10,2)").alias("annual_income_adj")
)

#    regexp_replace(format_number(avg(c), 3), ",", "").alias(c)


#display(customers_df.filter("dob is null"))
display(customers_df)