# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
SOURCE_SCHEMA  = f"{user_schema}_bronze"
SILVER_SCHEMA  = f"{user_schema}_silver"

print(f"Source:  {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target:  {CATALOG}.{SILVER_SCHEMA}")

input_customers_table = f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_customers"
input_repayments_table = f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_repayments"
input_transactions_table = f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest_transactions"

output_customers_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_customers"
output_repayments_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_repayments"
output_transactions_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_clean_transactions"

# COMMAND ----------

display(spark.table(input_customers_table))

# COMMAND ----------

# Clean customer table
# customer_id - ok
# first_name - empty spaces at the beginning, change so starts with upper case
# last_name - empty spaces at the beginning, change so starts with upper case
# date_of_birth - change format to date
# city - empty spaces at the beginning, change so starts with upper case
# employment_status - ok
# annual_income - remove £ from beginning, make int format £80,461
# product_type - check distinct values
# account_opened - change format to date
# credit_limit - change empty values to 0

# Clean customer table - String variables
# first_name
clean_customers = spark.table(input_customers_table).select(F.concat(F.upper(F.substring(F.trim("first_name"),1,1)), F.lower(F.substring(F.trim("first_name"),2,F.length("first_name")))).alias("first_name"))

# last_name
clean_customers = spark.table(input_customers_table).select(F.concat(F.upper(F.substring(F.trim("last_name"),1,1)), F.lower(F.substring(F.trim("last_name"),2,F.length("last_name")))).alias("last_name"))

# city
clean_customers = spark.table(input_customers_table).select(F.concat(F.upper(F.substring(F.trim("city"),1,1)), F.lower(F.substring(F.trim("city"),2,F.length("city")))).alias("city"))


# COMMAND ----------

# Clean customer table - Numeric variables
# annual_income
clean_customers = spark.table(input_customers_table).select(F.regexp_replace("annual_income", "£", "").alias("annual_income"))

# COMMAND ----------

# Clean customer table - date variables
# date_of_birth - change format to date
# account_opened - change format to date

# date_of_birth
# 10-02-1963
cust_date_1 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",3,1) == "-")
cust_date_1_fix = cust_date_1.select("*",F.to_date("date_of_birth",'dd-MM-yyyy').alias("date_of_birth_n"))

# 2003-08-31
cust_date_2 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",5,1) == "-")
cust_date_2_fix = cust_date_2.select("*",F.to_date("date_of_birth",'yyyy-MM-dd').alias("date_of_birth_n"))

# 1977/11/13
cust_date_3 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",5,1) == "/")
cust_date_3_fix = cust_date_3.select("*",F.to_date("date_of_birth",'yyyy/MM/dd').alias("date_of_birth_n"))

# 12/07/1964
cust_date_4 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",3,1) == "/")
cust_date_4_fix = cust_date_4.select("*",F.to_date("date_of_birth",'dd/MM/yyyy').alias("date_of_birth_n"))

# 06 Oct 1993
cust_date_5 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",3,1) == " ")
cust_date_5_fix = cust_date_5.select("*",F.to_date("date_of_birth",'dd MMM yyyy').alias("date_of_birth_n"))

# Combine together
union_df = cust_date_1_fix.union(cust_date_2_fix).union(cust_date_3_fix).union(cust_date_4_fix).union(cust_date_5_fix)

union_df_2 = union_df.drop("date_of_birth")
union_df_final = union_df_2.withColumnRenamed("date_of_birth_n", "date_of_birth")



# COMMAND ----------

# Clean customer table - date variables
# date_of_birth - change format to date
# account_opened - change format to date

# date_of_birth
# 10-02-1963
cust_date_1 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",3,1) == "-")
cust_date_1_fix = cust_date_1.select("*",F.to_date("date_of_birth",'dd-MM-yyyy').alias("date_of_birth_n"))

# 2003-08-31
cust_date_2 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",5,1) == "-")
cust_date_2_fix = cust_date_2.select("*",F.to_date("date_of_birth",'yyyy-MM-dd').alias("date_of_birth_n"))

# 1977/11/13
cust_date_3 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",5,1) == "/")
cust_date_3_fix = cust_date_3.select("*",F.to_date("date_of_birth",'yyyy/MM/dd').alias("date_of_birth_n"))

# 12/07/1964
cust_date_4 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",3,1) == "/")
cust_date_4_fix = cust_date_4.select("*",F.to_date("date_of_birth",'dd/MM/yyyy').alias("date_of_birth_n"))

# 06 Oct 1993
cust_date_5 = spark.table(input_customers_table).select("*").where(F.substring("date_of_birth",3,1) == " ")
cust_date_5_fix = cust_date_5.select("*",F.to_date("date_of_birth",'dd MMM yyyy').alias("date_of_birth_n"))

# Combine together
union_df = cust_date_1_fix.union(cust_date_2_fix).union(cust_date_3_fix).union(cust_date_4_fix).union(cust_date_5_fix)

union_df_2 = union_df.drop("date_of_birth")
union_df_final = union_df_2.withColumnRenamed("date_of_birth_n", "date_of_birth")

# COMMAND ----------

display(union_df_final)

# COMMAND ----------

distinct_column = clean_customers.dropDuplicates(["annual_income"]).select("annual_income")
display(distinct_column)

# COMMAND ----------

silver_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.bronze_ingest").select(
    F.lower(F.col("customer_id")).alias("customer_id"),
    F.upper(F.col("product_type")).alias("product_type"),
    F.col("credit_limit"),
    F.col("city")
)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_clean")