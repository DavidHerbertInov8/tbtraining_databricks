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

# Clean customer table

# customer_id - ok
# first_name - empty spaces at the beginning, change so starts with upper case
# last_name - empty spaces at the beginning, change so starts with upper case
# date_of_birth - change format to date
# city - empty spaces at the beginning, change so starts with upper case
# employment_status - ok
# annual_income - remove £ from beginning, make int format (currently £80,461)
# product_type - ok
# account_opened - change format to date
# credit_limit - change empty values to 0
# ingested_at - ok
# source_file - ok

# Clean customer table - String and integer variables
customers_clean_str_int = spark.table(input_customers_table).select(
    "customer_id",
    F.concat(F.upper(F.substring(F.trim("first_name"),1,1)), F.lower(F.substring(F.trim("first_name"),2,F.length("first_name")))).alias("first_name"),
    F.concat(F.upper(F.substring(F.trim("last_name"),1,1)), F.lower(F.substring(F.trim("last_name"),2,F.length("last_name")))).alias("last_name"),
    "date_of_birth",
    F.concat(F.upper(F.substring(F.trim("city"),1,1)), F.lower(F.substring(F.trim("city"),2,F.length("city")))).alias("city"),
    "employment_status",
    F.regexp_replace("annual_income", "£|,", "").alias("annual_income").cast("decimal(10,0)"),
    "product_type",
    "account_opened",
    "credit_limit",
    "ingested_at",
    "source_file"
)

# COMMAND ----------

# Clean customer table - credit_limit
customers_clean_cl = customers_clean_str_int.fillna(0, subset=["credit_limit"])

# COMMAND ----------

# Clean customer table - date_of_birth

# 10-02-1963
cust_date_1 = customers_clean_cl.select("*").where(F.substring("date_of_birth",3,1) == "-")
cust_date_1_fix = cust_date_1.select(
    "customer_id",
    "first_name",
    "last_name",
    F.to_date(F.col("date_of_birth"),'dd-MM-yyyy').alias("date_of_birth"),
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    "account_opened",
    "credit_limit",
    "ingested_at",
    "source_file"
)

# 2003-08-31
cust_date_2 = customers_clean_cl.select("*").where(F.substring("date_of_birth",5,1) == "-")
cust_date_2_fix = cust_date_2.select(
    "customer_id",
    "first_name",
    "last_name",
    F.to_date("date_of_birth",'yyyy-MM-dd').alias("date_of_birth"),
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    "account_opened",
    "credit_limit",
    "ingested_at",
    "source_file"   
)

# 1977/11/13
cust_date_3 = customers_clean_cl.select("*").where(F.substring("date_of_birth",5,1) == "/")
cust_date_3_fix = cust_date_3.select(
    "customer_id",
    "first_name",
    "last_name",
    F.to_date("date_of_birth",'yyyy/MM/dd').alias("date_of_birth"),
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    "account_opened",
    "credit_limit",
    "ingested_at",
    "source_file" 
)

# 12/07/1964
cust_date_4 = customers_clean_cl.select("*").where(F.substring("date_of_birth",3,1) == "/")
cust_date_4_fix = cust_date_4.select(
    "customer_id",
    "first_name",
    "last_name",
    F.to_date("date_of_birth",'dd/MM/yyyy').alias("date_of_birth"),
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    "account_opened",
    "credit_limit",
    "ingested_at",
    "source_file" 
)

# 06 Oct 1993
cust_date_5 = customers_clean_cl.select("*").where(F.substring("date_of_birth",3,1) == " ")
cust_date_5_fix = cust_date_5.select(
    "customer_id",
    "first_name",
    "last_name",
    F.to_date("date_of_birth",'dd MMM yyyy').alias("date_of_birth"),
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    "account_opened",
    "credit_limit",
    "ingested_at",
    "source_file" 
)

# Combine together
customers_clean_dob = cust_date_1_fix.union(cust_date_2_fix).union(cust_date_3_fix).union(cust_date_4_fix).union(cust_date_5_fix)

# COMMAND ----------

# Clean customer table - account_opened

# 10-02-1963
cust_date_1 = customers_clean_dob.select("*").where(F.substring("account_opened",3,1) == "-")
cust_date_1_fix = cust_date_1.select(
    "customer_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    F.to_date(F.col("account_opened"),'dd-MM-yyyy').alias("account_opened"),
    "credit_limit",
    "ingested_at",
    "source_file"
)

# 2003-08-31
cust_date_2 = customers_clean_dob.select("*").where(F.substring("account_opened",5,1) == "-")
cust_date_2_fix = cust_date_2.select(
    "customer_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    F.to_date("account_opened",'yyyy-MM-dd').alias("account_opened"),
    "credit_limit",
    "ingested_at",
    "source_file"    
)

# 1977/11/13
cust_date_3 = customers_clean_dob.select("*").where(F.substring("account_opened",5,1) == "/")
cust_date_3_fix = cust_date_3.select(
    "customer_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    F.to_date("account_opened",'yyyy/MM/dd').alias("account_opened"),
    "credit_limit",
    "ingested_at",
    "source_file"  
)

# 12/07/1964
cust_date_4 = customers_clean_dob.select("*").where(F.substring("account_opened",3,1) == "/")
cust_date_4_fix = cust_date_4.select(
    "customer_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    F.to_date("account_opened",'dd/MM/yyyy').alias("account_opened"),
    "credit_limit",
    "ingested_at",
    "source_file"   
)

# 06 Oct 1993
cust_date_5 = customers_clean_dob.select("*").where(F.substring("account_opened",3,1) == " ")
cust_date_5_fix = cust_date_5.select(
    "customer_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "city",
    "employment_status",
    "annual_income",
    "product_type",
    F.to_date("account_opened",'dd MMM yyyy').alias("account_opened"),
    "credit_limit",
    "ingested_at",
    "source_file"   
)

# Combine together
customers_clean = cust_date_1_fix.union(cust_date_2_fix).union(cust_date_3_fix).union(cust_date_4_fix).union(cust_date_5_fix)

# COMMAND ----------

# Customers - remove any duplicates
customers_nodups = customers_clean.distinct()

# COMMAND ----------

# Clean repayments table

# repayment_id - ok
# customer_id - ok
# due_date - change format to date
# amount_due - ok
# amount_paid - ok
# payment_status - ok
# ingested_at - ok
# source_file - ok

repayments = spark.table(input_repayments_table)

# Clean repayments table - due_date
# 10-02-1963
rep_date_1 = repayments.select("*").where(F.substring("due_date",3,1) == "-")
rep_date_1_fix = rep_date_1.select(
    "repayment_id",
    "customer_id",
    F.to_date(F.col("due_date"),'dd-MM-yyyy').alias("due_date"),
    "amount_due",
    "amount_paid",
    "payment_status",
    "ingested_at",
    "source_file"
)

# 2003-08-31
rep_date_2 = repayments.select("*").where(F.substring("due_date",5,1) == "-")
rep_date_2_fix = rep_date_2.select(
    "repayment_id",
    "customer_id",
    F.to_date("due_date",'yyyy-MM-dd').alias("due_date"),
    "amount_due",
    "amount_paid",
    "payment_status",
    "ingested_at",
    "source_file"
)

# 1977/11/13
rep_date_3 = repayments.select("*").where(F.substring("due_date",5,1) == "/")
rep_date_3_fix = rep_date_3.select(
    "repayment_id",
    "customer_id",
    F.to_date("due_date",'yyyy/MM/dd').alias("due_date"),
    "amount_due",
    "amount_paid",
    "payment_status",
    "ingested_at",
    "source_file" 
)

# 12/07/1964
rep_date_4 = repayments.select("*").where(F.substring("due_date",3,1) == "/")
rep_date_4_fix = rep_date_4.select(
    "repayment_id",
    "customer_id",
    F.to_date("due_date",'dd/MM/yyyy').alias("due_date"),
    "amount_due",
    "amount_paid",
    "payment_status",
    "ingested_at",
    "source_file" 
)

# 06 Oct 1993
rep_date_5 = repayments.select("*").where(F.substring("due_date",3,1) == " ")
rep_date_5_fix = rep_date_5.select(
    "repayment_id",
    "customer_id",
    F.to_date("due_date",'dd MMM yyyy').alias("due_date"),
    "amount_due",
    "amount_paid",
    "payment_status",
    "ingested_at",
    "source_file"    
)

# Combine together
repayments_clean = rep_date_1_fix.union(rep_date_2_fix).union(rep_date_3_fix).union(rep_date_4_fix).union(rep_date_5_fix)

# COMMAND ----------

# Repayments - remove any duplicates
repayments_nodups = repayments_clean.distinct()

# COMMAND ----------

# Clean transactions table

# transaction_id - ok
# customer_id - ok
# transaction_date - change format to date
# amount - ok
# transaction_type - spaces at the beginning, change so starts with upper case
# merchant_category - ok
# ingested_at - ok
# source_file - ok

# Clean transactions table - transaction_type
transactions_clean_str = spark.table(input_transactions_table).select(
    "transaction_id",
    "customer_id",
    "transaction_date",
    "amount",
    F.concat(F.upper(F.substring(F.trim("transaction_type"),1,1)), F.lower(F.substring(F.trim("transaction_type"),2,F.length("transaction_type")))).alias("transaction_type"),
    "merchant_category",
    "ingested_at",
    "source_file"
)



# COMMAND ----------

# Clean transactions table - transaction_date
# 10-02-1963
tran_date_1 = transactions_clean_str.select("*").where(F.substring("transaction_date",3,1) == "-")
tran_date_1_fix = tran_date_1.select(
    "transaction_id",
    "customer_id",
    F.to_date(F.col("transaction_date"),'dd-MM-yyyy').alias("transaction_date"),
    "amount",
    "transaction_type",
    "merchant_category",
    "ingested_at",
    "source_file"
)

# 2003-08-31
tran_date_2 = transactions_clean_str.select("*").where(F.substring("transaction_date",5,1) == "-")
tran_date_2_fix = tran_date_2.select(
    "transaction_id",
    "customer_id",
    F.to_date("transaction_date",'yyyy-MM-dd').alias("transaction_date"),
    "amount",
    "transaction_type",
    "merchant_category",
    "ingested_at",
    "source_file"
)

# 1977/11/13
tran_date_3 = transactions_clean_str.select("*").where(F.substring("transaction_date",5,1) == "/")
tran_date_3_fix = tran_date_3.select(
    "transaction_id",
    "customer_id",
    F.to_date("transaction_date",'yyyy/MM/dd').alias("transaction_date"),
    "amount",
    "transaction_type",
    "merchant_category",
    "ingested_at",
    "source_file"
)

# 12/07/1964
tran_date_4 = transactions_clean_str.select("*").where(F.substring("transaction_date",3,1) == "/")
tran_date_4_fix = tran_date_4.select(
    "transaction_id",
    "customer_id",
    F.to_date("transaction_date",'dd/MM/yyyy').alias("transaction_date"),
    "amount",
    "transaction_type",
    "merchant_category",
    "ingested_at",
    "source_file"
)

# 06 Oct 1993
tran_date_5 = transactions_clean_str.select("*").where(F.substring("transaction_date",3,1) == " ")
tran_date_5_fix = tran_date_5.select(
    "transaction_id",
    "customer_id",
    F.to_date("transaction_date",'dd MMM yyyy').alias("transaction_date"),
    "amount",
    "transaction_type",
    "merchant_category",
    "ingested_at",
    "source_file"   
)

# Combine together
transactions_clean = tran_date_1_fix.union(tran_date_2_fix).union(tran_date_3_fix).union(tran_date_4_fix).union(tran_date_5_fix)

# COMMAND ----------

# Transactions - remove any duplicates
transactions_nodups = transactions_clean.distinct()

# COMMAND ----------

# Write out customers table to silver and check if 0 obs
customers_nodups.write.mode("overwrite").saveAsTable(output_customers_table)

customers_check = spark.table(output_customers_table).select(F.count("customer_id")).collect()[0][0]
if customers_check == 0:
    raise ValueError("O observations in the silver_clean_customers table")

# COMMAND ----------

# Write out repayments table to silver and check if 0 obs
repayments_nodups.write.mode("overwrite").saveAsTable(output_repayments_table)

repayments_check = spark.table(output_repayments_table).select(F.count("repayment_id")).collect()[0][0]
if repayments_check == 0:
    raise ValueError("O observations in the silver_clean_repayments table")

# COMMAND ----------

# Write out transactions table to bronze and check if 0 obs
transactions_nodups.write.mode("overwrite").saveAsTable(output_transactions_table)

transactions_check = spark.table(output_transactions_table).select(F.count("transaction_id")).collect()[0][0]
if transactions_check == 0:
    raise ValueError("O observations in the silver_clean_transations table")