# Databricks notebook source
# MAGIC %run Shared/PythonUtils

# COMMAND ----------

# MAGIC %md ##RedShift Examples how to use different modes for PythonUtils
# MAGIC 
# MAGIC This notebook walks through the process of:
# MAGIC 
# MAGIC 1. How to create/update/append a redshift table 
# MAGIC 2. How to use different mode: 
# MAGIC 
# MAGIC       a. default mode "error" create new table.  If the table exists it throws an exception. 
# MAGIC       
# MAGIC       b. mode "overwrite" drops a table and creates new table from dataframe data.
# MAGIC       
# MAGIC       c. mode "append" appends rows from dataframe to the table. Side effect duplications rows.
# MAGIC       
# MAGIC 3. Work around how to upsert dataframe to a redshift table

# COMMAND ----------

# MAGIC %md ##"ERROR" mode
# MAGIC 
# MAGIC Initial mode to create new table from dataframe

# COMMAND ----------

from pyspark.sql import Row

#  create data frame 

mesage01 = Row(id=1, description='01 description')
mesage02 = Row(id=2, description='02 description')
mesage03 = Row(id=3, description='03 description')

list = [mesage01, mesage02, mesage03]

df = spark.createDataFrame(list)

display(df)

# COMMAND ----------

# create a table test_upsert
#                                    MODE  E R R O R
#  mode = "error" is default
#  if table is already exist - we have an error: Table <table name> already exists! (SaveMode is set to ErrorIfExists)
#  and we need to use OVERWRITE mode

table_name = "public.test_upsert"

write_to_redshift(df, table_name)


# COMMAND ----------

# read table 
query_df = read_from_redshift(query="SELECT * FROM public.test_upsert")
display(query_df)

# COMMAND ----------

# MAGIC %md ##"OVERWRITE" mode
# MAGIC 
# MAGIC A mode to overwrite redshift table with new data from dataframe

# COMMAND ----------

# create a table test_upsert
#                                    MODE  O V E R W R I T E

table_name = "public.test_upsert"
mode = "overwrite"

write_to_redshift(df, table_name, mode)


# COMMAND ----------

# read table 
query_df = read_from_redshift(query="SELECT * FROM public.test_upsert")
display(query_df)

# COMMAND ----------

# MAGIC %md ##"APPEND" mode
# MAGIC 
# MAGIC A mode to append dataframe to redshift table

# COMMAND ----------

#                                    MODE  A P P E N D

# create new data frame to append into redshift table 

mesage01 = Row(id=4, description='04 description')
mesage02 = Row(id=5, description='05 description')
mesage03 = Row(id=6, description='06 description')

list = [mesage01, mesage02, mesage03]

df = spark.createDataFrame(list)

table_name = "public.test_upsert"
mode = "append"

write_to_redshift(df, table_name, mode)


# COMMAND ----------

# read table 
query_df = read_from_redshift(query="SELECT * FROM public.test_upsert")
display(query_df)

# COMMAND ----------

# MAGIC %md ##Upsert work around
# MAGIC 
# MAGIC Create updated dataframe and overwrite a redshift table

# COMMAND ----------

# Ubion 2 dataframes before update

mesage01 = Row(id=4, description='04 update')
mesage02 = Row(id=5, description='05 update')
mesage03 = Row(id=6, description='06 update')

list = [mesage01, mesage02, mesage03]

df = spark.createDataFrame(list)

table_name = "public.test_upsert"
mode = "append"

write_to_redshift(df, table_name, mode)

# COMMAND ----------

# read table 
df1 = read_from_redshift(query="SELECT * FROM public.test_upsert")
display(df1)

# COMMAND ----------

# MAGIC %md ##Union two data frames and delete duplications

# COMMAND ----------

from pyspark.sql import Row

# create new dataframe with dups ids from frame 1  

mesage01 = Row(id=4, description='04 merge')
mesage02 = Row(id=5, description='05 merge')
mesage03 = Row(id=6, description='06 merge')
mesage04 = Row(id=7, description='07 merge')

list = [mesage01, mesage02, mesage03, mesage04]

df2 = spark.createDataFrame(list).sort("id")

display(df2)

# COMMAND ----------

# MAGIC %md ###Union data frames and delete duplications

# COMMAND ----------


df = df2.union(df1).dropDuplicates(["id"]).sort("id")
display(df)

# COMMAND ----------

df = df1.union(df2).dropDuplicates(["id"]).sort("id")
display(df)

# COMMAND ----------

