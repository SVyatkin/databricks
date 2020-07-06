# Databricks notebook source
# MAGIC %run Shared/PythonUtils

# COMMAND ----------

# 
# script to make list all files with "csv.gz" extentions in s3 bucket 
# 

list = dbutils.fs.ls("s3://tunein-liveramp")
files = []

for line in list: 
  lst = dbutils.fs.ls(line.path)
  for ln in lst:
    if (ln.path.find ("csv.gz") > -1):
      files.append(ln)  
      

display(files)    

# COMMAND ----------

list = dbutils.fs.ls("s3://tunein-liveramp")

files =[]

for line in list:
  print(line.path)  
  dirs.append(line.path)
  lst = dbutils.fs.ls(line.path)
  for ln in lst:
    if ln.path.find ("csv.gz"):
      print(ln.path)
  
# display(files)

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

