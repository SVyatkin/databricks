# Databricks notebook source
# MAGIC %run Shared/PythonUtils

# COMMAND ----------

# LiveRamp Upload Project
#
# upload data to s3 bucket s3:tunein-liveramp
# we split records for reasonable size due to limitaion spark and s3 bucket 
# count - how many records on paticular date
# lim - limit per one upload
# steps - how many times we need upload data per date
# list_dates - array of date and amount of records per date

list_dates = [ \
["2020-06-14",297322036], \
["2020-06-13",317377920], \
["2020-06-12",335230672], \
["2020-06-11",325642718], \
["2020-06-10",316770287], \
["2020-06-09",307529727], \
["2020-06-08",305451416], \
["2020-06-07",261712776] \
            ]

for dd in list_dates:
  print (dd[0] +":" + str(dd[1]) )
  activity_date = dd[0]
  count = dd[1]
  offset = 0
  lim = 60000000
  limit = " LIMIT " + str(lim)

  steps = int(count / lim) +1

  query = """
SELECT user_id,
       ip_address,
       REPLACE(user_agent,',',' ') as user_agent,
       unique_id,
       partner_id,
       LEFT(activity_date,10) AS date
FROM logs.raw_site_event
WHERE activity_date =  '""" + activity_date + """'
ORDER BY event_date 
OFFSET """

  for x in range(steps):
    print(activity_date, ": split-", str(x+1), " offset: ", str(offset))
    query_run = query + str(offset) + limit
#     print(query_run)
    query_df = read_from_redshift(query=query_run)
    try:
      query_df.write.csv("s3://tunein-liveramp/"+ activity_date, header=True, mode="append", compression="gzip")
    except:
      print("error")
    offset +=lim
    
print("complete: " + activity_date)


# COMMAND ----------

