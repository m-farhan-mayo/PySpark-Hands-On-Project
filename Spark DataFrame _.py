# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------


raw_fire_df = spark.read \
    .format("csv")\
    .option("header","true") \
    .option("inferSchema", "true")\
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(raw_fire_df)

# COMMAND ----------

renamed_fire_df = raw_fire_df\
.withColumnRenamed ("Call Number", "CallNumber") \
.withColumnRenamed ("Unit ID", "UnitID")\
.withColumnRenamed ("Incident Number", "IncidentNumber")\
.withColumnRenamed ("Call Date", "CallDate")\
.withColumnRenamed ("Watch Date", "WatchDate") \
.withColumnRenamed ("Call Final Disposition", "CallFinalDisposition")\
.withColumnRenamed ("Available DtTm", "AvailableDtTm")\
.withColumnRenamed ("Zipcode of Incident", "Zipcode")\
.withColumnRenamed ("Station Area", "StationArea")\
.withColumnRenamed ("Final Priority", "FinalPriority")\
.withColumnRenamed ("ALS Unit", "ALSUnit")\
.withColumnRenamed ("Call Type Group", "CallTypeGroup")\
.withColumnRenamed ("Unit sequence in call dispatch", "UnitSequenceInCallDispatch")\
.withColumnRenamed ("Fire Prevention District", "FirePreventionDistrict")\
.withColumnRenamed ("Supervisor District", "SupervisorDistrict")

# COMMAND ----------

display(renamed_fire_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

renamed_fire_df.printSchema()

# COMMAND ----------

fire_df = renamed_fire_df \
    .withColumn("CallDate",to_date("CallDate","MM/dd/yyyy"))\
    .withColumn("WatchDate", to_date("WatchDate", "MM/dd/yyyy"))\
    .withColumn("AvailableDtTm", to_timestamp ("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))\
    .withColumn("Delay", round("Delay",2))

# COMMAND ----------

display(fire_df)

# COMMAND ----------

fire_df.printSchema()

# COMMAND ----------

fire_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #####
# MAGIC - Q1.How Many Distinct Type Of Calls Were Made To The Fire Department ?
# MAGIC
# MAGIC - Select count(distinct calltype) as Distinct_Calls
# MAGIC - from Spark.fire_service_calls
# MAGIC - Where calltype is not null

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

fire_df.createOrReplaceTempView("fire_service_calls_view")
ql_sql_df = spark.sql("""
    Select count(distinct calltype) as Distinct_Calls
    from fire_service_calls
    Where calltype is not null 
     """)
display(ql_sql_df)

# COMMAND ----------

sl_df = fire_df.where("CallType is not null")
sl_df2 = sl_df.select("CallType")
sl_df3 = sl_df2.distinct()

print(sl_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Q2. What Were Distinct Type Of Calls Made To The Fire Department?
# MAGIC select distinct calltype as Distinct_Type
# MAGIC
# MAGIC from Spark.fire_service_calls
# MAGIC
# MAGIC where CallType is not null

# COMMAND ----------

fire_df.createOrReplaceTempView("fire_service_calls_view")
al_sql_df = spark.sql("""
        select distinct calltype as Distinct_Type
        from fire_service_calls
        where CallType is not null
                      """)

display(al_sql_df)

# COMMAND ----------

ql_df = fire_df.where("CallType is not null")\
                .select("CallType")\
                .distinct()

print(ql_df.count())

# COMMAND ----------

ql_df = fire_df.where("CallType is not null")
ql_df2 = ql_df.select("CallType")
ql_df3 = ql_df2.distinct()

# display(ql_df3.count())
ql_df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Q3. Find Out All Response For delayed times greater than 5 mint?
# MAGIC SELECT CallNumber, Delay
# MAGIC
# MAGIC FROM Spark.fire_service_calls
# MAGIC
# MAGIC where Delay > 5

# COMMAND ----------

q5_df = fire_df.where("CallType is not null")\
    .select(expr("CallNumber as Distinct_CallNumber"))\
    .show()


# COMMAND ----------

fire_df.where("Delay > 5")\
    .select("CallNumber","Delay")\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Q4. What Were the Most Most Call Type?
# MAGIC Select callType, count(*) as Count
# MAGIC
# MAGIC FROM Spark.fire_service_calls
# MAGIC
# MAGIC Where CallType is not null 
# MAGIC
# MAGIC group by CallType
# MAGIC
# MAGIC ORDER BY Count desc

# COMMAND ----------

q5_df = fire_df.where("CallType is not null")\
    .select("CallType")\
    .groupby("CallType")\
    .count()\
    .orderBy("count", ascending=False)\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q5. What ZIP Code Accounted For Most Common Calls?
# MAGIC select callType, zipCode, count(*) as Count
# MAGIC
# MAGIC from spark.fire_service_calls
# MAGIC
# MAGIC where CallType is not null 
# MAGIC
# MAGIC group by CallType, Zipcode
# MAGIC
# MAGIC order by count desc
# MAGIC

# COMMAND ----------

q6_df = fire_df.where("CallType is not null")\
    .select("CallType","ZipCode")\
    .groupBy("CallType","ZipCode")\
    .count()\
    .orderBy("CallType", descending = True)\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q6. What San Francisco Neighborhoods Are In The ZIP codes 94102 and 94103?
# MAGIC select  ZipCode, Neighborhood
# MAGIC
# MAGIC from Spark.fire_service_calls
# MAGIC
# MAGIC where Zipcode == 94102 OR ZipCode == 94103

# COMMAND ----------

q7_df = fire_df.where((ZipCode == 94102) | (ZipCode == 94103)) \
    .select("ZipCode","Neighborhood")\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC What Was The Sum Of All Call Alarms, Average, Min And Max Of The Call Response Times?
# MAGIC
# MAGIC SELECT SUM(AvgNumAlarms + MinNumAlarms + MaxNumAlarms)  
# MAGIC FROM (  
# MAGIC     SELECT   
# MAGIC         AVG(NumAlarms) AS AvgNumAlarms,  
# MAGIC         MIN(NumAlarms) AS MinNumAlarms,  
# MAGIC         MAX(NumAlarms) AS MaxNumAlarms  
# MAGIC     FROM fire_service_calls  
# MAGIC     GROUP BY NumAlarms  
# MAGIC )   

# COMMAND ----------

q8_df = fire_df.where("CallType is not null")\
    .select(avg("NumAlarms"))\
    .groupBy("NumAlarms")\

display(q8_df)