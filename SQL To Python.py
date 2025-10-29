# Databricks notebook source
fire_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ######
# MAGIC - Q1.How Many Distinct Type Of Calls Were Made To The Fire Department ?
# MAGIC
# MAGIC - Select count(distinct calltype) as Distinct_Calls
# MAGIC - from Spark.fire_service_calls
# MAGIC - Where calltype is not null

# COMMAND ----------

fire_df.createOrReplaceTempView(fire_service_calls_view)
ql_sql_df = spark.sql ("""
    Select count(distinct calltype) as Distinct_Calls
    from Spark.fire_service_calls
    Where calltype is not null  """)
display(ql_sql_df)