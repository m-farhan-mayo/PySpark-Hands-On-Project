# Databricks notebook source
fire_df = spark.read \
    .format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")    

# COMMAND ----------

fire_df.show(10)

# COMMAND ----------

display(fire_df)

# COMMAND ----------

fire_df.createGlobalTempView("fire_service_calls_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.fire_service_calls_view