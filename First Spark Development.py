# Databricks notebook source
diamonds_df = spark.read.format("csv")\
    .option("Header", "True")\
    .option("inferSchema","True")\
    .load("/FileStore/data/asdf-1.csv")


diamonds_df.show(10)

# COMMAND ----------

from pyspark.sql.functions import avg  

result_df = diamonds_df.select("name", "Price") \
    .groupBy("name") \
    .agg(avg("Price").alias("Average_Price")) \
    .sort("Average_Price")  

result_df.show()  

# COMMAND ----------

display(result_df)

# COMMAND ----------

