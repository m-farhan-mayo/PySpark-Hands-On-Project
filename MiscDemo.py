# Databricks notebook source
import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession  
from pyspark.sql.functions import *
from pyspark.sql.types import *

sparkSessions = SparkSession.builder \
    .appName("MiscDemo") \
    .getOrCreate() 


data_list = [("Ravi", "28","1","23"),
             ("Abdul", "22","5", "98"),
             ("Farhab", "2","1","12"),
             ("RaJ", "2","1","2002"),
             ("Chinku", "28","1","23")
             ]

raw_df = spark.createDataFrame(data_list).toDF("Name","Day","Month","Year").repartition(3)
raw_df.printSchema()

# COMMAND ----------

df1 = raw_df.withColumn("ID", monotonically_increasing_id())
df1.show

# COMMAND ----------

df2 = df1.withColumn("Year", expr("""
        case when year <21 then year + 2000
        when year < 100 then year + 1900
        else year
        end"""  ))

df2.show()

# COMMAND ----------

df3 = df1.withColumn("Year", expr("""
        case when year <21 then cast(year as int) + 2000
        when year < 100 then cast(year as int) + 1900
        else year
        end"""  ))

df3.show()

# COMMAND ----------

df4 = df1.withColumn("Year", expr("""
        case when year <21 then year + 2000
        when year < 100 then year + 1900
        else year
        end""").cast(IntegerType()))
df4.show()
df4.printSchema()

# COMMAND ----------

df5 =  df1.withColumn("Day", col("Day").cast(IntegerType())) \
          .withColumn("Month", col("Month").cast(IntegerType())) \
          .withColumn("Year", col("Year").cast(IntegerType())) 

df6 = df5.withColumn("year", expr("""
        case when year <21 then year + 2000
        when year < 100 then year + 1900
        else year
        end"""))

df6.show()

# COMMAND ----------

df7 = df5.withColumn("Year", \
    when(col("Year") < 25, col("Year") + 2000)\
    .when(col("Year") < 100, col("Year") + 1900)\
    .otherwise(col("Year")))

df7.show()

# COMMAND ----------

df8 = df7.withColumn("dob", expr("to_date(concat(Day,'/',Month,'/',Year), 'd/M/y')"))
df8.show()

# COMMAND ----------

df9 = df7.withColumn("DOB", expr("to_date(concat(Day,'/',Month,'/',Year), 'd/M/y')")) \
    .drop("Day","Month", "Year") \
    .dropDuplicates(["Name", "DOB"]) \
    .sort(expr("DOB desc"))

df9.show()

# COMMAND ----------

