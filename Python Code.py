# Databricks notebook source
from pyspark.sql import*
from pyspark.sql.functions import*
from pyspark.sql.types import*

def to_date_df(df, fmt, fld):
    return df.withcolumn(fld, to_date(col(fld), fmt))

# COMMAND ----------

my_schema = StructType([
StructField("ID", StringType()),
StructField("EventDate", StringType())])

my_rows= [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
my_rdd= spark.sparkContext.parallelize (my_rows, 2)
my_df= spark.createDataFrame (my_rdd, my_schema)



# COMMAND ----------

my_df.printSchema()  
my_df.show()  
  
new_df = my_df.withColumn("EventDate", to_date(col("EventDate"), "M/d/yyyy"))  
 
new_df.printSchema()  
new_df.show()  
