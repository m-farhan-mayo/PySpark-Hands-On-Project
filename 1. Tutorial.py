# Databricks notebook source
# MAGIC %md
# MAGIC ### Jason File

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Data

# COMMAND ----------

df_json = spark.read.format('json') \
    .option('inferSchema', True) \
    .option('header', True) \
    .option('multiLine', True) \
    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV File

# COMMAND ----------

df = spark.read.format('csv')\
    .option('inferSchema', True)\
    .option('header', True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_schema = '''
                    Item_Identifier STRING, 
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE, 
                    Item_Type STRING, 
                    Item_MRP DOUBLE, 
                    Outlet_Identifier STRING, 
                    Outlet_Establishment_Year INT, 
                    Outlet_Size STRING, 
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING, 
                    Item_Outlet_Sales DOUBLE
                    '''

# COMMAND ----------

df = spark.read.format('csv')\
    .schema(my_schema)\
    .option('header', True)\
    .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df.select('Item_Identifier', 'Item_Weight', 'Item_Fat_Content').display()

# COMMAND ----------

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filters

# COMMAND ----------

df.filter(col('Item_Fat_content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scnerio 2

# COMMAND ----------

df.filter((col('Item_Fat_Content')=='Regular') & (col('Item_Type')=='Frozen Foods') ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenrio 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & col('Outlet_Location_Type').isin('Tier 1','Tier 2')).display()

# COMMAND ----------

df.withColumnRenamed('Item_Fat_Content', 'Item_FC').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## With Column

# COMMAND ----------

df.withColumn('Flag', lit('New')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## -> 2 Transformation

# COMMAND ----------

df.withColumn('Multiply', col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Low Fat', 'LF')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Casting

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sorting ->1

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ->2

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ->3

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'],ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## -> 3

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'],ascending = [0,1]).display()

# COMMAND ----------

df.limit(15).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP

# COMMAND ----------

df.drop('Item_MRP', 'Outlet_Identifier').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Duplicate
# MAGIC

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type','Item_MRP']).display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

data1 = [(1, 'Farhab'),
         (2, 'MaYo')]

schema1 = 'Id STRING, name STRING'

df1 = spark.createDataFrame(data1,schema1)

data2 = [(3, 'Faraz'),
         (4, 'Anaya')]

schema2 = ' Id STRING , name STRING'

df2 = spark.createDataFrame(data2,schema2)



# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union By Name

# COMMAND ----------

df1.unionByName(df2).display

# COMMAND ----------

# MAGIC %md
# MAGIC # String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## InItCap()

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOWER

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPPER

# COMMAND ----------

df.select(upper('Item_Type').alias('Upper Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Date Function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Date

# COMMAND ----------

df = df.withColumn('Current Date', current_date())

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date_Add()

# COMMAND ----------

df = df.withColumn('Week After', date_add(current_date(),7))

display(df)
                   


# COMMAND ----------

# MAGIC %md
# MAGIC ## date_sub()

# COMMAND ----------

 df.withColumn('Week Before', date_sub(current_date(),7)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATEDIFF

# COMMAND ----------

df = df.withColumn('Datediff', datediff('Current Date', 'Week After'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Format

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
df = df.withColumn('Week After', date_format('Week After', ' dd-MM-YYYY')) # Keep your original pattern
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling NULLS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dropping NULLS

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filling NULLS

# COMMAND ----------

df.fillna('Not Available').display()

# COMMAND ----------

df.fillna('Not Available', subset=['outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Split & Indexing
# MAGIC

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type', ' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type', ' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Explode

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type', ' '))

# COMMAND ----------

df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Array Contains

# COMMAND ----------

df_exp.withColumn('Array Contain', array_contains('Outlet_type', 'Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # GroupBy

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP').alias('Total_Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP').alias('Avg_Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP').alias('Total_Items'),avg('Item_MRP').alias('Avg_ITems')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Collect List

# COMMAND ----------

data3 = (['user1', 'book1'],
         ['user1', 'book2'],
         ['user2', 'book2'],
         ['user2', 'book4'],
         ['user3', 'book1'],
         )

schema3 = 'user STRING, book STRING'

df_book = spark.createDataFrame(data3,schema3)

display(df_book)

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # When OtherWise

# COMMAND ----------

df5 = df.withColumn('OtherWise_When', when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Veg'))
df5.display()

# COMMAND ----------

df5.withColumn('flag_exp',
    when((col('OtherWise_When') == 'Veg') & (col('Item_MRP') < 100), 'Veg_Inexpensive')
    .when((col('OtherWise_When') == 'Veg') & (col('Item_MRP') > 100), 'Veg_Expensive')
    .otherwise('Non_Veg')
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # JOINS

# COMMAND ----------

dataj1 = [('1', 'farhab', 'd01'),
          ('2', 'faraz', 'd02'),
          ('3','faraj', 'd03'),
          ('4', 'anaya', 'd03'),
          ('5', 'bhalu', 'd05'),
          ('6', 'farhan', 'd06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame (dataj1, schemaj1) 

dataj2 = [('d01', 'HR'),
          ('d02', 'Marketing'),
          ('d03', 'Accounts'),
          ('d04', 'IT'),
          ('d05', 'Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame (dataj2, schemaj2) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## INNER JOIN
# MAGIC

# COMMAND ----------

df1.join(df2, df1['dept_id']== df2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OUTER JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id']== df2['dept_id'], 'outer').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## LEFT JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id']== df2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## RIGHT JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id']== df2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ANTI JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id']== df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Window Function

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## ROW NUMBER

# COMMAND ----------

df.withColumn('Col_ID', row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## RANK()

# COMMAND ----------

df.withColumn('Rank', rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DENSE RANK()

# COMMAND ----------

df.withColumn('Dense_Rank', dense_rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cumulative Sum

# COMMAND ----------

df.withColumn('Cum_Sum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('Cum_Sum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # User Define Functions

# COMMAND ----------

def my_fun(x):
    return x*x

# COMMAND ----------

my_udf = udf(my_fun)

# COMMAND ----------

df.withColumn('My Funct', my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format('csv')\
  .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mode Of Writing Data

# COMMAND ----------

df.write.format('csv')\
    .mode('append')\
    .option('path', 'FileStore/tables/CSV/data.csv')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet Format + Overwrite Mode

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path', 'FileStore/tables/CSV/data.parquet')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table
# MAGIC

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .saveAsTable('my_Table')

# COMMAND ----------

