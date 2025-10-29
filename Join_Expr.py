# Databricks notebook source


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]")\
        .appName("HelloSparkSQL")\
        .getOrCreate()

    logger = Log4J(spark)

    orders_list = [("01", "02", 350, 1),
                   ("01", "03", 150, 2),
                   ("01", "04", 190, 3),
                   ("01", "05", 260, 1)]

    orders_list = spark.createDataFrame(orders_list).toDF("Order_id", "Prod_id", "Unit_Price", "Qty")

    product_list = [("01", "Power Bank", 350, 10),
                    ("01", "Phone Case", 150, 20),
                    ("01", "Bed Sheet", 190, 30),
                    ("01", "Keyboard", 260, 10)]

    product_list = spark.createDataFrame(product_list).toDF("Product_ID", "Product_Name", "List_Price", "Qty")

    product_list.show()
    orders_list.show()