# Databricks notebook source
df = (spark.read
      .format("csv")
      .option("Header","True")
      .option("inferSchema", "True")
      .load("/FileStore/data/Sample.csv") )

display(df)