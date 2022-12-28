# Databricks notebook source
# MAGIC %scala
# MAGIC val mydataframe = spark.read
# MAGIC .option("header", "true")
# MAGIC .option("inferSchema", "true")
# MAGIC .csv("dbfs:/mnt/databricksjobsdata/*csv")
# MAGIC 
# MAGIC val selectexprssion = mydataframe.select("name", "location","country")
# MAGIC 
# MAGIC val renameddata = selectexprssion.withColumnRenamed("name", "Destination_Country")
# MAGIC 
# MAGIC display(renameddata)
# MAGIC 
# MAGIC renameddata.createOrReplaceTempView("usertraveldata")
# MAGIC 
# MAGIC val aggregatedata = spark.sql(""" SELECT location , Destination_Country , country  FROM usertraveldata
# MAGIC  """)
# MAGIC 
# MAGIC aggregatedata.write.option("header", "true")
# MAGIC .format("com.databricks.spark.csv")
# MAGIC .save("/mnt/databricksjobsdata/traveloutputJob.csv")