# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC val df1 = spark.read.format("csv")
# MAGIC .option("header", "true")
# MAGIC .load("dbfs:/FileStore/shared_uploads/posarao.kodi2@gds.ey.com/Log.csv")
# MAGIC val df2=df1.select("Operation name","Status")
# MAGIC display(df2)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(df1.filter(df1("Status")==="Succeeded"))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val df3 = spark.read.format("csv")
# MAGIC .option("header", "true")
# MAGIC .load("dbfs:/FileStore/shared_uploads/posarao.kodi2@gds.ey.com/Log.csv")
# MAGIC 
# MAGIC display(df3.groupBy(df3("Status")).count())

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(df3.groupBy(df3("Status")).count())

# COMMAND ----------

# MAGIC %scala