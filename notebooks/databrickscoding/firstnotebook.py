# Databricks notebook source
# MAGIC %md #This is my first notebook

# COMMAND ----------

sum = 1+2
print(sum)

# COMMAND ----------

# MAGIC %sh ls -al

# COMMAND ----------

# MAGIC %sh mkdir logs/posarao

# COMMAND ----------

# MAGIC %sh ls -al logs/

# COMMAND ----------

sc

# COMMAND ----------

display(dbutils.fs.mounts())