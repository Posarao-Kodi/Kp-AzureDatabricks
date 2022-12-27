# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/circuits-1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
display(df)




# COMMAND ----------

display(df)

# COMMAND ----------

display(df)



# COMMAND ----------

dbutils.fs.ls('/mnt/custommount')

# COMMAND ----------

# MAGIC  %scala
# MAGIC val mydataframe = spark.read
# MAGIC .option("header", "true")
# MAGIC  .option("inferSchema", "true")
# MAGIC  .csv("dbfs:/mnt/custommount/circuits.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC val selectexpression = mydataframe.select("circuitId","circuitRef","name","country")
# MAGIC 
# MAGIC display(selectexpression)

# COMMAND ----------

