# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

containername ="costomcontainer"
storagename = "azuredatabrickstorg0909"
mountpoint = "/mnt/custommount"
sas = "N+kVW/MHbP87Vru9A1VBN+MgfItMnPN6ypuXYWDw85bweSsZPQS5NcNN/cvNLwWbMaSsDzhoLUao+AStJgcQRw=="
try:
    
    dbutils.fs.mount( source = "wasbs://" + containername + "@" + storagename + ".blob.core.windows.net",
                 mount_point = mountpoint, 
                 extra_configs = {"fs.azure.account.key." + storagename + '.blob.core.windows.net': sas}
)

except Exception as e:
    print("already mounted, please unmount using dbutilis.fs.unmount")

# COMMAND ----------

if any

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls('/mnt/custommount')

# COMMAND ----------

dbutils.fs.unmount("/mnt/custommount")

# COMMAND ----------

# MAGIC %fs head /mnt/custommount/circuits.csv

# COMMAND ----------

# MAGIC %scala 
# MAGIC val mydataframe = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/custommount/circuits.csv")
# MAGIC dispaly(mydataframe)

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

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types.{StructType,StructField, IntegerType,StringType }
# MAGIC 
# MAGIC var fireschema = StructType (Array(StructField("circuitId", IntegerType, true),
# MAGIC                                  StructField("circuitRef", StringType, true),
# MAGIC                                   StructField("name", StringType, true),
# MAGIC                                   StructField("location", StringType, true),
# MAGIC                                   StructField("country", StringType, true),
# MAGIC                                   StructField("lat", StringType, true),
# MAGIC                                   StructField("lng", StringType, true),
# MAGIC                                   StructField("alt", IntegerType, true),
# MAGIC                                   StructField("url", StringType, true)))
# MAGIC 
# MAGIC val FireFile= "dbfs:/mnt/custommount/circuits.csv"
# MAGIC val fireDF = spark.read.schema(fireschema).option("header","true").csv(FireFile)
# MAGIC display(fireDF)