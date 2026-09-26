# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name","ecomm_db","catalog")

dbutils.widgets.text("storage_account","shopvistastorage","storage_account")
dbutils.widgets.text("container","raw","container")

# COMMAND ----------

catalog=dbutils.widgets.get("catalog_name")
container=dbutils.widgets.get("container")
storage=dbutils.widgets.get("storage_account")

# COMMAND ----------

files_path = f'abfss://{container}@{storage}.dfs.core.windows.net/files/order_returns/'
bronze_check_point_path=f'abfss://{container}@{storage}.dfs.core.windows.net/checkpoint/bronze/fact_order_returns/'

# COMMAND ----------

df=spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','csv')\
    .option('header',True)\
    .option('cloudFiles.inferColumnTypes',True)\
    .option('cloudFiles.schemaLocation',bronze_check_point_path)\
    .option('cloudFiles.schemaEvolutionMode','rescue')\
    .option('cloudFiles.rescuedDataColumn','_rescue_data')\
    .option('cloudFiles.includeExistingFiles',True)\
    .option('pathGlobFilter','*.csv')\
    .load(files_path)\
    .withColumn('file_path',F.col('_metadata.file_path'))\
    .withColumn("ingested_at",F.current_timestamp())

# COMMAND ----------

df.writeStream.outputMode('append')\
            .option('checkpointLocation',bronze_check_point_path)\
            .trigger(availableNow=True)\
            .toTable(f'{catalog}.bronze.fact_order_returns')
            

# COMMAND ----------

df= spark.read.format('delta').table(f'{catalog}.bronze.fact_order_returns')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

