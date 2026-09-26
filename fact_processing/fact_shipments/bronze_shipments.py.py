# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog","ecomm_db","catalog")
dbutils.widgets.text("storage","shopvistastorage","storage")
dbutils.widgets.text("container","raw","container")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog")
storage_name = dbutils.widgets.get("storage")
container_name = dbutils.widgets.get("container")

# COMMAND ----------

# MAGIC %md
# MAGIC Path

# COMMAND ----------

shipments_file_path = f'abfss://{container_name}@{storage_name}.dfs.core.windows.net/files/order_shipments/'
bronze_checkpoint = f'abfss://{container_name}@{storage_name}.dfs.core.windows.net/checkpoint/bronze/fact_shipments/'

# COMMAND ----------

# MAGIC %md
# MAGIC Read Files

# COMMAND ----------

df = spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','csv')\
    .option('header',True)\
     .option('cloudFiles.schemaLocation',bronze_checkpoint)\
    .option('cloudFiles.inferColumnTypes',True)\
    .option('cloudFiles.schemaEvolutionMode','rescue')\
    .option('rescuedDataColumn','_rescue_data')\
    .option('cloudFiles.includeExistingFiles',True)\
    .option('globPathFilter','*.csv')\
    .load(shipments_file_path)\
    .withColumn('file_path',F.col('_metadata.file_path'))\
    .withColumn('bronze_ingested_at',F.current_timestamp())

# COMMAND ----------

df.writeStream\
            .format('delta')\
            .option('checkpointLocation',bronze_checkpoint)\
            .trigger(availableNow=True)\
            .toTable(f'{catalog_name}.bronze.fact_order_shipments')

                

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select * from ecomm_db.bronze.fact_order_shipments