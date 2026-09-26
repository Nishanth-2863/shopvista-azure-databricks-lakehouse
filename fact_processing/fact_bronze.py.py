# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name","ecomm_db","catalog")
dbutils.widgets.text("storage_account_name","shopvistastorage","storage")
dbutils.widgets.text("container","raw","container")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalog = dbutils.widgets.get("catalog_name")
storage_account= dbutils.widgets.get("storage_account_name")
print(catalog,storage_account,container)

# COMMAND ----------

fact_files_path=f'abfss://{container}@{storage_account}.dfs.core.windows.net/files/order_items/landing/'
checkpoint_path=f'abfss://{container}@{storage_account}.dfs.core.windows.net/checkpoint/bronze/fact_order_items/'

# COMMAND ----------

df = (spark.readStream.format('cloudFiles')
      .option('cloudFiles.format','csv')
      .option('header',True)
      .option('cloudFiles.inferColumnTypes','true')
      .option('cloudFiles.schemaLocation',checkpoint_path)
      .option('cloudFiles.schemaEvolutionMode','rescue')
      .option('cloudFiles.rescuedDataColumn','rescue_data')
      .option('cloudFiles.includeExistingFiles','true')
      .option('pathGlobFilter','*.csv')
      .load(fact_files_path)
      .withColumn("file_name",F.col("_metadata.file_path"))
      .withColumn("ingested_at",F.current_timestamp())
      )


# COMMAND ----------

df.writeStream\
    .outputMode('append')\
    .option('checkpointLocation',checkpoint_path)\
    .trigger(availableNow=True)\
    .toTable(f'{catalog}.bronze.fact_order_items')\
    .awaitTermination()

# COMMAND ----------

df=spark.table(f"{catalog}.bronze.fact_order_items")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Processed files count

# COMMAND ----------

display(spark.sql(
    f"SELECT count(*) FROM CLOUD_FILES_STATE('{checkpoint_path}')"
))