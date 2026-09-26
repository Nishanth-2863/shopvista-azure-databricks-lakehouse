# Databricks notebook source
from pyspark.sql import functions as F
from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.text("catalog_name","ecomm_db","catalog")
dbutils.widgets.text("schema","bronze","schema")
dbutils.widgets.text("container","raw","container")
dbutils.widgets.text("storage","shopvistastorage","storage")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema")
container_name = dbutils.widgets.get("container")
storage_name = dbutils.widgets.get("storage")

# COMMAND ----------

bronze_schema_location= f'abfss://{container_name}@{storage_name}.dfs.windows.core.net/checkpoint/bronze/fact_order_returns/'

# COMMAND ----------

# read 
df = spark.readStream.table(f"{catalog_name}.{schema_name}.fact_order_returns")


# COMMAND ----------

# MAGIC %md
# MAGIC read table (for testing)

# COMMAND ----------

# df_b= spark.read.table(f'{catalog_name}.{schema_name}.fact_order_returns')

# cleaned_sil_df = (df_b
#                   .dropDuplicates(subset=['order_id','order_dt','return_ts'])
#                   .withColumn('order_dt',F.to_date(F.col('order_dt')))
#                   .withColumn('return_ts',F.to_timestamp(F.col("return_ts")))
#                   .withColumn("reason",F.upper(F.trim(F.col("reason"))))
#                   .withColumn('processed_time',F.current_timestamp())
#                   )

# COMMAND ----------

cleaned_df = (df
                  .dropDuplicates(subset=['order_id','order_dt','return_ts'])
                  .withColumn('order_dt',F.to_date(F.col('order_dt')))
                  .withColumn('return_ts',F.to_timestamp(F.col("return_ts")))
                  .withColumn("reason",F.upper(F.trim(F.col("reason"))))
                  .withColumn('processed_time',F.current_timestamp())
                  )

# COMMAND ----------

# MAGIC %md
# MAGIC Write 

# COMMAND ----------

# creating new checkpoint for silver

silver_checkpoint_path=f'abfss://{container_name}@{storage_name}.dfs.core.windows.net/checkpoint/silver/fact_order_returns'

# COMMAND ----------

def upsert(microBatchf,batch_id):
    table_name = f'{catalog_name}.silver.slv_fact_order_returns'
    if not spark.catalog.tableExists(table_name):
        print(f'creating new table')
        microBatchf.write.format('delta').mode('overwrite').option('delta.enableChangeDataFeed',True).saveAsTable(table_name)
    else:
        delta_tbl = DeltaTable.forName(spark,f'{catalog_name}.silver.slv_fact_order_returns')
        delta_tbl.alias('target').merge(
            source=microBatchf.alias('source'),
            condition='target.order_id=source.order_id and target.return_ts=source.return_ts'
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        

# COMMAND ----------

cleaned_df.writeStream\
    .format('delta')\
    .option('checkpointLocation',silver_checkpoint_path)\
    .foreachBatch(upsert)\
    .trigger(availableNow=True)\
    .start()\
    .awaitTermination()

# COMMAND ----------



# COMMAND ----------

df=spark.read.table(f'{catalog_name}.silver.slv_fact_order_returns')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC