# Databricks notebook source
from pyspark.sql import functions as F
from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.text("catalog","ecomm_db","catalog")
dbutils.widgets.text('schema','bronze','schema')
dbutils.widgets.text('container','raw','container')
dbutils.widgets.text('storage','shopvistastorage','storage_name')


# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get('schema')
container_name=dbutils.widgets.get("container")
storage_name = dbutils.widgets.get('storage')

# COMMAND ----------

bronze_checkpoint = f'abfss://{container_name}@{storage_name}.dfs.core.widnows.net/checkpoint/bronze/fact_shipments'

# COMMAND ----------

bronze_shipments = spark.readStream.table(f"{catalog_name}.{schema_name}.fact_order_shipments")

# COMMAND ----------

silver_cleaned_ord_shipments = (bronze_shipments
                           .withColumn('order_dt',F.to_date(F.col("order_dt")))
                            .withColumn('carrier',F.upper(F.trim(F.col("carrier"))))
                            .withColumn('silver_processed_at',F.current_timestamp())
                                
)

# COMMAND ----------

# df=spark.read.table(f'{catalog_name}.{schema_name}.fact_order_shipments')
# df=(df
#     .withColumn('order_dt',F.to_date(F.col("order_dt")))
#     .withColumn('carrier',F.upper(F.trim(F.col("carrier"))))
#     .withColumn('silver_processed_at',F.current_timestamp())
# )

# COMMAND ----------

# MAGIC %md
# MAGIC write 

# COMMAND ----------

# silver checkpoint 

silver_checkpoint_path = f'abfss://{container_name}@{storage_name}.dfs.core.windows.net/checkpoint/silver/fact_shipments'
print(silver_checkpoint_path)

# COMMAND ----------

def silver_upsert(microBatchDf,batch_id):
    table_name = f'{catalog_name}.silver.slv_fact_order_shipments'
    if not spark.catalog.tableExists(table_name):
        print(f'Creating table')
        microBatchDf.write.format('delta').option('delta.enableChangeDataFeed',True).mode('overwrite').saveAsTable(table_name)
    else:
        delta_tbl = DeltaTable.forName(spark,table_name)
        delta_tbl.alias('target').merge(
            source=microBatchDf.alias('source'),
            condition='target.shipment_id=source.shipment_id and target.order_id=source.order_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

silver_cleaned_ord_shipments.writeStream\
        .format('delta')\
        .option('checkpointLocation',silver_checkpoint_path)\
        .foreachBatch(silver_upsert)\
        .trigger(availableNow=True)\
        .start()\
        .awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ecomm_db.silver.slv_fact_order_shipments