# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text('catalog','ecomm_db','catalog_name')
dbutils.widgets.text('container','raw','container')
dbutils.widgets.text('storage_account_name','shopvistastorage','storage')

# COMMAND ----------

catalog=dbutils.widgets.get("catalog")
container=dbutils.widgets.get("container")
storage=dbutils.widgets.get("storage_account_name")

# COMMAND ----------

# MAGIC %md
# MAGIC file & checkpoint location

# COMMAND ----------

checkpoint_path = f'abfss://{container}@{storage}.dfs.core.windows.net/checkpoint/bronze/fact_order_items/'

# COMMAND ----------

# MAGIC %md
# MAGIC read file

# COMMAND ----------

df=spark.readStream.format('delta')\
    .table(f'{catalog}.bronze.fact_order_items')



# COMMAND ----------

#df.display()

# COMMAND ----------



# COMMAND ----------

# display(
#     df,
#     checkpointLocation=f"{checkpoint_path}/offset/1"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC Testing Transformation

# COMMAND ----------

# df = spark.read.table(f'{catalog}.bronze.fact_order_items')

# COMMAND ----------

# df.display()

# COMMAND ----------


# dup_df = (df
#           .dropDuplicates(subset=['order_id','item_seq'])
#           .withColumn('quantity',F.when(F.lower(F.col("quantity"))=='two',2).otherwise(F.col("quantity")).cast('integer'))
#           .withColumn('unit_price',F.regexp_replace(F.col("unit_price"),r'[^0-9]','').cast('double'))
#           .withColumn('discount_pct',F.regexp_replace(F.col("discount_pct"),r'[^0-9]','').cast('double'))
#           .withColumn('coupon_code',F.lower(F.col("coupon_code")))
#           .withColumn('channel',F.lower(F.when(F.col("channel")=='web','Website').when(F.col("channel")=='app','Mobile').otherwise(F.col('channel'))))
#           .withColumn("ingested_at",F.current_timestamp())
#           )

# dup_df.display()
          


# COMMAND ----------

df=(df.dropDuplicates(subset=['order_id','item_seq'])
          .withColumn('quantity',F.when(F.lower(F.col("quantity"))=='two',2).otherwise(F.col("quantity")).cast('integer'))
          .withColumn('unit_price',F.regexp_replace(F.col("unit_price"),r'[^0-9]','').cast('double'))
          .withColumn('discount_pct',F.regexp_replace(F.col("discount_pct"),r'[^0-9]','').cast('double'))
          .withColumn('coupon_code',F.lower(F.col("coupon_code")))
          .withColumn('channel',F.lower(F.when(F.col("channel")=='web','Website').when(F.col("channel")=='app','Mobile').otherwise(F.col('channel'))))
          .withColumn("ingested_at",F.current_timestamp()))


# COMMAND ----------

silver_checkpoint_path = f'abfss://{container}@{storage}.dfs.core.windows.net/checkpoint/silver/fact_order_items'

# COMMAND ----------

def upsert_silver(microBatchDf,batchId):
    table_name = f'{catalog}.silver.slv_fact_order_items'
    if not spark.catalog.tableExists(table_name):
        microBatchDf.write.format('delta').option('delta.enableChangeDataFeed',True).mode('overwrite').saveAsTable(table_name)
    else:
        delta_tbl=DeltaTable.forName(spark,table_name)
        delta_tbl.alias('target').merge(
            source=microBatchDf.alias('source'),
            condition='target.order_id=source.order_id and target.item_seq=source.item_seq'
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------



# COMMAND ----------

df.writeStream \
    .foreachBatch(upsert_silver) \
    .option('checkpointLocation', silver_checkpoint_path) \
    .trigger(availableNow=True) \
    .start() \
    .awaitTermination()


# COMMAND ----------



# COMMAND ----------

df=spark.table(f'{catalog}.silver.slv_fact_order_items')
df.display()