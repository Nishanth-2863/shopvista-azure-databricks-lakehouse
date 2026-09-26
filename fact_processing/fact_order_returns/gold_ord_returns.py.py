# Databricks notebook source
# MAGIC %md
# MAGIC GOLD 

# COMMAND ----------

from pyspark.sql import functions as F
from delta import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog","ecomm_db","catalog")
dbutils.widgets.text("schema","silver","schema")
dbutils.widgets.text("container","raw","container")
dbutils.widgets.text("storage","shopvistastorage","storage")

# COMMAND ----------

catalog_name=dbutils.widgets.get("catalog")
schema_name=dbutils.widgets.get("schema")
container_name = dbutils.widgets.get("container")
storage_name = dbutils.widgets.get("storage")

# COMMAND ----------

df_silver = spark.readStream\
            .format('delta')\
            .option('readChangeFeed',True)\
            .option('startingVersion',0)\
            .table(f"{catalog_name}.{schema_name}.slv_fact_order_returns")

# COMMAND ----------

df_gld_ord_returns=(df_silver
                    .filter(F.col("_change_type").isin('insert','update_postimage'))
                    .withColumn('date_id',F.date_format(F.col("order_dt"),'yyyyMMdd'))
                       .withColumn('return_days',F.datediff(F.col('return_ts').cast('date'),F.col("order_dt")))
                       .withColumn('is_within_policy',F.when(F.col("return_days")<=15,1).otherwise(F.lit(0)))
                       .withColumn('is_late_return',F.when(F.col("return_days")>15,1).otherwise(F.lit(0)))


)

# COMMAND ----------

# MAGIC %md
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC Testing with spark read 

# COMMAND ----------

# silver_df_ord_returns=spark.read.format('delta')\
# .option("readChangeFeed", "true")\
# .option('startingversion',0)\
# .table(f'{catalog_name}.{schema_name}.slv_fact_order_returns')



# COMMAND ----------

# silver_df_ord_returns=silver_df_ord_returns.filter(F.col("_change_type").isin('insert','update_postimage'))

# s=(silver_df_ord_returns
#                        .withColumn('date_id',F.date_format(F.col("order_dt"),'yyyyMMdd'))
#                        .withColumn('return_days',F.datediff(F.col('return_ts').cast('date'),F.col("order_dt")))
#                        .withColumn('is_within_policy',F.when(F.col("return_days")<=15,1).otherwise(F.lit(0)))
#                        .withColumn('is_late_return',F.when(F.col("return_days")>15,1).otherwise(F.lit(0)))


# )

# COMMAND ----------

# MAGIC %md
# MAGIC write

# COMMAND ----------

#gold_checkpoint 
gld_checkpoint_path = f'abfss://{container_name}@{storage_name}.dfs.core.windows.net/checkpoint/gold/fact_order_returns'

# COMMAND ----------

print(gld_checkpoint_path)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

def gld_upsert(microBatchDf,batch_id):
    table_name=f'{catalog_name}.gold.gld_fact_order_returns'
    window_spec=Window.partitionBy('order_id','return_ts').orderBy(F.col('_commit_timestamp').desc())
    microBatch_dedup = (microBatchDf
                        .withColumn('rn',F.row_number().over(window_spec))
                        .filter(F.col('rn')==1)
                        .drop("rn","_change_type", "_commit_version", "_commit_timestamp")
                        )
    if not spark.catalog.tableExists(table_name):
        print(f'creating table ')
        microBatch_dedup.write.format('delta').option('delta.enableChangeDataFeed',True).mode('overwrite').saveAsTable(table_name)

    else:
        delta_tbl = DeltaTable.forName(spark,f"{catalog_name}.gold.gld_fact_order_returns")
        delta_tbl.alias('target').merge(
        source=microBatch_dedup.alias('source'),
        condition='target.order_id=source.order_id and target.return_ts=source.return_ts'
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()



# COMMAND ----------

(df_gld_ord_returns
    .writeStream
    .foreachBatch(gld_upsert)
    .option('checkpointLocation', gld_checkpoint_path)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecomm_db.gold.gld_fact_order_returns

# COMMAND ----------

# df=spark.read.format('delta')\
#     .option('readChangeFeed',True)\
#     .option('startingVersion',0)\
#     .table(f'{catalog_name}.{schema_name}.slv_fact_order_returns')\
    

# COMMAND ----------

# window_spec=Window.partitionBy('order_id','return_ts').orderBy(F.col('_commit_timestamp').desc())

# sample_df = (df 
#              .withColumn('ran',F.row_number().over(window_spec))
#              .filter(F.col('ran')==1)
#              )

# COMMAND ----------

# sample_df.display()

# COMMAND ----------

# dbutils.fs.rm(gld_checkpoint_path,True)

# COMMAND ----------

# %sql
# drop table if exists ecomm_db.gold.gld_fact_order_returns