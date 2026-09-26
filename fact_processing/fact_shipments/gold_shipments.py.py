# Databricks notebook source
from pyspark.sql import functions as F
from delta import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog","ecomm_db","catalog")
dbutils.widgets.text('schema','silver','schema')
dbutils.widgets.text('container','raw','container')
dbutils.widgets.text('storage','shopvistastorage','storage_name')


# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get('schema')
container_name=dbutils.widgets.get("container")
storage_name = dbutils.widgets.get('storage')

# COMMAND ----------

silver_checkpoint_path = f'abfss://{container_name}@{storage_name}.dfs.core.windows.net/checkpoint/silver/fact_shipments'

# COMMAND ----------

silver_shipments_df = spark.readStream\
                    .format('delta')\
                    .option('readChangeFeed',True)\
                    .option('startingVersion',0)\
                    .table(f'{catalog_name}.{schema_name}.slv_fact_order_shipments')

# COMMAND ----------

# MAGIC %md
# MAGIC Testing with read table 

# COMMAND ----------

# df=spark.read.table(f'{catalog_name}.{schema_name}.slv_fact_order_shipments')

# COMMAND ----------

# df_new = (df
#           .withColumn('carrier_group',F.when(F.col('carrier').isin('ECOMEXPRESS','DELHIVERY','XPRESSBEES','BLUEDART'),'Domestic').otherwise(F.lit('International')))
#           .withColumn('is_weekend',
#                       F.when(F.date_format(F.col('order_dt'),'E').isin('Sat','Sun'),1).otherwise(F.lit(0)))
          
#           )

# COMMAND ----------

# df_new.display()

# COMMAND ----------

gld_fact_order_shipments= (silver_shipments_df
            .filter(F.col('_change_type').isin('update_postimage','insert'))
            .withColumn('carrier_group',F.when(F.col('carrier').isin('ECOMEXPRESS','DELHIVERY','XPRESSBEES','BLUEDART'),'Domestic').otherwise(F.lit('International')))
          .withColumn('is_weekend',
                      F.when(F.date_format(F.col('order_dt'),'E').isin('Sat','Sun'),1).otherwise(F.lit(0)))
          .drop("_change_type")
                           )

# COMMAND ----------

# MAGIC %md
# MAGIC write 

# COMMAND ----------

gold_checkpoint_path = f'abfss://{container_name}@{storage_name}.dfs.core.windows.net/checkpoint/gold/fact_shipments'


# COMMAND ----------

def gold_upsert(microBatchDf,batch_id):
    window_spec= Window.partitionBy('order_id','shipment_id').orderBy(F.col("_commit_timestamp").desc())
    table_name = f'{catalog_name}.gold.gld_fact_order_shipments'
    microBatch_dedup = (microBatchDf
                        .withColumn('rn',F.row_number().over(window_spec))
                        .filter(F.col('rn')==1)
                        .drop('rn','_change_type'	,'_commit_version'	,'_commit_timestamp')

    )
    if not spark.catalog.tableExists(table_name):
        print(f'creating a table')
        microBatch_dedup.write.format('delta').option('delta.enableChangeDataFeed',True).mode('overwrite').saveAsTable(table_name)

    else:
        delta_tbl = DeltaTable.forName(spark,table_name)
        delta_tbl.alias('target').merge(
            source=microBatch_dedup.alias('source'),
            condition='target.order_id=source.order_id and target.shipment_id=source.shipment_id'
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        

# COMMAND ----------

gld_fact_order_shipments.writeStream\
                    .foreachBatch(gold_upsert)\
                    .option('checkpointLocation',gold_checkpoint_path)\
                    .trigger(availableNow=True)\
                    .start()\
                    .awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecomm_db.gold.gld_fact_order_shipments

# COMMAND ----------

# df = spark.read.format('delta')\
#     .option('readChangeFeed',True)\
#     .option('startingVersion',0)\
#     .table(f'{catalog_name}.gold.fact_order_shipments')

# COMMAND ----------

# df.display()

# COMMAND ----------

# dbutils.fs.rm(gold_checkpoint_path,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists ecomm_db.gold.fact_order_shipments