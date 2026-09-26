# Databricks notebook source
from pyspark.sql import functions as F
from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.text('catalog','ecomm_db','catalog')
dbutils.widgets.text("container","raw",'container')
dbutils.widgets.text('storage','shopvistastorage','storage')

# COMMAND ----------

catalog=dbutils.widgets.get('catalog')
container=dbutils.widgets.get('container')
storage=dbutils.widgets.get('storage')

# COMMAND ----------

silver_check_point=f'abfss://{container}@{storage}.dfs.core.windows.net/checkpoint/silver/fact_order_items/'

# COMMAND ----------

df = spark.readStream.format('delta')\
        .option('readChangeFeed',True)\
        .table(f'{catalog}.silver.slv_fact_order_items')

# COMMAND ----------

gold_fact_order_items = df.filter(
    F.col("_change_type").isin("update_postimage", "insert")
)

# COMMAND ----------

gold_fact_order_items = (gold_fact_order_items
            .withColumn('gross_amount',F.col('quantity')*F.col('unit_price'))
      .withColumn('total_discount_amount',F.round(F.col('gross_amount')*(F.col('discount_pct')/100),2))
      .withColumn('sale_amount',F.col('gross_amount')-F.col('total_discount_amount')+F.col('tax_amount'))
      .withColumn('coupon_flag',F.when(F.col('coupon_code').isNotNull(),1).otherwise(F.lit(0)))
      .withColumn('date_id',F.date_format(F.col('dt'),'yyyyMMdd'))

)

# COMMAND ----------

gold_fact_order_items = gold_fact_order_items.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("total_discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount")
)

# COMMAND ----------

# MAGIC %md
# MAGIC create columns for gold table using read.table (for testing)

# COMMAND ----------

# df = (
#     spark.read
#     .format("delta")
#     .option("readChangeFeed", "true")
#     .option('startingversion',0)
#     .table(f"{catalog}.silver.slv_fact_order_items")
# )


# df = (df
#       .withColumn('gross_amount',F.col('quantity')*F.col('unit_price'))
#       .withColumn('total_discount_amount',F.round(F.col('gross_amount')*(F.col('discount_pct')/100),2))
#       .withColumn('sale_amount',F.col('gross_amount')-F.col('total_discount_amount')+F.col('tax_amount'))
#       .withColumn('coupon_flag',F.when(F.col('coupon_code').isNotNull(),1).otherwise(F.lit(0)))
#       .withColumn('date_id',F.date_format(F.col('dt'),'yyyyMMdd'))
      
# )

 

# COMMAND ----------

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write to Gold 

# COMMAND ----------

gold_checkpoint_path=f'abfss://{container}@{storage}.dfs.core.windows.net/checkpoint/gold/fact_order_items'

# COMMAND ----------

def upsert_gold(microBatchDf,batch_id):
    table_name = f'{catalog}.gold.gld_fact_order_items'
    if not spark.catalog.tableExists(table_name):
        print('creating new table')
        microBatchDf.write.format('delta').option('delta.enablechangeDataFeed','true').mode('overwrite').saveAsTable(table_name)
    else:
        delta_tbl = DeltaTable.forName(spark,table_name)
        delta_tbl.alias('target').merge(
            source=microBatchDf.alias('source'),
            condition='target.transaction_id=source.transaction_id and target.seq_no=source.seq_no '
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll()



# COMMAND ----------

gold_fact_order_items.writeStream\
    .foreachBatch(upsert_gold)\
    .option('checkpointLocation',gold_checkpoint_path)\
    .trigger(availableNow=True)\
    .start()\
    .awaitTermination()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from ecomm_db.gold.gld_fact_order_items