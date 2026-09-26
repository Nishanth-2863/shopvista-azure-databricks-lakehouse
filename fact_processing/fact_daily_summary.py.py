# Databricks notebook source
from pyspark.sql import functions as F
from delta import DeltaTable

# COMMAND ----------

catalog='ecomm_db'

# COMMAND ----------

source_table = 'gld_fact_order_items'
sink_table = 'daily_summary_table'
days_cutoff=30

# COMMAND ----------

max_date=spark.sql(f"""
                   select max(transaction_date) as max_transaction_date from {catalog}.gold.{source_table}
                   """).collect()[0]['max_transaction_date']

print(max_date)

# COMMAND ----------

if spark.catalog.tableExists(f'{catalog}.gold.{sink_table}'):
    where_clause = f"transaction_date >= date_sub(date('{max_date}'), {days_cutoff})"

else:
    where_clause ='1=1'

# COMMAND ----------

summary_query=f"""
select 
date_id,
unit_price_currency as currency,
sum(quantity) as total_quantity,
sum(gross_amount) as total_gross_amount,
round(sum(total_discount_amount),2) as total_discount_amount,
sum(tax_amount) as total_tax_amount,
round(sum(net_amount),2) as total_net_amount
from {catalog}.gold.{source_table}
where {where_clause}
group by 
1,2
order by date_id,currency

"""

summary_query_sql=spark.sql(summary_query)

# COMMAND ----------

summary_query_sql.display()

# COMMAND ----------

# MAGIC %md
# MAGIC write delta table

# COMMAND ----------

if not spark.catalog.tableExists(f'{catalog}.gold.{sink_table}'):
    print(f'creating a delta table')
    summary_query_sql.write.mode('overwrite').saveAsTable(f'{catalog}.gold.{sink_table}')
else:
    delta_tbl=DeltaTable.forName(spark,f'{catalog}.gold.{sink_table}')
    delta_tbl.alias('target').merge(
        source=summary_query_sql.alias('source'),
        condition='target.date_id=source.date_id and target.currency=source.currency'
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

