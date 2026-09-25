# ShopVista: End-to-End E-Commerce Lakehouse on Azure Databricks

![Azure](https://img.shields.io/badge/Azure-0078D4?logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Azure%20Databricks-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?logo=deltalake&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)

## Problem Statement

- E-commerce operational data was fragmented across orders, shipments, returns, and master dimension files.
- Teams relied on spreadsheets and ad-hoc scripts to reconcile order, shipment, and return records.
- Revenue, return-rate, and logistics SLA reporting required multi-hour manual processing cycles.
- There was no centralized source of truth for supply-chain tracking and post-purchase customer returns.
- Historical and incremental data processing required a scalable architecture capable of handling both batch backfills and streaming updates.
- ShopVista addresses these limitations through an automated Azure Lakehouse using **Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, and Power BI**.
- The platform implements a **Bronze → Silver → Gold Medallion Architecture** with incremental processing, Delta Change Data Feed, SCD Type 1 merges, and governed storage access.

---

## End-to-End Architecture & Medallion Data Flow

```text
                           ┌──────────────────────────────┐
                           │       Source Data            │
                           │                              │
                           │ Orders                       │
                           │ Shipments                    │
                           │ Returns                      │
                           │ Products / Customers / etc.  │
                           └──────────────┬───────────────┘
                                          │
                                          │ Raw Files
                                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Azure Data Lake Storage Gen2                         │
│                                                                              │
│  ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐   │
│  │     BRONZE      │──────▶│     SILVER      │──────▶│      GOLD       │   │
│  │                 │       │                 │       │                 │   │
│  │ Raw ingestion   │       │ Cleaned data    │       │ Dimensional     │   │
│  │ Auto Loader     │       │ Deduplication   │       │ model           │   │
│  │ Schema tracking │       │ Enrichment      │       │ SCD Type 1      │   │
│  │ _rescued_data   │       │ CDF enabled     │       │ Incremental     │   │
│  └─────────────────┘       └─────────────────┘       └────────┬────────┘   │
│                                                               │            │
└───────────────────────────────────────────────────────────────┼────────────┘
                                                                │
                                                                ▼
                                                    ┌─────────────────────┐
                                                    │      Power BI       │
                                                    │                     │
                                                    │ Revenue Performance │
                                                    │ Delivery SLAs       │
                                                    │ Return Patterns     │
                                                    └─────────────────────┘
```

> **Pipeline Screenshot**  
> `docs/screenshots/pipeline_overview.png`

---

## Security & Cloud Infrastructure

### Azure Data Lake Storage Gen2

ADLS Gen2 provides the persistent storage layer for the Lakehouse.

```text
ADLS Gen2
│
├── bronze/
│   └── Raw source data
│
├── silver/
│   └── Validated and enriched Delta tables
│
└── gold/
    └── Analytics-ready dimensional model
```

The storage architecture uses hierarchical namespaces to organize data according to the Medallion layers.

### Authentication

ShopVista uses a **System-Assigned Managed Identity** through the **Azure Access Connector for Databricks**.

```text
Azure Databricks
       │
       ▼
Azure Access Connector
       │
       │ System-Assigned Managed Identity
       ▼
Microsoft Entra ID
       │
       │ RBAC authorization
       ▼
ADLS Gen2
```

No storage account keys, SAS tokens, or hardcoded credentials are required in notebooks.

### RBAC

The Managed Identity is granted:

```text
Storage Blob Data Contributor
```

at the appropriate ADLS Gen2 container/storage scope.

### Unity Catalog

Unity Catalog External Locations provide governed access to ADLS Gen2 paths.

```text
Unity Catalog
     │
     └── External Location
             │
             └── abfss://<container>@<storage-account>.dfs.core.windows.net/
```

The architecture uses `abfss://` URIs rather than embedding storage credentials.

---

## Data Modeling: Snowflake Schema Design

The Gold layer uses a normalized **Snowflake Schema** to reduce redundancy and separate reusable business dimensions from transactional facts.

### Gold Data Model

```text
                         ┌──────────────────────┐
                         │    dim_categories    │
                         │──────────────────────│
                         │ category_code (PK)   │
                         │ category_name        │
                         └──────────┬───────────┘
                                    │
                         ┌──────────▼───────────┐
                         │      dim_brands      │
                         │──────────────────────│
                         │ brand_code (PK)      │
                         │ brand_name           │
                         │ category_code (FK)   │
                         └──────────┬───────────┘
                                    │
                         ┌──────────▼───────────┐
                         │     dim_products     │
                         │──────────────────────│
                         │ product_id (PK)      │
                         │ sku                  │
                         │ price                │
                         │ category_code (FK)   │
                         └──────────┬───────────┘
                                    │
                  ┌─────────────────┼─────────────────┐
                  │                 │                 │
                  ▼                 ▼                 ▼
       ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
       │fact_order_items  │ │fact_order_       │ │fact_order_      │
       │                  │ │shipments         │ │returns          │
       │ order_id         │ │ order_id         │ │ return_id       │
       │ item_id          │ │ shipment_id      │ │ order_id        │
       │ product_id (FK)  │ │ carrier          │ │ product_id (FK) │
       │ pricing          │ │ carrier_group    │ │ refund_amount   │
       │ discounts        │ │ order_dt         │ │ return_turnaround│
       └──────────────────┘ └──────────────────┘ └──────────────────┘

       ┌──────────────────────┐       ┌──────────────────────┐
       │    dim_customers     │       │       dim_date       │
       │──────────────────────│       │──────────────────────│
       │ customer_id (PK)     │       │ date_key (PK)        │
       │ email                │       │ year                 │
       │ shipping_address     │       │ month                │
       │ registration metadata│       │ quarter              │
       └──────────────────────┘       │ day_of_week          │
                                      │ is_weekend           │
                                      └──────────────────────┘
```

> **Snowflake Schema Screenshot**  
> `docs/screenshots/data_modeling_schema.png`

### Gold Schema Directory

| Table | Type | Primary Key | Important Attributes | Purpose |
|---|---|---|---|---|
| `dim_categories` | Dimension | `category_code` | `category_name` | Product category master |
| `dim_brands` | Dimension | `brand_code` | `brand_name`, `category_code` | Brand hierarchy |
| `dim_products` | Dimension | `product_id` | `sku`, `price`, `category_code` | Product master |
| `dim_customers` | Dimension | `customer_id` | `email`, shipping address, registration metadata | Customer master |
| `dim_date` | Dimension | `date_key` | `year`, `month`, `quarter`, `day_of_week`, `is_weekend` | Calendar dimension |
| `fact_order_items` | Fact | `order_id`, `item_id` | `product_id`, line-item pricing, discounts | Order-item transactions |
| `fact_order_shipments` | Fact | `order_id`, `shipment_id` | `carrier`, `carrier_group`, `order_dt` | Shipment and logistics events |
| `fact_order_returns` | Fact | `return_id` | `order_id`, `product_id`, refund amounts, return turnaround | Product return transactions |

### Key Relationships

```text
dim_categories
      │
      ├────────────── dim_brands
      │                    │
      └────────────── dim_products
                           │
                           ├────────────── fact_order_items
                           │
                           └────────────── fact_order_returns

dim_customers ───────────────▶ Order domain

dim_date ────────────────────▶ Transaction / shipment / return dates

fact_order_items
        │
        ├──────────────▶ Revenue analysis
        └──────────────▶ Product analysis

fact_order_shipments
        │
        └──────────────▶ Carrier SLA analysis

fact_order_returns
        │
        └──────────────▶ Return pattern analysis
```

---

## Medallion Pipeline Implementation

### Bronze Layer

The Bronze layer ingests raw source files using **Databricks Auto Loader** with the `cloudFiles` source.

Key capabilities:

- Incremental file discovery.
- Schema inference.
- Schema tracking.
- Persistent metadata using `cloudFiles.schemaLocation`.
- Automatic handling of malformed records through `_rescued_data`.
- Raw data preservation for downstream reprocessing.

```text
Source Files
     │
     ▼
Databricks Auto Loader
     │
     ├── Schema Inference
     ├── Schema Tracking
     └── Corrupt Record Handling
     │
     ▼
Bronze Delta Tables
```

### Silver Layer

The Silver layer converts raw Bronze data into validated and business-ready Delta datasets.

Processing includes:

- Schema enforcement.
- Data type standardization.
- Null filtering.
- Record deduplication.
- Business rule enrichment.
- Carrier classification.
- Return turnaround calculation.
- Delta Change Data Feed activation.

Example business enrichment:

```text
carrier
   │
   ├── Domestic
   └── International
        │
        ▼
carrier_group
```

Return turnaround:

```text
datediff(return_date, order_date)
                │
                ▼
       Return turnaround window
```

Delta CDF is enabled with:

```sql
ALTER TABLE silver_table
SET TBLPROPERTIES (
    delta.enableChangeDataFeed = true
);
```

### Gold Layer

Gold processing consumes Silver Delta Change Data Feed records incrementally.

Only relevant changes are processed:

```text
_change_type IN (
    'insert',
    'update_postimage'
)
```

The pipeline uses:

- `readStream`
- Delta Change Data Feed
- `foreachBatch`
- `MERGE`
- SCD Type 1 semantics
- Micro-batch deduplication
- Partition pruning
- `availableNow=True`

> **Databricks Sample Notebook Screenshot**  
> `docs/screenshots/databricks_sample_notebook.png`

> **SCD Implementation Screenshot**  
> `docs/screenshots/scd_implementation.png`

---

## Key Engineering Solutions & PySpark Code

### 1. Bronze Ingestion with Auto Loader

```python
from pyspark.sql import functions as F

source_path = (
    "abfss://bronze@<storage-account>.dfs.core.windows.net/orders/"
)

schema_location = (
    "abfss://bronze@<storage-account>.dfs.core.windows.net/"
    "_schemas/orders/"
)

checkpoint_location = (
    "abfss://bronze@<storage-account>.dfs.core.windows.net/"
    "_checkpoints/orders/"
)

bronze_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .load(source_path)
)

(
    bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .toTable("bronze.orders")
)
```

### 2. Silver-to-Gold CDF Processing

CDF records are read incrementally from Silver instead of repeatedly scanning the complete historical dataset.

```python
from pyspark.sql import functions as F

cdf_df = (
    spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
        .table("silver.order_shipments")
        .filter(
            F.col("_change_type").isin(
                "insert",
                "update_postimage"
            )
        )
)
```

### 3. SCD Type 1 `MERGE` with `foreachBatch`

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(
    spark,
    "gold.fact_order_shipments"
)

def upsert_batch(microBatchDf, batch_id):

    # Prevent multiple source records from matching
    # the same target row during MERGE.
    deduped_df = (
        microBatchDf
        .dropDuplicates([
            "order_id",
            "shipment_id",
            "order_dt"
        ])
    )

    (
        target.alias("target")
        .merge(
            deduped_df.alias("source"),
            """
            target.order_id = source.order_id
            AND target.shipment_id = source.shipment_id
            AND target.order_dt = source.order_dt
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


query = (
    cdf_df.writeStream
        .foreachBatch(upsert_batch)
        .option(
            "checkpointLocation",
            "abfss://gold@<storage-account>.dfs.core.windows.net/"
            "_checkpoints/fact_order_shipments/"
        )
        .trigger(availableNow=True)
        .start()
)

query.awaitTermination()
```

### Engineering Mitigations

#### Multiple Source Row Matching

Delta Lake can reject a `MERGE` when multiple source records match the same target row.

The pipeline mitigates this within every micro-batch:

```python
microBatchDf.dropDuplicates([
    "order_id",
    "shipment_id",
    "order_dt"
])
```

This addresses:

```text
[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW]
```

#### Partition Pruning

The `order_dt` predicate is included in the merge condition:

```sql
target.order_dt = source.order_dt
```

This allows Delta to prune irrelevant date partitions instead of evaluating the entire target table.

```text
Source order_dt
      │
      ▼
Relevant order_dt partition
      │
      ▼
MERGE
```

#### `availableNow=True`

The pipeline uses:

```python
.trigger(availableNow=True)
```

to process all currently available data and then terminate.

```text
Scheduled Job
     │
     ▼
Start Stream
     │
     ▼
Process Available CDF
     │
     ▼
Commit Delta Changes
     │
     ▼
Terminate
```

This provides a scheduled incremental execution model without requiring continuously running streaming compute.

---

## Data Volumes & Execution Strategy

| Dataset | Historical Processing | Incremental Processing | Volume / Strategy |
|---|---|---|---|
| `fact_order_items` | `2024-01-01` → `2024-08-31` | `2025-09-01` → `2025-12-31` | 1,000,000+ total rows; historical Auto Loader backfill followed by daily incremental streaming |
| `fact_order_shipments` | `2024-01` → `2025-08` | `2025-09` → `2025-12` | Month-over-Month historical backfill followed by incremental streaming |
| `fact_order_returns` | `2024-01` → `2025-08` | `2025-09` → `2026-01` | Month-over-Month historical backfill followed by incremental streaming |

### Historical Backfill Strategy

Large historical ranges are processed in controlled periods rather than treating the entire history as a single workload.

```text
2024-01
   │
2024-02
   │
2024-03
   │
 ...
   │
2025-08
   │
   ▼
Historical Gold Baseline
```

For shipments and returns, the historical pipeline follows a **Month-over-Month (MoM)** execution strategy.

### Incremental Strategy

After historical initialization:

```text
Historical Backfill
       │
       ▼
Gold Baseline
       │
       ▼
Delta CDF
       │
       ▼
Scheduled Incremental Processing
       │
       ▼
SCD Type 1 MERGE
```

### Compute Cost Control

`availableNow=True` allows the pipeline to process currently available incremental data as a finite workload.

```text
Scheduled Trigger
      │
      ▼
Available Data
      │
      ▼
Process
      │
      ▼
Stop Compute
```

This reduces unnecessary compute runtime for workloads that do not require continuously active streaming infrastructure.

---

## Analytics & BI Dashboards

Power BI consumes the **Gold Delta tables** to provide analytical reporting across three primary domains.

### Revenue Performance

Uses `fact_order_items` and product/category dimensions for:

- Revenue trends.
- Product-level performance.
- Category-level analysis.
- Discount analysis.
- Order-item metrics.
- Time-based revenue analysis using `dim_date`.

### Carrier Delivery SLAs

Uses `fact_order_shipments` and date dimensions for:

- Carrier-level shipment analysis.
- Domestic vs. International carrier grouping.
- Order-date trends.
- Delivery SLA monitoring.
- Carrier performance comparisons.

### Product Return Patterns

Uses `fact_order_returns` with product, category, and date dimensions for:

- Return volumes.
- Refund amounts.
- Product-level return patterns.
- Category-level return analysis.
- Return turnaround windows.
- Time-based return trends.

### Dashboard Architecture

```text
                  Power BI
                     │
          ┌──────────┼───────────┐
          │          │           │
          ▼          ▼           ▼
      Revenue      Carrier      Returns
    Performance      SLA        Patterns
          │          │           │
          └──────────┼───────────┘
                     │
                     ▼
              Gold Lakehouse
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
   Fact Tables    Dimensions    Dim Date
```

> **Power BI Dashboard Screenshot**  
> `docs/screenshots/dashboard_overview.png`

---

## Repository Structure

```text
shopvista-lakehouse/
│
├── README.md
│
├── architecture/
│   ├── architecture-diagram.drawio
│   ├── data-flow.md
│   └── security-architecture.md
│
├── notebooks/
│   │
│   ├── bronze/
│   │   ├── 01_bronze_orders_autoloader.py
│   │   ├── 02_bronze_shipments_autoloader.py
│   │   ├── 03_bronze_returns_autoloader.py
│   │   └── 04_bronze_master_data.py
│   │
│   ├── silver/
│   │   ├── 01_silver_orders.py
│   │   ├── 02_silver_shipments.py
│   │   ├── 03_silver_returns.py
│   │   └── 04_silver_master_data.py
│   │
│   └── gold/
│       ├── 01_dim_categories.py
│       ├── 02_dim_brands.py
│       ├── 03_dim_products.py
│       ├── 04_dim_customers.py
│       ├── 05_dim_date.py
│       ├── 06_fact_order_items.py
│       ├── 07_fact_order_shipments.py
│       └── 08_fact_order_returns.py
│
├── src/
│   ├── ingestion/
│   │   ├── autoloader.py
│   │   └── schema_utils.py
│   │
│   ├── transformations/
│   │   ├── orders.py
│   │   ├── shipments.py
│   │   └── returns.py
│   │
│   └── utilities/
│       ├── cdf_utils.py
│       ├── merge_utils.py
│       └── data_quality.py
│
├── config/
│   ├── bronze_config.yml
│   ├── silver_config.yml
│   ├── gold_config.yml
│   └── environment.yml
│
├── sql/
│   ├── ddl/
│   │   ├── bronze.sql
│   │   ├── silver.sql
│   │   └── gold.sql
│   │
│   └── validation/
│       ├── data_quality.sql
│       └── reconciliation.sql
│
├── powerbi/
│   ├── ShopVista.pbix
│   ├── data_model.md
│   └── measures.md
│
├── tests/
│   ├── unit/
│   │   ├── test_transformations.py
│   │   └── test_data_quality.py
│   │
│   └── integration/
│       └── test_delta_pipeline.py
│
├── docs/
│   ├── screenshots/
│   │   ├── pipeline_overview.png
│   │   ├── data_modeling_schema.png
│   │   ├── databricks_sample_notebook.png
│   │   ├── scd_implementation.png
│   │   └── dashboard_overview.png
│   │
│   └── design/
│       ├── medallion-architecture.md
│       ├── data-model.md
│       └── security.md
│
└── .gitignore
```
