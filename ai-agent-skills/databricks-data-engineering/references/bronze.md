# Bronze Layer - Raw Data Ingestion

## Purpose
The Bronze layer ingests raw data exactly as received from source systems with minimal transformation. It serves as the **single source of truth** for all raw data, preserving data lineage and enabling reprocessing.

## Core Principles

### 1. Preserve Raw Data
- Store data exactly as received from source
- No business logic transformations
- No data filtering (log issues, don't drop data)
- Maintain original data types when possible

### 2. Add Audit Metadata
Every Bronze table must include these audit columns:
- `_ingested_timestamp` - When data arrived in Databricks
- `_source_file` - Source file path (for file-based ingestion)
- `_source_system` - Source system identifier (e.g., "salesforce", "mysql_prod")
- `_ingestion_id` - Job/run ID for traceability

### 3. Idempotent Ingestion
Re-running ingestion should not create duplicates:
- Use Auto Loader checkpointing for files
- Track watermarks for database ingestion
- Use MERGE or deduplication for batch loads

## Bronze Table Design

### Standard Bronze Table Schema
```sql
CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    -- Original source columns (preserve as-is)
    order_id STRING,              -- Keep as STRING even if should be INT
    customer_id STRING,
    order_date STRING,            -- Keep as STRING, convert in Silver
    order_amount STRING,
    order_status STRING,
    
    -- Audit columns (always include)
    _ingested_timestamp TIMESTAMP,
    _source_file STRING,
    _source_system STRING,
    _ingestion_id STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Raw orders data from source system - no transformations applied';
```

## Ingestion Patterns

### Pattern 1: File Ingestion with Auto Loader (Recommended)
```python
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# Bronze ingestion from cloud storage
def ingest_to_bronze(source_path, target_table, checkpoint_path, source_system):
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")  # or csv, parquet, avro
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_system", lit(source_system))
        .withColumn("_ingestion_id", lit(spark.conf.get("spark.databricks.job.runId", "manual_run")))
    )
    
    # Write to Bronze with exactly-once semantics
    query = (df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/checkpoint")
        .option("mergeSchema", "true")  # Allow schema evolution
        .trigger(availableNow=True)     # Process all available files
        .toTable(target_table)
    )
    
    return query

# Usage
query = ingest_to_bronze(
    source_path="s3://bucket/landing/orders/",
    target_table="bronze.raw_orders",
    checkpoint_path="/mnt/checkpoints/bronze_orders",
    source_system="ecommerce_api"
)
query.awaitTermination()
```

### Pattern 2: Database Ingestion with Watermark
```python
from pyspark.sql.functions import col, current_timestamp, lit

def incremental_jdbc_to_bronze(
    jdbc_url, 
    source_table, 
    target_table, 
    watermark_column="updated_at",
    secret_scope="db-credentials"
):
    # Get last watermark from Bronze
    try:
        last_watermark = spark.sql(f"""
            SELECT COALESCE(MAX({watermark_column}), '1900-01-01 00:00:00') as max_ts
            FROM {target_table}
        """).collect()[0]['max_ts']
    except:
        last_watermark = '1900-01-01 00:00:00'  # First run
    
    print(f"Incremental load since: {last_watermark}")
    
    # Read incremental data from source
    query = f"(SELECT * FROM {source_table} WHERE {watermark_column} > '{last_watermark}') as incremental"
    
    df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", dbutils.secrets.get(scope=secret_scope, key="username"))
        .option("password", dbutils.secrets.get(scope=secret_scope, key="password"))
        .option("numPartitions", "4")
        .load()
        .withColumn("_ingested_timestamp", current_timestamp())
        .withColumn("_source_system", lit("mysql_prod"))
        .withColumn("_source_file", lit(None).cast("string"))
        .withColumn("_ingestion_id", lit(spark.conf.get("spark.databricks.job.runId", "manual")))
    )
    
    # Append to Bronze
    row_count = df.count()
    if row_count > 0:
        df.write.mode("append").saveAsTable(target_table)
        print(f"✓ Ingested {row_count} new records to {target_table}")
    else:
        print("No new records to ingest")
    
    return row_count

# Usage
incremental_jdbc_to_bronze(
    jdbc_url="jdbc:mysql://prod-db.company.com:3306/sales",
    source_table="orders",
    target_table="bronze.raw_mysql_orders",
    watermark_column="updated_at"
)
```

### Pattern 3: API Ingestion
```python
import requests
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime

def ingest_api_to_bronze(api_url, api_key_scope, target_table):
    # Fetch from API
    headers = {"Authorization": f"Bearer {dbutils.secrets.get(scope=api_key_scope, key='api_key')}"}
    
    all_data = []
    page = 1
    while True:
        response = requests.get(f"{api_url}?page={page}", headers=headers)
        data = response.json()
        
        if not data.get('results'):
            break
        
        all_data.extend(data['results'])
        page += 1
        
        if page > 100:  # Safety limit
            break
    
    print(f"Fetched {len(all_data)} records from API")
    
    # Convert to DataFrame (keep everything as strings in Bronze)
    if all_data:
        df = (spark.createDataFrame(all_data)
            .withColumn("_ingested_timestamp", current_timestamp())
            .withColumn("_source_system", lit("external_api"))
            .withColumn("_source_file", lit(None).cast("string"))
            .withColumn("_ingestion_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        )
        
        df.write.mode("append").saveAsTable(target_table)
        print(f"✓ Ingested to {target_table}")
        return df.count()
    else:
        print("No data received from API")
        return 0

# Usage
ingest_api_to_bronze(
    api_url="https://api.external.com/v1/orders",
    api_key_scope="api-keys",
    target_table="bronze.raw_api_orders"
)
```

## Schema Evolution

Bronze tables should support schema evolution to handle new columns from source:

```python
# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Or per-write
df.write     .mode("append")     .option("mergeSchema", "true")     .saveAsTable("bronze.raw_orders")
```

## Data Quality in Bronze

### Log Issues, Don't Filter
Bronze should capture ALL data, including bad records:

```python
from pyspark.sql.functions import when, col

# Add data quality flags (but keep all records)
bronze_df = (source_df
    .withColumn("_dq_null_customer_id", col("customer_id").isNull())
    .withColumn("_dq_invalid_date_format", 
        when(~col("order_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), True).otherwise(False))
    .withColumn("_ingested_timestamp", current_timestamp())
)

# Write everything - no filtering!
bronze_df.write.mode("append").saveAsTable("bronze.raw_orders")
```

## Bronze Best Practices

✓ **Preserve raw data exactly as received**  
✓ **Include all audit columns**  
✓ **Make ingestion idempotent**  
✓ **Enable schema evolution**  
✓ **Log data quality issues, don't filter**  
✓ **Use Auto Loader for file ingestion**  
✓ **Track watermarks for incremental loads**  
✓ **Never hardcode credentials**  
✓ **Enable auto-optimization**  

## Bronze Anti-patterns

❌ **Transforming data** - Keep Bronze raw  
❌ **Filtering bad records** - Log them, keep everything  
❌ **Type conversions** - Keep original types, convert in Silver  
❌ **Joining tables** - Single source tables only  
❌ **Business logic** - Belongs in Silver/Gold  
❌ **No audit columns** - Can't trace data lineage  
❌ **Not idempotent** - Creates duplicates on re-run  

## Monitoring Bronze

```python
# Check Bronze table health
def check_bronze_health(table_name):
    # Row count and freshness
    stats = spark.sql(f"""
        SELECT 
            COUNT(*) as total_rows,
            MAX(_ingested_timestamp) as last_ingestion,
            DATEDIFF(NOW(), MAX(_ingested_timestamp)) as days_since_last_ingestion,
            COUNT(DISTINCT _source_file) as unique_source_files
        FROM {table_name}
    """).collect()[0]
    
    print(f"Bronze Table: {table_name}")
    print(f"  Total Rows: {stats['total_rows']:,}")
    print(f"  Last Ingestion: {stats['last_ingestion']}")
    print(f"  Days Since Last: {stats['days_since_last_ingestion']}")
    print(f"  Unique Files: {stats['unique_source_files']}")
    
    # Alert if stale
    if stats['days_since_last_ingestion'] > 1:
        print("  ⚠️  WARNING: Data may be stale!")
    else:
        print("  ✓ Data is fresh")

check_bronze_health("bronze.raw_orders")
```

## See Also
- [Silver Layer](bronze-to-silver-pipeline.md) - Next step: cleansing and validation
- [02-ingestion.md](02-ingestion.md) - Detailed ingestion patterns
- [Code Examples](../code/examples/bronze_ingestion.py) - Full working examples

---

**Remember**: Bronze is your insurance policy. Keep everything raw so you can always reprocess if business logic changes.
