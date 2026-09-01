# Data Ingestion Patterns

## Full Load vs Incremental Load

### Full Load (Snapshot)
**When to use**:
- Small source tables (< 1GB)
- No reliable timestamp or CDC mechanism
- Source data changes unpredictably
- Simplicity preferred over efficiency
- Historical changes don't need tracking

**Pattern**:
```python
# Read entire source table
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "source_table")
    .option("user", dbutils.secrets.get(scope="db", key="username"))
    .option("password", dbutils.secrets.get(scope="db", key="password"))
    .load()
    .withColumn("_ingested_timestamp", current_timestamp())
)

# Overwrite target table
df.write.mode("overwrite").saveAsTable("bronze.source_table")
```

**Pros**: Simple, guaranteed consistency  
**Cons**: Inefficient for large tables, high source system load

### Incremental Load (Append)
**When to use**:
- Large source tables (> 1GB)
- Reliable timestamp column exists (`created_at`, `updated_at`)
- Source supports watermark tracking
- Insert-only data (logs, events, transactions)

**Pattern**:
```python
# Get last watermark
last_watermark = spark.sql("""
    SELECT COALESCE(MAX(_ingested_timestamp), '1900-01-01') as max_ts 
    FROM bronze.source_table
""").collect()[0]['max_ts']

# Read only new records
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", f"SELECT * FROM source_table WHERE updated_at > '{last_watermark}'")
    .load()
    .withColumn("_ingested_timestamp", current_timestamp())
)

# Append new records
df.write.mode("append").saveAsTable("bronze.source_table")
```

**Pros**: Efficient, lower load on source  
**Cons**: Requires reliable timestamp, doesn't handle deletes

## Auto Loader (Cloud Files)

**When to use**: Ingesting files from cloud storage (S3, ADLS, GCS)

**Supported formats**: JSON, CSV, Parquet, Avro, ORC, text, binary

**Key features**:
- Automatic schema inference and evolution
- Scalable file discovery (even millions of files)
- Exactly-once processing guarantees
- Checkpointing for fault tolerance

### Basic Auto Loader pattern
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")  # or csv, parquet, etc.
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
    .withColumn("_ingested_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/checkpoint")
    .trigger(availableNow=True)  # or processingTime="5 minutes" for continuous
    .toTable("bronze.raw_data"))
```

### Auto Loader with schema hints (recommended for production)
```python
# Define schema hints for critical columns
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.schemaHints", "order_id BIGINT, order_date DATE, amount DECIMAL(10,2)")
    .option("header", "true")
    .load(source_path)
    .withColumn("_ingested_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
```

### Auto Loader for backfill (process once, then stop)
```python
# Use trigger(availableNow=True) for backfill jobs
(df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/checkpoint")
    .trigger(availableNow=True)  # Process all available files, then stop
    .toTable("bronze.raw_data"))
```

## Database Ingestion (JDBC)

### Best practices for JDBC reads
```python
# Use partitioning for large tables
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "large_table")
    .option("user", dbutils.secrets.get(scope="db", key="username"))
    .option("password", dbutils.secrets.get(scope="db", key="password"))
    .option("numPartitions", "10")
    .option("partitionColumn", "id")
    .option("lowerBound", "1")
    .option("upperBound", "1000000")
    .load()
)
```

### Incremental JDBC with watermark
```python
# Function for incremental JDBC load
def incremental_jdbc_load(source_table, watermark_column, target_table):
    # Get last watermark
    max_value_query = f"SELECT COALESCE(MAX({watermark_column}), '1900-01-01 00:00:00') as max_value FROM {target_table}"
    last_watermark = spark.sql(max_value_query).collect()[0]['max_value']
    
    # Build incremental query
    query = f"(SELECT * FROM {source_table} WHERE {watermark_column} > '{last_watermark}') as incremental_data"
    
    # Read and append
    df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", dbutils.secrets.get(scope="db", key="username"))
        .option("password", dbutils.secrets.get(scope="db", key="password"))
        .load()
        .withColumn("_ingested_timestamp", current_timestamp())
    )
    
    df.write.mode("append").saveAsTable(target_table)
    
    return df.count()
```

## API Ingestion

### REST API pattern with pagination
```python
import requests
from pyspark.sql.types import *

def fetch_api_data(base_url, headers):
    all_data = []
    page = 1
    
    while True:
        response = requests.get(f"{base_url}?page={page}", headers=headers)
        data = response.json()
        
        if not data:
            break
            
        all_data.extend(data)
        page += 1
    
    return all_data

# Define schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("created_at", StringType(), True)
])

# Fetch and create DataFrame
api_data = fetch_api_data(api_url, headers)
df = (spark.createDataFrame(api_data, schema)
    .withColumn("_ingested_timestamp", current_timestamp())
)

df.write.mode("append").saveAsTable("bronze.api_data")
```

## Backfill and Reprocessing

### Backfill strategy
```python
# Backfill historical data by date range
from datetime import datetime, timedelta

def backfill_by_date_range(start_date, end_date, source_table, target_table):
    current_date = start_date
    
    while current_date <= end_date:
        next_date = current_date + timedelta(days=1)
        
        print(f"Processing {current_date}")
        
        query = f"SELECT * FROM {source_table} WHERE date_column >= '{current_date}' AND date_column < '{next_date}'"
        
        df = (spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .load()
            .withColumn("_ingested_timestamp", current_timestamp())
        )
        
        df.write.mode("append").saveAsTable(target_table)
        
        current_date = next_date

# Run backfill
backfill_by_date_range(
    datetime(2024, 1, 1),
    datetime(2024, 12, 31),
    "source_orders",
    "bronze.orders"
)
```

### Reprocessing with checkpoint reset
```python
# For Auto Loader: delete checkpoint to reprocess
dbutils.fs.rm(f"{checkpoint_path}/checkpoint", recurse=True)
dbutils.fs.rm(f"{checkpoint_path}/schema", recurse=True)

# For SQL: truncate and reload
spark.sql("TRUNCATE TABLE bronze.source_table")
# Then run ingestion again
```

## Audit Columns (Standard Set)

Every ingested table should include:

```python
df = (source_df
    .withColumn("_ingested_timestamp", current_timestamp())      # When ingested
    .withColumn("_source_file", input_file_name())               # Source file (if file-based)
    .withColumn("_source_system", lit("SAP"))                    # Source system name
    .withColumn("_ingestion_id", lit(dbutils.jobs.taskValues.get("run_id")))  # Job run ID
)
```

## Best Practices

1. **Always use secrets for credentials** - Never hardcode passwords
2. **Include audit columns** - Essential for debugging and lineage
3. **Prefer Auto Loader for files** - More robust than spark.read for cloud storage
4. **Partition large JDBC reads** - Use `numPartitions` to parallelize
5. **Use watermarks for incremental** - Track last processed timestamp
6. **Make ingestion idempotent** - Re-running should be safe
7. **Handle schema evolution** - Plan for new columns in source
8. **Monitor for failures** - Log ingestion metrics (row counts, durations)

## Anti-patterns to Avoid

❌ **Reading entire large table every time** - Use incremental when possible  
❌ **No watermark tracking** - Leads to duplicate processing  
❌ **Hardcoded credentials** - Security risk  
❌ **No audit columns** - Can't trace data origin  
❌ **Ignoring schema changes** - Source schema will evolve  
❌ **No monitoring** - Can't detect silent failures  
❌ **Processing files multiple times** - Use checkpointing with Auto Loader

---

**Remember**: Choose the right pattern based on data volume, update frequency, and source capabilities. Start simple (full load), then optimize to incremental when needed.
