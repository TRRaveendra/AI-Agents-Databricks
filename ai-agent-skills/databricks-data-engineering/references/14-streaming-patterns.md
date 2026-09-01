# Streaming Patterns with Auto Loader and Structured Streaming

## Overview
Databricks provides robust streaming capabilities through Auto Loader (for file ingestion) and Structured Streaming (for continuous data processing). Both enable incremental, exactly-once processing of data.

---

## Auto Loader (cloudFiles)

Auto Loader incrementally and efficiently processes new files as they arrive in cloud storage.

### Basic Auto Loader Pattern

```python
# Read streaming data from cloud storage
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/path/to/schema")
    .load("s3://bucket/landing/orders/")
)

# Write to Delta table
(df.writeStream
    .option("checkpointLocation", "/path/to/checkpoint")
    .trigger(availableNow=True)  # Process all available files, then stop
    .table("bronze.raw_orders")
)
```

### Auto Loader Features

#### 1. Automatic Schema Inference and Evolution
```python
# Initial load - infer schema
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoint/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("s3://bucket/data/")
)

# Schema evolution - automatically add new columns
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoint/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("s3://bucket/data/")
)
```

**Schema evolution modes**:
- `none`: Fail if schema changes (default, safest)
- `addNewColumns`: Add new columns automatically
- `rescue`: Put unexpected columns in `_rescued_data` column

#### 2. File Notification Modes

**Directory listing** (default):
- Lists directory periodically
- Works on all clouds
- Good for < 10K files

**File notification**:
- Uses cloud events (S3 SQS, Azure Event Grid, GCS Pub/Sub)
- Scales to millions of files
- Lower latency

```python
# Enable file notification mode
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.region", "us-east-1")  # For AWS
    .option("cloudFiles.queueUrl", "https://sqs.us-east-1.amazonaws.com/123456789/my-queue")  # Optional
    .load("s3://bucket/data/")
)
```

#### 3. Support Multiple File Formats
```python
# JSON
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(path)

# CSV
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .load(path)
)

# Parquet
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(path)

# Avro
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "avro").load(path)

# Text
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "text").load(path)

# Binary (for images, PDFs, etc.)
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "binaryFile").load(path)
```

#### 4. Metadata Columns
Auto Loader adds helpful metadata columns:

```python
from pyspark.sql.functions import col, input_file_name

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(path)
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingested_timestamp", current_timestamp())
)

# Metadata columns available:
# - _metadata.file_path: Full path to source file
# - _metadata.file_name: File name only
# - _metadata.file_size: File size in bytes
# - _metadata.file_modification_time: Last modified timestamp
```

### Auto Loader Best Practices

#### Pattern 1: Bronze Ingestion with Auto Loader
```python
# Bronze: Ingest raw files with full lineage
def bronze_auto_loader(source_path, table_name, checkpoint_path):
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Capture schema changes
        .load(source_path)
        .withColumn("_ingested_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
    )
    
    (df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)  # Trigger-once semantics
        .table(table_name)
    )

# Usage
bronze_auto_loader(
    source_path="s3://bucket/landing/orders/",
    table_name="bronze.raw_orders",
    checkpoint_path="/checkpoints/bronze/raw_orders"
)
```

#### Pattern 2: Archive Processed Files
```python
# Archive files after processing (optional)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint)
    .option("cloudFiles.pathGlobFilter", "*.json")
    .load(source_path)
)

# After writing to Delta, use cloud provider tools to archive
# E.g., S3 lifecycle policies, Azure Blob lifecycle management
```

#### Pattern 3: Filter Files by Pattern
```python
# Only process specific file patterns
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.pathGlobFilter", "order_*.json")  # Only files matching pattern
    .load(source_path)
)
```

#### Pattern 4: Handle Bad Records
```python
# Use rescue mode to capture schema mismatches
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(source_path)
)

# Rescued data goes to _rescued_data column
# Query bad records
spark.sql("""
    SELECT * FROM bronze.raw_orders 
    WHERE _rescued_data IS NOT NULL
""")
```

---

## Structured Streaming

Process streaming data from various sources (Kafka, Delta, Event Hubs, Kinesis).

### Basic Structured Streaming

```python
# Read stream from Delta table
df = spark.readStream.table("bronze.raw_orders")

# Transform
df_transformed = (df
    .filter(col("order_status") != "cancelled")
    .withColumn("order_amount", col("order_amount").cast("decimal(10,2)"))
    .withColumn("_processed_timestamp", current_timestamp())
)

# Write stream
(df_transformed.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/silver/orders")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("silver.clean_orders")
)
```

### Streaming Sources

#### 1. Delta Table Source
```python
# Read from Delta table
df = spark.readStream.table("bronze.raw_orders")

# Read from specific version onwards
df = (spark.readStream
    .format("delta")
    .option("startingVersion", 10)
    .table("bronze.raw_orders")
)

# Read from timestamp
df = (spark.readStream
    .format("delta")
    .option("startingTimestamp", "2024-01-01 00:00:00")
    .table("bronze.raw_orders")
)
```

#### 2. Kafka Source
```python
# Read from Kafka
df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "orders-topic")
    .option("startingOffsets", "latest")
    .load()
)

# Parse Kafka message
from pyspark.sql.functions import from_json, col

schema = "order_id BIGINT, customer_id BIGINT, amount DECIMAL(10,2)"
df_parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
```

#### 3. Event Hubs (Azure)
```python
# Read from Azure Event Hubs
connection_string = dbutils.secrets.get(scope="azure", key="eventhub-connection")

df = (spark.readStream
    .format("eventhubs")
    .option("eventhubs.connectionString", connection_string)
    .option("eventhubs.consumerGroup", "$Default")
    .load()
)
```

#### 4. Kinesis (AWS)
```python
# Read from AWS Kinesis
df = (spark.readStream
    .format("kinesis")
    .option("streamName", "orders-stream")
    .option("region", "us-east-1")
    .option("initialPosition", "latest")
    .load()
)
```

### Output Modes

#### 1. Append (Default)
Only new rows are written to sink.
```python
df.writeStream.outputMode("append").table("silver.orders")
```

#### 2. Complete
Entire result table is written every trigger.
```python
# Use for aggregations
df.groupBy("region").count().writeStream.outputMode("complete").table("gold.region_counts")
```

#### 3. Update
Only updated rows are written (for aggregations with watermarks).
```python
df.groupBy("region").count().writeStream.outputMode("update").table("gold.region_counts")
```

### Triggers

#### 1. availableNow (Trigger Once)
Process all available data, then stop.
```python
df.writeStream.trigger(availableNow=True).table("silver.orders")
```

#### 2. processingTime (Micro-Batch)
Process data every N seconds/minutes.
```python
# Process every 5 minutes
df.writeStream.trigger(processingTime="5 minutes").table("silver.orders")
```

#### 3. once (Legacy, use availableNow)
```python
df.writeStream.trigger(once=True).table("silver.orders")
```

#### 4. Continuous (Low Latency)
Sub-second latency processing.
```python
df.writeStream.trigger(continuous="1 second").table("silver.orders")
```

### Watermarks and Late Data

Handle late-arriving data with watermarks.

```python
from pyspark.sql.functions import window

# Define watermark (10 minutes)
df_with_watermark = df.withWatermark("event_timestamp", "10 minutes")

# Window aggregation with watermark
windowed_counts = (df_with_watermark
    .groupBy(
        window("event_timestamp", "1 hour"),
        "product_id"
    )
    .count()
)

# Late data within 10 minutes is processed, older data is dropped
(windowed_counts.writeStream
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/windowed_counts")
    .table("gold.hourly_product_counts")
)
```

**Watermark considerations**:
- Set based on expected late data arrival
- Too short: Drop valid late data
- Too long: Higher memory usage, longer state retention

---

## Streaming Joins

### 1. Stream-Static Join
Join streaming data with static dimension table.

```python
# Stream-static join (no watermark needed)
stream_df = spark.readStream.table("bronze.raw_orders")
static_df = spark.table("silver.customers")

joined = stream_df.join(static_df, "customer_id")

joined.writeStream.table("silver.enriched_orders")
```

### 2. Stream-Stream Join
Join two streaming DataFrames.

```python
# Both streams need watermarks
stream1 = (spark.readStream.table("bronze.orders")
    .withWatermark("order_timestamp", "10 minutes")
)

stream2 = (spark.readStream.table("bronze.payments")
    .withWatermark("payment_timestamp", "10 minutes")
)

# Join within time window
joined = stream1.join(
    stream2,
    expr("""
        order_id = payment_order_id AND
        payment_timestamp >= order_timestamp AND
        payment_timestamp <= order_timestamp + interval 1 hour
    """)
)

joined.writeStream.table("silver.orders_with_payments")
```

---

## Best Practices

### 1. Checkpointing
Always set `checkpointLocation` for exactly-once semantics.

```python
# Checkpoint location stores:
# - Stream progress (offset tracking)
# - State information (for aggregations)
# - Metadata

(df.writeStream
    .option("checkpointLocation", "/checkpoints/my_stream")  # Required
    .table("target_table")
)
```

**Checkpoint best practices**:
- Use unique checkpoint per stream
- Store in reliable storage (cloud storage, not /tmp)
- Don't modify stream logic after setting checkpoint (breaks progress tracking)
- Delete checkpoint to restart from beginning

### 2. Idempotency
Structured Streaming provides exactly-once guarantees, but design for idempotency.

```python
# Use MERGE for upsert semantics (safe for reprocessing)
def upsert_to_target(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    spark.sql("""
        MERGE INTO silver.customers target
        USING updates source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(df.writeStream
    .foreachBatch(upsert_to_target)
    .option("checkpointLocation", checkpoint)
    .start()
)
```

### 3. Monitoring Streaming Queries

```python
# Get active streams
active_streams = spark.streams.active

# Check stream status
for stream in active_streams:
    print(f"Stream: {stream.name}")
    print(f"Status: {stream.status}")
    print(f"Recent Progress: {stream.recentProgress}")

# Monitor specific stream
query = df.writeStream.table("target")
print(query.lastProgress)  # Last batch metrics
print(query.status)  # Current status

# Wait for stream to finish (for availableNow)
query.awaitTermination()
```

### 4. Error Handling

```python
# Capture and log errors
try:
    query = (df.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .table("target_table")
    )
    query.awaitTermination()
except Exception as e:
    print(f"Stream failed: {e}")
    # Log to monitoring system
    raise
```

### 5. Schema Evolution

```python
# Allow schema evolution in streaming
(df.writeStream
    .option("checkpointLocation", checkpoint)
    .option("mergeSchema", "true")  # Allow new columns
    .table("target_table")
)
```

---

## Common Streaming Patterns

### Pattern 1: Bronze Ingestion with Auto Loader
```python
# Ingest files incrementally into bronze
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/bronze/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load("s3://landing/orders/")
    .withColumn("_ingested_timestamp", current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/bronze/orders")
    .trigger(availableNow=True)
    .table("bronze.raw_orders")
)
```

### Pattern 2: Silver Transformation
```python
# Transform bronze to silver
(spark.readStream
    .table("bronze.raw_orders")
    .select(
        col("order_id").cast("bigint"),
        col("customer_id").cast("bigint"),
        to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
        col("order_amount").cast("decimal(10,2)")
    )
    .filter(col("order_amount") > 0)
    .withColumn("_processed_timestamp", current_timestamp())
    .writeStream
    .option("checkpointLocation", "/checkpoints/silver/orders")
    .trigger(availableNow=True)
    .table("silver.clean_orders")
)
```

### Pattern 3: Gold Aggregation with CDF
```python
# Incrementally update gold aggregates using Change Data Feed
(spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("silver.orders")
    .groupBy("customer_id")
    .agg(
        sum("order_amount").alias("total_spent"),
        count("*").alias("order_count")
    )
    .writeStream
    .option("checkpointLocation", "/checkpoints/gold/customer_summary")
    .outputMode("complete")  # Complete for aggregations
    .table("gold.customer_summary")
)
```

### Pattern 4: Real-Time Enrichment
```python
# Enrich streaming orders with customer data
stream_orders = spark.readStream.table("bronze.raw_orders")
customers = spark.table("silver.customers")

enriched = (stream_orders
    .join(customers, "customer_id", "left")
    .select(
        "order_id",
        "customer_id",
        "customer_name",
        "customer_region",
        "order_amount",
        "_ingested_timestamp"
    )
)

(enriched.writeStream
    .option("checkpointLocation", "/checkpoints/silver/enriched_orders")
    .trigger(processingTime="5 minutes")
    .table("silver.enriched_orders")
)
```

### Pattern 5: Deduplication
```python
# Deduplicate streaming data
(spark.readStream
    .table("bronze.raw_events")
    .dropDuplicates(["event_id"])  # Maintain state for dedup
    .withWatermark("event_timestamp", "1 hour")  # Clear old state
    .writeStream
    .option("checkpointLocation", "/checkpoints/silver/deduplicated_events")
    .table("silver.deduplicated_events")
)
```

---

## Troubleshooting

### Issue 1: Checkpoint Corruption
```python
# If stream fails with checkpoint errors:
# 1. Check if stream logic changed (not compatible with existing checkpoint)
# 2. Delete checkpoint and restart (loses progress)
dbutils.fs.rm("/checkpoints/my_stream", recurse=True)

# 3. Or restore from backup if available
```

### Issue 2: Memory Pressure from State
```python
# For stateful operations (aggregations, dedup), limit state growth:
# 1. Use watermarks to clear old state
df.withWatermark("timestamp", "1 day")

# 2. Limit window size
window("timestamp", "1 hour")  # Instead of "1 day"

# 3. Use append mode instead of complete
.outputMode("append")
```

### Issue 3: Slow Processing
```python
# 1. Check Spark UI for bottlenecks
# 2. Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "200")

# 3. Optimize input files (OPTIMIZE bronze tables)
# 4. Enable Photon
# 5. Use trigger(availableNow=True) for batch-like processing
```

---

## Checklist

### Auto Loader Setup
✓ Checkpoint location set and unique per stream  
✓ Schema location specified for schema inference  
✓ File format configured correctly  
✓ Schema evolution mode chosen appropriately  
✓ Metadata columns captured (_source_file, _ingested_timestamp)  
✓ File notification enabled for large file volumes

### Structured Streaming Setup
✓ Checkpoint location set  
✓ Output mode chosen correctly (append/complete/update)  
✓ Trigger mode appropriate for use case  
✓ Watermarks defined for late data handling  
✓ Join conditions include time bounds (for stream-stream joins)  
✓ Error handling and monitoring in place

### Production Readiness
✓ Idempotent operations (MERGE, not INSERT)  
✓ Monitoring and alerting configured  
✓ Checkpoint backed up regularly  
✓ Schema evolution strategy defined  
✓ State management optimized (watermarks, limited windows)  
✓ Performance validated under load

---

**Remember**: Auto Loader simplifies file ingestion with automatic schema inference and evolution. Structured Streaming provides exactly-once processing with checkpoints. Always use `availableNow=True` trigger for batch-like semantics, and set watermarks to manage late data and state growth.
