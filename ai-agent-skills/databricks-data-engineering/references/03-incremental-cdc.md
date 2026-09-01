# Incremental Processing and CDC

## Timestamp-Based Incremental Load

### Basic watermark pattern
```python
from pyspark.sql.functions import col, current_timestamp, max as spark_max

# Get last watermark from target table
last_watermark = spark.sql("""
    SELECT COALESCE(MAX(updated_at), '1900-01-01 00:00:00') as max_updated
    FROM silver.customers
""").collect()[0]['max_updated']

# Read only new/updated records from source
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", f"(SELECT * FROM source.customers WHERE updated_at > '{last_watermark}') as new_data")
    .load()
    .withColumn("_processed_timestamp", current_timestamp())
)

# Upsert into target table (update existing, insert new)
df.write.mode("append").saveAsTable("silver.customers")
```

### Watermark with safe buffer (handle clock skew)
```python
# Add 5-minute buffer to handle clock skew between systems
last_watermark_buffered = spark.sql("""
    SELECT COALESCE(MAX(updated_at) - INTERVAL 5 MINUTES, '1900-01-01') as safe_watermark
    FROM silver.customers
""").collect()[0]['safe_watermark']

# This prevents missing records due to time sync issues
```

## Change Data Capture (CDC)

### CDC with operation type (INSERT/UPDATE/DELETE)
```python
# Source provides CDC feed with operation column
cdc_df = (spark.read
    .format("delta")
    .table("bronze.customers_cdc")
    .filter(col("_captured_timestamp") > last_watermark)
)

# Apply CDC operations using MERGE
from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "silver.customers")

(target_table.alias("target")
    .merge(
        cdc_df.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedDelete(condition="source.operation = 'DELETE'")
    .whenMatchedUpdate(
        condition="source.operation = 'UPDATE'",
        set={
            "customer_name": "source.customer_name",
            "email": "source.email",
            "updated_at": "source.updated_at",
            "_processed_timestamp": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        condition="source.operation = 'INSERT'",
        values={
            "customer_id": "source.customer_id",
            "customer_name": "source.customer_name",
            "email": "source.email",
            "created_at": "source.created_at",
            "updated_at": "source.updated_at",
            "_processed_timestamp": "current_timestamp()"
        }
    )
    .execute()
)
```

## Delta Change Data Feed (CDF)

### Enabling CDF on a table
```sql
-- Enable CDF for tracking all changes
ALTER TABLE silver.customers 
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### Reading changes from CDF
```python
# Read only changes since last processed version
last_version = spark.sql("""
    SELECT COALESCE(MAX(source_version), 0) as last_ver 
    FROM gold.customer_history
""").collect()[0]['last_ver']

# Get all changes since that version
changes_df = (spark.read
    .format("delta")
    .option("readChangeData", "true")
    .option("startingVersion", last_version + 1)
    .table("silver.customers")
)

# Process changes (update_preimage, update_postimage, insert, delete)
changes_df.show()
```

### Applying CDF to target table
```python
# Stream changes from source table with CDF enabled
changes_stream = (spark.readStream
    .format("delta")
    .option("readChangeData", "true")
    .option("startingVersion", 0)
    .table("silver.orders")
)

# Process each change type
def process_changes(batch_df, batch_id):
    inserts = batch_df.filter(col("_change_type") == "insert")
    updates = batch_df.filter(col("_change_type").isin("update_postimage"))
    deletes = batch_df.filter(col("_change_type") == "delete")
    
    # Apply inserts
    if inserts.count() > 0:
        inserts.write.mode("append").saveAsTable("gold.orders_history")
    
    # Apply updates (could trigger downstream recalculations)
    if updates.count() > 0:
        # Custom update logic here
        pass

(changes_stream.writeStream
    .foreachBatch(process_changes)
    .trigger(processingTime="5 minutes")
    .start()
)
```

## Handling Inserts, Updates, and Deletes

### MERGE pattern (UPSERT)
```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Read incremental data
incremental_df = spark.read.table("bronze.orders_incremental")

# Load target Delta table
target_table = DeltaTable.forName(spark, "silver.orders")

# Perform MERGE (upsert: insert new, update existing)
(target_table.alias("target")
    .merge(
        incremental_df.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(set={
        "order_amount": "source.order_amount",
        "order_status": "source.order_status",
        "updated_at": "source.updated_at",
        "_processed_timestamp": "current_timestamp()"
    })
    .whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "order_amount": "source.order_amount",
        "order_status": "source.order_status",
        "created_at": "source.created_at",
        "updated_at": "source.updated_at",
        "_processed_timestamp": "current_timestamp()"
    })
    .execute()
)
```

### Handling deletes (soft delete pattern)
```python
# Add is_deleted flag to track deletions
incremental_df_with_flag = incremental_df.withColumn(
    "is_deleted", 
    when(col("operation") == "DELETE", True).otherwise(False)
)

# MERGE with soft delete
(target_table.alias("target")
    .merge(
        incremental_df_with_flag.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(
        condition="source.is_deleted = true",
        set={"is_deleted": "true", "deleted_at": "current_timestamp()"}
    )
    .whenMatchedUpdate(
        condition="source.is_deleted = false",
        set={
            "order_amount": "source.order_amount",
            "order_status": "source.order_status",
            "updated_at": "source.updated_at"
        }
    )
    .whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "order_amount": "source.order_amount",
        "is_deleted": "false"
    })
    .execute()
)
```

### Hard delete pattern (actually remove records)
```python
# MERGE with actual delete
(target_table.alias("target")
    .merge(
        incremental_df.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedDelete(condition="source.operation = 'DELETE'")
    .whenMatchedUpdate(
        condition="source.operation = 'UPDATE'",
        set={
            "order_amount": "source.order_amount",
            "updated_at": "source.updated_at"
        }
    )
    .whenNotMatchedInsert(
        condition="source.operation = 'INSERT'",
        values={
            "order_id": "source.order_id",
            "order_amount": "source.order_amount"
        }
    )
    .execute()
)
```

## Idempotency

### Hash-based deduplication
```python
from pyspark.sql.functions import md5, concat_ws, col

# Create hash of all columns to detect duplicates
df_with_hash = df.withColumn(
    "_hash_key",
    md5(concat_ws("||", 
        col("order_id"), 
        col("customer_id"), 
        col("order_amount"), 
        col("order_date")
    ))
)

# Deduplicate before merging
df_deduped = (df_with_hash
    .withColumn("row_num", row_number().over(
        Window.partitionBy("order_id").orderBy(col("updated_at").desc())
    ))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# Now safe to merge - no duplicate keys
```

### Idempotent MERGE (avoid duplicate processing)
```python
# Add processing batch ID to track what's been processed
processing_batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

df_with_batch = df.withColumn("_processing_batch_id", lit(processing_batch_id))

# MERGE only if not already processed
(target_table.alias("target")
    .merge(
        df_with_batch.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(
        condition="target._processing_batch_id != source._processing_batch_id",
        set={
            "order_amount": "source.order_amount",
            "_processing_batch_id": "source._processing_batch_id",
            "_processed_timestamp": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "order_amount": "source.order_amount",
        "_processing_batch_id": "source._processing_batch_id",
        "_processed_timestamp": "current_timestamp()"
    })
    .execute()
)
```

## Handling Late-Arriving Data

### Grace period pattern
```python
# Accept data up to 7 days late
grace_period_days = 7

# Filter late-arriving data
late_data_df = (incremental_df
    .filter(col("event_date") >= date_sub(current_date(), grace_period_days))
)

# MERGE late data back into target
(target_table.alias("target")
    .merge(
        late_data_df.alias("source"),
        "target.event_id = source.event_id AND target.event_date = source.event_date"
    )
    .whenMatchedUpdate(set={
        "metric_value": "source.metric_value",
        "updated_at": "current_timestamp()"
    })
    .whenNotMatchedInsert(values={
        "event_id": "source.event_id",
        "event_date": "source.event_date",
        "metric_value": "source.metric_value"
    })
    .execute()
)
```

### Watermark with windowing
```python
# Structured streaming with watermark for late data
late_data_stream = (spark.readStream
    .format("delta")
    .table("bronze.events")
    .withWatermark("event_timestamp", "2 hours")  # Accept up to 2 hours late
    .groupBy(
        window(col("event_timestamp"), "1 hour"),
        col("user_id")
    )
    .agg(count("*").alias("event_count"))
)

(late_data_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .toTable("silver.hourly_events")
)
```

## Best Practices

1. **Always track watermarks** - Don't reprocess the entire table
2. **Use MERGE for upserts** - More efficient than delete + insert
3. **Add buffer to watermark** - Handle clock skew between systems
4. **Make operations idempotent** - Safe to rerun without duplicates
5. **Choose soft vs hard deletes carefully** - Soft delete preserves history
6. **Use hash keys for deduplication** - Detect true duplicates
7. **Handle late-arriving data** - Define grace period based on business rules
8. **Enable CDF when tracking history** - Simplifies downstream change propagation
9. **Monitor watermark lag** - Detect processing delays early

## Anti-patterns to Avoid

❌ **No watermark tracking** - Reprocessing entire table every time  
❌ **Not handling deletes** - Data inconsistency with source  
❌ **Ignoring late data** - Missing records in aggregates  
❌ **No deduplication** - Duplicate keys cause MERGE failures  
❌ **Hard deletes without audit trail** - Can't recover or audit  
❌ **Clock skew not handled** - Missing records at boundary  
❌ **Processing non-idempotent** - Reprocessing creates duplicates

---

**Remember**: Incremental processing is about efficiency, but must handle edge cases (late data, deletes, clock skew) to maintain correctness.
