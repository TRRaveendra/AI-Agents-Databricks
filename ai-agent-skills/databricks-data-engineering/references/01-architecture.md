# Medallion Architecture (Bronze → Silver → Gold)

## Layer Responsibilities

### Bronze Layer (Raw/Landing)
**Purpose**: Ingest raw data exactly as received from source systems

**Characteristics**:
- Minimal to no transformation
- Preserve source data structure and types
- Include audit columns (`_ingested_timestamp`, `_source_file`, `_source_system`)
- Store as Delta tables for ACID guarantees
- Schema enforcement optional (can use schema evolution)
- Idempotent ingestion (can re-run without duplicates)

**What belongs here**:
- Raw CSV/JSON/Parquet files → Delta tables
- Database CDC captures
- API responses
- Streaming event data
- File metadata preservation

**What does NOT belong here**:
- Business logic transformations
- Data type conversions (beyond basic casting for Delta compatibility)
- Joins with other tables
- Aggregations
- Data quality filtering (log issues, don't filter)

**Example bronze patterns**:
```python
# Bronze ingestion with Auto Loader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(source_path)
    .withColumn("_ingested_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name()))

df.writeStream.format("delta").option("checkpointLocation", checkpoint_path).toTable("bronze.raw_events")
```

### Silver Layer (Cleansed/Conformed)
**Purpose**: Clean, validate, and standardize data for analytics

**Characteristics**:
- Apply business rules and data quality checks
- Standardize data types, formats, and naming
- Handle nulls, duplicates, and data quality issues
- Enforce schema strictly
- Denormalize or join when needed for downstream use
- Quarantine or flag bad records
- Still relatively granular (record-level detail preserved)

**What belongs here**:
- Type conversions (strings → dates, numbers)
- Standardization (upper/lower case, date formats, country codes)
- Deduplication logic
- Null handling and default values
- Data quality validation and flagging
- Business key generation
- Referential integrity checks
- Record-level transformations

**What does NOT belong here**:
- Heavy aggregations (belongs in Gold)
- Report-specific joins (belongs in Gold)
- Multi-table business logic requiring complex joins (may belong in Gold)

**Example silver patterns**:
```python
# Silver transformation with data quality
silver_df = (bronze_df
    .withColumn("customer_id", col("customer_id").cast("bigint"))
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("_is_valid", 
        when(col("customer_id").isNull() | col("order_date").isNull(), False)
        .otherwise(True))
    .withColumn("_processed_timestamp", current_timestamp())
)

# Write valid records to silver
silver_df.filter(col("_is_valid") == True).write.mode("append").saveAsTable("silver.orders")

# Quarantine invalid records
silver_df.filter(col("_is_valid") == False).write.mode("append").saveAsTable("silver.orders_quarantine")
```

### Gold Layer (Business/Consumption)
**Purpose**: Business-level aggregates and denormalized tables optimized for analytics

**Characteristics**:
- Aggregated, summarized data
- Multi-table joins implementing business logic
- Dimension and fact tables (star/snowflake schema)
- Optimized for query performance
- Domain-specific business views
- SCD (Slowly Changing Dimension) implementations
- Analytics-ready datasets

**What belongs here**:
- Pre-aggregated metrics (daily/monthly summaries)
- Fact and dimension tables
- Customer 360 views
- Financial reports
- KPI calculations
- ML feature tables
- Dashboard-ready datasets

**Example gold patterns**:
```python
# Gold aggregation for reporting
gold_df = (silver_orders
    .join(silver_customers, "customer_id")
    .join(silver_products, "product_id")
    .groupBy("order_date", "customer_segment", "product_category")
    .agg(
        count("order_id").alias("total_orders"),
        sum("order_amount").alias("total_revenue"),
        avg("order_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    )
)

gold_df.write.mode("overwrite").saveAsTable("gold.daily_sales_summary")
```

## Common Architecture Decisions

### When to use batch vs streaming

**Batch (triggered/scheduled)**:
- Source data arrives in batches (daily files, database extracts)
- Near-real-time not required (hourly/daily refresh acceptable)
- Complex transformations requiring full dataset context
- Lower cost preference (streaming has continuous compute cost)

**Streaming**:
- Source data is continuous (event streams, CDC, log files)
- Low latency required (minutes)
- Incremental processing benefits (process only new data)
- Real-time dashboards or alerts needed

**Hybrid approach**: Bronze streaming → Silver streaming → Gold batch (common pattern)

### How many layers should you use?

**Standard: Bronze → Silver → Gold** (recommended for most projects)

**Simplified: Bronze → Gold** (only if):
- Very simple transformations (no complex business logic)
- Small project with limited scope
- Source data already clean and well-structured

**Extended: Bronze → Silver → Gold → Platinum/Diamond** (only if):
- Multiple business domains consuming different aggregates
- ML features require additional derived layers
- Clear organizational benefit from extra layer

**Rule**: Don't add layers "just because". Each layer has cost (storage, compute, maintenance).

### Naming conventions

**Catalogs/Schemas**:
- `bronze` / `bronze_<domain>`
- `silver` / `silver_<domain>`
- `gold` / `gold_<domain>`

**Tables**:
- Bronze: `raw_<source>_<entity>` (e.g., `raw_salesforce_accounts`)
- Silver: `<entity>` (e.g., `customers`, `orders`)
- Gold: `<business_context>` (e.g., `customer_360`, `daily_sales_summary`)

**Audit columns** (prefix with `_` to distinguish from business columns):
- `_ingested_timestamp` - When record arrived in Bronze
- `_processed_timestamp` - When record processed in Silver/Gold
- `_source_file` - Source file name (for Bronze)
- `_source_system` - Source system identifier
- `_is_current` - For SCD Type 2 (current flag)
- `_is_valid` - For data quality flagging
- `_hash_key` - For deduplication or CDC

## Best Practices

1. **Bronze ingestion is idempotent** - Re-running should not create duplicates
2. **Every table has audit columns** - Trace data lineage back to source
3. **Data quality checks happen in Silver** - Bronze preserves everything
4. **Gold optimizes for read performance** - Use liquid clustering, pre-aggregate
5. **Separate quarantine tables** - Don't mix bad records with good
6. **Document business logic** - Complex transformations need clear comments
7. **One-way flow** - Data moves Bronze → Silver → Gold, never backwards
8. **Avoid over-engineering** - Start simple, add complexity only when needed

## Anti-patterns to Avoid

❌ **Transforming data in Bronze** - Bronze should be as raw as possible  
❌ **Skipping Silver layer** - Leads to repeated cleansing logic in Gold  
❌ **Business logic in Bronze** - Keep it in Silver/Gold  
❌ **Aggregating in Silver** - Silver is cleansed details, Gold is aggregates  
❌ **Multiple reads from source** - Ingest once to Bronze, then transform  
❌ **No audit columns** - Makes debugging and lineage impossible  
❌ **Mixing batch and streaming without reason** - Be consistent unless requirements dictate hybrid  

---

**Remember**: The medallion architecture is about separating concerns: Bronze = raw ingestion, Silver = cleansing/standardization, Gold = business logic/aggregation.
