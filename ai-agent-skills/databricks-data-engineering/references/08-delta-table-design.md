# Delta Table Design and Features

## Overview
Delta Lake is an open-source storage layer that brings ACID transactions, scalable metadata handling, time travel, and unified batch/streaming processing to data lakes.

---

## Table Creation Best Practices

### Standard Delta Table with Liquid Clustering

```sql
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id BIGINT NOT NULL,
    customer_name STRING,
    email STRING,
    region STRING,
    account_status STRING,
    total_purchases DECIMAL(10,2),
    _processed_timestamp TIMESTAMP,
    _ingested_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (region, account_status)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.logRetentionDuration' = 'interval 90 days'
)
COMMENT 'Customer dimension table with SCD Type 2';
```

---

## Core Delta Lake Features

### 1. ACID Transactions

Delta Lake guarantees ACID properties for all operations.

```python
# Multiple concurrent writes - Delta handles conflicts
df1.write.mode("append").saveAsTable("silver.orders")
df2.write.mode("append").saveAsTable("silver.orders")  # Safe, no corruption

# Atomic operation - either fully completes or fully fails
spark.sql("""
    MERGE INTO silver.customers target
    USING updates source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### 2. Time Travel

Query historical versions of tables for auditing, rollback, or reproducing ML models.

#### Query by Version
```sql
-- See table history
DESCRIBE HISTORY silver.customers LIMIT 10;

-- Query specific version
SELECT * FROM silver.customers VERSION AS OF 5;

-- Query as of timestamp
SELECT * FROM silver.customers TIMESTAMP AS OF '2024-01-15 10:00:00';
```

#### PySpark Time Travel
```python
# Read historical version
df_v5 = spark.read.format("delta").option("versionAsOf", 5).table("silver.customers")

# Read as of timestamp
df_hist = (spark.read
    .format("delta")
    .option("timestampAsOf", "2024-01-15")
    .table("silver.customers")
)

# Compare current vs previous
df_current = spark.table("silver.customers")
df_previous = spark.read.format("delta").option("versionAsOf", 10).table("silver.customers")

# Find changes
changes = df_current.subtract(df_previous)
```

#### Rollback Table to Previous Version
```sql
-- Restore table to version 10
RESTORE TABLE silver.customers TO VERSION AS OF 10;

-- Restore to timestamp
RESTORE TABLE silver.customers TO TIMESTAMP AS OF '2024-01-15';
```

### 3. Schema Evolution

Delta Lake supports safe schema changes.

#### Add Column
```sql
-- Add new column
ALTER TABLE silver.customers ADD COLUMN loyalty_tier STRING;

-- Add column with default value (Databricks SQL)
ALTER TABLE silver.customers ADD COLUMN vip_status BOOLEAN DEFAULT false;
```

#### Change Column Type (with rewrite)
```sql
-- Change column type (requires rewrite for some types)
ALTER TABLE silver.customers ALTER COLUMN total_purchases TYPE DECIMAL(12,2);
```

#### Rename Column
```sql
-- Rename column
ALTER TABLE silver.customers RENAME COLUMN customer_name TO full_name;
```

#### Schema Enforcement and Evolution
```python
# Schema enforcement (default) - reject incompatible writes
df_bad_schema.write.mode("append").saveAsTable("silver.customers")
# Raises: AnalysisException - schema mismatch

# Allow schema evolution - automatically add new columns
df_new_columns.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver.customers")
```

```sql
-- Enable auto schema evolution at table level
ALTER TABLE silver.customers 
SET TBLPROPERTIES ('delta.autoMerge.mergeSchema' = 'true');
```

### 4. Change Data Feed (CDF)

Track row-level changes (inserts, updates, deletes) for incremental processing and CDC pipelines.

#### Enable CDF
```sql
-- Enable on existing table
ALTER TABLE silver.customers 
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Enable at creation
CREATE TABLE silver.orders (...)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

#### Read Change Data
```python
# Read changes between versions
changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 5)
    .option("endingVersion", 10)
    .table("silver.customers")
)

# Columns: [original columns] + _change_type, _commit_version, _commit_timestamp
# _change_type: insert, update_preimage, update_postimage, delete

# Read changes since timestamp
changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2024-01-15 00:00:00")
    .table("silver.customers")
)

# Process only inserts and updates (ignore deletes)
new_and_updated = changes.filter(col("_change_type").isin(["insert", "update_postimage"]))
```

#### Use Case: Incremental Gold Aggregation
```python
# Incrementally update gold aggregate using CDF
last_version = get_last_processed_version("gold.customer_summary")

changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_version + 1)
    .table("silver.customers")
)

# Aggregate changes
updated_summary = changes.groupBy("region").agg(
    sum("total_purchases").alias("total_revenue"),
    count("*").alias("customer_count")
)

# Merge into gold table
updated_summary.createOrReplaceTempView("updates")
spark.sql("""
    MERGE INTO gold.customer_summary target
    USING updates source
    ON target.region = source.region
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### 5. MERGE (Upsert)

Efficiently insert, update, and delete rows in a single atomic operation.

#### Basic MERGE
```sql
MERGE INTO silver.customers target
USING updates source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

#### MERGE with Conditions
```sql
MERGE INTO silver.customers target
USING updates source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.updated_at > target._processed_timestamp THEN
    UPDATE SET 
        customer_name = source.customer_name,
        email = source.email,
        _processed_timestamp = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, _processed_timestamp)
    VALUES (source.customer_id, source.customer_name, source.email, current_timestamp());
```

#### MERGE with DELETE
```sql
MERGE INTO silver.inventory target
USING inventory_updates source
ON target.product_id = source.product_id
WHEN MATCHED AND source.quantity = 0 THEN DELETE
WHEN MATCHED THEN UPDATE SET target.quantity = source.quantity
WHEN NOT MATCHED THEN INSERT *;
```

#### PySpark MERGE
```python
from delta.tables import DeltaTable

# Load target Delta table
target = DeltaTable.forName(spark, "silver.customers")

# Merge updates
(target.alias("target")
    .merge(
        updates.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdate(
        condition="source.updated_at > target._processed_timestamp",
        set={
            "customer_name": "source.customer_name",
            "email": "source.email",
            "_processed_timestamp": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "customer_id": "source.customer_id",
            "customer_name": "source.customer_name",
            "email": "source.email",
            "_processed_timestamp": "current_timestamp()"
        }
    )
    .execute()
)
```

### 6. DELETE

Delete rows with optional predicates.

```sql
-- Delete specific rows
DELETE FROM silver.customers WHERE account_status = 'closed';

-- Delete with join
DELETE FROM silver.orders
WHERE customer_id IN (SELECT customer_id FROM deleted_customers);
```

```python
from delta.tables import DeltaTable

# Delete rows
target = DeltaTable.forName(spark, "silver.orders")
target.delete("order_status = 'cancelled' AND order_date < '2023-01-01'")
```

### 7. UPDATE

Update rows with conditions.

```sql
-- Update specific rows
UPDATE silver.customers
SET account_status = 'inactive'
WHERE last_order_date < '2023-01-01';

-- Update with complex conditions
UPDATE silver.products
SET 
    price = price * 1.10,
    updated_at = current_timestamp()
WHERE category = 'electronics' AND stock_quantity > 0;
```

---

## Table Optimization

### 1. OPTIMIZE - Compact Small Files

Small files hurt query performance. OPTIMIZE combines small files into larger ones.

```sql
-- Optimize table
OPTIMIZE silver.customers;

-- Optimize with Z-ordering (legacy, use liquid clustering instead)
OPTIMIZE silver.customers ZORDER BY (region, account_status);

-- Optimize specific partition
OPTIMIZE silver.orders WHERE order_date >= '2024-01-01';
```

```python
# Optimize programmatically
spark.sql("OPTIMIZE silver.customers")

# Check file sizes before/after
display(spark.sql("DESCRIBE DETAIL silver.customers"))
```

**When to run OPTIMIZE**:
- After large batch writes
- When file count is high (> 1000 files)
- Query performance degrades
- Regularly (daily/weekly) for active tables

### 2. Auto-Optimize

Enable automatic file compaction and optimized writes.

```sql
-- Enable auto-optimize on existing table
ALTER TABLE silver.customers SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

**Optimized Writes**: Shuffles data during write to reduce file count.  
**Auto Compaction**: Automatically runs OPTIMIZE after writes.

### 3. Liquid Clustering (Recommended)

Modern replacement for partitioning and Z-ordering. Automatically optimizes layout.

```sql
-- Create table with liquid clustering
CREATE TABLE silver.sales (
    sale_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    sale_date DATE,
    amount DECIMAL(10,2)
)
USING DELTA
CLUSTER BY (sale_date, product_id);

-- Add clustering to existing table
ALTER TABLE silver.sales CLUSTER BY (sale_date, customer_id);

-- Change clustering columns
ALTER TABLE silver.sales CLUSTER BY (customer_id, product_id);

-- Remove clustering
ALTER TABLE silver.sales CLUSTER BY NONE;
```

**Benefits**:
- No partition management overhead
- Better query performance than partitioning
- Adapts to query patterns automatically
- No "too many small partitions" problem

### 4. VACUUM - Clean Up Old Files

Delta keeps old data files for time travel. VACUUM deletes them to save storage.

```sql
-- Preview files to be deleted (dry run)
VACUUM silver.customers RETAIN 168 HOURS DRY RUN;

-- Delete files older than retention period
VACUUM silver.customers RETAIN 168 HOURS;

-- Vacuum all tables in schema
-- Note: Run carefully, cannot be undone
VACUUM bronze.raw_orders RETAIN 168 HOURS;
VACUUM silver.clean_orders RETAIN 168 HOURS;
```

```python
# Vacuum programmatically
spark.sql("VACUUM silver.customers RETAIN 168 HOURS")

# Check space savings
before = get_table_size("silver.customers")
spark.sql("VACUUM silver.customers RETAIN 168 HOURS")
after = get_table_size("silver.customers")
print(f"Saved {before - after} bytes")
```

**Important**:
- Default retention: 7 days (168 hours)
- Can't time travel before retention period
- Set `delta.deletedFileRetentionDuration` for automatic policy
- Don't set retention < 7 days unless you're sure

```sql
-- Configure retention at table level
ALTER TABLE silver.customers SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 30 days'
);
```

### 5. ANALYZE TABLE - Update Statistics

Delta automatically collects statistics, but you can manually trigger updates.

```sql
-- Collect table statistics
ANALYZE TABLE silver.customers COMPUTE STATISTICS;

-- Collect column statistics (for query optimization)
ANALYZE TABLE silver.customers COMPUTE STATISTICS FOR COLUMNS customer_id, region;
```

---

## Constraints and Data Quality

### 1. NOT NULL Constraints
```sql
-- Add NOT NULL constraint
ALTER TABLE silver.customers ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE silver.customers ALTER COLUMN email SET NOT NULL;
```

### 2. CHECK Constraints
```sql
-- Add check constraint
ALTER TABLE silver.customers 
ADD CONSTRAINT valid_email CHECK (email LIKE '%@%');

ALTER TABLE silver.orders 
ADD CONSTRAINT positive_amount CHECK (amount > 0);

-- Drop constraint
ALTER TABLE silver.customers DROP CONSTRAINT valid_email;

-- Show constraints
SHOW TBLPROPERTIES silver.customers;
```

### 3. Primary Key and Foreign Key (Informational)
```sql
-- Add primary key (informational, not enforced)
ALTER TABLE silver.customers 
ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

-- Add foreign key (informational, not enforced)
ALTER TABLE silver.orders 
ADD CONSTRAINT fk_customer 
FOREIGN KEY (customer_id) REFERENCES silver.customers(customer_id);
```

**Note**: Primary and foreign keys are informational only in Delta Lake - not enforced at write time. Use CHECK constraints for enforcement.

---

## Advanced Features

### 1. Table Properties

```sql
-- Set custom properties
ALTER TABLE silver.customers SET TBLPROPERTIES (
    'business_owner' = 'sales_team',
    'data_classification' = 'confidential',
    'refresh_frequency' = 'daily'
);

-- View properties
SHOW TBLPROPERTIES silver.customers;

-- Remove property
ALTER TABLE silver.customers UNSET TBLPROPERTIES ('refresh_frequency');
```

### 2. Table Comments and Column Comments
```sql
-- Add table comment
COMMENT ON TABLE silver.customers IS 'Customer master table with SCD Type 2 tracking';

-- Add column comments
ALTER TABLE silver.customers ALTER COLUMN customer_id COMMENT 'Unique customer identifier';
ALTER TABLE silver.customers ALTER COLUMN _processed_timestamp COMMENT 'Timestamp when record was processed into silver layer';
```

### 3. Shallow Clone vs Deep Clone

```sql
-- Shallow clone (metadata only, shares data files)
CREATE TABLE silver.customers_dev SHALLOW CLONE silver.customers;

-- Deep clone (full copy of data)
CREATE TABLE silver.customers_backup DEEP CLONE silver.customers;

-- Clone at specific version
CREATE TABLE silver.customers_snapshot 
SHALLOW CLONE silver.customers VERSION AS OF 10;
```

**Use shallow clone** for testing, dev environments (fast, no storage duplication).  
**Use deep clone** for backups, long-term snapshots.

---

## Best Practices

### Table Design Checklist

✓ Use **managed tables** in Unity Catalog unless external storage required  
✓ Apply **liquid clustering** on columns used in WHERE/JOIN (not legacy partitioning)  
✓ Enable **Change Data Feed** if downstream needs incremental processing  
✓ Enable **auto-optimize** for write-heavy tables  
✓ Set **appropriate retention** periods (deletedFileRetentionDuration, logRetentionDuration)  
✓ Add **NOT NULL constraints** on required columns  
✓ Add **CHECK constraints** for data quality rules  
✓ Include **audit columns** (_processed_timestamp, _ingested_timestamp, _source_system)  
✓ Add **table and column comments** for documentation  
✓ Run **OPTIMIZE** regularly on large tables  
✓ Run **VACUUM** to control storage costs  
✓ Use **MERGE** for upserts instead of DELETE + INSERT

### Maintenance Schedule

**Daily**:
- OPTIMIZE high-write tables (if auto-optimize not enabled)

**Weekly**:
- ANALYZE TABLE for query optimization
- Review table sizes and file counts

**Monthly**:
- VACUUM tables (after verifying time travel needs)
- Review table properties and retention settings
- Audit constraints and data quality rules

---

## Common Patterns

### Pattern 1: SCD Type 2 with MERGE
```sql
MERGE INTO silver.customers target
USING (
    SELECT 
        customer_id, 
        customer_name, 
        email,
        current_timestamp() as effective_start_date
    FROM updates
) source
ON target.customer_id = source.customer_id 
   AND target.is_current = true
WHEN MATCHED AND (
    target.customer_name != source.customer_name OR
    target.email != source.email
) THEN UPDATE SET
    is_current = false,
    effective_end_date = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_name, email,
    effective_start_date, effective_end_date, is_current
) VALUES (
    source.customer_id, source.customer_name, source.email,
    source.effective_start_date, NULL, true
);

-- Insert new versions of changed records
INSERT INTO silver.customers
SELECT 
    customer_id, customer_name, email,
    current_timestamp() as effective_start_date,
    NULL as effective_end_date,
    true as is_current
FROM updates u
WHERE EXISTS (
    SELECT 1 FROM silver.customers c
    WHERE c.customer_id = u.customer_id
        AND c.is_current = false
        AND c.effective_end_date = current_timestamp()
);
```

### Pattern 2: Incremental Processing with CDF
```python
# Track last processed version in control table
def get_last_version(table_name):
    return spark.sql(f"""
        SELECT COALESCE(MAX(last_version), 0) 
        FROM control.processing_log 
        WHERE table_name = '{table_name}'
    """).collect()[0][0]

last_version = get_last_version("silver.customers")

# Read changes since last version
changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_version + 1)
    .table("silver.customers")
)

# Process changes
process_changes(changes)

# Update control table
current_version = spark.sql("DESCRIBE HISTORY silver.customers LIMIT 1").select("version").collect()[0][0]
spark.sql(f"""
    INSERT INTO control.processing_log VALUES 
    ('silver.customers', {current_version}, current_timestamp())
""")
```

### Pattern 3: Data Quality with Constraints
```sql
-- Create table with comprehensive constraints
CREATE TABLE silver.orders (
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    order_amount DECIMAL(10,2) NOT NULL,
    order_status STRING NOT NULL,
    _processed_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (order_date, customer_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Add constraints
ALTER TABLE silver.orders ADD CONSTRAINT positive_amount CHECK (order_amount > 0);
ALTER TABLE silver.orders ADD CONSTRAINT valid_status CHECK (order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled'));
ALTER TABLE silver.orders ADD CONSTRAINT valid_date CHECK (order_date >= '2020-01-01');

-- Writes violating constraints will fail
INSERT INTO silver.orders VALUES (1, 100, '2024-01-15', -50.00, 'confirmed', current_timestamp());
-- Error: CHECK constraint valid_amount violated
```

---

**Remember**: Delta Lake provides enterprise-grade reliability. Use time travel for auditing, CDF for incremental processing, OPTIMIZE for performance, and constraints for data quality. Always test schema changes and MERGE operations in dev first.
