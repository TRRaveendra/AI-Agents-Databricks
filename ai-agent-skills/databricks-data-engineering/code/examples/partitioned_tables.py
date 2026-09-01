# Legacy Partitioning Examples (Use Clustering Instead!)

## ⚠️ Important Notice

**Liquid clustering is preferred over partitioning for most use cases.**

Only use partitioning when:
- Table is very large (> 1TB)
- Partition column has low cardinality (year, month, region)
- You need partition-level operations (DROP PARTITION)

## Example 1: Partitioned Table Creation

```python
from pyspark.sql.functions import year, month

# Prepare data with partition columns
orders_df = spark.read.table("bronze.raw_orders")

partitioned_df = (orders_df
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
)

# Write with partitioning
partitioned_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .saveAsTable("silver.orders_partitioned")

print("✓ Created partitioned table")
```

## Example 2: Create Partitioned Table with SQL

```sql
CREATE TABLE IF NOT EXISTS silver.orders_partitioned (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount DECIMAL(10, 2),
    order_date DATE,
    order_year INT,
    order_month INT
)
USING DELTA
PARTITIONED BY (order_year, order_month)
COMMENT 'Legacy partitioned table - consider clustering instead';
```

## Example 3: Query with Partition Pruning

```python
# Query with partition filter (efficient)
spark.sql("""
    SELECT * FROM silver.orders_partitioned
    WHERE order_year = 2024 AND order_month = 1
""").show()

# Check that partitions are pruned
spark.sql("""
    SELECT * FROM silver.orders_partitioned
    WHERE order_year = 2024 AND order_month = 1
""").explain()
```

## Example 4: Manage Partitions

```sql
-- Show partitions
SHOW PARTITIONS silver.orders_partitioned;

-- Add partition
ALTER TABLE silver.orders_partitioned ADD IF NOT EXISTS
PARTITION (order_year=2024, order_month=12);

-- Drop old partitions (data retention)
ALTER TABLE silver.orders_partitioned 
DROP IF EXISTS PARTITION (order_year=2020, order_month=1);

-- Repair partitions (after manual file operations)
MSCK REPAIR TABLE silver.orders_partitioned;
```

## Example 5: Optimize Partitioned Tables

```sql
-- Optimize specific partition
OPTIMIZE silver.orders_partitioned
WHERE order_year = 2024 AND order_month = 1;

-- Compact files within partitions
OPTIMIZE silver.orders_partitioned;

-- Z-ORDER within partitions (legacy, use clustering instead)
OPTIMIZE silver.orders_partitioned
ZORDER BY (customer_id);
```

## Common Partitioning Problems

### Problem 1: Small File Problem
```python
# Bad: Creates many small partitions
df.write.partitionBy("customer_id").saveAsTable("table")  # Millions of partitions!

# Good: Use clustering for high-cardinality
df.write.clusterBy("customer_id").saveAsTable("table")
```

### Problem 2: Partition Discovery Overhead
```python
# With many partitions, listing is slow
spark.sql("SELECT COUNT(*) FROM highly_partitioned_table")  # Slow!

# Better: Fewer, larger partitions or use clustering
```

### Problem 3: Uneven Partition Sizes
```python
# Some partitions huge, others tiny
spark.sql("SELECT order_month, COUNT(*) FROM orders GROUP BY order_month").show()
# Dec: 1M rows, Jan: 100 rows <- Unbalanced!

# Solution: Use clustering which handles skew automatically
```

## Migration: Partitioned → Clustered

```python
# Step 1: Read partitioned table
partitioned_df = spark.read.table("silver.orders_partitioned")

# Step 2: Write to new clustered table
partitioned_df.write \
    .format("delta") \
    .mode("overwrite") \
    .clusterBy("order_year", "order_month", "customer_id") \
    .saveAsTable("silver.orders_clustered")

# Step 3: Swap tables
spark.sql("DROP TABLE silver.orders_partitioned")
spark.sql("ALTER TABLE silver.orders_clustered RENAME TO silver.orders_partitioned")

print("✓ Migrated to liquid clustering")
```

## When Partitioning Makes Sense

✓ **Time-series with retention policies**
```sql
-- Easy to drop old months
ALTER TABLE logs DROP PARTITION (year=2020);
```

✓ **Very large tables with clear partition boundaries**
```sql
-- 10TB table with ~100 partitions is reasonable
CREATE TABLE huge_logs (...) PARTITIONED BY (year, month);
```

✓ **Regulatory requirements for data isolation**
```sql
-- Separate by region for GDPR compliance
CREATE TABLE customer_data (...) PARTITIONED BY (region);
```

---

**Remember**: Default to liquid clustering. Only partition if you have a specific reason from the list above.
