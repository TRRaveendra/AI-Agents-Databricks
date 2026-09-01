# Performance Optimization

## Overview
Performance optimization on Databricks involves table design, query patterns, cluster configuration, and leveraging Photon and Adaptive Query Execution (AQE).

---

## Primary Optimization Strategies

### 1. Liquid Clustering

**Best practice for Delta tables > 1GB with common filter/join patterns.**

```sql
-- Create table with liquid clustering
CREATE TABLE silver.sales (
    sale_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    region STRING,
    sale_date DATE,
    amount DECIMAL(10,2)
)
USING DELTA
CLUSTER BY (region, sale_date);

-- Add clustering to existing table
ALTER TABLE silver.sales CLUSTER BY (region, customer_id);
```

**Choosing clustering columns**:
1. Columns in WHERE clauses (filter predicates)
2. Columns in JOIN conditions
3. High cardinality columns first (customer_id before region)
4. Limit to 3-4 columns maximum

**Run OPTIMIZE to apply clustering**:
```sql
OPTIMIZE silver.sales;
```

---

### 2. Avoid Small Files

Small files (< 128 MB) dramatically hurt read performance.

#### Check File Count and Size
```sql
-- View file statistics
DESCRIBE DETAIL silver.customers;

-- Shows: numFiles, sizeInBytes
-- Problem: numFiles > 1000 or avg file size < 100 MB
```

#### Solutions

**A. Enable Auto-Optimize**
```sql
ALTER TABLE silver.customers SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

**B. Manual OPTIMIZE**
```sql
-- Run OPTIMIZE regularly
OPTIMIZE silver.customers;

-- Optimize specific partition/filter
OPTIMIZE silver.orders WHERE order_date >= '2024-01-01';
```

**C. Repartition Before Write**
```python
# Control file size by repartitioning
df.repartition(10).write.mode("append").saveAsTable("silver.customers")

# Or coalesce for small DataFrames
df.coalesce(1).write.mode("overwrite").saveAsTable("silver.lookup_table")
```

---

### 3. Broadcast Small Tables

For joins with small tables (< 10 MB), broadcast to avoid shuffles.

```python
from pyspark.sql.functions import broadcast

# Explicit broadcast
large_df.join(broadcast(small_df), "customer_id")

# Check broadcast threshold
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")  # Default: 10MB

# Adjust threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")  # 20MB
```

**When to broadcast**:
- Dimension tables (customers, products, regions)
- Lookup tables
- Reference data
- Tables < 10-20 MB

**When NOT to broadcast**:
- Tables > 50 MB (memory pressure)
- Frequently updated tables
- Tables in streaming queries

---

### 4. Handle Data Skew

Skew occurs when data is unevenly distributed, causing some tasks to take much longer.

#### Identify Skew
```python
# Check distribution
df.groupBy("customer_id").count().orderBy(desc("count")).show()

# Problem: One customer_id has 10M rows, others have ~1000
```

#### Solution A: Salting
```python
from pyspark.sql.functions import col, concat, lit, rand, floor

# Add salt column
skewed_df = df.withColumn("salt", floor(rand() * 10))
small_df = small_df.withColumn("salt", lit(None))

# Explode small table
small_exploded = small_df.select("*").crossJoin(
    spark.range(10).select(col("id").alias("salt"))
)

# Join on key + salt
result = skewed_df.join(small_exploded, 
    (skewed_df.customer_id == small_exploded.customer_id) & 
    (skewed_df.salt == small_exploded.salt)
)
```

#### Solution B: Skew Hint (Databricks SQL)
```sql
-- Skew hint for joins
SELECT /*+ SKEW('large', 'customer_id') */ 
    large.*, small.*
FROM large_table large
JOIN small_table small ON large.customer_id = small.customer_id;
```

#### Solution C: Adaptive Query Execution (AQE)
```python
# Enable AQE (enabled by default on Databricks)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# AQE automatically detects and handles skew
```

---

### 5. Minimize Shuffle Operations

Shuffle operations are expensive - they move data across executors.

#### Identify Shuffles
```python
# View query plan
df.explain()

# Look for: "Exchange", "Sort", "ShuffleExchange"
```

#### Reduce Shuffles

**A. Filter Early**
```python
# BAD: Filter after expensive operations
df.join(other_df, "key").groupBy("region").agg(sum("amount")).filter(col("region") == "US")

# GOOD: Filter before join
df.filter(col("region") == "US").join(other_df, "key").groupBy("region").agg(sum("amount"))
```

**B. Partition Before Multiple Aggregations**
```python
# BAD: Multiple shuffles
df.groupBy("customer_id").agg(sum("amount")).join(...)
df.groupBy("customer_id").agg(count("*")).join(...)

# GOOD: Partition once, reuse
df_partitioned = df.repartition("customer_id").cache()
agg1 = df_partitioned.groupBy("customer_id").agg(sum("amount"))
agg2 = df_partitioned.groupBy("customer_id").agg(count("*"))
```

**C. Use Bucketing for Repeated Joins**
```python
# For ETL jobs that repeatedly join same tables
df.write \
    .bucketBy(100, "customer_id") \
    .sortBy("order_date") \
    .saveAsTable("silver.orders")
```

---

## Adaptive Query Execution (AQE)

AQE dynamically optimizes queries at runtime. **Enabled by default on Databricks.**

### AQE Features

#### 1. Dynamically Coalesce Shuffle Partitions
Reduces partition count after shuffle to avoid small partitions.

```python
# Enable AQE features
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Configure target partition size
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128MB
```

#### 2. Dynamically Switch Join Strategies
Converts sort-merge join to broadcast join if one side is small.

```python
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10485760")  # 10MB
```

#### 3. Dynamically Optimize Skew Joins
Splits skewed partitions and processes them separately.

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")  # 256MB
```

---

## Photon Engine

Photon is a vectorized query engine that accelerates SQL and DataFrame queries.

### Enable Photon
Photon is enabled at cluster creation:
- Select **Photon Acceleration** when creating cluster
- Available on Databricks Runtime 9.1+
- Works automatically - no code changes needed

### When to Use Photon
✓ Large data scans (> 1 TB)  
✓ Complex SQL queries with multiple joins  
✓ Aggregations on large datasets  
✓ Heavy use of SQL functions  
✓ ETL workloads with high data volumes

### Photon Performance Tips
1. **Use columnar formats** - Parquet, Delta (best performance)
2. **Avoid UDFs** - Photon can't accelerate Python/Scala UDFs
3. **Use built-in functions** - SQL functions are Photon-optimized
4. **Enable auto-optimize** - Photon benefits from fewer, larger files

---

## Caching Strategies

### 1. DataFrame Cache
Cache DataFrames that are reused multiple times.

```python
# Cache DataFrame
df = spark.table("silver.customers").filter(col("region") == "US")
df.cache()

# Use cached DataFrame multiple times
df.groupBy("state").count().show()
df.groupBy("city").count().show()

# Unpersist when done
df.unpersist()
```

### 2. Delta Cache (Disk-Based)
Databricks automatically caches frequently accessed data on local SSD.

```python
# Delta cache is automatic, but you can optimize:
# 1. Use Photon-enabled clusters (better cache utilization)
# 2. Query same data multiple times in session
# 3. Use CACHE SELECT for SQL queries

# Preload data into cache
spark.sql("CACHE SELECT * FROM silver.customers WHERE region = 'US'")

# Clear cache
spark.sql("UNCACHE TABLE silver.customers")
```

### 3. When to Cache
✓ DataFrame/table used 2+ times in same job  
✓ Small to medium data (< 1 TB)  
✓ Iterative algorithms (ML training)  
✓ Interactive analysis

✗ Large data (> memory capacity)  
✗ Used only once  
✗ Source data changes frequently

---

## Query Optimization Techniques

### 1. Predicate Pushdown
Filters are pushed to the data source, reducing data read.

```python
# Automatic predicate pushdown
df = spark.table("silver.orders").filter(col("order_date") >= "2024-01-01")
# Only reads data for 2024 and later
```

### 2. Projection Pushdown (Column Pruning)
Only read columns actually used.

```python
# GOOD: Only read needed columns
df = spark.table("silver.orders").select("order_id", "amount", "order_date")

# BAD: Read all columns then select
df = spark.table("silver.orders").select("*").select("order_id", "amount")
```

### 3. Partition Pruning
Delta automatically skips partitions based on filters.

```python
# With liquid clustering on sale_date
df = spark.table("silver.sales").filter(col("sale_date") == "2024-01-15")
# Only reads files containing 2024-01-15 data
```

### 4. Dynamic Partition Pruning (DPP)
Prunes fact table partitions based on dimension table filters.

```python
# Automatically enabled in Databricks
# Example: Only read orders for US customers
us_customers = spark.table("silver.customers").filter(col("region") == "US")
orders = spark.table("silver.orders")

# DPP prunes orders table based on us_customers
result = orders.join(us_customers, "customer_id")
```

---

## Cluster Configuration

### 1. Choose Right Cluster Type

**Single Node** (1 worker):
- Small data (< 10 GB)
- Development/testing
- Single-user notebooks

**Multi-Node** (2+ workers):
- Large data (> 100 GB)
- Production ETL
- Parallel processing

**Job Clusters**:
- Scheduled jobs
- Cost-effective (auto-terminates)
- Right-sized for workload

**All-Purpose Clusters**:
- Interactive development
- Multiple users
- Persistent across sessions

### 2. Worker Type Selection

**Memory-Optimized** (e.g., r5 instances):
- Large aggregations
- Joins on large tables
- Caching large datasets

**Compute-Optimized** (e.g., c5 instances):
- Complex transformations
- CPU-heavy operations
- ML feature engineering

**Storage-Optimized** (e.g., i3 instances):
- Large data scans
- ETL with high I/O
- Delta cache benefits

### 3. Autoscaling Configuration
```python
# Configure autoscaling
# Min workers: 2, Max workers: 10
# Scales based on workload demand

# For batch jobs: Disable autoscaling, use fixed size
# For interactive: Enable autoscaling for cost savings
```

---

## Troubleshooting Performance Issues

### Issue 1: Slow Queries

#### Diagnosis
```sql
-- Check query profile
-- Run query, then check "View Query Profile" in UI

-- Check table statistics
DESCRIBE DETAIL silver.customers;

-- View execution plan
EXPLAIN SELECT * FROM silver.customers WHERE region = 'US';
```

#### Solutions
1. Add liquid clustering on filter columns
2. Run OPTIMIZE to compact small files
3. Enable Photon
4. Check for data skew
5. Verify predicate pushdown is working

---

### Issue 2: Out of Memory Errors

#### Diagnosis
```python
# Check DataFrame size
df.cache().count()
spark.catalog.cacheTable("silver.customers")

# View cached data size in Spark UI -> Storage tab
```

#### Solutions
1. Reduce `spark.sql.shuffle.partitions` for small data
2. Increase `spark.driver.memory` or `spark.executor.memory`
3. Don't cache unnecessarily large DataFrames
4. Use `.unpersist()` when done with cached data
5. Process data in chunks instead of all at once

---

### Issue 3: Shuffle Spill to Disk

#### Diagnosis
```
# Look in Spark UI -> Stages
# "Spill (Memory)" and "Spill (Disk)" columns show spill size
```

#### Solutions
1. Increase executor memory
2. Reduce `spark.sql.shuffle.partitions`
3. Filter data earlier to reduce shuffle size
4. Use broadcast joins for small tables
5. Handle data skew

---

### Issue 4: Task Skew (Some Tasks Take Much Longer)

#### Diagnosis
```python
# In Spark UI -> Stages, check task duration distribution
# Problem: Max task duration >> median task duration
```

#### Solutions
1. Enable AQE skew join optimization
2. Use salting technique
3. Repartition skewed data
4. Use skew hints in SQL

---

## Performance Checklist

### Table Design
✓ Use **liquid clustering** for large tables (not legacy partitioning)  
✓ Enable **auto-optimize** (optimizeWrite + autoCompact)  
✓ Run **OPTIMIZE** regularly to compact files  
✓ Keep file sizes between **128 MB - 1 GB**  
✓ Aim for **< 1000 files per table**  
✓ Use **VACUUM** to clean up old files

### Query Optimization
✓ **Filter early** in query plans  
✓ **Select only needed columns** (projection pushdown)  
✓ **Broadcast small tables** in joins (< 10 MB)  
✓ **Cache DataFrames** used multiple times  
✓ Avoid expensive operations on large datasets (UDFs, explode)  
✓ Use **built-in Spark functions** instead of UDFs

### Cluster Configuration
✓ Enable **Photon** for SQL/DataFrame workloads  
✓ Enable **AQE** (enabled by default)  
✓ Choose appropriate **worker type** for workload  
✓ Use **job clusters** for scheduled jobs  
✓ Enable **autoscaling** for interactive workloads  
✓ Set correct **driver/executor memory**

### Monitoring
✓ Check **Spark UI** for bottlenecks  
✓ Review **query profiles** for slow queries  
✓ Monitor **shuffle spill** to disk  
✓ Track **task duration** distribution (skew)  
✓ Monitor **Delta cache hit rate**  
✓ Review **file counts and sizes** regularly

---

## Cost Optimization

### 1. Right-Size Clusters
```python
# Don't over-provision
# Start small, scale up if needed
# Monitor CPU/memory usage in Spark UI
```

### 2. Use Spot Instances (for non-critical jobs)
```python
# Configure spot instances for workers
# Savings: 60-90% vs on-demand
# Trade-off: Can be interrupted
```

### 3. Auto-Terminate Clusters
```python
# Set auto-termination (e.g., 30 minutes)
# Saves costs when clusters idle
```

### 4. Optimize Storage
```sql
-- Run VACUUM to reduce storage costs
VACUUM silver.customers RETAIN 168 HOURS;

-- Archive old data to cheaper storage
-- Move infrequently accessed data to external tables
```

### 5. Scheduled Jobs Instead of Always-On Clusters
```python
# Use scheduled jobs with job clusters
# Clusters start, run job, terminate automatically
# More cost-effective than persistent clusters
```

---

## Monitoring and Observability

### 1. Spark UI
- **Jobs tab**: Overall job progress
- **Stages tab**: Task-level metrics, shuffle sizes
- **Storage tab**: Cached DataFrames and tables
- **Executors tab**: CPU, memory usage per executor
- **SQL tab**: Query execution plans

### 2. Query Profile
- Available in SQL Warehouse queries
- Shows operator-level metrics
- Identifies bottlenecks (scans, joins, aggregations)

### 3. Table Metrics
```sql
-- Monitor table growth
DESCRIBE DETAIL silver.customers;

-- Check file count trend
-- Run weekly, alert if numFiles > 1000

-- Monitor table history
DESCRIBE HISTORY silver.customers;
```

---

## Advanced Tips

### 1. Broadcast Nested DataFrames in UDFs
```python
# Broadcast small reference data for use in UDFs
lookup_broadcast = spark.sparkContext.broadcast(lookup_dict)

def enrich_udf(value):
    return lookup_broadcast.value.get(value, "Unknown")
```

### 2. Use Vectorized UDFs (pandas UDF)
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Much faster than row-by-row UDFs
@pandas_udf("double")
def complex_calculation(amounts: pd.Series) -> pd.Series:
    return amounts * 1.1 + 5.0

df.withColumn("adjusted_amount", complex_calculation(col("amount")))
```

### 3. Pushdown Filters to External Sources
```python
# When reading from JDBC, push filters to source database
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "orders")
    .option("pushDownPredicate", "True")  # Enable pushdown
    .load()
    .filter(col("order_date") >= "2024-01-01")  # Pushed to database
)
```

### 4. Optimize Window Functions
```python
from pyspark.sql.window import Window

# BAD: Unbounded window (expensive)
window = Window.partitionBy("customer_id").orderBy("order_date")
df.withColumn("running_total", sum("amount").over(window))

# GOOD: Bounded window (cheaper)
window = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(-7, 0)
df.withColumn("last_7_days_total", sum("amount").over(window))
```

---

**Remember**: Performance optimization is iterative. Start with table design (liquid clustering, file sizes), then query patterns (filters, broadcasts), then cluster configuration (Photon, AQE). Always measure before and after to validate improvements.
