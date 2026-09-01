# Liquid Clustering Examples for Databricks Delta Tables

## Why Liquid Clustering?

Liquid clustering is the modern replacement for partitioning in Databricks. Benefits:
- Automatically optimizes data layout based on query patterns
- No small file problem
- Can change clustering columns without full rewrite
- Better for high-cardinality columns
- Handles data skew automatically

## Example 1: Simple Clustered Table

```python
from pyspark.sql.functions import col, current_timestamp

# Create sample data
data = [
    (1, "2024-01-15", 1001, 250.00, "completed"),
    (2, "2024-01-15", 1002, 150.00, "completed"),
    (3, "2024-01-16", 1001, 300.00, "pending"),
    (4, "2024-01-16", 1003, 450.00, "completed"),
    (5, "2024-01-17", 1002, 200.00, "shipped"),
]

orders_df = spark.createDataFrame(data, 
    ["order_id", "order_date", "customer_id", "amount", "status"])

# Create table with liquid clustering
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .clusterBy("customer_id", "order_date") \
    .saveAsTable("silver.orders_clustered")

print("✓ Created clustered table: silver.orders_clustered")
```

## Example 2: Alter Existing Table to Add Clustering

```sql
-- Add clustering to existing table
ALTER TABLE silver.orders 
CLUSTER BY (customer_id, order_date);

-- Run OPTIMIZE to apply clustering
OPTIMIZE silver.orders;

-- Check table properties
DESCRIBE DETAIL silver.orders;
```

## Example 3: Change Clustering Columns

```sql
-- Easy to change clustering (no full rewrite needed!)
ALTER TABLE silver.orders 
CLUSTER BY (order_date, customer_id, status);

-- Run OPTIMIZE to reorganize based on new clustering
OPTIMIZE silver.orders;
```

## Example 4: Complete Clustered Table with Auto-Optimization

```python
# Create production-ready clustered table
def create_clustered_table():
    schema_sql = """
    CREATE TABLE IF NOT EXISTS gold.fact_sales (
        sale_id BIGINT NOT NULL,
        sale_date DATE NOT NULL,
        customer_id BIGINT NOT NULL,
        product_id BIGINT NOT NULL,
        store_id BIGINT,
        quantity INT,
        unit_price DECIMAL(10, 2),
        total_amount DECIMAL(10, 2),
        _created_timestamp TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (sale_date, customer_id)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true'
    )
    COMMENT 'Sales fact table with liquid clustering on date and customer';
    """
    
    spark.sql(schema_sql)
    print("✓ Created gold.fact_sales with clustering")

create_clustered_table()
```

## Example 5: Query Performance with Clustering

```python
# Without clustering: scans all files
spark.sql("""
    SELECT * FROM silver.orders_not_clustered
    WHERE customer_id = 1001 AND order_date = '2024-01-15'
""").explain()

# With clustering: scans only relevant files (data pruning)
spark.sql("""
    SELECT * FROM silver.orders_clustered
    WHERE customer_id = 1001 AND order_date = '2024-01-15'
""").explain()

# Check data skipping metrics
spark.sql("DESCRIBE HISTORY silver.orders_clustered").show()
```

## Clustering Best Practices

1. **Cluster by columns frequently used in WHERE clauses**
   - Order matters: most selective first
   - Typically: date, high-cardinality IDs

2. **Run OPTIMIZE regularly**
   ```sql
   -- Manual optimize
   OPTIMIZE silver.orders;
   
   -- Or enable auto-optimize
   ALTER TABLE silver.orders SET TBLPROPERTIES (
       'delta.autoOptimize.optimizeWrite' = 'true',
       'delta.autoOptimize.autoCompact' = 'true'
   );
   ```

3. **Monitor clustering effectiveness**
   ```sql
   -- Check file count and sizes
   DESCRIBE DETAIL silver.orders;
   
   -- View optimization history
   DESCRIBE HISTORY silver.orders;
   ```

## When to Use Clustering

✓ Tables > 1 GB
✓ Queries filter on specific columns
✓ High-cardinality columns (customer_id, product_id)
✓ Date range queries
✓ Tables that grow over time

❌ Very small tables (< 100 MB)
❌ No consistent query patterns
❌ Tables scanned in full every query

---

**Remember**: Liquid clustering is the default choice for Databricks. Only use legacy partitioning for specific edge cases (very large tables > 1TB with low-cardinality partition keys).
