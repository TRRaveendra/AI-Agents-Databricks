# SCD Type 2 (Slowly Changing Dimension) - Complete Example

## What is SCD Type 2?

SCD Type 2 tracks historical changes in dimension tables by creating a new row for each change, 
preserving the complete history of changes over time.

## Example 1: Create SCD Type 2 Dimension Table

```sql
CREATE TABLE IF NOT EXISTS gold.dim_customer_scd2 (
    -- Surrogate key (system-generated, never changes)
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Business key (from source system)
    customer_id STRING NOT NULL,
    
    -- Dimension attributes (these can change over time)
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    customer_segment STRING,
    
    -- SCD Type 2 tracking columns
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,               -- NULL = current record
    is_current BOOLEAN NOT NULL,           -- TRUE for current version
    
    -- Audit columns
    _created_timestamp TIMESTAMP,
    _updated_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (customer_id, is_current)
COMMENT 'Customer dimension with SCD Type 2 - tracks historical changes';
```

## Example 2: Initial Load (First Time)

```python
from pyspark.sql.functions import col, lit, current_timestamp, current_date

def initial_load_scd2(source_table, target_table):
    """
    Initial load of dimension table - all records marked as current.
    """
    source_df = spark.read.table(source_table)
    
    initial_dim = (source_df
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("_created_timestamp", current_timestamp())
        .withColumn("_updated_timestamp", current_timestamp())
    )
    
    initial_dim.write.mode("overwrite").saveAsTable(target_table)
    print(f"✓ Initial load: {initial_dim.count()} customers")

# Run initial load
initial_load_scd2("silver.customers", "gold.dim_customer_scd2")
```

## Example 3: Incremental SCD Type 2 Updates

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, when, current_date, current_timestamp, lit, md5, concat_ws

def apply_scd2_changes(source_table, target_table):
    """
    Apply SCD Type 2 logic:
    1. Detect changes by comparing source to current target
    2. Expire old versions (set is_current=false, set end_date)
    3. Insert new versions
    """
    # Read source changes
    source_df = spark.read.table(source_table)
    
    # Read current dimension records
    current_dim = spark.read.table(target_table).filter(col("is_current") == True)
    
    # Create hash of attributes to detect changes
    def hash_attributes(df):
        return df.withColumn("_attribute_hash",
            md5(concat_ws("||",
                coalesce(col("customer_name"), lit("")),
                coalesce(col("email"), lit("")),
                coalesce(col("phone"), lit("")),
                coalesce(col("address"), lit("")),
                coalesce(col("customer_segment"), lit(""))
            ))
        )
    
    source_with_hash = hash_attributes(source_df)
    current_with_hash = hash_attributes(current_dim)
    
    # Identify changed records
    changes = (source_with_hash.alias("src")
        .join(
            current_with_hash.alias("tgt"),
            col("src.customer_id") == col("tgt.customer_id"),
            "left"
        )
        .filter(
            # New customer OR attributes changed
            col("tgt.customer_id").isNull() | 
            (col("src._attribute_hash") != col("tgt._attribute_hash"))
        )
        .select("src.*")
    )
    
    if changes.count() == 0:
        print("No changes detected")
        return
    
    print(f"Detected {changes.count()} changes")
    
    # Step 1: Expire old versions
    target = DeltaTable.forName(spark, target_table)
    
    (target.alias("tgt")
        .merge(
            changes.alias("src"),
            "tgt.customer_id = src.customer_id AND tgt.is_current = true"
        )
        .whenMatchedUpdate(set={
            "is_current": "false",
            "effective_end_date": "date_sub(current_date(), 1)",
            "_updated_timestamp": "current_timestamp()"
        })
        .execute()
    )
    
    # Step 2: Insert new versions
    new_versions = (changes
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("_created_timestamp", current_timestamp())
        .withColumn("_updated_timestamp", current_timestamp())
        .drop("_attribute_hash")
    )
    
    new_versions.write.mode("append").saveAsTable(target_table)
    
    print(f"✓ Expired old versions and inserted {new_versions.count()} new versions")

# Apply changes
apply_scd2_changes("silver.customers", "gold.dim_customer_scd2")
```

## Example 4: Efficient SCD Type 2 (Single MERGE)

```python
def apply_scd2_single_merge(source_table, target_table):
    """
    More efficient: Prepare both expired and new records, then single MERGE.
    """
    source_df = spark.read.table(source_table)
    target_df = spark.read.table(target_table)
    
    # Get current records
    current = target_df.filter(col("is_current") == True)
    
    # Detect changes (same as above)
    source_hash = hash_attributes(source_df)
    current_hash = hash_attributes(current)
    
    changed_customers = (source_hash.alias("src")
        .join(current_hash.alias("tgt"), "customer_id", "inner")
        .filter(col("src._attribute_hash") != col("tgt._attribute_hash"))
        .select("src.customer_id")
        .distinct()
    )
    
    # Prepare expired versions
    expired_versions = (target_df
        .join(changed_customers, "customer_id", "inner")
        .filter(col("is_current") == True)
        .withColumn("is_current", lit(False))
        .withColumn("effective_end_date", date_sub(current_date(), 1))
        .withColumn("_updated_timestamp", current_timestamp())
    )
    
    # Prepare new versions
    new_versions = (source_df
        .join(changed_customers, "customer_id", "inner")
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("_created_timestamp", current_timestamp())
        .withColumn("_updated_timestamp", current_timestamp())
    )
    
    # Union expired + new
    all_changes = expired_versions.unionByName(new_versions)
    
    # Single MERGE
    target = DeltaTable.forName(spark, target_table)
    
    (target.alias("tgt")
        .merge(
            all_changes.alias("src"),
            "tgt.customer_key = src.customer_key"
        )
        .whenMatchedUpdate(set={
            "is_current": "src.is_current",
            "effective_end_date": "src.effective_end_date",
            "_updated_timestamp": "src._updated_timestamp"
        })
        .whenNotMatchedInsert(values={
            "customer_id": "src.customer_id",
            "customer_name": "src.customer_name",
            "email": "src.email",
            "effective_start_date": "src.effective_start_date",
            "effective_end_date": "src.effective_end_date",
            "is_current": "src.is_current",
            "_created_timestamp": "src._created_timestamp"
        })
        .execute()
    )
    
    print(f"✓ SCD Type 2 processing complete")

apply_scd2_single_merge("silver.customers", "gold.dim_customer_scd2")
```

## Example 5: Query SCD Type 2 Tables

```sql
-- Get current state only
SELECT customer_id, customer_name, email, customer_segment
FROM gold.dim_customer_scd2
WHERE is_current = true;

-- Point-in-time query (as of specific date)
SELECT customer_id, customer_name, email, customer_segment
FROM gold.dim_customer_scd2
WHERE '2024-06-15' BETWEEN effective_start_date 
    AND COALESCE(effective_end_date, '9999-12-31');

-- Full history for a customer
SELECT 
    customer_key,
    customer_name,
    email,
    customer_segment,
    effective_start_date,
    effective_end_date,
    is_current
FROM gold.dim_customer_scd2
WHERE customer_id = 'CUST_12345'
ORDER BY effective_start_date;

-- Customers who changed segment in last 30 days
SELECT 
    curr.customer_id,
    curr.customer_name,
    prev.customer_segment as old_segment,
    curr.customer_segment as new_segment,
    curr.effective_start_date as change_date
FROM gold.dim_customer_scd2 curr
JOIN gold.dim_customer_scd2 prev
    ON curr.customer_id = prev.customer_id
    AND prev.effective_end_date = date_sub(curr.effective_start_date, 1)
WHERE curr.is_current = true
    AND curr.effective_start_date >= date_sub(current_date(), 30)
    AND curr.customer_segment != prev.customer_segment;
```

## Example 6: Join Fact Table with SCD Type 2 Dimension

```sql
-- Join fact table with dimension (point-in-time accuracy)
SELECT 
    f.sale_id,
    f.sale_date,
    d.customer_name,
    d.customer_segment,  -- Segment at time of sale
    f.sale_amount
FROM gold.fact_sales f
JOIN gold.dim_customer_scd2 d
    ON f.customer_id = d.customer_id
    AND f.sale_date BETWEEN d.effective_start_date 
        AND COALESCE(d.effective_end_date, '9999-12-31')
WHERE f.sale_date = '2024-01-15';
```

## SCD Type 2 Best Practices

✓ Use surrogate keys (never use business key in fact tables)  
✓ Always include is_current flag  
✓ Use NULL for open-ended effective_end_date  
✓ Hash attributes to detect changes efficiently  
✓ Cluster by (business_key, is_current)  
✓ Document what triggers a new version  
✓ Test point-in-time queries  

## Common Pitfalls

❌ Using business key in fact table (breaks history)  
❌ No is_current flag (requires date logic in every query)  
❌ Not handling NULL in effective_end_date  
❌ Creating versions with no actual changes  
❌ Incorrect date boundary logic (gaps or overlaps)  

---

**Remember**: SCD Type 2 is powerful but adds complexity. Only use it for dimensions where historical context matters for reporting.
