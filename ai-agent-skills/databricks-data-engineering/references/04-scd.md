# Slowly Changing Dimensions (SCD)

## When to Use SCD

**Use SCD Type 1** when:
- You only need current state
- History is not important
- Overwriting old values is acceptable
- Example: Customer email address, phone number corrections

**Use SCD Type 2** when:
- Historical changes must be preserved
- Need to track effective date ranges
- Point-in-time reporting required
- Example: Customer address changes, product price history, employee department changes

## SCD Type 1 (Overwrite)

### Simple overwrite pattern
```python
from delta.tables import DeltaTable

# Load incremental changes
changes_df = spark.read.table("bronze.customer_changes")

# Merge with overwrite on match
target = DeltaTable.forName(spark, "silver.dim_customer")

(target.alias("target")
    .merge(
        changes_df.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdate(set={
        "customer_name": "source.customer_name",
        "email": "source.email",
        "phone": "source.phone",
        "updated_at": "current_timestamp()"
    })
    .whenNotMatchedInsert(values={
        "customer_id": "source.customer_id",
        "customer_name": "source.customer_name",
        "email": "source.email",
        "phone": "source.phone",
        "created_at": "current_timestamp()",
        "updated_at": "current_timestamp()"
    })
    .execute()
)
```

**Result**: Only current state preserved, old values lost

## SCD Type 2 (Track History with Effective Dates)

### Table design for SCD Type 2
```sql
CREATE TABLE silver.dim_customer_scd2 (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    customer_id STRING,                                 -- Business key (natural key)
    customer_name STRING,
    email STRING,
    phone STRING,
    effective_start_date DATE,                         -- When this version became effective
    effective_end_date DATE,                           -- When this version was superseded (NULL = current)
    is_current BOOLEAN,                                -- Flag for current record
    _processed_timestamp TIMESTAMP
) USING DELTA;
```

### SCD Type 2 implementation
```python
from pyspark.sql.functions import col, current_timestamp, lit, current_date, date_add, when
from delta.tables import DeltaTable

# Incoming changes
changes_df = (spark.read.table("bronze.customer_changes")
    .withColumn("effective_start_date", current_date())
    .withColumn("effective_end_date", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
    .withColumn("_processed_timestamp", current_timestamp())
)

# Get target table
target = DeltaTable.forName(spark, "silver.dim_customer_scd2")

# Step 1: Expire old records (set is_current = false, set end date)
(target.alias("target")
    .merge(
        changes_df.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true"
    )
    .whenMatchedUpdate(set={
        "is_current": "false",
        "effective_end_date": "date_sub(source.effective_start_date, 1)"
    })
    .execute()
)

# Step 2: Insert new versions
(target.alias("target")
    .merge(
        changes_df.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true"
    )
    .whenNotMatchedInsert(values={
        "customer_id": "source.customer_id",
        "customer_name": "source.customer_name",
        "email": "source.email",
        "phone": "source.phone",
        "effective_start_date": "source.effective_start_date",
        "effective_end_date": "source.effective_end_date",
        "is_current": "source.is_current",
        "_processed_timestamp": "source._processed_timestamp"
    })
    .execute()
)
```

### Simplified SCD Type 2 (single MERGE)
```python
# More efficient: handle both expire and insert in single MERGE

# Prepare expired versions (for existing records that changed)
expired_df = (changes_df.alias("new")
    .join(
        spark.read.table("silver.dim_customer_scd2").filter(col("is_current") == True).alias("curr"),
        "customer_id"
    )
    .select(
        col("curr.customer_key"),
        col("curr.customer_id"),
        col("curr.customer_name"),
        col("curr.email"),
        col("curr.phone"),
        col("curr.effective_start_date"),
        date_sub(col("new.effective_start_date"), 1).alias("effective_end_date"),
        lit(False).alias("is_current"),
        current_timestamp().alias("_processed_timestamp")
    )
)

# Union expired + new versions
all_changes = expired_df.unionByName(changes_df)

# Single merge to handle both
(target.alias("target")
    .merge(
        all_changes.alias("source"),
        "target.customer_key = source.customer_key"
    )
    .whenMatchedUpdate(set={
        "effective_end_date": "source.effective_end_date",
        "is_current": "source.is_current"
    })
    .whenNotMatchedInsert(values={
        "customer_id": "source.customer_id",
        "customer_name": "source.customer_name",
        "email": "source.email",
        "phone": "source.phone",
        "effective_start_date": "source.effective_start_date",
        "effective_end_date": "source.effective_end_date",
        "is_current": "source.is_current",
        "_processed_timestamp": "source._processed_timestamp"
    })
    .execute()
)
```

### Querying SCD Type 2 tables

**Get current records only**:
```sql
SELECT * FROM silver.dim_customer_scd2 WHERE is_current = true;
```

**Point-in-time query (as of specific date)**:
```sql
SELECT * 
FROM silver.dim_customer_scd2 
WHERE '2024-06-15' BETWEEN effective_start_date AND COALESCE(effective_end_date, '9999-12-31');
```

**Get full history for a customer**:
```sql
SELECT * 
FROM silver.dim_customer_scd2 
WHERE customer_id = 'C12345' 
ORDER BY effective_start_date;
```

## Business Keys vs Surrogate Keys

### Business Key (Natural Key)
- Comes from source system
- Example: `customer_id`, `product_code`, `employee_number`
- May change over time (though ideally stable)
- Used for matching during MERGE

### Surrogate Key
- System-generated unique identifier
- Example: `customer_key`, `dim_customer_sk`
- Never changes or reused
- Used in fact table joins for SCD Type 2

```sql
-- Dimension table: surrogate key + business key
CREATE TABLE silver.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate
    product_code STRING,                               -- Business key
    product_name STRING,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN
) USING DELTA;

-- Fact table: uses surrogate key for joins
CREATE TABLE gold.fact_sales (
    sale_id BIGINT,
    product_key BIGINT,  -- References dim_product.product_key (not product_code)
    customer_key BIGINT,
    sale_amount DECIMAL(10,2),
    sale_date DATE
) USING DELTA;
```

## Handling Deletes in SCD Type 2

### Soft delete (mark as inactive)
```python
# Add is_deleted flag to SCD Type 2 table
deletes_df = (spark.read.table("bronze.customer_deletes")
    .withColumn("is_deleted", lit(True))
)

(target.alias("target")
    .merge(
        deletes_df.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true"
    )
    .whenMatchedUpdate(set={
        "is_current": "false",
        "is_deleted": "true",
        "effective_end_date": "current_date()"
    })
    .execute()
)
```

## Best Practices

1. **Use surrogate keys for SCD Type 2** - Required for fact table joins
2. **Index business keys** - Critical for MERGE performance
3. **Always set is_current flag** - Simplifies queries for current state
4. **Use NULL for open-ended date** - More intuitive than '9999-12-31'
5. **Hash-based change detection** - Only create new version if data actually changed
6. **Document effective date logic** - Clarify timezone and boundary behavior
7. **Consider partitioning by date** - Improves query performance for point-in-time queries

## Anti-patterns to Avoid

❌ **Using business key in fact tables** - Breaks historical reporting with SCD Type 2  
❌ **No current flag** - Requires complex date logic for every query  
❌ **Creating versions with no changes** - Inflates table size unnecessarily  
❌ **Using TIMESTAMP for effective dates** - DATE is sufficient and simpler  
❌ **Not handling initial load** - First load must set effective_start_date  
❌ **Gaps in effective date ranges** - Should be continuous (end_date + 1 = next start_date)

---

**Remember**: Choose SCD type based on business requirements. Most dimensions need Type 1 (overwrite). Reserve Type 2 for dimensions where historical context matters for reporting.
