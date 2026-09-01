# Silver Layer - Cleansed and Validated Data

## Purpose
The Silver layer cleanses, validates, and standardizes data from Bronze. It enforces data quality rules, handles schema enforcement, and prepares data for business consumption.

## Core Principles

### 1. Data Quality First
- Validate all required fields
- Enforce data types strictly
- Handle nulls and defaults
- Deduplicate records
- Check referential integrity

### 2. Standardization
- Consistent naming conventions
- Standard date/time formats
- Normalized values (e.g., country codes, statuses)
- Trimmed and cleaned strings

### 3. Quarantine Bad Data
- Invalid records go to `<table>_quarantine`
- Capture validation errors
- Never silently drop bad data

## Silver Table Design

### Standard Silver Table Schema
```sql
CREATE TABLE IF NOT EXISTS silver.orders (
    -- Business columns (standardized and typed)
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    order_amount DECIMAL(10, 2) NOT NULL,
    order_status STRING NOT NULL,
    
    -- Audit columns
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _processed_timestamp TIMESTAMP NOT NULL,
    _source_system STRING,
    
    -- Constraints
    CONSTRAINT valid_amount CHECK (order_amount >= 0),
    CONSTRAINT valid_status CHECK (order_status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled'))
)
USING DELTA
CLUSTER BY (customer_id, order_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Cleansed and validated orders - production ready';
```

### Quarantine Table
```sql
CREATE TABLE IF NOT EXISTS silver.orders_quarantine (
    -- Original columns (keep as-is for investigation)
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    order_amount STRING,
    order_status STRING,
    
    -- Validation tracking
    _validation_errors ARRAY<STRING>,
    _quarantine_timestamp TIMESTAMP,
    _source_system STRING,
    _bronze_record STRING  -- JSON representation of original record
)
USING DELTA;
```

## Bronze to Silver Transformation Patterns

### Pattern 1: Full Bronze to Silver Pipeline
```python
from pyspark.sql.functions import (
    col, when, current_timestamp, to_date, trim, lower, upper,
    array, lit, struct, to_json, coalesce
)
from pyspark.sql.types import DecimalType, LongType

def transform_bronze_to_silver(bronze_table, silver_table, quarantine_table):
    """
    Complete Bronze → Silver transformation with validation and quarantine.
    """
    # Read from Bronze
    bronze_df = spark.read.table(bronze_table)
    
    # Step 1: Type conversions and standardization
    transformed_df = (bronze_df
        # Cast to proper types (with error handling)
        .withColumn("order_id", col("order_id").cast(LongType()))
        .withColumn("customer_id", col("customer_id").cast(LongType()))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("order_amount", col("order_amount").cast(DecimalType(10, 2)))
        
        # Standardize text fields
        .withColumn("order_status", lower(trim(col("order_status"))))
        
        # Add processing metadata
        .withColumn("_processed_timestamp", current_timestamp())
    )
    
    # Step 2: Build validation rules
    validation_df = (transformed_df
        .withColumn("_validation_errors", array())  # Start with empty array
        
        # Null validations
        .withColumn("_validation_errors",
            when(col("order_id").isNull(), 
                array_union(col("_validation_errors"), array(lit("order_id is null"))))
            .otherwise(col("_validation_errors")))
        
        .withColumn("_validation_errors",
            when(col("customer_id").isNull(),
                array_union(col("_validation_errors"), array(lit("customer_id is null"))))
            .otherwise(col("_validation_errors")))
        
        .withColumn("_validation_errors",
            when(col("order_date").isNull(),
                array_union(col("_validation_errors"), array(lit("order_date is null or invalid format"))))
            .otherwise(col("_validation_errors")))
        
        # Business rule validations
        .withColumn("_validation_errors",
            when((col("order_amount").isNull()) | (col("order_amount") < 0),
                array_union(col("_validation_errors"), array(lit("order_amount is null or negative"))))
            .otherwise(col("_validation_errors")))
        
        .withColumn("_validation_errors",
            when(~col("order_status").isin("pending", "processing", "shipped", "delivered", "cancelled"),
                array_union(col("_validation_errors"), array(lit("order_status not in allowed values"))))
            .otherwise(col("_validation_errors")))
        
        # Set validation flag
        .withColumn("_is_valid", 
            when(size(col("_validation_errors")) == 0, True).otherwise(False))
    )
    
    # Step 3: Split valid and invalid records
    valid_df = (validation_df
        .filter(col("_is_valid") == True)
        .select(
            "order_id", "customer_id", "order_date", "order_amount", "order_status",
            "_processed_timestamp", "_source_system"
        )
    )
    
    invalid_df = (validation_df
        .filter(col("_is_valid") == False)
        .select(
            bronze_df["*"],  # Keep original bronze columns
            col("_validation_errors"),
            current_timestamp().alias("_quarantine_timestamp"),
            to_json(struct(bronze_df.columns)).alias("_bronze_record")
        )
    )
    
    # Step 4: Write to Silver and Quarantine
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    
    if valid_count > 0:
        valid_df.write.mode("append").saveAsTable(silver_table)
        print(f"✓ Wrote {valid_count} valid records to {silver_table}")
    
    if invalid_count > 0:
        invalid_df.write.mode("append").saveAsTable(quarantine_table)
        print(f"⚠️  Quarantined {invalid_count} invalid records to {quarantine_table}")
    
    # Step 5: Data quality metrics
    total = valid_count + invalid_count
    quality_rate = (valid_count / total * 100) if total > 0 else 0
    
    print(f"\nData Quality Summary:")
    print(f"  Total Records: {total:,}")
    print(f"  Valid: {valid_count:,} ({quality_rate:.2f}%)")
    print(f"  Invalid: {invalid_count:,} ({100-quality_rate:.2f}%)")
    
    # Alert if quality below threshold
    if quality_rate < 95:
        print(f"  ⚠️  WARNING: Data quality below 95% threshold!")
    
    return {
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "quality_rate": quality_rate
    }

# Usage
metrics = transform_bronze_to_silver(
    bronze_table="bronze.raw_orders",
    silver_table="silver.orders",
    quarantine_table="silver.orders_quarantine"
)
```

### Pattern 2: Incremental Silver Upsert (MERGE)
```python
from delta.tables import DeltaTable

def upsert_bronze_to_silver(bronze_table, silver_table, business_key):
    """
    Incremental upsert from Bronze to Silver using MERGE.
    Updates existing records, inserts new ones.
    """
    # Get new/updated records from Bronze (since last Silver run)
    last_processed = spark.sql(f"""
        SELECT COALESCE(MAX(_processed_timestamp), '1900-01-01') as max_ts
        FROM {silver_table}
    """).collect()[0]['max_ts']
    
    # Transform new Bronze records
    new_records = (spark.read.table(bronze_table)
        .filter(col("_ingested_timestamp") > last_processed)
    )
    
    if new_records.count() == 0:
        print("No new records to process")
        return
    
    # Apply transformations (same as above)
    transformed_df = transform_and_validate(new_records)
    valid_df = transformed_df.filter(col("_is_valid") == True)
    
    # MERGE into Silver
    silver_target = DeltaTable.forName(spark, silver_table)
    
    (silver_target.alias("target")
        .merge(
            valid_df.alias("source"),
            f"target.{business_key} = source.{business_key}"
        )
        .whenMatchedUpdate(set={
            "order_amount": "source.order_amount",
            "order_status": "source.order_status",
            "updated_at": "current_timestamp()",
            "_processed_timestamp": "source._processed_timestamp"
        })
        .whenNotMatchedInsert(values={
            "order_id": "source.order_id",
            "customer_id": "source.customer_id",
            "order_date": "source.order_date",
            "order_amount": "source.order_amount",
            "order_status": "source.order_status",
            "created_at": "current_timestamp()",
            "_processed_timestamp": "source._processed_timestamp"
        })
        .execute()
    )
    
    print(f"✓ MERGE completed for {valid_df.count()} records")

# Usage
upsert_bronze_to_silver(
    bronze_table="bronze.raw_orders",
    silver_table="silver.orders",
    business_key="order_id"
)
```

### Pattern 3: Deduplication in Silver
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

def deduplicate_to_silver(bronze_table, silver_table, dedup_keys, order_column):
    """
    Deduplicate Bronze data before writing to Silver.
    Keeps most recent record based on order_column.
    """
    bronze_df = spark.read.table(bronze_table)
    
    # Define deduplication window
    window_spec = Window.partitionBy(dedup_keys).orderBy(col(order_column).desc())
    
    # Keep only the most recent record per key
    deduped_df = (bronze_df
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    
    # Apply transformations and write
    transformed_df = transform_and_validate(deduped_df)
    valid_df = transformed_df.filter(col("_is_valid") == True)
    
    valid_df.write.mode("overwrite").saveAsTable(silver_table)
    print(f"✓ Wrote {valid_df.count()} deduplicated records to {silver_table}")

# Usage
deduplicate_to_silver(
    bronze_table="bronze.raw_orders",
    silver_table="silver.orders",
    dedup_keys=["order_id"],
    order_column="_ingested_timestamp"
)
```

## Referential Integrity

### Check foreign keys before Silver
```python
def enforce_referential_integrity(orders_df, customers_table):
    """
    Ensure all orders have valid customer references.
    Quarantine orders with missing customers.
    """
    customers_df = spark.read.table(customers_table).select("customer_id").distinct()
    
    # Left join to find orphaned orders
    validated_df = (orders_df.alias("o")
        .join(
            customers_df.alias("c"),
            col("o.customer_id") == col("c.customer_id"),
            "left"
        )
        .withColumn("_has_valid_customer", col("c.customer_id").isNotNull())
    )
    
    # Separate valid and orphaned
    valid_orders = validated_df.filter(col("_has_valid_customer") == True)
    orphaned_orders = validated_df.filter(col("_has_valid_customer") == False)
    
    return valid_orders, orphaned_orders

# Usage
valid, orphaned = enforce_referential_integrity(
    orders_df=bronze_df,
    customers_table="silver.customers"
)
```

## Silver Best Practices

✓ **Enforce schema strictly** - No schema evolution in Silver  
✓ **Validate all required fields** - Null checks on NOT NULL columns  
✓ **Standardize formats** - Dates, decimals, text casing  
✓ **Deduplicate** - Keep only latest/valid records  
✓ **Quarantine bad data** - Never silently drop records  
✓ **Track validation errors** - Capture why records failed  
✓ **Use MERGE for updates** - More efficient than overwrite  
✓ **Check referential integrity** - Validate foreign keys  
✓ **Enable Change Data Feed** - For downstream tracking  

## Silver Anti-patterns

❌ **No data quality checks** - Garbage in, garbage out  
❌ **Silently dropping bad records** - Always quarantine  
❌ **Not deduplicating** - Creates downstream issues  
❌ **Mixing valid and invalid data** - Separate them!  
❌ **No referential integrity** - Orphaned records cause errors  
❌ **Overwriting instead of MERGE** - Inefficient for incremental  

## See Also
- [Bronze Layer](bronze.md) - Input from Bronze
- [Gold Layer](gold.md) - Output to Gold
- [05-data-quality.md](05-data-quality.md) - Detailed validation patterns
- [Code Examples](../code/examples/silver_transformation.py) - Full examples

---

**Remember**: Silver is where you enforce quality. Bad data stops here, good data flows to Gold.
