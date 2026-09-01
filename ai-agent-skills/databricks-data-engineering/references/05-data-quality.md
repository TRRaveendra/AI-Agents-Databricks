# Data Quality Patterns

## Data Quality Checks by Layer

### Bronze Layer
**Goal**: Log data quality issues, don't filter

```python
from pyspark.sql.functions import col, when, current_timestamp

bronze_df = (raw_df
    .withColumn("_dq_null_check", 
        when(col("customer_id").isNull(), "customer_id is null").otherwise(None))
    .withColumn("_dq_format_check",
        when(~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), "invalid email format").otherwise(None))
    .withColumn("_ingested_timestamp", current_timestamp())
)

# Write everything to Bronze (including bad records)
bronze_df.write.mode("append").saveAsTable("bronze.raw_customers")
```

### Silver Layer
**Goal**: Validate and quarantine bad records

```python
from pyspark.sql.functions import coalesce, array, col

# Apply all data quality rules
silver_df = (bronze_df
    .withColumn("_is_valid",
        when(
            col("customer_id").isNull() |
            col("order_date").isNull() |
            col("order_amount").isNull() |
            (col("order_amount") < 0),
            False
        ).otherwise(True)
    )
    .withColumn("_validation_errors",
        array(
            when(col("customer_id").isNull(), lit("customer_id is null")),
            when(col("order_date").isNull(), lit("order_date is null")),
            when(col("order_amount").isNull(), lit("order_amount is null")),
            when(col("order_amount") < 0, lit("order_amount is negative"))
        )
    )
    .withColumn("_processed_timestamp", current_timestamp())
)

# Write valid records to silver
silver_df.filter(col("_is_valid") == True).write.mode("append").saveAsTable("silver.orders")

# Quarantine invalid records
silver_df.filter(col("_is_valid") == False).write.mode("append").saveAsTable("silver.orders_quarantine")
```

### Gold Layer
**Goal**: Business rule validation and referential integrity

```python
# Ensure referential integrity before aggregation
gold_df = (silver_orders
    .join(silver_customers, "customer_id", "inner")  # Only orders with valid customers
    .join(silver_products, "product_id", "inner")    # Only orders with valid products
    .filter(col("order_status").isin("completed", "shipped"))  # Business rule
    .groupBy("customer_id", "order_date")
    .agg(
        sum("order_amount").alias("total_amount"),
        count("order_id").alias("order_count")
    )
)
```

## Common Data Quality Checks

### Null validation
```python
from pyspark.sql.functions import col, when

def check_nulls(df, required_columns):
    for column in required_columns:
        df = df.withColumn(
            f"_null_check_{column}",
            when(col(column).isNull(), True).otherwise(False)
        )
    return df

# Usage
df_with_null_checks = check_nulls(df, ["customer_id", "order_date", "order_amount"])
```

### Duplicate detection
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count

# Method 1: Count duplicates by business key
duplicate_check = (df
    .groupBy("order_id")
    .agg(count("*").alias("duplicate_count"))
    .filter(col("duplicate_count") > 1)
)

# Method 2: Add row number and keep first occurrence
window_spec = Window.partitionBy("order_id").orderBy(col("order_timestamp").desc())

df_deduped = (df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
```

### Data type validation
```python
from pyspark.sql.functions import col, to_date, to_timestamp

def validate_data_types(df):
    return (df
        # Validate date format
        .withColumn("order_date_valid", 
            to_date(col("order_date"), "yyyy-MM-dd").isNotNull())
        
        # Validate numeric
        .withColumn("amount_valid",
            col("order_amount").cast("decimal(10,2)").isNotNull())
        
        # Validate email format
        .withColumn("email_valid",
            col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"))
    )
```

### Range and business rule validation
```python
from pyspark.sql.functions import col, when, current_date, datediff

def validate_business_rules(df):
    return (df
        # Amount must be positive
        .withColumn("amount_valid", col("order_amount") > 0)
        
        # Date must not be future
        .withColumn("date_valid", col("order_date") <= current_date())
        
        # Age must be reasonable (18-120)
        .withColumn("age_valid", 
            (col("age") >= 18) & (col("age") <= 120))
        
        # Status must be in allowed list
        .withColumn("status_valid",
            col("order_status").isin("pending", "completed", "cancelled", "shipped"))
    )
```

## Referential Integrity

### Check for orphaned records
```python
# Find orders with no matching customer
orphaned_orders = (orders_df
    .join(customers_df, "customer_id", "left")
    .filter(col("customers_df.customer_id").isNull())
)

# Quarantine orphaned records
orphaned_orders.write.mode("append").saveAsTable("silver.orders_orphaned")

# Keep only valid orders
valid_orders = (orders_df
    .join(customers_df, "customer_id", "inner")
)
```

### Enforce foreign key relationships
```python
# Validate before loading to Gold
from pyspark.sql.functions import broadcast

gold_df = (silver_orders
    .join(broadcast(silver_customers), "customer_id", "inner")  # Must have customer
    .join(broadcast(silver_products), "product_id", "inner")   # Must have product
)

# Any orders that don't match are automatically excluded
```

## Quarantine Pattern

### Complete quarantine implementation
```python
from pyspark.sql.functions import col, current_timestamp, array, lit

def apply_data_quality_with_quarantine(df, table_name):
    # Apply all validations
    validated_df = (df
        .withColumn("_is_valid", 
            when(
                col("customer_id").isNull() |
                col("email").isNull() |
                ~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$") |
                col("created_date").isNull(),
                False
            ).otherwise(True)
        )
        .withColumn("_validation_errors", 
            array(
                when(col("customer_id").isNull(), lit("customer_id is null")),
                when(col("email").isNull(), lit("email is null")),
                when(~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), lit("invalid email format")),
                when(col("created_date").isNull(), lit("created_date is null"))
            )
        )
        .withColumn("_processed_timestamp", current_timestamp())
    )
    
    # Split valid and invalid
    valid_df = validated_df.filter(col("_is_valid") == True)
    invalid_df = validated_df.filter(col("_is_valid") == False)
    
    # Write valid records
    valid_count = valid_df.count()
    valid_df.write.mode("append").saveAsTable(table_name)
    
    # Quarantine invalid records
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        invalid_df.write.mode("append").saveAsTable(f"{table_name}_quarantine")
    
    print(f"Valid records: {valid_count}, Quarantined: {invalid_count}")
    
    return valid_df

# Usage
clean_df = apply_data_quality_with_quarantine(raw_df, "silver.customers")
```

## Reconciliation

### Row count reconciliation
```python
# Compare source and target counts
source_count = spark.read.table("bronze.orders").count()
target_count = spark.read.table("silver.orders").count()
quarantine_count = spark.read.table("silver.orders_quarantine").count()

print(f"Source: {source_count}")
print(f"Target: {target_count}")
print(f"Quarantine: {quarantine_count}")
print(f"Total processed: {target_count + quarantine_count}")

# Assert reconciliation
assert source_count == (target_count + quarantine_count), "Row count mismatch!"
```

### Sum reconciliation
```python
# Compare aggregate values
source_sum = spark.sql("SELECT SUM(order_amount) as total FROM bronze.orders").collect()[0]['total']
target_sum = spark.sql("SELECT SUM(order_amount) as total FROM silver.orders").collect()[0]['total']

print(f"Source sum: {source_sum}")
print(f"Target sum: {target_sum}")
print(f"Difference: {abs(source_sum - target_sum)}")

# Allow small floating point tolerance
assert abs(source_sum - target_sum) < 0.01, "Sum reconciliation failed!"
```

### Key-level reconciliation
```python
# Find missing keys between source and target
source_keys = spark.read.table("bronze.orders").select("order_id").distinct()
target_keys = spark.read.table("silver.orders").select("order_id").distinct()

missing_keys = source_keys.subtract(target_keys)
missing_count = missing_keys.count()

if missing_count > 0:
    print(f"WARNING: {missing_count} order_ids missing in target")
    missing_keys.show()
```

## Best Practices

1. **Validate early** - Catch issues in Bronze/Silver before they reach Gold
2. **Always quarantine, never drop** - Bad data needs investigation
3. **Track validation errors explicitly** - Use arrays to capture all rule violations
4. **Reconcile at each layer** - Verify row counts and sums
5. **Use broadcast joins for reference data** - Efficient referential integrity checks
6. **Log data quality metrics** - Track trends over time
7. **Alert on threshold violations** - E.g., > 5% quarantine rate
8. **Document business rules clearly** - Future maintainers need context

## Anti-patterns to Avoid

❌ **Silently dropping bad records** - Always quarantine for investigation  
❌ **No validation error tracking** - Need to know why records failed  
❌ **Validating in Gold only** - Fix issues earlier in the pipeline  
❌ **No reconciliation** - Can't detect silent data loss  
❌ **Hardcoded validation rules** - Use config tables for flexibility  
❌ **Not monitoring quarantine tables** - Bad data piles up unnoticed

---

**Remember**: Data quality is not optional. Build validation into every layer, quarantine bad records, and always reconcile.
