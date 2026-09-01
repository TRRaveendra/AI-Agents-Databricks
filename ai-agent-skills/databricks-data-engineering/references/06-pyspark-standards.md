# PySpark Coding Standards

## General Coding Conventions

### Imports
```python
# Standard library
from datetime import datetime, timedelta

# PySpark imports (group logically)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce,
    current_timestamp, current_date,
    sum as spark_sum, max as spark_max, count as spark_count
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Delta imports
from delta.tables import DeltaTable
```

### Naming conventions
```python
# Variables: snake_case
customer_df = spark.read.table("silver.customers")
order_count = df.count()

# Functions: snake_case
def calculate_total_revenue(df: DataFrame) -> DataFrame:
    return df.groupBy("customer_id").agg(spark_sum("amount").alias("total_revenue"))

# Classes: PascalCase
class DataQualityValidator:
    pass

# Constants: UPPER_SNAKE_CASE
MAX_RETRIES = 3
DEFAULT_PARTITION_SIZE = 200
```

## DataFrame Operations

### Chaining transformations (readable style)
```python
# GOOD: Each transformation on its own line
result_df = (df
    .filter(col("order_date") >= "2024-01-01")
    .withColumn("order_year", year(col("order_date")))
    .groupBy("customer_id", "order_year")
    .agg(
        spark_sum("order_amount").alias("total_amount"),
        spark_count("order_id").alias("order_count")
    )
    .orderBy("total_amount", ascending=False)
)

# BAD: Everything on one line
result_df = df.filter(col("order_date") >= "2024-01-01").withColumn("order_year", year(col("order_date"))).groupBy("customer_id", "order_year").agg(spark_sum("order_amount").alias("total_amount"))
```

### Use col() for column references
```python
# GOOD: Explicit col() function
df.filter(col("age") > 18)
df.select(col("name"), col("email"))

# AVOID: String column references (less type-safe)
df.filter("age > 18")
df.select("name", "email")
```

### Alias aggregations clearly
```python
# GOOD: Descriptive aliases
summary_df = df.groupBy("customer_id").agg(
    spark_sum("order_amount").alias("total_revenue"),
    spark_count("order_id").alias("order_count"),
    spark_max("order_date").alias("last_order_date"),
    avg("order_amount").alias("avg_order_value")
)

# BAD: Generic aliases
summary_df = df.groupBy("customer_id").agg(
    spark_sum("order_amount").alias("sum"),
    spark_count("order_id").alias("count")
)
```

## Joins

### Broadcast small tables
```python
from pyspark.sql.functions import broadcast

# Small reference table (< 10MB) - use broadcast
result_df = large_df.join(
    broadcast(small_reference_df),
    "customer_id",
    "inner"
)
```

### Explicit join conditions
```python
# GOOD: Explicit join condition
result_df = customers_df.join(
    orders_df,
    customers_df["customer_id"] == orders_df["customer_id"],
    "inner"
)

# ACCEPTABLE: When column name is same in both
result_df = customers_df.join(orders_df, "customer_id", "inner")

# For multiple keys
result_df = df1.join(
    df2,
    (df1["customer_id"] == df2["customer_id"]) & (df1["region"] == df2["region"]),
    "inner"
)
```

### Handle duplicate column names after join
```python
# Method 1: Drop duplicate column after join
result_df = (customers_df
    .join(orders_df, "customer_id", "inner")
    # customer_id exists in both, but join handles it
)

# Method 2: Alias tables to disambiguate
result_df = (customers_df.alias("c")
    .join(orders_df.alias("o"), col("c.customer_id") == col("o.customer_id"))
    .select("c.*", "o.order_id", "o.order_amount")
)
```

## Window Functions

### Clear window specifications
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag

# Define window spec separately
customer_window = Window.partitionBy("customer_id").orderBy(col("order_date").desc())

# Apply window functions
result_df = (orders_df
    .withColumn("row_num", row_number().over(customer_window))
    .withColumn("rank", rank().over(customer_window))
    .withColumn("prev_order_amount", lag("order_amount", 1).over(customer_window))
    .withColumn("next_order_amount", lead("order_amount", 1).over(customer_window))
)

# Get most recent order per customer
most_recent_orders = result_df.filter(col("row_num") == 1)
```

## Deduplication

### Standard deduplication pattern
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define deduplication logic
dedup_window = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())

deduplicated_df = (df
    .withColumn("row_num", row_number().over(dedup_window))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
```

### Distinct vs dropDuplicates
```python
# When you want unique rows across ALL columns
unique_df = df.distinct()

# When you want unique rows based on specific columns (keep first occurrence)
unique_df = df.dropDuplicates(["customer_id", "order_date"])

# dropDuplicates is usually preferred (more explicit)
```

## Reusable Functions

### Create modular transformation functions
```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

def add_audit_columns(df: DataFrame, source_system: str) -> DataFrame:
    """Add standard audit columns to a DataFrame."""
    return (df
        .withColumn("_processed_timestamp", current_timestamp())
        .withColumn("_source_system", lit(source_system))
    )

def deduplicate_by_key(df: DataFrame, key_columns: list, order_column: str) -> DataFrame:
    """Deduplicate DataFrame by key columns, keeping the latest record by order_column."""
    window_spec = Window.partitionBy(*key_columns).orderBy(col(order_column).desc())
    return (df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

def validate_required_columns(df: DataFrame, required_columns: list) -> DataFrame:
    """Add validation flag for required columns."""
    validation_condition = reduce(
        lambda a, b: a & b,
        [col(c).isNotNull() for c in required_columns]
    )
    return df.withColumn("_is_valid", validation_condition)

# Usage
processed_df = (raw_df
    .transform(add_audit_columns, "salesforce")
    .transform(deduplicate_by_key, ["customer_id"], "updated_at")
    .transform(validate_required_columns, ["customer_id", "email"])
)
```

## Error Handling

### Wrap risky operations in try-except
```python
from pyspark.sql.utils import AnalysisException

def safe_read_table(table_name: str) -> DataFrame:
    """Read table with error handling."""
    try:
        df = spark.read.table(table_name)
        print(f"Successfully read {df.count()} rows from {table_name}")
        return df
    except AnalysisException as e:
        print(f"ERROR: Table {table_name} not found: {e}")
        raise
    except Exception as e:
        print(f"ERROR reading {table_name}: {e}")
        raise

def safe_write_table(df: DataFrame, table_name: str, mode: str = "append"):
    """Write table with error handling and logging."""
    try:
        row_count = df.count()
        df.write.mode(mode).saveAsTable(table_name)
        print(f"Successfully wrote {row_count} rows to {table_name}")
    except Exception as e:
        print(f"ERROR writing to {table_name}: {e}")
        raise
```

## Performance Tips

### Cache strategically
```python
# Cache when DataFrame is used multiple times
df_cached = df.filter(col("order_date") >= "2024-01-01").cache()

# Use it multiple times
count = df_cached.count()
summary = df_cached.groupBy("customer_id").agg(spark_sum("amount"))

# Unpersist when done
df_cached.unpersist()
```

### Avoid collect() on large datasets
```python
# BAD: Brings entire dataset to driver
all_customers = df.collect()  # OOM risk!

# GOOD: Use take() for sampling
sample_customers = df.take(10)

# GOOD: Use first() for single row
first_customer = df.first()

# GOOD: Aggregate before collecting
summary = df.groupBy("region").count().collect()  # Much smaller result
```

### Repartition for performance
```python
# After filtering (reduce partitions)
small_df = large_df.filter(col("region") == "US").coalesce(10)

# Before wide transformations (increase partitions)
large_df = small_df.repartition(200, "customer_id")

# Repartition before writing to avoid small files
df.repartition(50).write.mode("overwrite").saveAsTable("target_table")
```

## Anti-patterns to Avoid

❌ **Using SELECT * unnecessarily**
```python
# BAD
df = spark.sql("SELECT * FROM large_table")

# GOOD: Select only needed columns
df = spark.sql("SELECT customer_id, order_date, order_amount FROM large_table")
```

❌ **Iterating over rows with collect()**
```python
# BAD: Brings all data to driver, slow
for row in df.collect():
    process(row)

# GOOD: Use DataFrame operations
result_df = df.withColumn("processed", udf_function(col("value")))
```

❌ **Not caching reused DataFrames**
```python
# BAD: DataFrame computed multiple times
df.filter(col("status") == "active").count()
df.filter(col("status") == "active").groupBy("region").count()

# GOOD: Cache once, use multiple times
active_df = df.filter(col("status") == "active").cache()
active_df.count()
active_df.groupBy("region").count()
active_df.unpersist()
```

❌ **Chaining multiple joins without broadcast**
```python
# BAD: Large shuffle for small reference tables
result = large_df.join(ref1, "key1").join(ref2, "key2").join(ref3, "key3")

# GOOD: Broadcast small tables
result = (large_df
    .join(broadcast(ref1), "key1")
    .join(broadcast(ref2), "key2")
    .join(broadcast(ref3), "key3")
)
```

---

**Remember**: Write PySpark code that is readable, maintainable, and performant. Use descriptive names, modular functions, and appropriate optimizations.
