# Databricks Migration Patterns

## Medallion Architecture

```
Bronze (Raw)          Silver (Cleansed)       Gold (Business)
┌──────────────┐     ┌──────────────┐        ┌──────────────┐
│ Source-aligned│ ──→ │ Deduplicated │  ──→   │ Dimensions   │
│ Append-only  │     │ Validated    │        │ Facts        │
│ No transforms│     │ Typed        │        │ Aggregates   │
└──────────────┘     └──────────────┘        └──────────────┘
```

## Bronze Patterns

### Pattern: JDBC Incremental Read
```python
# Watermark-based incremental (replaces SSIS parameterized query)
watermark = spark.sql("SELECT MAX(ModifiedDate) FROM bronze.table").collect()[0][0]

df = (spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", f"(SELECT * FROM src WHERE ModifiedDate > '{watermark}') q")
    .option("fetchsize", "10000")
    .load()
    .withColumn("_etl_load_timestamp", F.current_timestamp())
    .withColumn("_etl_source", F.lit("source_system.schema.table"))
)

df.write.format("delta").mode("append").saveAsTable("bronze.table")
```

### Pattern: Full Load with Overwrite
```python
# Replaces SSIS Truncate + Insert
df = spark.read.format("jdbc").option(...).load()
df.write.format("delta").mode("overwrite").saveAsTable("bronze.table")
```

## Silver Patterns

### Pattern: Deduplication
```python
from pyspark.sql.window import Window

w = Window.partitionBy("BusinessKey").orderBy(F.desc("ModifiedDate"))
df_deduped = (df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
```

### Pattern: Conditional Split
```python
# SSIS: Conditional Split with Active/Inactive branches
df_active = df.filter(F.col("IsActive") == True)
df_inactive = df.filter(F.col("IsActive") == False)
df_unknown = df.filter(F.col("IsActive").isNull())
```

### Pattern: Lookup with Broadcast
```python
# SSIS: Lookup transform with redirect to no-match output
from pyspark.sql.functions import broadcast

lookup_df = spark.table("ref.lookup_table").select("key", "value")

df_matched = df.join(broadcast(lookup_df), on="key", how="inner")
df_no_match = df.join(broadcast(lookup_df), on="key", how="left_anti")
```

## Gold Patterns

### Pattern: SCD Type 2
```python
# Step 1: Hash tracked columns
hash_cols = ["Name", "Email", "Phone", "Address"]
df_source = df.withColumn("record_hash",
    F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]), 256)
)

# Step 2: Expire changed records
spark.sql("""
    UPDATE gold.dim_table SET is_current = false, effective_end_date = current_timestamp()
    WHERE customer_id IN (SELECT id FROM changes) AND is_current = true
""")

# Step 3: Insert new versions
df_new_versions.write.format("delta").mode("append").saveAsTable("gold.dim_table")
```

### Pattern: Fact Merge with Surrogate Keys
```python
# Resolve surrogate keys via joins
df_resolved = (df_fact
    .join(dim_customer, on="CustomerID", how="inner")      # Get customer_key
    .join(dim_date, on="OrderDate", how="left")             # Get date_key
    .join(dim_region, on="RegionCode", how="left")          # Get region_key
)

# MERGE for upsert
df_resolved.createOrReplaceTempView("staged")
spark.sql("""
    MERGE INTO gold.fact_table t USING staged s ON t.OrderID = s.OrderID
    WHEN MATCHED AND (t.Status != s.Status) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Pattern: Late-Arriving Dimension
```python
# Create placeholder records for unknown dimension members
df_unknown = df_fact.join(dim_customer, on="CustomerID", how="left_anti").select("CustomerID").distinct()
if df_unknown.count() > 0:
    df_placeholder = df_unknown.select(
        F.col("CustomerID"),
        F.lit("Unknown - Late Arriving").alias("Name"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("effective_start_date")
    )
    df_placeholder.write.format("delta").mode("append").saveAsTable("gold.dim_customer")
```

## Error Handling Pattern

```python
# Replaces SSIS OnError event handler + Send Mail Task
try:
    # Main pipeline logic
    df = spark.read.format("jdbc").load()
    df.write.saveAsTable("target")
except Exception as e:
    # Log error (replaces audit table INSERT in CATCH block)
    spark.sql(f"""
        INSERT INTO bronze._audit_log
        VALUES ('pipeline_name', 'target_table', 0, current_timestamp(), 'Failed', '{str(e)[:500]}')
    """)
    # Re-raise for Databricks Workflow to catch and send notification
    raise

# Workflow JSON handles notification (replaces Send Mail Task):
# "email_notifications": {"on_failure": ["team@company.com"]}
```

## Databricks Workflow JSON Pattern

```json
{
  "name": "daily_etl_pipeline",
  "tasks": [
    {
      "task_key": "bronze_customers",
      "notebook_task": {
        "notebook_path": "/Repos/migration/notebooks/bronze/load_raw_customers"
      }
    },
    {
      "task_key": "silver_customers",
      "depends_on": [{"task_key": "bronze_customers"}],
      "notebook_task": {
        "notebook_path": "/Repos/migration/notebooks/silver/cleanse_customers"
      }
    },
    {
      "task_key": "gold_dim_customer",
      "depends_on": [{"task_key": "silver_customers"}],
      "notebook_task": {
        "notebook_path": "/Repos/migration/notebooks/gold/merge_dim_customer"
      }
    }
  ],
  "email_notifications": {
    "on_failure": ["data-team@company.com"]
  },
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}
```
