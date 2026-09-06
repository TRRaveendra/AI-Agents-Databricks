# ADF Copy Activity → Spark Read/Write

## 1. Overview

### What is an ADF Copy Activity?
The Copy Activity is the fundamental data movement operation in Azure Data Factory. It provides a scalable way to copy data between various supported source and sink data stores with built-in data integration capabilities including schema mapping, type conversion, and performance optimization.

### Where is it used?
* Ingesting data from external systems into data lakes
* Moving data between storage layers (Raw → Bronze → Silver → Gold)
* Exporting data to analytics or reporting systems
* Migrating data between databases
* Archiving data to long-term storage
* Replicating data across regions

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF Copy Activity | Spark Equivalent |
-------------------|------------------|
 Source dataset | `spark.read.format()` |
 Sink dataset | `df.write.format()` |
 Column mapping | `select()`, `withColumnRenamed()` |
 Type conversion | `cast()`, schema inference |
 Data transformation | DataFrame transformations |
 Partition handling | `partitionBy()`, `repartition()` |
 Staging | Intermediate Delta tables |
 Performance tuning | Spark configurations, partitioning |

### Key Differences
* **Declarative vs Programmatic**: ADF uses JSON config vs Spark uses code
* **Data Formats**: More format support in Spark (Delta, Parquet, ORC, Avro, JSON, CSV, JDBC, etc.)
* **Transformation**: Limited in ADF Copy vs full DataFrame API in Spark
* **Performance**: Spark provides fine-grained control over parallelism and optimization
* **Incremental Load**: Manual watermark logic vs native Delta Lake change data feed
* **Schema Evolution**: Limited in ADF vs robust schema evolution in Delta Lake

## 3. Migration Strategy (Step-by-Step)

### Step 1: Analyze Source and Sink
```python
def analyze_copy_activity(copy_activity: dict) -> dict:
    """Extract source and sink information from Copy Activity"""
    
    type_properties = copy_activity['typeProperties']
    
    return {
        'source': {
            'type': type_properties['source']['type'],
            'properties': type_properties['source']
        },
        'sink': {
            'type': type_properties['sink']['type'],
            'properties': type_properties['sink']
        },
        'translator': type_properties.get('translator', {}),
        'staging': type_properties.get('enableStaging', False),
        'parallel_copies': type_properties.get('parallelCopies', 1)
    }
```

### Step 2: Generate Spark Read Logic
```python
def generate_spark_read(source: dict) -> str:
    """Generate Spark read code based on source type"""
    
    source_type = source['type']
    
    if source_type == 'AzureSqlSource':
        return generate_jdbc_read(source)
    elif source_type in ['BlobSource', 'AzureBlobFSSource']:
        return generate_cloud_storage_read(source)
    elif source_type == 'ParquetSource':
        return generate_parquet_read(source)
    elif source_type == 'DelimitedTextSource':
        return generate_csv_read(source)
    else:
        return generate_generic_read(source)

def generate_jdbc_read(source: dict) -> str:
    """Generate JDBC read code"""
    query = source.get('sqlReaderQuery', 'SELECT * FROM table')
    
    return f'''
# Read from SQL database
jdbc_url = dbutils.secrets.get("jdbc", "connection-string")

source_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "({query}) AS source")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .load()
)
'''
```

### Step 3: Apply Column Mappings
```python
def apply_column_mappings(translator: dict) -> str:
    """Generate column mapping transformations"""
    
    mappings = translator.get('mappings', [])
    
    if not mappings:
        return "# No column mappings required\ntransformed_df = source_df"
    
    select_expr = []
    for mapping in mappings:
        source_col = mapping['source']['name']
        sink_col = mapping['sink']['name']
        
        if source_col != sink_col:
            select_expr.append(f'col("{source_col}").alias("{sink_col}")')
        else:
            select_expr.append(f'col("{source_col}")')
    
    return f'''
# Apply column mappings
from pyspark.sql.functions import col

transformed_df = source_df.select(
    {", ".join(select_expr)}
)
'''
```

### Step 4: Generate Spark Write Logic
```python
def generate_spark_write(sink: dict, target_format: str = 'delta') -> str:
    """Generate Spark write code based on sink type"""
    
    sink_type = sink['type']
    write_mode = sink.get('writeBehavior', 'Insert')
    
    mode_mapping = {
        'Insert': 'append',
        'Upsert': 'merge',
        'Overwrite': 'overwrite'
    }
    
    spark_mode = mode_mapping.get(write_mode, 'append')
    
    return f'''
# Write to target
(
    transformed_df
    .write
    .format("{target_format}")
    .mode("{spark_mode}")
    .option("overwriteSchema", "true")
    .saveAsTable("catalog.schema.table")
)
'''
```

## 4. Code Conversion

### ADF Example (JSON)

#### Example 1: SQL to Delta Lake
```json
{
  "name": "CopyFromSQLToDataLake",
  "type": "Copy",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT customer_id, name, email, created_date FROM dbo.customers WHERE created_date >= '@{pipeline().parameters.StartDate}'",
      "queryTimeout": "02:00:00",
      "partitionOption": "None"
    },
    "sink": {
      "type": "ParquetSink",
      "storeSettings": {
        "type": "AzureBlobFSWriteSettings",
        "copyBehavior": "PreserveHierarchy"
      },
      "formatSettings": {
        "type": "ParquetWriteSettings",
        "compressionCodec": "snappy"
      }
    },
    "translator": {
      "type": "TabularTranslator",
      "mappings": [
        {"source": {"name": "customer_id"}, "sink": {"name": "id"}},
        {"source": {"name": "name"}, "sink": {"name": "customer_name"}},
        {"source": {"name": "email"}, "sink": {"name": "email_address"}},
        {"source": {"name": "created_date"}, "sink": {"name": "registration_date"}}
      ]
    },
    "enableStaging": false,
    "parallelCopies": 4,
    "dataIntegrationUnits": 8
  }
}
```

#### Example 2: Blob CSV to Delta with Transformation
```json
{
  "name": "CopyCSVToDataLake",
  "type": "Copy",
  "typeProperties": {
    "source": {
      "type": "DelimitedTextSource",
      "storeSettings": {
        "type": "AzureBlobStorageReadSettings",
        "recursive": true,
        "wildcardFileName": "*.csv"
      },
      "formatSettings": {
        "type": "DelimitedTextReadSettings",
        "skipLineCount": 1
      }
    },
    "sink": {
      "type": "ParquetSink"
    },
    "translator": {
      "type": "TabularTranslator",
      "typeConversion": true,
      "typeConversionSettings": {
        "allowDataTruncation": false,
        "culture": "en-US",
        "dateTimeFormat": "yyyy-MM-dd HH:mm:ss"
      }
    }
  }
}
```

### Databricks Equivalent

#### PySpark (Example 1: SQL to Delta)
```python
# Databricks notebook: /Workspace/ETL/copy_sql_to_delta
# COMMAND ----------
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime

# COMMAND ----------
# Get parameters
dbutils.widgets.text("start_date", "2024-01-01")
dbutils.widgets.text("target_catalog", "production")
dbutils.widgets.text("target_schema", "bronze")

start_date = dbutils.widgets.get("start_date")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------
# Read from SQL Server (replacing ADF source)
jdbc_url = dbutils.secrets.get(scope="azure-sql", key="connection-string")
jdbc_user = dbutils.secrets.get(scope="azure-sql", key="username")
jdbc_password = dbutils.secrets.get(scope="azure-sql", key="password")

query = f"""
(
  SELECT 
    customer_id, 
    name, 
    email, 
    created_date 
  FROM dbo.customers 
  WHERE created_date >= '{start_date}'
) AS customers
"""

source_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .option("numPartitions", "4")  # Equivalent to parallelCopies
    .load()
)

# COMMAND ----------
# Apply column mappings (replacing ADF translator)
transformed_df = (
    source_df
    .select(
        col("customer_id").alias("id"),
        col("name").alias("customer_name"),
        col("email").alias("email_address"),
        col("created_date").alias("registration_date")
    )
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("sql_server"))
)

# COMMAND ----------
# Write to Delta Lake (replacing ADF sink)
target_table = f"{target_catalog}.{target_schema}.customers"

(
    transformed_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)

# COMMAND ----------
# Collect metrics and return
row_count = transformed_df.count()
print(f"Successfully copied {row_count} rows to {target_table}")

dbutils.notebook.exit(f'{{"status": "SUCCESS", "row_count": {row_count}}}')
```

#### PySpark (Example 2: CSV to Delta with Auto Loader)
```python
# Databricks notebook: /Workspace/ETL/copy_csv_to_delta
# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------
# Parameters
dbutils.widgets.text("source_path", "abfss://raw@storage.dfs.core.windows.net/customers/")
dbutils.widgets.text("checkpoint_path", "/checkpoints/customers")
dbutils.widgets.text("target_table", "production.bronze.customers")

source_path = dbutils.widgets.get("source_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
target_table = dbutils.widgets.get("target_table")

# COMMAND ----------
# Define schema (optional but recommended for production)
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("registration_date", DateType(), True)
])

# COMMAND ----------
# Read CSV using Auto Loader (superior to ADF Copy Activity)
# Auto Loader handles schema inference, evolution, and incremental processing
source_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .load(source_path)
)

# COMMAND ----------
# Apply transformations and add metadata
transformed_df = (
    source_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("file_name", input_file_name())
    .withColumn("processing_date", current_date())
)

# COMMAND ----------
# Write to Delta Lake using streaming (replaces ADF scheduled Copy)
query = (
    transformed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available files and stop
    .toTable(target_table)
)

query.awaitTermination()

# COMMAND ----------
# Return success
dbutils.notebook.exit('{"status": "SUCCESS"}')
```

#### PySpark (Example 3: Incremental Load with Delta Merge)
```python
# Databricks notebook: /Workspace/ETL/incremental_load_with_merge
# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# COMMAND ----------
# Parameters
dbutils.widgets.text("source_table", "source_catalog.raw.customers")
dbutils.widgets.text("target_table", "production.silver.customers")
dbutils.widgets.text("merge_key", "customer_id")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
merge_key = dbutils.widgets.get("merge_key")

# COMMAND ----------
# Read source data
source_df = spark.table(source_table)

# COMMAND ----------
# Check if target table exists
if spark.catalog.tableExists(target_table):
    # Load existing Delta table
    target_delta = DeltaTable.forName(spark, target_table)
    
    # Perform MERGE (Upsert) - replaces ADF Copy with Upsert
    (
        target_delta.alias("target")
        .merge(
            source_df.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        )
        .whenMatchedUpdateAll()  # Update existing records
        .whenNotMatchedInsertAll()  # Insert new records
        .execute()
    )
    
    operation = "MERGE"
else:
    # Initial load - simple write
    (
        source_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )
    
    operation = "INITIAL_LOAD"

# COMMAND ----------
# Collect metrics
row_count = source_df.count()
print(f"{operation}: Processed {row_count} rows")

dbutils.notebook.exit(f'{{"status": "SUCCESS", "operation": "{operation}", "row_count": {row_count}}}')
```

## 5. Design Patterns

### Pattern 1: Medallion Architecture with Copy Activities
```python
# Bronze Layer: Raw ingestion (replaces ADF Copy from source)
def bronze_ingestion(source_path: str, target_table: str):
    """
    Ingest raw data with minimal transformation
    """
    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"/checkpoints/{target_table}/schema")
        .load(source_path)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"/checkpoints/{target_table}")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

# Silver Layer: Cleansed and conformed (replaces ADF Copy with transformations)
def silver_transformation(bronze_table: str, silver_table: str):
    """
    Apply business rules and data quality checks
    """
    df = (
        spark.table(bronze_table)
        .filter(col("_ingestion_timestamp") > lit(get_watermark()))
        .dropDuplicates(["id"])
        .filter(col("email").isNotNull())
        .withColumn("email_domain", split(col("email"), "@")[1])
    )
    
    DeltaTable.forName(spark, silver_table).alias("target") \
        .merge(df.alias("source"), "target.id = source.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# Gold Layer: Business aggregates (replaces ADF Copy to analytics)
def gold_aggregation(silver_table: str, gold_table: str):
    """
    Create analytics-ready datasets
    """
    (
        spark.table(silver_table)
        .groupBy("email_domain", "registration_month")
        .agg(
            count("*").alias("customer_count"),
            countDistinct("id").alias("unique_customers")
        )
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_table)
    )
```

### Pattern 2: Incremental Load with Watermark
```python
def incremental_load_with_watermark(
    source_query: str,
    target_table: str,
    watermark_column: str = "modified_date",
    watermark_table: str = "control.watermarks"
):
    """
    Efficient incremental data loading pattern
    """
    # Get last watermark
    last_watermark = spark.sql(f"""
        SELECT COALESCE(MAX(watermark_value), '1900-01-01') as last_value
        FROM {watermark_table}
        WHERE table_name = '{target_table}'
    """).collect()[0]['last_value']
    
    # Read incremental data
    query_with_filter = source_query.replace(
        "WHERE",
        f"WHERE {watermark_column} > '{last_watermark}' AND"
    )
    
    incremental_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"({query_with_filter}) AS incremental",
        properties=jdbc_properties
    )
    
    # Merge into target
    if incremental_df.count() > 0:
        target_delta = DeltaTable.forName(spark, target_table)
        
        target_delta.alias("target").merge(
            incremental_df.alias("source"),
            "target.id = source.id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
        # Update watermark
        new_watermark = incremental_df.agg(max(watermark_column)).collect()[0][0]
        
        spark.sql(f"""
            MERGE INTO {watermark_table} AS target
            USING (SELECT '{target_table}' as table_name, '{new_watermark}' as watermark_value) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET target.watermark_value = source.watermark_value
            WHEN NOT MATCHED THEN INSERT (table_name, watermark_value) VALUES (source.table_name, source.watermark_value)
        """)
```

### Pattern 3: Schema Evolution Handling
```python
def copy_with_schema_evolution(
    source_path: str,
    target_table: str,
    merge_schema: bool = True
):
    """
    Handle schema changes automatically (not available in ADF)
    """
    # Read with schema inference
    source_df = spark.read.format("parquet").load(source_path)
    
    # Write with schema evolution enabled
    (
        source_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", str(merge_schema))
        .option("autoMigrate", "true")  # Databricks 13.0+
        .saveAsTable(target_table)
    )
    
    # Log schema changes
    new_columns = set(source_df.columns) - set(spark.table(target_table).columns)
    if new_columns:
        print(f"New columns added: {new_columns}")
```

### Pattern 4: Partitioned Copy with Optimization
```python
def optimized_partitioned_copy(
    source_table: str,
    target_table: str,
    partition_columns: list = ["year", "month"],
    optimize: bool = True
):
    """
    Efficient partitioned data copy with automatic optimization
    """
    # Read source
    source_df = spark.table(source_table)
    
    # Write partitioned
    (
        source_df
        .repartition(*partition_columns)
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy(*partition_columns)
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    
    # Optimize and Z-Order
    if optimize:
        spark.sql(f"""
            OPTIMIZE {target_table}
            ZORDER BY (id)
        """)
        
        # Vacuum old files (optional)
        spark.sql(f"VACUUM {target_table} RETAIN 168 HOURS")
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Not Using Auto Loader for Cloud Files
**Problem**: Manual file tracking and processing like in ADF.
```python
# WRONG: Batch processing requires manual tracking
files = dbutils.fs.ls(source_path)
for file in files:
    if not is_processed(file):
        df = spark.read.csv(file.path)
        df.write.mode("append").saveAsTable(target)
        mark_as_processed(file)
```

**Solution**: Use Auto Loader for automatic, scalable file processing.
```python
# CORRECT: Auto Loader handles everything
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .load(source_path)
 .writeStream
 .format("delta")
 .trigger(availableNow=True)
 .toTable(target))
```

### ❌ Anti-Pattern 2: Not Leveraging Delta Merge for Upserts
**Problem**: Separate delete and insert operations.
```python
# WRONG: Manual upsert logic
existing_ids = spark.table(target).select("id").rdd.flatMap(lambda x: x).collect()
new_data = source_df.filter(~col("id").isin(existing_ids))
spark.sql(f"DELETE FROM {target} WHERE id IN ({','.join(map(str, existing_ids))})")
new_data.write.mode("append").saveAsTable(target)
```

**Solution**: Use Delta MERGE.
```python
# CORRECT: Atomic merge operation
DeltaTable.forName(spark, target).alias("t") \
    .merge(source_df.alias("s"), "t.id = s.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

### ❌ Anti-Pattern 3: Ignoring Spark Optimization
**Problem**: Not configuring Spark for optimal performance.
```python
# WRONG: Default configuration
df = spark.read.jdbc(url, table)
df.write.saveAsTable(target)
```

**Solution**: Configure for performance.
```python
# CORRECT: Optimized configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

df = (spark.read
     .option("numPartitions", "16")
     .option("fetchsize", "10000")
     .jdbc(url, table))

df.write.format("delta").saveAsTable(target)
spark.sql(f"OPTIMIZE {target}")
```

### ❌ Anti-Pattern 4: Not Handling Data Quality
**Problem**: Blindly copying data without validation.
```python
# WRONG: No validation
source_df.write.saveAsTable(target)
```

**Solution**: Implement data quality checks.
```python
# CORRECT: Validate before writing
from pyspark.sql.functions import col, when

validated_df = (
    source_df
    .withColumn("is_valid", 
                when(col("email").isNotNull() & 
                     col("email").rlike("^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$"), True)
                .otherwise(False))
)

valid_records = validated_df.filter(col("is_valid") == True)
invalid_records = validated_df.filter(col("is_valid") == False)

# Write valid records
valid_records.write.saveAsTable(target)

# Log invalid records
if invalid_records.count() > 0:
    invalid_records.write.mode("append").saveAsTable(f"{target}_quarantine")
```

## 7. Performance Considerations

### Connection Pooling and Parallelism
```python
# Optimize JDBC reads
jdbc_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000",
    "batchsize": "10000",
    "numPartitions": "16",
    "partitionColumn": "id",
    "lowerBound": "1",
    "upperBound": "1000000"
}

df = spark.read.jdbc(url, table, properties=jdbc_properties)
```

### Compression and File Format
```python
# Optimize storage format
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB

(df.write
 .format("delta")
 .option("dataChange", "false")  # Metadata-only operation
 .saveAsTable(target))
```

### Adaptive Query Execution
```python
# Enable AQE for better performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Auto Optimize
```python
# Enable automatic optimization
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")

# Or set at table level
spark.sql(f"""
    ALTER TABLE {target_table}
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")
```

## 8. Governance & Security

### Unity Catalog Integration
```python
# Read and write using Unity Catalog
source_df = spark.table("production_catalog.bronze.customers")

(source_df
 .write
 .format("delta")
 .mode("append")
 .saveAsTable("production_catalog.silver.customers"))

# Lineage is automatically tracked
```

### Secure Credential Management
```python
# Never hardcode credentials
# WRONG:
# jdbc_url = "jdbc:sqlserver://server.database.windows.net;password=MyPassword"

# CORRECT: Use secrets
jdbc_url = dbutils.secrets.get(scope="jdbc-connections", key="sql-url")
jdbc_user = dbutils.secrets.get(scope="jdbc-connections", key="sql-user")
jdbc_password = dbutils.secrets.get(scope="jdbc-connections", key="sql-password")

df = (spark.read
     .format("jdbc")
     .option("url", jdbc_url)
     .option("user", jdbc_user)
     .option("password", jdbc_password)
     .load())
```

### Data Masking and Access Control
```python
# Apply column-level security before writing
from pyspark.sql.functions import sha2, when

secured_df = (
    source_df
    .withColumn("ssn_masked", 
                when(col("user_role") == "admin", col("ssn"))
                .otherwise(sha2(col("ssn"), 256)))
)

secured_df.write.saveAsTable("production_catalog.pii.customers")

# Unity Catalog enforces row/column-level security automatically
```

## 9. Automation Hints (IMPORTANT)

### AI Agent Transformation Engine
```python
class CopyActivityTransformer:
    """
    AI Agent for transforming ADF Copy Activity to Spark code
    """
    
    def __init__(self):
        self.source_handlers = {
            'AzureSqlSource': self._gen_jdbc_read,
            'ParquetSource': self._gen_parquet_read,
            'DelimitedTextSource': self._gen_csv_read,
            'BlobSource': self._gen_blob_read,
            'CosmosDbSqlApiSource': self._gen_cosmos_read
        }
        
        self.sink_handlers = {
            'AzureSqlSink': self._gen_jdbc_write,
            'ParquetSink': self._gen_delta_write,
            'AzureBlobFSSink': self._gen_delta_write
        }
    
    def transform(self, copy_activity: dict) -> str:
        """Main transformation method"""
        type_props = copy_activity['typeProperties']
        
        # Generate read code
        source_type = type_props['source']['type']
        read_code = self.source_handlers.get(
            source_type, 
            self._gen_generic_read
        )(type_props['source'])
        
        # Generate transformation code
        translator = type_props.get('translator', {})
        transform_code = self._gen_transformations(translator)
        
        # Generate write code
        sink_type = type_props['sink']['type']
        write_code = self.sink_handlers.get(
            sink_type,
            self._gen_generic_write
        )(type_props['sink'])
        
        # Combine into notebook
        return self._build_notebook(read_code, transform_code, write_code)
    
    def _gen_jdbc_read(self, source: dict) -> str:
        """Generate JDBC read code"""
        query = source.get('sqlReaderQuery', 'SELECT * FROM table')
        
        return f'''
jdbc_url = dbutils.secrets.get("jdbc", "url")
jdbc_user = dbutils.secrets.get("jdbc", "user")
jdbc_password = dbutils.secrets.get("jdbc", "password")

source_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "({query}) AS source")
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .load()
)
'''
    
    def _gen_transformations(self, translator: dict) -> str:
        """Generate DataFrame transformation code"""
        mappings = translator.get('mappings', [])
        
        if not mappings:
            return "transformed_df = source_df"
        
        select_list = []
        for m in mappings:
            src = m['source']['name']
            sink = m['sink']['name']
            if src != sink:
                select_list.append(f'col("{src}").alias("{sink}")')
            else:
                select_list.append(f'col("{src}")')
        
        return f'''
from pyspark.sql.functions import col, current_timestamp

transformed_df = (
    source_df
    .select({", ".join(select_list)})
    .withColumn("_ingestion_timestamp", current_timestamp())
)
'''
    
    def _gen_delta_write(self, sink: dict) -> str:
        """Generate Delta Lake write code"""
        write_behavior = sink.get('writeBehavior', 'Insert')
        
        mode_map = {
            'Insert': 'append',
            'Upsert': 'merge',
            'Overwrite': 'overwrite'
        }
        
        mode = mode_map.get(write_behavior, 'append')
        
        return f'''
target_table = "{{{{job.parameters.target_table}}}}"

(
    transformed_df
    .write
    .format("delta")
    .mode("{mode}")
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)
'''
    
    def _build_notebook(self, read: str, transform: str, write: str) -> str:
        """Assemble complete notebook"""
        return f'''
# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-generated from ADF Copy Activity
# MAGIC Generated by ADF-to-Lakeflow Migration Tool

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
dbutils.widgets.text("target_table", "catalog.schema.table")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Source

# COMMAND ----------
{read}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Transform Data

# COMMAND ----------
{transform}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Target

# COMMAND ----------
{write}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Return Results

# COMMAND ----------
row_count = transformed_df.count()
dbutils.notebook.exit(f'{{"status": "SUCCESS", "row_count": {row_count}}}')
'''
```

### Pattern Detection for Optimization
```python
def detect_copy_pattern(copy_activity: dict) -> str:
    """Detect pattern to apply appropriate optimization"""
    
    type_props = copy_activity['typeProperties']
    source = type_props['source']
    sink = type_props['sink']
    
    # Full load pattern
    if 'sqlReaderQuery' not in source and sink.get('writeBehavior') == 'Overwrite':
        return 'full_load'
    
    # Incremental pattern
    if 'sqlReaderQuery' in source:
        query = source['sqlReaderQuery'].lower()
        if any(kw in query for kw in ['where', '>', 'modified', 'updated']):
            return 'incremental'
    
    # Upsert pattern
    if sink.get('writeBehavior') == 'Upsert':
        return 'upsert'
    
    # Streaming/CDC pattern
    if type_props.get('enableStaging'):
        return 'streaming'
    
    return 'standard'

def apply_optimization_template(pattern: str) -> str:
    """Return optimized code template for pattern"""
    templates = {
        'full_load': 'templates/full_load_optimized.py',
        'incremental': 'templates/incremental_with_watermark.py',
        'upsert': 'templates/delta_merge_optimized.py',
        'streaming': 'templates/auto_loader_streaming.py'
    }
    
    return templates.get(pattern, 'templates/standard_copy.py')
```

## 10. Validation Checklist

### Pre-Migration Validation
- [ ] Source connection string extracted
- [ ] Source query/table identified
- [ ] Column mappings documented
- [ ] Data types mapped correctly
- [ ] Sink location determined
- [ ] Write behavior understood (Insert/Upsert/Overwrite)
- [ ] Performance settings noted (parallelCopies, DIU)
- [ ] Staging requirements identified
- [ ] Credentials moved to secrets

### Post-Migration Validation
```python
def validate_copy_migration(
    adf_source: str,
    spark_target: str,
    sample_size: int = 10000
):
    """Comprehensive validation of migrated Copy Activity"""
    
    # Row count comparison
    adf_count = get_adf_row_count(adf_source)  # From ADF logs
    spark_count = spark.table(spark_target).count()
    
    assert spark_count == adf_count, f"Row count mismatch: {adf_count} vs {spark_count}"
    
    # Schema validation
    spark_schema = spark.table(spark_target).schema
    validate_schema(spark_schema, expected_columns)
    
    # Data sampling
    sample_df = spark.table(spark_target).sample(fraction=sample_size/spark_count)
    
    # Check for nulls in required fields
    for col in required_columns:
        null_count = sample_df.filter(f"{col} IS NULL").count()
        assert null_count == 0, f"Unexpected nulls in {col}"
    
    # Data type validation
    for col, expected_type in column_types.items():
        actual_type = [f.dataType for f in spark_schema if f.name == col][0]
        assert actual_type == expected_type, f"Type mismatch for {col}"
    
    print("✅ Validation passed")
```

### Performance Comparison
```sql
-- Compare execution times
WITH adf_metrics AS (
  SELECT 
    AVG(duration_seconds) as avg_duration,
    AVG(rows_copied) as avg_rows,
    AVG(data_read_mb) as avg_data_read
  FROM adf_monitoring.copy_activity_runs
  WHERE activity_name = 'CopyCustomerData'
    AND run_date >= CURRENT_DATE - INTERVAL 30 DAYS
),
spark_metrics AS (
  SELECT 
    AVG(execution_duration/1000) as avg_duration,
    AVG(CAST(run_output:row_count AS BIGINT)) as avg_rows,
    AVG(CAST(run_output:bytes_read AS BIGINT)/1024/1024) as avg_data_read
  FROM system.lakeflow.task_run_timeline
  WHERE task_key = 'copy_customer_data'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
)
SELECT 
  a.avg_duration as adf_seconds,
  s.avg_duration as spark_seconds,
  ((s.avg_duration - a.avg_duration) / a.avg_duration * 100) as pct_change,
  a.avg_rows as adf_rows,
  s.avg_rows as spark_rows
FROM adf_metrics a
CROSS JOIN spark_metrics s;
```

---

**Related Skills:**
* [adf_activity_to_task.md](adf_activity_to_task.md) - General activity transformation
* [adf_dataset_to_delta_table.md](adf_dataset_to_delta_table.md) - Dataset migration
* [adf_linked_service_to_unity_catalog.md](adf_linked_service_to_unity_catalog.md) - Connection migration
* [migration_index.md](migration_index.md) - Complete migration guide
