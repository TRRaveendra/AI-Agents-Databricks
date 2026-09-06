# ADF Dataset → Delta Table / Path

## 1. Overview

### What is an ADF Dataset?
An ADF Dataset represents the structure of data within a data store referenced by a Linked Service. It defines the schema, location, format, and other properties of data being read or written by Copy Activities and other data transformation activities.

### Where is it used?
* Defining source data structure in Copy Activities
* Specifying sink data location and format
* Schema mapping and validation
* Parameterization of file paths and table names
* Data format specifications (Parquet, CSV, JSON, Avro, etc.)

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF Dataset | Databricks Equivalent |
-------------|----------------------|
 Dataset (Parquet) | Delta table or path |
 Dataset (CSV) | DataFrame with schema |
 Dataset (SQL Table) | Unity Catalog table or JDBC read |
 Dataset parameters | Job parameters / widgets |
 File path | DBFS path or external location |
 Folder path | Delta table location |
 Schema definition | Spark schema or table DDL |

### Key Differences
* **Format**: ADF supports multiple formats vs Delta Lake as standard in Databricks
* **Schema**: Explicit JSON schema vs Spark StructType or DDL
* **Location**: ADF dataset path vs Unity Catalog managed/external locations
* **Partitioning**: Dataset folder structure vs Delta partitioning
* **Evolution**: Limited in ADF vs robust schema evolution in Delta

## 3. Migration Strategy (Step-by-Step)

### Step 1: Catalog All Datasets
```python
def catalog_datasets(adf_export: dict) -> list:
    """Extract all datasets from ADF export"""
    
    datasets = []
    
    for ds in adf_export.get('datasets', []):
        datasets.append({
            'name': ds['name'],
            'type': ds['properties']['type'],
            'linked_service': ds['properties']['linkedServiceName']['referenceName'],
            'location': extract_location(ds),
            'schema': ds['properties'].get('schema', []),
            'format': ds['properties'].get('type', 'Unknown')
        })
    
    return datasets
```

### Step 2: Determine Delta Mapping Strategy
```python
def determine_delta_strategy(dataset: dict) -> str:
    """Determine how to represent dataset in Databricks"""
    
    # Database table → Unity Catalog table or JDBC read
    if dataset['type'] in ['AzureSqlTable', 'SqlServerTable']:
        return 'unity_catalog_table'
    
    # Parquet files → Delta table
    if dataset['type'] == 'Parquet':
        return 'delta_table'
    
    # CSV/JSON → Delta table with Auto Loader
    if dataset['type'] in ['DelimitedText', 'Json']:
        return 'delta_table_from_files'
    
    # Binary/Avro → Specialized handling
    if dataset['type'] in ['Binary', 'Avro', 'Orc']:
        return 'specialized_format'
    
    return 'generic_path'
```

### Step 3: Generate Delta Table DDL
```python
def generate_delta_table_ddl(dataset: dict, catalog: str, schema: str) -> str:
    """Generate CREATE TABLE statement"""
    
    table_name = dataset['name'].lower()
    location = convert_to_external_location(dataset['location'])
    
    # Convert ADF schema to Spark DDL
    columns = []
    for col in dataset.get('schema', []):
        spark_type = map_adf_type_to_spark(col['type'])
        columns.append(f"  {col['name']} {spark_type}")
    
    columns_ddl = ",\n".join(columns)
    
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
{columns_ddl}
)
USING DELTA
LOCATION '{location}'
PARTITIONED BY (year, month)  -- Adjust as needed
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
"""
```

## 4. Code Conversion

### ADF Example (JSON)

#### Example 1: Parquet Dataset
```json
{
  "name": "CustomerParquetDataset",
  "properties": {
    "type": "Parquet",
    "linkedServiceName": {
      "referenceName": "ADLSGen2LinkedService",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "location": {
        "type": "AzureBlobFSLocation",
        "fileName": "*.parquet",
        "folderPath": "customers",
        "fileSystem": "raw"
      },
      "compressionCodec": "snappy"
    },
    "schema": [
      {"name": "customer_id", "type": "Int32"},
      {"name": "name", "type": "String"},
      {"name": "email", "type": "String"},
      {"name": "created_date", "type": "DateTime"}
    ]
  }
}
```

#### Example 2: CSV Dataset with Parameters
```json
{
  "name": "DynamicCSVDataset",
  "properties": {
    "type": "DelimitedText",
    "linkedServiceName": {
      "referenceName": "BlobStorageLinkedService",
      "type": "LinkedServiceReference"
    },
    "parameters": {
      "folderPath": {"type": "String"},
      "fileName": {"type": "String"}
    },
    "typeProperties": {
      "location": {
        "type": "AzureBlobStorageLocation",
        "fileName": {"value": "@dataset().fileName", "type": "Expression"},
        "folderPath": {"value": "@dataset().folderPath", "type": "Expression"},
        "container": "data"
      },
      "columnDelimiter": ",",
      "escapeChar": "\\",
      "quoteChar": "\"",
      "firstRowAsHeader": true
    }
  }
}
```

#### Example 3: SQL Table Dataset
```json
{
  "name": "AzureSqlCustomerTable",
  "properties": {
    "type": "AzureSqlTable",
    "linkedServiceName": {
      "referenceName": "AzureSqlLinkedService",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "schema": "dbo",
      "table": "customers"
    },
    "schema": [
      {"name": "customer_id", "type": "int"},
      {"name": "name", "type": "nvarchar"},
      {"name": "email", "type": "nvarchar"}
    ]
  }
}
```

### Databricks Equivalent

#### SQL (Parquet → Delta Table)
```sql
-- Create Delta table from Parquet files
CREATE TABLE IF NOT EXISTS production_catalog.bronze.customers (
  customer_id INT,
  name STRING,
  email STRING,
  created_date TIMESTAMP,
  -- Add audit columns
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING
)
USING DELTA
LOCATION 'abfss://bronze@datalake.dfs.core.windows.net/customers/'
PARTITIONED BY (DATE(created_date))
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Customer master data from source system';

-- Initial load from Parquet
INSERT INTO production_catalog.bronze.customers
SELECT 
  customer_id,
  name,
  email,
  created_date,
  CURRENT_TIMESTAMP() as _ingestion_timestamp,
  INPUT_FILE_NAME() as _source_file
FROM parquet.`abfss://raw@datalake.dfs.core.windows.net/customers/*.parquet`;

-- Optimize
OPTIMIZE production_catalog.bronze.customers
ZORDER BY (customer_id);
```

#### PySpark (Dynamic CSV → Delta with Auto Loader)
```python
# Databricks notebook: Load CSV to Delta
# COMMAND ----------
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------
# Parameters (replaces ADF dataset parameters)
dbutils.widgets.text("folder_path", "customers")
dbutils.widgets.text("file_name", "*.csv")
dbutils.widgets.text("target_table", "production_catalog.bronze.customers")

folder_path = dbutils.widgets.get("folder_path")
file_name = dbutils.widgets.get("file_name")
target_table = dbutils.widgets.get("target_table")

# COMMAND ----------
# Construct source path
source_path = f"abfss://data@storage.dfs.core.windows.net/{folder_path}/{file_name}"
checkpoint_path = f"/checkpoints/{target_table.replace('.', '_')}"

print(f"Source: {source_path}")
print(f"Target: {target_table}")

# COMMAND ----------
# Define schema (replaces ADF dataset schema)
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_date", DateType(), True)
])

# COMMAND ----------
# Read CSV using Auto Loader (superior to ADF Copy Activity)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\\")
    .load(source_path)
)

# COMMAND ----------
# Add audit columns
enriched_df = (
    df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_processing_date", current_date())
)

# COMMAND ----------
# Write to Delta table
query = (
    enriched_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available files
    .toTable(target_table)
)

query.awaitTermination()

# COMMAND ----------
# Return success
dbutils.notebook.exit('{"status": "SUCCESS", "target_table": "' + target_table + '"}')
```

#### PySpark (SQL Table → Unity Catalog Table)
```python
# Databricks notebook: Sync SQL Server to Unity Catalog
# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# COMMAND ----------
# Parameters
dbutils.widgets.text("source_schema", "dbo")
dbutils.widgets.text("source_table", "customers")
dbutils.widgets.text("target_table", "production_catalog.bronze.customers")
dbutils.widgets.text("merge_key", "customer_id")

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
merge_key = dbutils.widgets.get("merge_key")

# COMMAND ----------
# Read from SQL Server (replaces ADF SQL dataset)
jdbc_url = dbutils.secrets.get("azure-sql", "connection-url")
jdbc_user = dbutils.secrets.get("azure-sql", "username")
jdbc_password = dbutils.secrets.get("azure-sql", "password")

source_query = f"(SELECT * FROM {source_schema}.{source_table}) AS source"

source_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", source_query)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .option("numPartitions", "8")
    .load()
)

# COMMAND ----------
# Add audit columns
enriched_df = (
    source_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_system", lit("sql_server"))
)

# COMMAND ----------
# Merge into target Delta table
if spark.catalog.tableExists(target_table):
    # Upsert (merge)
    target_delta = DeltaTable.forName(spark, target_table)
    
    (
        target_delta.alias("target")
        .merge(
            enriched_df.alias("source"),
            f"target.{merge_key} = source.{merge_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print(f"Merged data into {target_table}")
else:
    # Initial load
    enriched_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Created and loaded {target_table}")

# COMMAND ----------
# Optimize table
spark.sql(f"OPTIMIZE {target_table}")

# Return metrics
row_count = enriched_df.count()
dbutils.notebook.exit(f'{{"status": "SUCCESS", "row_count": {row_count}}}')
```

## 5. Design Patterns

### Pattern 1: Schema Registry Pattern
```python
# Centralized schema definitions
SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_date", TimestampType(), True)
    ]),
    "orders": StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", DateType(), True),
        StructField("amount", DecimalType(10, 2), True)
    ])
}

def get_schema(dataset_name: str) -> StructType:
    """Get schema for dataset"""
    return SCHEMAS.get(dataset_name)

# Usage
schema = get_schema("customers")
df = spark.read.schema(schema).csv(path)
```

### Pattern 2: Medallion Architecture Mapping
```sql
-- Bronze: Raw data (replaces ADF raw datasets)
CREATE TABLE production_catalog.bronze.customers
USING DELTA
LOCATION 'abfss://bronze@datalake.dfs.core.windows.net/customers/';

-- Silver: Cleansed and conformed (replaces ADF staging datasets)
CREATE TABLE production_catalog.silver.customers
USING DELTA
LOCATION 'abfss://silver@datalake.dfs.core.windows.net/customers/';

-- Gold: Business aggregates (replaces ADF processed datasets)
CREATE TABLE production_catalog.gold.customer_summary
USING DELTA
LOCATION 'abfss://gold@datalake.dfs.core.windows.net/customer_summary/';
```

### Pattern 3: Parameterized Table Creation
```python
def create_table_from_dataset_def(
    dataset_name: str,
    catalog: str,
    schema: str,
    location: str,
    columns: list[dict],
    partition_cols: list[str] = None
):
    """Dynamically create Delta table from dataset definition"""
    
    # Build DDL
    column_defs = []
    for col in columns:
        spark_type = map_type(col['type'])
        nullable = "NULL" if col.get('nullable', True) else "NOT NULL"
        column_defs.append(f"  {col['name']} {spark_type} {nullable}")
    
    columns_ddl = ",\n".join(column_defs)
    
    partition_clause = ""
    if partition_cols:
        partition_clause = f"PARTITIONED BY ({', '.join(partition_cols)})"
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{dataset_name} (
    {columns_ddl}
    )
    USING DELTA
    LOCATION '{location}'
    {partition_clause}
    """
    
    spark.sql(ddl)
    print(f"Created table {catalog}.{schema}.{dataset_name}")
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Not Using Delta Lake
**Problem**: Using Parquet directly instead of Delta.
```python
# WRONG: Writing raw Parquet
df.write.format("parquet").save(path)
```

**Solution**: Always use Delta format.
```python
# CORRECT: Using Delta Lake
df.write.format("delta").saveAsTable(table_name)
```

### ❌ Anti-Pattern 2: Ignoring Schema Evolution
**Problem**: Not planning for schema changes.
```python
# WRONG: Fails when schema changes
df.write.mode("append").saveAsTable(table)  # Error if new columns
```

**Solution**: Enable schema evolution.
```python
# CORRECT: Handle schema changes
df.write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(table)
```

### ❌ Anti-Pattern 3: Not Partitioning Large Tables
**Problem**: No partitioning strategy.
```sql
-- WRONG: No partitioning for large table
CREATE TABLE huge_table (...)
USING DELTA;
```

**Solution**: Partition appropriately.
```sql
-- CORRECT: Partition by date
CREATE TABLE huge_table (...)
USING DELTA
PARTITIONED BY (year, month);
```

## 7. Performance Considerations

### Optimize Table Layout
```sql
-- Z-order for better query performance
OPTIMIZE production_catalog.silver.customers
ZORDER BY (customer_id, created_date);

-- Vacuum old files
VACUUM production_catalog.silver.customers RETAIN 168 HOURS;
```

### Enable Auto-Optimization
```sql
ALTER TABLE production_catalog.silver.customers
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

### Partitioning Strategy
```python
# Good partitioning for time-series data
df.write.format("delta") \
    .partitionBy("year", "month", "day") \
    .saveAsTable(table)

# Avoid over-partitioning
# WRONG: Too many partitions
# .partitionBy("year", "month", "day", "hour", "minute")  # Creates millions of files
```

## 8. Governance & Security

### Unity Catalog Table Properties
```sql
CREATE TABLE production_catalog.gold.customers (
  customer_id INT,
  name STRING,
  email STRING
)
USING DELTA
COMMENT 'Customer master data - contains PII'
TBLPROPERTIES (
  'data_classification' = 'confidential',
  'contains_pii' = 'true',
  'owner' = 'data-engineering-team',
  'refresh_frequency' = 'daily'
);

-- Add column comments
COMMENT ON COLUMN production_catalog.gold.customers.email IS 'PII: Customer email address';
```

### Row-Level Security
```sql
-- Create row filter function
CREATE FUNCTION production_catalog.gold.customer_filter(region STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('regional_analysts'), 
          region = CURRENT_USER_REGION(), 
          TRUE);

-- Apply to table
ALTER TABLE production_catalog.gold.customers
SET ROW FILTER production_catalog.gold.customer_filter(region);
```

## 9. Automation Hints (IMPORTANT)

### AI Agent Transformation Logic
```python
class DatasetTransformer:
    """Transform ADF Datasets to Delta tables"""
    
    def transform(self, dataset: dict, catalog: str, schema: str) -> dict:
        """Main transformation method"""
        
        dataset_type = dataset['properties']['type']
        
        if dataset_type == 'Parquet':
            return self.generate_parquet_to_delta(dataset, catalog, schema)
        
        elif dataset_type == 'DelimitedText':
            return self.generate_csv_to_delta(dataset, catalog, schema)
        
        elif dataset_type in ['AzureSqlTable', 'SqlServerTable']:
            return self.generate_sql_to_delta(dataset, catalog, schema)
        
        else:
            return self.generate_generic_delta(dataset, catalog, schema)
    
    def generate_parquet_to_delta(self, dataset: dict, catalog: str, schema: str) -> dict:
        """Generate Parquet → Delta migration"""
        
        table_name = dataset['name'].lower()
        location = self._convert_location(dataset['properties']['typeProperties']['location'])
        
        columns = self._convert_schema(dataset['properties'].get('schema', []))
        
        ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
  {columns}
)
USING DELTA
LOCATION '{location}'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
);
        """
        
        return {
            'table_name': f"{catalog}.{schema}.{table_name}",
            'ddl': ddl,
            'migration_type': 'parquet_to_delta',
            'location': location
        }
    
    def _convert_schema(self, adf_schema: list) -> str:
        """Convert ADF schema to Spark DDL"""
        
        type_mapping = {
            'String': 'STRING',
            'Int32': 'INT',
            'Int64': 'BIGINT',
            'Double': 'DOUBLE',
            'Boolean': 'BOOLEAN',
            'DateTime': 'TIMESTAMP',
            'Decimal': 'DECIMAL(18,2)'
        }
        
        columns = []
        for col in adf_schema:
            spark_type = type_mapping.get(col['type'], 'STRING')
            columns.append(f"  {col['name']} {spark_type}")
        
        return ",\n".join(columns)
```

## 10. Validation Checklist

### Pre-Migration Validation
- [ ] All datasets cataloged
- [ ] Schemas documented
- [ ] File formats identified
- [ ] Locations mapped
- [ ] Partition strategies defined
- [ ] Target table names determined

### Post-Migration Validation
```python
def validate_dataset_migration(
    adf_dataset_name: str,
    delta_table_name: str
):
    """Validate dataset migration"""
    
    # Check table exists
    assert spark.catalog.tableExists(delta_table_name), \
        f"Table {delta_table_name} does not exist"
    
    # Verify schema
    delta_schema = spark.table(delta_table_name).schema
    print(f"Delta schema: {delta_schema}")
    
    # Check row count
    row_count = spark.table(delta_table_name).count()
    print(f"Row count: {row_count}")
    
    # Verify partitioning
    table_details = spark.sql(f"DESCRIBE DETAIL {delta_table_name}").collect()[0]
    partitions = table_details['partitionColumns']
    print(f"Partitions: {partitions}")
    
    # Check Delta properties
    properties = spark.sql(f"SHOW TBLPROPERTIES {delta_table_name}").collect()
    for prop in properties:
        print(f"{prop['key']}: {prop['value']}")
    
    print(f"✅ {adf_dataset_name} → {delta_table_name} validation passed")
```

---

**Related Skills:**
* [adf_linked_service_to_unity_catalog.md](adf_linked_service_to_unity_catalog.md) - Connection management
* [adf_copy_activity_to_spark.md](adf_copy_activity_to_spark.md) - Data movement
* [migration_index.md](migration_index.md) - Complete migration guide
