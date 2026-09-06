# ADF Lookup Activity → Spark SQL

## 1. Overview

### What is an ADF Lookup Activity?
The Lookup Activity in Azure Data Factory retrieves a dataset or query result from a supported data source. It's commonly used to fetch configuration values, watermarks, control table data, or metadata that drives the pipeline's execution logic.

### Where is it used?
* Retrieving watermark values for incremental data loads
* Fetching configuration parameters from control tables
* Getting metadata (row counts, file lists, schema information)
* Validating data existence before processing
* Retrieving business rules or lookup data
* Dynamic pipeline parameter generation

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF Lookup | Spark/Lakeflow Equivalent |
------------|---------------------------|
 Lookup Activity | `sql_task` or `spark.sql()` |
 First row only | `LIMIT 1` or `.first()` |
 All rows | Full result set |
 Activity output | Task values or notebook return |
 Parameter passing | Task value propagation |

### Key Differences
* **Execution**: ADF Lookup runs on integration runtime vs Spark runs on compute/SQL warehouse
* **Output Format**: ADF returns JSON activity output vs Spark returns DataFrame or task values
* **Result Size**: ADF has 5000 row limit vs Spark handles unlimited rows
* **Query Language**: Limited to source system SQL vs full Spark SQL capabilities
* **Performance**: Better performance with Databricks SQL warehouses and caching

## 3. Migration Strategy (Step-by-Step)

### Step 1: Analyze Lookup Purpose
```python
def classify_lookup_purpose(lookup_activity: dict) -> str:
    """Determine the purpose of the lookup activity"""
    
    query = lookup_activity['typeProperties']['source'].get('sqlReaderQuery', '').lower()
    
    # Watermark lookup pattern
    if 'max(' in query or 'watermark' in query:
        return 'watermark'
    
    # Configuration lookup
    if 'config' in query or 'parameter' in query or 'setting' in query:
        return 'configuration'
    
    # Metadata lookup
    if 'count(' in query or 'information_schema' in query:
        return 'metadata'
    
    # List lookup
    if lookup_activity['typeProperties'].get('firstRowOnly', True) == False:
        return 'list'
    
    return 'generic'
```

### Step 2: Extract Query and Settings
```python
def extract_lookup_details(lookup_activity: dict) -> dict:
    """Extract key details from ADF Lookup Activity"""
    
    type_props = lookup_activity['typeProperties']
    source = type_props['source']
    
    return {
        'query': source.get('sqlReaderQuery', ''),
        'stored_proc': source.get('storedProcedureName', ''),
        'first_row_only': type_props.get('firstRowOnly', True),
        'dataset': type_props.get('dataset', {}).get('referenceName', ''),
        'timeout': lookup_activity.get('policy', {}).get('timeout', '00:05:00')
    }
```

### Step 3: Generate Spark SQL Task
```python
def generate_sql_task(lookup_details: dict, purpose: str) -> dict:
    """Generate Lakeflow sql_task configuration"""
    
    query = lookup_details['query']
    
    # For watermark lookups, optimize query
    if purpose == 'watermark':
        query = optimize_watermark_query(query)
    
    task = {
        'task_key': generate_task_key(lookup_details),
        'sql_task': {
            'query': {
                'query': query
            },
            'warehouse_id': '${var.warehouse_id}'
        },
        'timeout_seconds': parse_timeout(lookup_details['timeout'])
    }
    
    return task
```

### Step 4: Handle Output Propagation
```python
def generate_output_handling(lookup_activity: dict, dependent_activities: list) -> str:
    """Generate code to handle lookup results in downstream tasks"""
    
    lookup_name = lookup_activity['name']
    
    # Generate parameter passing
    parameters = {}
    for dep_activity in dependent_activities:
        if has_reference_to_lookup(dep_activity, lookup_name):
            parameters[f'{lookup_name}_result'] = f'{{{{tasks.{lookup_name}.values.result}}}}'
    
    return parameters
```

## 4. Code Conversion

### ADF Example (JSON)

#### Example 1: Watermark Lookup
```json
{
  "name": "GetLastProcessedDate",
  "type": "Lookup",
  "dependsOn": [],
  "policy": {
    "timeout": "0.00:05:00",
    "retry": 2
  },
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT MAX(processed_date) as watermark_value FROM control.watermarks WHERE table_name = 'customers'"
    },
    "dataset": {
      "referenceName": "ControlDB",
      "type": "DatasetReference"
    },
    "firstRowOnly": true
  }
}
```

#### Example 2: Configuration Lookup (Multiple Rows)
```json
{
  "name": "GetActiveConnections",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT connection_name, connection_string, is_active FROM control.connections WHERE is_active = 1"
    },
    "dataset": {
      "referenceName": "ControlDB",
      "type": "DatasetReference"
    },
    "firstRowOnly": false
  }
}
```

#### Example 3: Metadata Lookup
```json
{
  "name": "CheckTableExists",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'customers'"
    },
    "firstRowOnly": true
  }
}
```

### Databricks Equivalent

#### YAML (sql_task for Watermark)
```yaml
tasks:
  # Watermark lookup
  - task_key: get_last_processed_date
    sql_task:
      query:
        query: |
          SELECT MAX(processed_date) as watermark_value 
          FROM production_catalog.control.watermarks 
          WHERE table_name = 'customers'
      warehouse_id: "{{job.parameters.warehouse_id}}"
    timeout_seconds: 300
    max_retries: 2
  
  # Use watermark in downstream task
  - task_key: incremental_load
    depends_on:
      - task_key: get_last_processed_date
    notebook_task:
      notebook_path: /Workspace/ETL/incremental_load_customers
      base_parameters:
        # Access previous task SQL result
        watermark: "{{tasks.get_last_processed_date.values.watermark_value}}"
    job_cluster_key: shared_cluster
```

#### PySpark (Notebook-based Lookup for Complex Logic)
```python
# Databricks notebook: /Workspace/ETL/get_watermark
# COMMAND ----------
from pyspark.sql.functions import max, col
from datetime import datetime

# COMMAND ----------
# Parameters
dbutils.widgets.text("table_name", "customers")
dbutils.widgets.text("control_table", "production_catalog.control.watermarks")

table_name = dbutils.widgets.get("table_name")
control_table = dbutils.widgets.get("control_table")

# COMMAND ----------
# Query watermark (replacing ADF Lookup)
watermark_df = spark.sql(f"""
    SELECT 
        COALESCE(MAX(processed_date), '1900-01-01') as watermark_value,
        COUNT(*) as record_count
    FROM {control_table}
    WHERE table_name = '{table_name}'
""")

# COMMAND ----------
# Get result (equivalent to firstRowOnly = true)
result = watermark_df.first()
watermark_value = result['watermark_value']
record_count = result['record_count']

print(f"Watermark for {table_name}: {watermark_value}")
print(f"Control records found: {record_count}")

# COMMAND ----------
# Pass value to downstream tasks using task values
dbutils.jobs.taskValues.set(key="watermark_value", value=str(watermark_value))
dbutils.jobs.taskValues.set(key="record_count", value=record_count)

# COMMAND ----------
# Return result (for job task output)
import json
result_dict = {
    "watermark_value": str(watermark_value),
    "record_count": record_count,
    "status": "SUCCESS"
}

dbutils.notebook.exit(json.dumps(result_dict))
```

#### PySpark (Multiple Row Lookup)
```python
# Databricks notebook: /Workspace/ETL/get_active_connections
# COMMAND ----------
# Query for multiple rows (firstRowOnly = false)
connections_df = spark.sql("""
    SELECT 
        connection_name,
        connection_string,
        is_active
    FROM production_catalog.control.connections
    WHERE is_active = 1
""")

# COMMAND ----------
# Convert to list of dictionaries (similar to ADF output)
connections = [row.asDict() for row in connections_df.collect()]

print(f"Found {len(connections)} active connections")

# COMMAND ----------
# Store as task value (for downstream consumption)
import json

# For task values (limited size)
dbutils.jobs.taskValues.set(key="connection_count", value=len(connections))

# For large datasets, write to temp table instead
temp_table = "temp.active_connections"
connections_df.write.mode("overwrite").saveAsTable(temp_table)

dbutils.jobs.taskValues.set(key="connections_table", value=temp_table)

# COMMAND ----------
# Return summary
result = {
    "connection_count": len(connections),
    "connections_table": temp_table,
    "status": "SUCCESS"
}

dbutils.notebook.exit(json.dumps(result))
```

#### SQL Task (Metadata Check)
```yaml
tasks:
  - task_key: check_table_exists
    sql_task:
      query:
        query: |
          SELECT COUNT(*) as table_count
          FROM system.information_schema.tables
          WHERE table_catalog = 'production_catalog'
            AND table_schema = 'bronze'
            AND table_name = 'customers'
      warehouse_id: "{{job.parameters.warehouse_id}}"
  
  - task_key: conditional_processing
    depends_on:
      - task_key: check_table_exists
    condition_task:
      op: GREATER_THAN
      left: "{{tasks.check_table_exists.values.table_count}}"
      right: "0"
      when_true:
        - task_key: process_existing_table
      when_false:
        - task_key: create_new_table
```

## 5. Design Patterns

### Pattern 1: Watermark-Driven Incremental Load
```yaml
tasks:
  # Get watermark
  - task_key: get_watermark
    sql_task:
      query:
        query: |
          SELECT COALESCE(MAX(watermark_value), '1900-01-01') as last_processed
          FROM production_catalog.control.watermarks
          WHERE table_name = '{{job.parameters.table_name}}'
      warehouse_id: "{{job.parameters.warehouse_id}}"
  
  # Incremental load using watermark
  - task_key: incremental_load
    depends_on:
      - task_key: get_watermark
    notebook_task:
      notebook_path: /Workspace/ETL/incremental_load
      base_parameters:
        table_name: "{{job.parameters.table_name}}"
        watermark: "{{tasks.get_watermark.values.last_processed}}"
  
  # Update watermark
  - task_key: update_watermark
    depends_on:
      - task_key: incremental_load
    sql_task:
      query:
        query: |
          MERGE INTO production_catalog.control.watermarks AS target
          USING (
            SELECT 
              '{{job.parameters.table_name}}' as table_name,
              CURRENT_TIMESTAMP() as watermark_value
          ) AS source
          ON target.table_name = source.table_name
          WHEN MATCHED THEN UPDATE SET target.watermark_value = source.watermark_value
          WHEN NOT MATCHED THEN INSERT (table_name, watermark_value) VALUES (source.table_name, source.watermark_value)
      warehouse_id: "{{job.parameters.warehouse_id}}"
```

### Pattern 2: Configuration-Driven Processing
```python
# Notebook: /Workspace/ETL/config_driven_pipeline
# COMMAND ----------
# Get configuration from control table
config_df = spark.sql("""
    SELECT 
        config_key,
        config_value,
        data_type
    FROM production_catalog.control.pipeline_config
    WHERE pipeline_name = getArgument('pipeline_name')
      AND is_active = true
""")

# COMMAND ----------
# Convert to dictionary
config_dict = {
    row['config_key']: row['config_value'] 
    for row in config_df.collect()
}

# Use configuration
source_path = config_dict.get('source_path', '/default/path')
target_table = config_dict.get('target_table', 'default.table')
batch_size = int(config_dict.get('batch_size', '1000'))

# COMMAND ----------
# Process using configuration
df = spark.read.format("delta").load(source_path)
df.write.mode("append").option("maxRecordsPerFile", batch_size).saveAsTable(target_table)
```

### Pattern 3: Dynamic Table List Processing
```yaml
tasks:
  # Get list of tables to process
  - task_key: get_table_list
    sql_task:
      query:
        query: |
          SELECT table_name, priority
          FROM production_catalog.control.tables_to_process
          WHERE status = 'pending'
            AND processing_date = CURRENT_DATE()
          ORDER BY priority
      warehouse_id: "{{job.parameters.warehouse_id}}"
  
  # Process each table (ForEach equivalent covered in separate skill)
  - task_key: process_tables
    depends_on:
      - task_key: get_table_list
    notebook_task:
      notebook_path: /Workspace/ETL/process_table_list
      base_parameters:
        # Pass entire result set
        table_list_result: "{{tasks.get_table_list.values}}"
```

### Pattern 4: Data Quality Validation Lookup
```python
# Databricks notebook: /Workspace/DQ/validate_source_data
# COMMAND ----------
# Check data quality metrics before processing
dq_check = spark.sql("""
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails,
        SUM(CASE WHEN email NOT RLIKE '^[^@]+@[^@]+\\.[^@]+$' THEN 1 ELSE 0 END) as invalid_emails,
        MIN(created_date) as min_date,
        MAX(created_date) as max_date
    FROM source_catalog.raw.customers
    WHERE ingestion_date = CURRENT_DATE()
""").first()

# COMMAND ----------
# Validate thresholds
total_rows = dq_check['total_rows']
null_email_pct = (dq_check['null_emails'] / total_rows * 100) if total_rows > 0 else 0
invalid_email_pct = (dq_check['invalid_emails'] / total_rows * 100) if total_rows > 0 else 0

# COMMAND ----------
# Determine if processing should continue
quality_threshold_met = (
    total_rows > 0 and
    null_email_pct < 5.0 and
    invalid_email_pct < 10.0
)

# COMMAND ----------
# Pass validation result to next task
dbutils.jobs.taskValues.set(key="quality_check_passed", value=quality_threshold_met)
dbutils.jobs.taskValues.set(key="total_rows", value=total_rows)
dbutils.jobs.taskValues.set(key="null_email_pct", value=round(null_email_pct, 2))

# COMMAND ----------
if not quality_threshold_met:
    raise Exception(f"Data quality check failed: {null_email_pct}% null emails, {invalid_email_pct}% invalid emails")

dbutils.notebook.exit('{"status": "SUCCESS", "quality_passed": true}')
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Excessive Lookups
**Problem**: Multiple small lookup queries instead of one optimized query.
```yaml
# WRONG: Multiple lookup tasks
tasks:
  - task_key: get_config_1
    sql_task:
      query: "SELECT value FROM config WHERE key = 'key1'"
  
  - task_key: get_config_2
    sql_task:
      query: "SELECT value FROM config WHERE key = 'key2'"
  
  - task_key: get_config_3
    sql_task:
      query: "SELECT value FROM config WHERE key = 'key3'"
```

**Solution**: Single lookup with all needed values.
```yaml
# CORRECT: Single lookup task
tasks:
  - task_key: get_all_config
    sql_task:
      query: |
        SELECT key, value
        FROM config
        WHERE key IN ('key1', 'key2', 'key3')
```

### ❌ Anti-Pattern 2: Not Caching Lookup Results
**Problem**: Repeatedly querying same lookup data.
```python
# WRONG: Query executed every time
for batch in batches:
    config = spark.sql("SELECT * FROM config WHERE key = 'setting'").first()
    process(batch, config)
```

**Solution**: Cache lookup results.
```python
# CORRECT: Cache once, reuse many times
config_df = spark.sql("SELECT * FROM config WHERE key = 'setting'").cache()
config = config_df.first()

for batch in batches:
    process(batch, config)
```

### ❌ Anti-Pattern 3: Using Notebook When SQL Task Suffices
**Problem**: Unnecessary notebook complexity for simple queries.
```python
# WRONG: Full notebook for simple lookup
# COMMAND ----------
result = spark.sql("SELECT MAX(date) FROM watermark").first()
dbutils.jobs.taskValues.set("max_date", str(result[0]))
dbutils.notebook.exit('{"status": "SUCCESS"}')
```

**Solution**: Use sql_task directly.
```yaml
# CORRECT: Direct SQL task
- task_key: get_max_date
  sql_task:
    query: "SELECT MAX(date) as max_date FROM watermark"
```

### ❌ Anti-Pattern 4: Not Handling Empty Results
**Problem**: Assuming lookup always returns data.
```python
# WRONG: No null check
watermark = spark.sql("SELECT MAX(date) FROM watermark").first()['date']
# Fails if no rows exist
```

**Solution**: Handle empty results gracefully.
```python
# CORRECT: Default value handling
result = spark.sql("SELECT COALESCE(MAX(date), '1900-01-01') as date FROM watermark").first()
watermark = result['date']
```

## 7. Performance Considerations

### SQL Warehouse Optimization
```yaml
# Use appropriate warehouse size for lookups
tasks:
  - task_key: lightweight_lookup
    sql_task:
      warehouse_id: "{{job.parameters.small_warehouse_id}}"  # Small warehouse for simple lookups
  
  - task_key: complex_lookup
    sql_task:
      warehouse_id: "{{job.parameters.large_warehouse_id}}"  # Large warehouse for complex queries
```

### Query Optimization
```sql
-- WRONG: Non-optimized watermark query
SELECT MAX(processed_date) 
FROM large_table;  -- Full table scan

-- CORRECT: Optimized with filter
SELECT MAX(processed_date) 
FROM large_table
WHERE table_name = 'customers'  -- Uses partition/index
  AND year = YEAR(CURRENT_DATE());
```

### Result Caching
```python
# Enable result caching for repeated lookups
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# Cache frequently accessed lookup tables
spark.sql("CACHE TABLE production_catalog.control.config")

# Use cached table
config = spark.table("production_catalog.control.config")
```

### Minimize Data Transfer
```python
# WRONG: Fetching large result set
all_data = spark.sql("SELECT * FROM large_lookup_table").collect()

# CORRECT: Fetch only needed columns and rows
needed_data = spark.sql("""
    SELECT key, value 
    FROM large_lookup_table 
    WHERE is_active = true
    LIMIT 100
""").collect()
```

## 8. Governance & Security

### Unity Catalog Integration
```sql
-- Lookups automatically respect Unity Catalog permissions
SELECT watermark_value
FROM production_catalog.control.watermarks
-- User must have SELECT permission on this table
```

### Secure Parameter Passing
```python
# WRONG: Hardcoded sensitive values in lookup
api_key = spark.sql("SELECT 'hardcoded_key' as key").first()['key']

# CORRECT: Use secrets
api_key = dbutils.secrets.get(scope="api-keys", key="external-api")
```

### Audit Logging
```sql
-- All SQL tasks are automatically audited in Unity Catalog
SELECT *
FROM system.access.audit
WHERE action_name = 'getTable'
  AND request_params.table_name = 'control.watermarks'
ORDER BY event_time DESC;
```

## 9. Automation Hints (IMPORTANT)

### AI Agent Transformation Logic
```python
class LookupActivityTransformer:
    """
    AI Agent for transforming ADF Lookup Activity to Spark SQL
    """
    
    def transform(self, lookup_activity: dict) -> dict:
        """Main transformation method"""
        
        details = self._extract_details(lookup_activity)
        purpose = self._classify_purpose(details['query'])
        
        # Decide between sql_task and notebook_task
        if self._is_simple_query(details['query']) and details['first_row_only']:
            return self._generate_sql_task(details)
        else:
            return self._generate_notebook_task(details)
    
    def _classify_purpose(self, query: str) -> str:
        """Classify lookup purpose for optimization"""
        query_lower = query.lower()
        
        patterns = {
            'watermark': ['max(', 'watermark', 'last_processed'],
            'configuration': ['config', 'parameter', 'setting'],
            'metadata': ['count(', 'information_schema', 'sys.'],
            'validation': ['exists', 'count(*)', 'having']
        }
        
        for purpose, keywords in patterns.items():
            if any(kw in query_lower for kw in keywords):
                return purpose
        
        return 'generic'
    
    def _is_simple_query(self, query: str) -> bool:
        """Determine if query is simple enough for sql_task"""
        query_lower = query.lower()
        
        # Simple if: single SELECT, no complex logic
        complexity_indicators = [
            'temp table', 'declare', 'if ', 'while ', 'cursor',
            'create ', 'drop ', 'alter ', 'insert ', 'update ', 'delete '
        ]
        
        return not any(indicator in query_lower for indicator in complexity_indicators)
    
    def _generate_sql_task(self, details: dict) -> dict:
        """Generate sql_task configuration"""
        
        # Convert query to Unity Catalog syntax
        converted_query = self._convert_to_uc_syntax(details['query'])
        
        return {
            'task_key': self._generate_task_key(details),
            'sql_task': {
                'query': {'query': converted_query},
                'warehouse_id': '{{job.parameters.warehouse_id}}'
            },
            'timeout_seconds': self._parse_timeout(details['timeout'])
        }
    
    def _generate_notebook_task(self, details: dict) -> dict:
        """Generate notebook_task for complex lookups"""
        
        # Generate notebook path
        notebook_path = f"/Workspace/Generated/lookup_{details['name']}"
        
        # Generate notebook content
        notebook_code = self._generate_lookup_notebook(details)
        
        return {
            'task_key': self._generate_task_key(details),
            'notebook_task': {
                'notebook_path': notebook_path,
                'base_parameters': {}
            },
            'generated_notebook': {
                'path': notebook_path,
                'content': notebook_code
            }
        }
    
    def _convert_to_uc_syntax(self, query: str) -> str:
        """Convert ADF SQL to Unity Catalog SQL"""
        
        # Replace database references with UC catalog.schema
        # Example: dbo.watermarks → production_catalog.control.watermarks
        conversions = {
            r'dbo\.': 'production_catalog.control.',
            r'GETDATE\(\)': 'CURRENT_TIMESTAMP()',
            r'DATEADD\(': 'DATE_ADD(',
            r'DATEDIFF\(': 'DATEDIFF(',
        }
        
        converted = query
        for pattern, replacement in conversions.items():
            converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
        
        return converted
    
    def _generate_lookup_notebook(self, details: dict) -> str:
        """Generate complete notebook for lookup"""
        
        template = '''
# Databricks notebook source
# MAGIC %md
# MAGIC # Lookup: {name}
# MAGIC Generated from ADF Lookup Activity

# COMMAND ----------
# Execute lookup query
result_df = spark.sql("""
{query}
""")

# COMMAND ----------
# Handle result
{result_handling}

# COMMAND ----------
# Return output
import json
result_dict = {result_output}
dbutils.notebook.exit(json.dumps(result_dict))
        '''
        
        # Generate result handling based on firstRowOnly
        if details['first_row_only']:
            result_handling = '''
result = result_df.first()
if result:
    # Set task values for each column
    for col in result_df.columns:
        dbutils.jobs.taskValues.set(key=col, value=str(result[col]))
else:
    raise Exception("Lookup returned no results")
            '''
            result_output = '{col: str(result[col]) for col in result_df.columns}'
        else:
            result_handling = '''
results = [row.asDict() for row in result_df.collect()]
dbutils.jobs.taskValues.set(key="row_count", value=len(results))

# For large result sets, write to temp table
if len(results) > 100:
    temp_table = "temp.lookup_{name}"
    result_df.write.mode("overwrite").saveAsTable(temp_table)
    dbutils.jobs.taskValues.set(key="result_table", value=temp_table)
            '''
            result_output = '{"row_count": len(results), "status": "SUCCESS"}'
        
        return template.format(
            name=details['name'],
            query=details['query'],
            result_handling=result_handling,
            result_output=result_output
        )
```

### Pattern-Based Optimization
```python
def optimize_lookup_for_purpose(query: str, purpose: str) -> str:
    """Apply purpose-specific optimizations"""
    
    optimizations = {
        'watermark': lambda q: add_partitioning(add_caching(q)),
        'configuration': lambda q: add_caching(q),
        'metadata': lambda q: use_system_tables(q),
        'validation': lambda q: add_limits(q)
    }
    
    optimizer = optimizations.get(purpose, lambda q: q)
    return optimizer(query)

def add_partitioning(query: str) -> str:
    """Add partition filters for better performance"""
    # Analyze query and add partition predicates
    if 'WHERE' in query.upper():
        # Add partition column to existing WHERE
        return query.replace('WHERE', 'WHERE year = YEAR(CURRENT_DATE()) AND')
    return query

def add_caching(query: str) -> str:
    """Wrap with caching hint"""
    return f"/* +CACHE */ {query}"
```

## 10. Validation Checklist

### Pre-Migration Validation
- [ ] Lookup query syntax validated
- [ ] Source dataset/connection identified
- [ ] First row only setting documented
- [ ] Timeout value recorded
- [ ] Retry policy captured
- [ ] Dependent activities identified
- [ ] Output references mapped
- [ ] Query converted to Unity Catalog syntax

### Post-Migration Validation
```python
def validate_lookup_migration(
    adf_lookup_output: dict,
    spark_task_output: dict
):
    """Validate migrated lookup produces same results"""
    
    # Compare row count
    adf_count = len(adf_lookup_output.get('value', []))
    spark_count = spark_task_output.get('row_count', 1)
    
    assert adf_count == spark_count, f"Row count mismatch: {adf_count} vs {spark_count}"
    
    # Compare values (for firstRowOnly = true)
    if adf_count == 1:
        adf_row = adf_lookup_output['value'][0]
        spark_row = spark_task_output
        
        for key in adf_row.keys():
            assert key in spark_row, f"Missing column: {key}"
            assert str(adf_row[key]) == str(spark_row[key]), \
                f"Value mismatch for {key}: {adf_row[key]} vs {spark_row[key]}"
    
    print("✅ Lookup validation passed")
```

### Performance Comparison
```sql
-- Compare lookup execution times
WITH adf_metrics AS (
  SELECT 
    activity_name,
    AVG(duration_seconds) as avg_duration,
    COUNT(*) as execution_count
  FROM adf_monitoring.lookup_activity_runs
  WHERE activity_name = 'GetLastProcessedDate'
    AND run_date >= CURRENT_DATE - INTERVAL 30 DAYS
  GROUP BY activity_name
),
spark_metrics AS (
  SELECT 
    task_key as activity_name,
    AVG(execution_duration/1000) as avg_duration,
    COUNT(*) as execution_count
  FROM system.lakeflow.task_run_timeline
  WHERE task_key = 'get_last_processed_date'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
  GROUP BY task_key
)
SELECT 
  a.activity_name,
  a.avg_duration as adf_seconds,
  s.avg_duration as spark_seconds,
  ((s.avg_duration - a.avg_duration) / a.avg_duration * 100) as pct_improvement
FROM adf_metrics a
JOIN spark_metrics s ON a.activity_name = s.activity_name;
```

---

**Related Skills:**
* [adf_activity_to_task.md](adf_activity_to_task.md) - General activity transformation
* [adf_foreach_to_task_loop.md](adf_foreach_to_task_loop.md) - Using lookup results in loops
* [migration_index.md](migration_index.md) - Complete migration guide
