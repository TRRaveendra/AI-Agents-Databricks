# ADF ForEach Activity → Task Loop / Parameterized Job

## 1. Overview

### What is an ADF ForEach Activity?
The ForEach Activity in Azure Data Factory iterates over a collection (array) and executes a set of activities for each item in the collection. It supports both sequential and parallel execution and is commonly used for processing multiple files, tables, or configuration-driven workloads.

### Where is it used?
* Processing multiple files from a directory listing
* Iterating over a list of tables for bulk operations
* Configuration-driven pipeline execution
* Multi-tenant data processing
* Batch processing with variable parameters
* Dynamic ETL based on metadata

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF ForEach | Lakeflow Equivalent |
-------------|---------------------|
 ForEach Activity | Parameterized notebook with loop OR dynamic job runs |
 Sequential execution | Notebook with for loop |
 Parallel execution | Multiple job runs OR notebook parallelization |
 Items array | Parameter list OR query result |
 Batch count | Spark parallelism settings |
 IsSequential flag | Loop vs parallel task spawning |

### Key Differences
* **Execution Model**: ADF's built-in iterator vs Databricks programmatic control
* **Parallelism**: ADF batch count vs Spark parallelism and cluster concurrency
* **Flexibility**: Lakeflow offers more control over execution patterns
* **Scalability**: Spark can handle much larger iteration sets
* **Dependencies**: More granular control in Lakeflow

## 3. Migration Strategy (Step-by-Step)

### Step 1: Analyze ForEach Pattern
```python
def analyze_foreach_pattern(foreach_activity: dict) -> dict:
    """Analyze ForEach activity to determine migration approach"""
    
    type_props = foreach_activity['typeProperties']
    
    return {
        'is_sequential': type_props.get('isSequential', False),
        'batch_count': type_props.get('batchCount', 20),
        'items_source': type_props.get('items', {}).get('value', ''),
        'activities': type_props.get('activities', []),
        'activity_count': len(type_props.get('activities', []))
    }
```

### Step 2: Choose Migration Approach
```python
def choose_migration_approach(pattern_analysis: dict) -> str:
    """Determine best Lakeflow pattern based on ForEach characteristics"""
    
    # Simple iteration → Single notebook with loop
    if pattern_analysis['activity_count'] == 1 and pattern_analysis['items_count'] < 100:
        return 'notebook_loop'
    
    # Parallel processing → Dynamic job runs
    if not pattern_analysis['is_sequential'] and pattern_analysis['items_count'] > 10:
        return 'dynamic_job_runs'
    
    # Complex dependencies → Task-per-item (static)
    if pattern_analysis['activity_count'] > 3:
        return 'static_task_per_item'
    
    # Default: Parameterized notebook
    return 'parameterized_notebook'
```

### Step 3: Generate Lakeflow Code
```python
def generate_loop_implementation(foreach_activity: dict, approach: str) -> str:
    """Generate appropriate Lakeflow implementation"""
    
    if approach == 'notebook_loop':
        return generate_notebook_loop(foreach_activity)
    elif approach == 'dynamic_job_runs':
        return generate_dynamic_job_runs(foreach_activity)
    elif approach == 'parameterized_notebook':
        return generate_parameterized_notebook(foreach_activity)
    else:
        return generate_static_tasks(foreach_activity)
```

## 4. Code Conversion

### ADF Example (JSON)

#### Example 1: Sequential File Processing
```json
{
  "name": "ForEachFile",
  "type": "ForEach",
  "typeProperties": {
    "items": {
      "value": "@activity('GetFileList').output.childItems",
      "type": "Expression"
    },
    "isSequential": true,
    "activities": [
      {
        "name": "ProcessFile",
        "type": "DatabricksNotebook",
        "typeProperties": {
          "notebookPath": "/ETL/process_file",
          "baseParameters": {
            "file_name": "@item().name",
            "file_path": "@item().path"
          }
        }
      }
    ]
  }
}
```

#### Example 2: Parallel Table Processing
```json
{
  "name": "ForEachTable",
  "type": "ForEach",
  "typeProperties": {
    "items": {
      "value": "@activity('GetTableList').output.value",
      "type": "Expression"
    },
    "isSequential": false,
    "batchCount": 4,
    "activities": [
      {
        "name": "CopyTableData",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT * FROM @{item().table_name}"
          },
          "sink": {
            "type": "ParquetSink"
          }
        }
      },
      {
        "name": "TransformData",
        "type": "DatabricksNotebook",
        "dependsOn": [{"activity": "CopyTableData"}],
        "typeProperties": {
          "notebookPath": "/ETL/transform",
          "baseParameters": {
            "table_name": "@item().table_name"
          }
        }
      }
    ]
  }
}
```

### Databricks Equivalent

#### Approach 1: Notebook with Loop (Sequential Processing)
```yaml
# Job definition
tasks:
  - task_key: get_file_list
    sql_task:
      query:
        query: |
          SELECT file_name, file_path
          FROM production_catalog.control.files_to_process
          WHERE status = 'pending'
      warehouse_id: "{{job.parameters.warehouse_id}}"
  
  - task_key: process_files_loop
    depends_on:
      - task_key: get_file_list
    notebook_task:
      notebook_path: /Workspace/ETL/process_files_loop
      base_parameters:
        file_list_task: "get_file_list"
    job_cluster_key: shared_cluster
```

```python
# Databricks notebook: /Workspace/ETL/process_files_loop
# COMMAND ----------
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# COMMAND ----------
# Get file list from previous task
file_list_task = dbutils.widgets.get("file_list_task")

# Retrieve task output (or query control table)
file_list_df = spark.sql("""
    SELECT file_name, file_path
    FROM production_catalog.control.files_to_process
    WHERE status = 'pending'
""")

files = [row.asDict() for row in file_list_df.collect()]

print(f"Processing {len(files)} files")

# COMMAND ----------
# Define processing function
def process_single_file(file_info):
    """Process a single file (replaces inner activity)"""
    file_name = file_info['file_name']
    file_path = file_info['file_path']
    
    try:
        # Call processing notebook
        result = dbutils.notebook.run(
            "/Workspace/ETL/process_file",
            timeout_seconds=3600,
            arguments={
                "file_name": file_name,
                "file_path": file_path
            }
        )
        
        return {
            "file_name": file_name,
            "status": "SUCCESS",
            "result": result
        }
    except Exception as e:
        return {
            "file_name": file_name,
            "status": "FAILED",
            "error": str(e)
        }

# COMMAND ----------
# Sequential processing (isSequential = true equivalent)
results = []

for file_info in files:
    result = process_single_file(file_info)
    results.append(result)
    print(f"Processed {file_info['file_name']}: {result['status']}")

# COMMAND ----------
# Summary
success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
failed_count = len(results) - success_count

print(f"Completed: {success_count} succeeded, {failed_count} failed")

# COMMAND ----------
# Update control table
for result in results:
    spark.sql(f"""
        UPDATE production_catalog.control.files_to_process
        SET status = '{result['status']}',
            processed_timestamp = CURRENT_TIMESTAMP()
        WHERE file_name = '{result['file_name']}'
    """)

# COMMAND ----------
# Return summary
dbutils.notebook.exit(json.dumps({
    "total": len(results),
    "success": success_count,
    "failed": failed_count
}))
```

#### Approach 2: Parallel Processing with ThreadPoolExecutor
```python
# Databricks notebook: /Workspace/ETL/process_tables_parallel
# COMMAND ----------
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col
import json

# COMMAND ----------
# Get table list
table_list_df = spark.sql("""
    SELECT table_name, priority
    FROM production_catalog.control.tables_to_process
    WHERE status = 'pending'
    ORDER BY priority
""")

tables = [row['table_name'] for row in table_list_df.collect()]

print(f"Processing {len(tables)} tables in parallel")

# COMMAND ----------
def process_single_table(table_name):
    """Process one table - replaces ForEach inner activities"""
    try:
        # Step 1: Copy table data (replaces Copy activity)
        copy_result = dbutils.notebook.run(
            "/Workspace/ETL/copy_table",
            timeout_seconds=3600,
            arguments={"table_name": table_name}
        )
        
        # Step 2: Transform data (replaces Transform activity)
        transform_result = dbutils.notebook.run(
            "/Workspace/ETL/transform_table",
            timeout_seconds=3600,
            arguments={"table_name": table_name}
        )
        
        return {
            "table_name": table_name,
            "status": "SUCCESS",
            "copy_result": copy_result,
            "transform_result": transform_result
        }
    except Exception as e:
        return {
            "table_name": table_name,
            "status": "FAILED",
            "error": str(e)
        }

# COMMAND ----------
# Parallel processing (isSequential = false, batchCount = 4 equivalent)
results = []
max_workers = 4  # Equivalent to batchCount

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submit all tasks
    future_to_table = {
        executor.submit(process_single_table, table): table 
        for table in tables
    }
    
    # Collect results as they complete
    for future in as_completed(future_to_table):
        table = future_to_table[future]
        try:
            result = future.result()
            results.append(result)
            print(f"Completed {table}: {result['status']}")
        except Exception as e:
            results.append({
                "table_name": table,
                "status": "FAILED",
                "error": str(e)
            })

# COMMAND ----------
# Summary
success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
failed_count = len(results) - success_count

dbutils.notebook.exit(json.dumps({
    "total": len(results),
    "success": success_count,
    "failed": failed_count,
    "results": results
}))
```

#### Approach 3: Spark Native Parallelization (Best Performance)
```python
# Databricks notebook: /Workspace/ETL/process_tables_spark_parallel
# COMMAND ----------
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------
# Get table list
tables_df = spark.sql("""
    SELECT table_name, schema_name, catalog_name
    FROM production_catalog.control.tables_to_process
    WHERE status = 'pending'
""")

# COMMAND ----------
# Define UDF for processing (runs distributed across Spark cluster)
def process_table_udf(table_name: str, schema_name: str, catalog_name: str) -> str:
    """
    Process single table - runs in parallel across Spark executors
    """
    try:
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        
        # Read source table
        df = spark.table(full_table_name)
        
        # Apply transformations
        transformed_df = (
            df
            .filter(col("is_active") == True)
            .withColumn("processing_timestamp", current_timestamp())
        )
        
        # Write to target
        target_table = f"production_catalog.gold.{table_name}"
        transformed_df.write.mode("overwrite").saveAsTable(target_table)
        
        return "SUCCESS"
    except Exception as e:
        return f"FAILED: {str(e)}"

# Register UDF
from pyspark.sql.functions import udf
process_udf = udf(process_table_udf, StringType())

# COMMAND ----------
# Process all tables in parallel using Spark
# This distributes work across cluster automatically
results_df = (
    tables_df
    .repartition(10)  # Control parallelism
    .withColumn("result", process_udf(col("table_name"), col("schema_name"), col("catalog_name")))
)

# COMMAND ----------
# Persist results
results_df.write.mode("overwrite").saveAsTable("production_catalog.control.processing_results")

# COMMAND ----------
# Summary statistics
summary = results_df.groupBy("result").count().collect()

for row in summary:
    print(f"{row['result']}: {row['count']} tables")

dbutils.notebook.exit('{"status": "COMPLETED"}')
```

#### Approach 4: Dynamic Job Runs (Enterprise Pattern)
```yaml
# Parent job that spawns child job runs
tasks:
  - task_key: get_table_list
    sql_task:
      query:
        query: |
          SELECT table_name
          FROM production_catalog.control.tables_to_process
          WHERE status = 'pending'
      warehouse_id: "{{job.parameters.warehouse_id}}"
  
  - task_key: spawn_processing_jobs
    depends_on:
      - task_key: get_table_list
    notebook_task:
      notebook_path: /Workspace/ETL/spawn_processing_jobs
      base_parameters:
        child_job_id: "{{job.parameters.processing_job_id}}"
        max_parallel: "4"
    job_cluster_key: shared_cluster
```

```python
# Databricks notebook: /Workspace/ETL/spawn_processing_jobs
# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNow
import time

# COMMAND ----------
# Parameters
dbutils.widgets.text("child_job_id", "")
dbutils.widgets.text("max_parallel", "4")

child_job_id = int(dbutils.widgets.get("child_job_id"))
max_parallel = int(dbutils.widgets.get("max_parallel"))

# COMMAND ----------
# Get table list
tables_df = spark.sql("""
    SELECT table_name
    FROM production_catalog.control.tables_to_process
    WHERE status = 'pending'
""")

tables = [row['table_name'] for row in tables_df.collect()]

# COMMAND ----------
# Initialize Databricks client
w = WorkspaceClient()

# COMMAND ----------
# Spawn job runs with concurrency control
active_runs = []
completed_runs = []

for table_name in tables:
    # Wait if max parallel reached
    while len(active_runs) >= max_parallel:
        time.sleep(10)
        
        # Check status of active runs
        for run_id in active_runs[:]:
            run = w.jobs.get_run(run_id)
            
            if run.state.life_cycle_state.value in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                active_runs.remove(run_id)
                completed_runs.append({
                    "run_id": run_id,
                    "result": run.state.result_state.value if run.state.result_state else "UNKNOWN"
                })
    
    # Spawn new run
    run = w.jobs.run_now(
        job_id=child_job_id,
        job_parameters={"table_name": table_name}
    )
    
    active_runs.append(run.run_id)
    print(f"Started run {run.run_id} for table {table_name}")

# COMMAND ----------
# Wait for all remaining runs to complete
while active_runs:
    time.sleep(10)
    
    for run_id in active_runs[:]:
        run = w.jobs.get_run(run_id)
        
        if run.state.life_cycle_state.value in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
            active_runs.remove(run_id)
            completed_runs.append({
                "run_id": run_id,
                "result": run.state.result_state.value if run.state.result_state else "UNKNOWN"
            })

# COMMAND ----------
# Summary
success_count = sum(1 for r in completed_runs if r['result'] == 'SUCCESS')
failed_count = len(completed_runs) - success_count

dbutils.notebook.exit(json.dumps({
    "total": len(completed_runs),
    "success": success_count,
    "failed": failed_count
}))
```

## 5. Design Patterns

### Pattern 1: Metadata-Driven Processing
```python
# Configuration table
spark.sql("""
    CREATE TABLE IF NOT EXISTS production_catalog.control.processing_config (
        config_id INT,
        source_table STRING,
        target_table STRING,
        transform_type STRING,
        is_active BOOLEAN,
        priority INT
    )
""")

# Process based on metadata
config_df = spark.table("production_catalog.control.processing_config") \
    .filter(col("is_active") == True) \
    .orderBy("priority")

for row in config_df.collect():
    process_with_config(row.asDict())
```

### Pattern 2: Chunked Processing for Large Lists
```python
def process_in_chunks(items: list, chunk_size: int = 100):
    """Process large lists in manageable chunks"""
    
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i + chunk_size]
        
        # Process chunk in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_item, item) for item in chunk]
            results = [f.result() for f in futures]
        
        # Checkpoint progress
        checkpoint_progress(i, len(items))
```

### Pattern 3: Fault-Tolerant Loop with Retry
```python
def process_with_retry(items: list, max_retries: int = 3):
    """Process items with automatic retry on failure"""
    
    failed_items = []
    
    for item in items:
        for attempt in range(max_retries):
            try:
                process_item(item)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    failed_items.append({"item": item, "error": str(e)})
                else:
                    time.sleep(2 ** attempt)  # Exponential backoff
    
    if failed_items:
        # Write to quarantine table
        spark.createDataFrame(failed_items).write \
            .mode("append") \
            .saveAsTable("production_catalog.control.failed_items")
```

### Pattern 4: Dynamic Parallelism Based on Cluster Size
```python
# Automatically adjust parallelism based on cluster
def get_optimal_parallelism() -> int:
    """Calculate optimal parallelism based on cluster size"""
    
    # Get executor count
    num_executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
    cores_per_executor = int(spark.conf.get("spark.executor.cores"))
    
    # Use 2x cores for I/O bound tasks, 1x for CPU bound
    return num_executors * cores_per_executor * 2

max_workers = get_optimal_parallelism()
print(f"Using {max_workers} parallel workers")

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(process_item, items)
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Not Using Parallelism
**Problem**: Sequential processing when parallel would be more efficient.
```python
# WRONG: Sequential when items are independent
for table in tables:
    process_table(table)  # Takes 1 hour per table × 100 tables = 100 hours
```

**Solution**: Use parallel processing.
```python
# CORRECT: Parallel processing
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(process_table, tables)  # Takes 1 hour × 10 batches = 10 hours
```

### ❌ Anti-Pattern 2: No Error Handling in Loops
**Problem**: One failure stops entire loop.
```python
# WRONG: First error breaks the loop
for item in items:
    process_item(item)  # Fails on item 5, items 6-100 never processed
```

**Solution**: Catch and log errors, continue processing.
```python
# CORRECT: Handle errors gracefully
for item in items:
    try:
        process_item(item)
    except Exception as e:
        log_error(item, e)
        continue
```

### ❌ Anti-Pattern 3: No Progress Tracking
**Problem**: Can't resume failed runs, no visibility into progress.
```python
# WRONG: No tracking
for item in items:
    process_item(item)
```

**Solution**: Track progress in control table.
```python
# CORRECT: Track progress
for item in items:
    try:
        process_item(item)
        mark_as_complete(item)
    except Exception as e:
        mark_as_failed(item, str(e))
```

## 7. Performance Considerations

### Right-Sizing Parallelism
```python
# Too low: Underutilizes cluster
max_workers = 1

# Too high: Overwhelms cluster, causes OOM
max_workers = 1000

# Just right: Based on cluster capacity
num_executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
max_workers = num_executors * 4  # 4× executors for I/O operations
```

### Memory Management
```python
# Clear cache between iterations to prevent OOM
for table in large_table_list:
    df = spark.table(table)
    process(df)
    df.unpersist()  # Free memory
    spark.catalog.clearCache()
```

### Batching for Large Item Lists
```python
# Process 10,000 items in batches of 100
batch_size = 100

for i in range(0, len(items), batch_size):
    batch = items[i:i+batch_size]
    process_batch(batch)
    checkpoint(i)
```

## 8. Governance & Security

### Unity Catalog ACLs in Loops
```python
# Automatically respects table-level permissions
for table_name in tables:
    try:
        # Only processes tables user has access to
        df = spark.table(f"production_catalog.gold.{table_name}")
        process(df)
    except Exception as e:
        # Permission denied will be caught here
        log_error(f"Cannot access {table_name}: {e}")
```

## 9. Automation Hints (IMPORTANT)

### AI Agent Transformation Logic
```python
class ForEachTransformer:
    """Transform ADF ForEach to Lakeflow patterns"""
    
    def transform(self, foreach_activity: dict) -> dict:
        """Main transformation entry point"""
        
        analysis = self.analyze(foreach_activity)
        approach = self.select_approach(analysis)
        
        if approach == 'spark_parallel':
            return self.generate_spark_parallel(foreach_activity)
        elif approach == 'thread_pool':
            return self.generate_thread_pool(foreach_activity)
        elif approach == 'dynamic_jobs':
            return self.generate_dynamic_jobs(foreach_activity)
        else:
            return self.generate_sequential_loop(foreach_activity)
    
    def select_approach(self, analysis: dict) -> str:
        """Select best migration approach"""
        
        # Large item count + simple processing → Spark parallel
        if analysis['item_count'] > 1000 and analysis['complexity'] == 'low':
            return 'spark_parallel'
        
        # Moderate count + parallel → ThreadPool
        if not analysis['is_sequential'] and analysis['item_count'] < 1000:
            return 'thread_pool'
        
        # Complex orchestration → Dynamic jobs
        if analysis['complexity'] == 'high':
            return 'dynamic_jobs'
        
        # Default: Sequential loop
        return 'sequential_loop'
```

## 10. Validation Checklist

### Pre-Migration Validation
- [ ] Item source identified (lookup output, parameter, etc.)
- [ ] Sequential vs parallel execution documented
- [ ] Batch count recorded
- [ ] Inner activities mapped
- [ ] Dependencies between inner activities understood
- [ ] Expected item count estimated
- [ ] Timeout requirements noted

### Post-Migration Validation
```python
def validate_foreach_migration(adf_item_count: int, spark_results: dict):
    """Validate all items were processed"""
    
    total_processed = spark_results['success'] + spark_results['failed']
    
    assert total_processed == adf_item_count, \
        f"Item count mismatch: {adf_item_count} expected, {total_processed} processed"
    
    # Verify no items were skipped
    processed_items = set(spark_results['processed_items'])
    expected_items = set(adf_results['items'])
    
    assert processed_items == expected_items, "Some items were not processed"
    
    print("✅ ForEach validation passed")
```

---

**Related Skills:**
* [adf_activity_to_task.md](adf_activity_to_task.md) - Activity transformation
* [adf_lookup_to_spark_sql.md](adf_lookup_to_spark_sql.md) - Getting item lists
* [migration_index.md](migration_index.md) - Complete migration guide
