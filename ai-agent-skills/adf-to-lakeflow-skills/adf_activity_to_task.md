# ADF Activity → Lakeflow Task

## 1. Overview

### What is an ADF Activity?
An ADF Activity is a processing unit within a pipeline that represents a single operation or action. Activities are the building blocks of data movement, data transformation, and control flow operations in Azure Data Factory.

### Where is it used?
* Data movement (Copy Activity)
* Data transformation (Mapping Data Flow, Databricks Notebook)
* Control flow (If Condition, ForEach, Until)
* External execution (Stored Procedure, Web Activity)
* Validation and testing (Lookup, Get Metadata)

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF Activity | Lakeflow Task Type |
--------------|-------------------|
 Copy Activity | `notebook_task` with Spark read/write |
 Databricks Notebook | `notebook_task` |
 Data Flow | `notebook_task` with Spark transformations |
 Stored Procedure | `sql_task` or `notebook_task` |
 Web Activity | `notebook_task` with REST API calls |
 Lookup | `sql_task` |
 Get Metadata | `notebook_task` with dbutils |
 ForEach | Multiple tasks with dynamic execution |
 If Condition | `condition_task` or `run_if` |
 Wait | `notebook_task` with sleep |
 Execute Pipeline | Separate job execution |

### Key Differences
* **Task Types**: Lakeflow uses notebook, SQL, Python, JAR, and pipeline tasks vs ADF's 30+ activity types
* **Execution Context**: All tasks run on Databricks compute vs distributed Azure services
* **Dependencies**: Explicit `depends_on` vs implicit `dependsOn` with conditions
* **Output Handling**: Task values and job parameters vs activity outputs
* **Error Handling**: Retry policies and run_if conditions vs complex policy definitions

## 3. Migration Strategy (Step-by-Step)

### Step 1: Classify Activity Types
```python
def classify_activity(activity: dict) -> str:
    """Determine Lakeflow task type from ADF activity"""
    
    activity_type = activity['type']
    
    mapping = {
        'Copy': 'notebook_task',
        'DatabricksNotebook': 'notebook_task',
        'SqlServerStoredProcedure': 'sql_task',
        'Lookup': 'sql_task',
        'WebActivity': 'notebook_task',
        'ForEach': 'multiple_tasks',
        'IfCondition': 'condition_task',
        'ExecutePipeline': 'run_job_task'
    }
    
    return mapping.get(activity_type, 'notebook_task')
```

### Step 2: Extract Dependencies
```python
def extract_dependencies(activity: dict) -> list:
    """Convert ADF dependsOn to Lakeflow depends_on"""
    
    depends_on = activity.get('dependsOn', [])
    lakeflow_deps = []
    
    for dep in depends_on:
        task_dep = {
            'task_key': dep['activity'].lower().replace(' ', '_')
        }
        
        # Map dependency conditions
        conditions = dep.get('dependencyConditions', ['Succeeded'])
        if 'Failed' in conditions:
            task_dep['outcome'] = 'false'
        elif 'Succeeded' in conditions:
            task_dep['outcome'] = 'true'
        
        lakeflow_deps.append(task_dep)
    
    return lakeflow_deps
```

### Step 3: Convert Activity Properties
```python
def convert_activity_properties(activity: dict) -> dict:
    """Map ADF activity properties to task configuration"""
    
    policy = activity.get('policy', {})
    
    task_config = {
        'task_key': activity['name'].lower().replace(' ', '_'),
        'timeout_seconds': parse_timeout(policy.get('timeout', '7.00:00:00')),
        'max_retries': policy.get('retry', 0),
        'retry_on_timeout': policy.get('retryIntervalInSeconds', 30) > 0
    }
    
    return task_config
```

### Step 4: Generate Task Definition
```python
def generate_task(activity: dict, task_type: str) -> dict:
    """Generate complete Lakeflow task definition"""
    
    base_config = convert_activity_properties(activity)
    dependencies = extract_dependencies(activity)
    
    task = {
        **base_config,
        'depends_on': dependencies
    }
    
    # Add task-specific configuration
    if task_type == 'notebook_task':
        task['notebook_task'] = {
            'notebook_path': determine_notebook_path(activity)
        }
    elif task_type == 'sql_task':
        task['sql_task'] = {
            'query': extract_sql_query(activity)
        }
    
    return task
```

## 4. Code Conversion

### ADF Example (JSON)

#### Example 1: Copy Activity
```json
{
  "name": "CopyCustomerData",
  "type": "Copy",
  "dependsOn": [],
  "policy": {
    "timeout": "0.12:00:00",
    "retry": 3,
    "retryIntervalInSeconds": 30,
    "secureOutput": false
  },
  "userProperties": [],
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "queryTimeout": "02:00:00"
    },
    "sink": {
      "type": "ParquetSink"
    },
    "enableStaging": false
  }
}
```

#### Example 2: Databricks Notebook Activity
```json
{
  "name": "TransformData",
  "type": "DatabricksNotebook",
  "dependsOn": [
    {
      "activity": "CopyCustomerData",
      "dependencyConditions": ["Succeeded"]
    }
  ],
  "policy": {
    "timeout": "0.02:00:00",
    "retry": 2
  },
  "typeProperties": {
    "notebookPath": "/ETL/transform_customers",
    "baseParameters": {
      "start_date": "@pipeline().parameters.StartDate",
      "env": "@pipeline().parameters.Environment"
    }
  }
}
```

#### Example 3: Lookup Activity
```json
{
  "name": "GetLastProcessedDate",
  "type": "Lookup",
  "dependsOn": [],
  "policy": {
    "timeout": "0.00:05:00",
    "retry": 0
  },
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT MAX(processed_date) as last_date FROM control.watermark"
    },
    "dataset": {
      "referenceName": "ControlDB",
      "type": "DatasetReference"
    }
  }
}
```

### Databricks Equivalent

#### YAML (Lakeflow Tasks)
```yaml
tasks:
  # Copy Activity → Notebook Task
  - task_key: copy_customer_data
    job_cluster_key: shared_cluster
    notebook_task:
      notebook_path: /Workspace/ETL/copy_customer_data
      base_parameters:
        source_connection: "{{job.parameters.source_connection}}"
        target_path: "{{job.parameters.target_path}}"
    timeout_seconds: 43200  # 12 hours
    max_retries: 3
    min_retry_interval_millis: 30000

  # Databricks Notebook Activity → Notebook Task (direct mapping)
  - task_key: transform_data
    depends_on:
      - task_key: copy_customer_data
        outcome: "true"  # Only run if succeeded
    job_cluster_key: shared_cluster
    notebook_task:
      notebook_path: /Workspace/ETL/transform_customers
      base_parameters:
        start_date: "{{job.parameters.start_date}}"
        env: "{{job.parameters.environment}}"
    timeout_seconds: 7200  # 2 hours
    max_retries: 2

  # Lookup Activity → SQL Task
  - task_key: get_last_processed_date
    sql_task:
      query:
        query: |
          SELECT MAX(processed_date) as last_date 
          FROM production_catalog.control.watermark
      warehouse_id: "{{job.parameters.warehouse_id}}"
    timeout_seconds: 300  # 5 minutes
    max_retries: 0
```

#### PySpark (Copy Activity Implementation)
```python
# Databricks notebook: /Workspace/ETL/copy_customer_data
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# COMMAND ----------
# Get parameters (replacing ADF parameters)
dbutils.widgets.text("source_connection", "")
dbutils.widgets.text("target_path", "")

source_connection = dbutils.widgets.get("source_connection")
target_path = dbutils.widgets.get("target_path")

# COMMAND ----------
# Read from source (replacing ADF Copy source)
jdbc_url = dbutils.secrets.get("azure-sql", "connection-string")
query = "(SELECT * FROM dbo.customers) AS customers"

source_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

# COMMAND ----------
# Add audit columns
enriched_df = (
    source_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("azure_sql"))
)

# COMMAND ----------
# Write to target (replacing ADF Copy sink)
(
    enriched_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(target_path)
)

# Register as table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS production_catalog.bronze.customers
    USING DELTA
    LOCATION '{target_path}'
""")

# COMMAND ----------
# Return metrics (replacing ADF output)
row_count = enriched_df.count()
dbutils.notebook.exit(f'{{"row_count": {row_count}, "status": "SUCCESS"}}')
```

#### SQL (Lookup Activity Implementation)
```sql
-- SQL Warehouse query (replacing ADF Lookup)
-- This runs directly as sql_task

SELECT MAX(processed_date) as last_date 
FROM production_catalog.control.watermark
WHERE table_name = 'customers'
  AND status = 'completed';
```

## 5. Design Patterns

### Pattern 1: Task Output Propagation
```yaml
tasks:
  # Task that produces output
  - task_key: get_watermark
    sql_task:
      query:
        query: "SELECT MAX(processed_date) as last_date FROM control.watermark"
      warehouse_id: "abc123"
  
  # Task that consumes output
  - task_key: incremental_load
    depends_on:
      - task_key: get_watermark
    notebook_task:
      notebook_path: /ETL/incremental_load
      base_parameters:
        # Access previous task output
        last_processed_date: "{{tasks.get_watermark.values.last_date}}"
```

### Pattern 2: Parallel Task Execution
```yaml
tasks:
  # Independent tasks run in parallel
  - task_key: load_customers
    notebook_task:
      notebook_path: /ETL/load_customers
  
  - task_key: load_orders
    notebook_task:
      notebook_path: /ETL/load_orders
  
  - task_key: load_products
    notebook_task:
      notebook_path: /ETL/load_products
  
  # Converge after parallel execution
  - task_key: join_data
    depends_on:
      - task_key: load_customers
      - task_key: load_orders
      - task_key: load_products
    notebook_task:
      notebook_path: /ETL/join_all_data
```

### Pattern 3: Error Handling with Conditional Tasks
```yaml
tasks:
  - task_key: risky_operation
    notebook_task:
      notebook_path: /ETL/risky_operation
    max_retries: 3
  
  # Run only if previous task failed
  - task_key: handle_failure
    depends_on:
      - task_key: risky_operation
    run_if: FAILED
    notebook_task:
      notebook_path: /ETL/error_handler
      base_parameters:
        failed_task: "risky_operation"
  
  # Run only if previous task succeeded
  - task_key: continue_pipeline
    depends_on:
      - task_key: risky_operation
    run_if: SUCCESS
    notebook_task:
      notebook_path: /ETL/continue_processing
```

### Pattern 4: Task Value Validation
```python
# Notebook: Validate and pass values
# COMMAND ----------
result = spark.sql("SELECT COUNT(*) as count FROM delta_table").collect()[0]
row_count = result['count']

# Validate threshold
if row_count < 1000:
    raise Exception(f"Row count {row_count} below threshold")

# Pass value to next task
dbutils.jobs.taskValues.set(key="validated_count", value=row_count)
dbutils.notebook.exit(f'{{"status": "SUCCESS", "count": {row_count}}}')
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Overly Complex Task Keys
**Problem**: Using complex naming that breaks task references.
```yaml
# WRONG
tasks:
  - task_key: "Copy-Customer-Data-From-SQL-Server-2024"  # Contains invalid characters
    # ...
```

**Solution**: Use simple, alphanumeric task keys.
```yaml
# CORRECT
tasks:
  - task_key: copy_customer_data
    # ...
```

### ❌ Anti-Pattern 2: Ignoring Dependency Outcomes
**Problem**: Not specifying when tasks should run based on previous task results.
```yaml
# WRONG: Cleanup runs even if main task succeeds
tasks:
  - task_key: main_process
  
  - task_key: cleanup
    depends_on:
      - task_key: main_process
    # Will run regardless of main_process outcome
```

**Solution**: Use `run_if` for conditional execution.
```yaml
# CORRECT
tasks:
  - task_key: main_process
  
  - task_key: cleanup
    depends_on:
      - task_key: main_process
    run_if: FAILED  # Only run if main_process failed
```

### ❌ Anti-Pattern 3: Hardcoding Paths and Credentials
**Problem**: Embedding sensitive information directly in task definitions.
```yaml
# WRONG
tasks:
  - task_key: load_data
    notebook_task:
      notebook_path: /Users/john@company.com/my_notebook  # User-specific path
      base_parameters:
        password: "MyPassword123"  # Hardcoded credential
```

**Solution**: Use workspace paths and secrets.
```yaml
# CORRECT
tasks:
  - task_key: load_data
    notebook_task:
      notebook_path: /Workspace/Shared/ETL/load_data
      base_parameters:
        secret_scope: "production-secrets"
```

### ❌ Anti-Pattern 4: Not Leveraging Task Clusters
**Problem**: Creating new cluster for each task unnecessarily.
```yaml
# WRONG: Each task gets its own cluster
tasks:
  - task_key: task1
    new_cluster:
      # ... cluster definition
  
  - task_key: task2
    new_cluster:
      # ... same cluster definition repeated
```

**Solution**: Use shared job clusters.
```yaml
# CORRECT
job_clusters:
  - job_cluster_key: shared
    new_cluster:
      # ... cluster definition

tasks:
  - task_key: task1
    job_cluster_key: shared
  
  - task_key: task2
    job_cluster_key: shared
```

## 7. Performance Considerations

### Compute Optimization
```yaml
# Use different clusters for different workload types
job_clusters:
  # Small cluster for lightweight tasks
  - job_cluster_key: small
    new_cluster:
      num_workers: 2
      node_type_id: i3.xlarge
  
  # Large cluster for heavy transformations
  - job_cluster_key: large
    new_cluster:
      autoscale:
        min_workers: 5
        max_workers: 20
      node_type_id: i3.2xlarge

tasks:
  - task_key: metadata_check
    job_cluster_key: small  # Lightweight task
  
  - task_key: large_transformation
    job_cluster_key: large  # Heavy processing
```

### Timeout Optimization
```yaml
tasks:
  # Set appropriate timeouts based on expected duration
  - task_key: quick_validation
    timeout_seconds: 300  # 5 minutes
  
  - task_key: large_etl
    timeout_seconds: 14400  # 4 hours
  
  - task_key: overnight_batch
    timeout_seconds: 43200  # 12 hours
```

### Retry Strategy
```yaml
tasks:
  # Transient failure handling
  - task_key: external_api_call
    max_retries: 5
    min_retry_interval_millis: 60000  # 1 minute
    retry_on_timeout: true
  
  # No retry for deterministic failures
  - task_key: data_validation
    max_retries: 0  # Fail fast on validation errors
```

### Library Management
```yaml
tasks:
  - task_key: advanced_analytics
    libraries:
      - pypi:
          package: "scikit-learn==1.3.0"
      - pypi:
          package: "pandas==2.0.0"
    notebook_task:
      notebook_path: /Analytics/advanced_models
```

## 8. Governance & Security

### Unity Catalog Integration
```python
# Task notebook with Unity Catalog
# COMMAND ----------
# Access governed tables
df = spark.read.table("production_catalog.gold.customers")

# Write to governed location
(df
 .write
 .mode("overwrite")
 .saveAsTable("production_catalog.gold.customer_summary"))

# Automatic lineage tracking and audit logging
```

### Secret Management
```python
# COMMAND ----------
# Retrieve secrets securely (no hardcoding)
api_key = dbutils.secrets.get(scope="external-apis", key="api-key")
db_password = dbutils.secrets.get(scope="databases", key="postgres-password")

# Use in connections
connection_string = f"postgresql://user:{db_password}@server:5432/db"
```

### Task-Level Access Control
```yaml
# Job definition with ACL
access_control_list:
  - user_name: "data.engineers@company.com"
    permission_level: "CAN_MANAGE"
  - group_name: "data-analysts"
    permission_level: "CAN_VIEW"

tasks:
  - task_key: sensitive_operation
    # Task inherits job-level permissions
    # Plus Unity Catalog table-level ACLs
    notebook_task:
      notebook_path: /Workspace/ETL/sensitive_data
```

## 9. Automation Hints (IMPORTANT)

### AI Agent Transformation Logic

```python
class ADFActivityTransformer:
    """
    AI Agent transformation engine for ADF Activities → Lakeflow Tasks
    """
    
    def __init__(self):
        self.activity_handlers = {
            'Copy': self._handle_copy_activity,
            'DatabricksNotebook': self._handle_notebook_activity,
            'Lookup': self._handle_lookup_activity,
            'SqlServerStoredProcedure': self._handle_stored_proc,
            'WebActivity': self._handle_web_activity,
            'ForEach': self._handle_foreach,
            'IfCondition': self._handle_if_condition
        }
    
    def transform(self, activity: dict) -> dict:
        """Main transformation entry point"""
        activity_type = activity['type']
        handler = self.activity_handlers.get(
            activity_type, 
            self._handle_generic_activity
        )
        return handler(activity)
    
    def _handle_copy_activity(self, activity: dict) -> dict:
        """Convert Copy Activity to notebook task with Spark I/O"""
        return {
            'task_key': self._sanitize_name(activity['name']),
            'notebook_task': {
                'notebook_path': self._generate_copy_notebook(activity),
                'base_parameters': self._extract_parameters(activity)
            },
            'timeout_seconds': self._parse_timeout(activity),
            'max_retries': activity.get('policy', {}).get('retry', 0)
        }
    
    def _handle_notebook_activity(self, activity: dict) -> dict:
        """Direct mapping for Databricks Notebook activities"""
        notebook_path = activity['typeProperties']['notebookPath']
        
        return {
            'task_key': self._sanitize_name(activity['name']),
            'notebook_task': {
                'notebook_path': notebook_path,
                'base_parameters': self._convert_parameters(
                    activity['typeProperties'].get('baseParameters', {})
                )
            },
            'depends_on': self._convert_dependencies(activity.get('dependsOn', []))
        }
    
    def _handle_lookup_activity(self, activity: dict) -> dict:
        """Convert Lookup to SQL task"""
        sql_query = activity['typeProperties']['source'].get('sqlReaderQuery', '')
        
        return {
            'task_key': self._sanitize_name(activity['name']),
            'sql_task': {
                'query': {'query': sql_query},
                'warehouse_id': '${var.warehouse_id}'
            }
        }
    
    def _convert_dependencies(self, adf_depends: list) -> list:
        """Convert ADF dependsOn to Lakeflow depends_on"""
        lakeflow_deps = []
        
        for dep in adf_depends:
            task_dep = {
                'task_key': self._sanitize_name(dep['activity'])
            }
            
            conditions = dep.get('dependencyConditions', ['Succeeded'])
            if 'Succeeded' in conditions:
                task_dep['outcome'] = 'true'
            elif 'Failed' in conditions:
                task_dep['outcome'] = 'false'
            
            lakeflow_deps.append(task_dep)
        
        return lakeflow_deps
    
    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Convert activity name to valid task_key"""
        return name.lower().replace(' ', '_').replace('-', '_')
```

### Pattern Recognition Engine

```python
def detect_activity_pattern(activities: list) -> dict:
    """
    Detect common activity patterns for template selection
    """
    patterns = {
        'etl_pipeline': False,
        'incremental_load': False,
        'full_refresh': False,
        'orchestration': False
    }
    
    # ETL pattern: Copy → Transform → Load
    if len(activities) >= 3:
        has_copy = any(a['type'] == 'Copy' for a in activities)
        has_transform = any('transform' in a['name'].lower() for a in activities)
        if has_copy and has_transform:
            patterns['etl_pipeline'] = True
    
    # Incremental pattern: Lookup watermark → Copy with filter
    if any(a['type'] == 'Lookup' for a in activities):
        lookup_activities = [a for a in activities if a['type'] == 'Lookup']
        for lookup in lookup_activities:
            query = lookup.get('typeProperties', {}).get('source', {}).get('sqlReaderQuery', '')
            if 'watermark' in query.lower() or 'max(' in query.lower():
                patterns['incremental_load'] = True
    
    # Orchestration pattern: Multiple ExecutePipeline or ForEach
    orchestration_types = ['ExecutePipeline', 'ForEach', 'IfCondition']
    orchestration_count = sum(1 for a in activities if a['type'] in orchestration_types)
    if orchestration_count >= 2:
        patterns['orchestration'] = True
    
    return patterns
```

### Code Generation Templates

```python
TASK_TEMPLATES = {
    'copy_activity': """
    - task_key: {task_key}
      notebook_task:
        notebook_path: /Workspace/Generated/copy_{task_key}
        base_parameters:
          source_path: "{source_path}"
          target_table: "{target_table}"
      timeout_seconds: {timeout}
      max_retries: {retries}
    """,
    
    'sql_task': """
    - task_key: {task_key}
      sql_task:
        query:
          query: |
            {sql_query}
        warehouse_id: "{{{{job.parameters.warehouse_id}}}}"
      timeout_seconds: {timeout}
    """,
    
    'conditional_task': """
    - task_key: {task_key}
      condition_task:
        op: {operator}
        left: "{{{{tasks.{dependency}.values.{left_field}}}}}"
        right: "{right_value}"
        when_true:
          - task_key: {true_task}
        when_false:
          - task_key: {false_task}
    """
}

def generate_task_code(activity: dict, template_name: str) -> str:
    """Generate task YAML from template"""
    template = TASK_TEMPLATES[template_name]
    params = extract_template_params(activity)
    return template.format(**params)
```

## 10. Validation Checklist

### Pre-Migration Validation
- [ ] Activity type is identified correctly
- [ ] All dependencies are documented
- [ ] Timeout values are recorded
- [ ] Retry policies are captured
- [ ] Parameters are extracted
- [ ] Linked services are identified
- [ ] Datasets are cataloged
- [ ] Custom scripts/queries are backed up

### Task Definition Validation
- [ ] Task key follows naming conventions (lowercase, underscores)
- [ ] Task type is appropriate for the operation
- [ ] Dependencies are correctly mapped
- [ ] Timeout is reasonable for the operation
- [ ] Retry strategy is defined
- [ ] Notebook/script path is valid
- [ ] Parameters are correctly passed
- [ ] Cluster assignment is appropriate

### Runtime Validation
```python
# Validation script
def validate_task_execution(job_run_id: int, task_key: str):
    """Validate task execution post-migration"""
    
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    run = w.jobs.get_run(job_run_id)
    
    # Find specific task
    task_run = next(
        (t for t in run.tasks if t.task_key == task_key),
        None
    )
    
    if not task_run:
        raise ValueError(f"Task {task_key} not found in run")
    
    # Validate execution
    checks = {
        'completed': task_run.state.life_cycle_state == 'TERMINATED',
        'successful': task_run.state.result_state == 'SUCCESS',
        'duration_reasonable': task_run.execution_duration < 3600000,  # < 1 hour
        'has_output': task_run.run_output is not None
    }
    
    return all(checks.values()), checks
```

### Output Validation
```python
# Compare ADF activity output vs Lakeflow task output
def compare_outputs(adf_output: dict, task_run_output: dict) -> bool:
    """Validate task produces equivalent output to ADF activity"""
    
    # Normalize outputs
    adf_normalized = normalize_adf_output(adf_output)
    task_normalized = normalize_task_output(task_run_output)
    
    # Compare key metrics
    validations = []
    
    # Row counts
    if 'rowsCopied' in adf_normalized:
        validations.append(
            adf_normalized['rowsCopied'] == task_normalized.get('row_count', 0)
        )
    
    # Data hash/checksum
    if 'dataChecksum' in adf_normalized:
        validations.append(
            adf_normalized['dataChecksum'] == task_normalized.get('checksum', '')
        )
    
    return all(validations)
```

### Performance Comparison
```sql
-- Compare execution metrics ADF vs Lakeflow
WITH adf_metrics AS (
  SELECT 
    activity_name,
    AVG(duration_seconds) as avg_duration,
    AVG(rows_processed) as avg_rows,
    COUNT(*) as execution_count
  FROM adf_monitoring.activity_runs
  WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAYS
  GROUP BY activity_name
),
lakeflow_metrics AS (
  SELECT 
    task_key as activity_name,
    AVG(execution_duration/1000) as avg_duration,
    AVG(CAST(run_output:row_count AS INT)) as avg_rows,
    COUNT(*) as execution_count
  FROM system.lakeflow.task_run_timeline
  WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS
  GROUP BY task_key
)
SELECT 
  COALESCE(a.activity_name, l.activity_name) as activity,
  a.avg_duration as adf_duration,
  l.avg_duration as lakeflow_duration,
  ((l.avg_duration - a.avg_duration) / a.avg_duration * 100) as duration_change_pct,
  a.avg_rows as adf_rows,
  l.avg_rows as lakeflow_rows
FROM adf_metrics a
FULL OUTER JOIN lakeflow_metrics l 
  ON a.activity_name = l.activity_name
ORDER BY activity;
```

---

**Related Skills:**
* [adf_pipeline_to_lakeflow_job.md](adf_pipeline_to_lakeflow_job.md) - Pipeline-level orchestration
* [adf_copy_activity_to_spark.md](adf_copy_activity_to_spark.md) - Detailed Copy Activity migration
* [migration_index.md](migration_index.md) - Complete migration guide
