# ADF Pipeline → Lakeflow Job

## 1. Overview

### What is an ADF Pipeline?
An Azure Data Factory Pipeline is a logical grouping of activities that together perform a data integration task. It is defined in JSON format and represents the orchestration layer that controls the execution flow, dependencies, and parameters of multiple activities.

### Where is it used?
* Orchestrating ETL/ELT workflows
* Managing dependencies between data processing tasks
* Scheduling and triggering data pipelines
* Handling error scenarios and retries
* Parameterization for dynamic execution

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF Pipeline | Lakeflow Job |
--------------|--------------|
 JSON-based definition | YAML-based definition |
 Activities array | Tasks array |
 Pipeline parameters | Job parameters (widgets) |
 Pipeline variables | Task values / notebook widgets |
 Annotations | Job tags |
 Concurrency control | Max concurrent runs |

### Key Differences
* **Declarative vs Imperative**: Lakeflow Jobs use YAML for declarative orchestration vs ADF's JSON
* **Compute Model**: Lakeflow Jobs use Databricks clusters/serverless compute vs ADF's managed integration runtime
* **Execution Engine**: Databricks Workflows engine vs Azure orchestration
* **Code Integration**: Direct notebook/JAR/wheel integration vs separate activity definitions
* **Observability**: Integrated with Databricks monitoring vs Azure Monitor

## 3. Migration Strategy (Step-by-Step)

### Step 1: Extract Pipeline Metadata
```python
import json

def extract_adf_pipeline(adf_json_path):
    """Extract key components from ADF pipeline JSON"""
    with open(adf_json_path, 'r') as f:
        pipeline = json.load(f)
    
    return {
        'name': pipeline['name'],
        'parameters': pipeline.get('parameters', {}),
        'variables': pipeline.get('variables', {}),
        'activities': pipeline['properties']['activities'],
        'annotations': pipeline['properties'].get('annotations', [])
    }
```

### Step 2: Map Parameters and Variables
```python
def map_parameters(adf_params):
    """Convert ADF parameters to Databricks job parameters"""
    job_params = []
    for param_name, param_def in adf_params.items():
        job_params.append({
            'name': param_name,
            'default': param_def.get('defaultValue', '')
        })
    return job_params
```

### Step 3: Transform Activities to Tasks
```python
def transform_activities_to_tasks(activities):
    """Convert ADF activities to Lakeflow tasks"""
    tasks = []
    for activity in activities:
        task = {
            'task_key': activity['name'].replace(' ', '_').lower(),
            'depends_on': extract_dependencies(activity),
            # Task-specific conversion based on activity type
        }
        tasks.append(task)
    return tasks
```

### Step 4: Generate Job YAML
```python
def generate_job_yaml(pipeline_metadata, tasks):
    """Generate Databricks Job YAML from pipeline metadata"""
    job_config = {
        'name': pipeline_metadata['name'],
        'tags': {tag: '' for tag in pipeline_metadata['annotations']},
        'tasks': tasks,
        'parameters': map_parameters(pipeline_metadata['parameters'])
    }
    return job_config
```

## 4. Code Conversion

### ADF Example (JSON)
```json
{
  "name": "CustomerDataPipeline",
  "properties": {
    "activities": [
      {
        "name": "CopyCustomerData",
        "type": "Copy",
        "dependsOn": [],
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 3,
          "retryIntervalInSeconds": 30
        }
      },
      {
        "name": "TransformData",
        "type": "DatabricksNotebook",
        "dependsOn": [
          {
            "activity": "CopyCustomerData",
            "dependencyConditions": ["Succeeded"]
          }
        ]
      }
    ],
    "parameters": {
      "StartDate": {
        "type": "string",
        "defaultValue": "2024-01-01"
      },
      "Environment": {
        "type": "string",
        "defaultValue": "dev"
      }
    },
    "annotations": ["customer", "daily", "production"]
  }
}
```

### Databricks Equivalent

#### YAML (Lakeflow Job)
```yaml
name: customer_data_pipeline
tags:
  environment: production
  domain: customer
  frequency: daily

job_clusters:
  - job_cluster_key: shared_cluster
    new_cluster:
      spark_version: 14.3.x-scala2.12
      node_type_id: i3.xlarge
      num_workers: 2
      spark_conf:
        spark.databricks.delta.preview.enabled: "true"

parameters:
  - name: start_date
    default: "2024-01-01"
  - name: environment
    default: "dev"

tasks:
  - task_key: copy_customer_data
    job_cluster_key: shared_cluster
    notebook_task:
      notebook_path: /Workspace/ETL/copy_customer_data
      base_parameters:
        start_date: "{{job.parameters.start_date}}"
        environment: "{{job.parameters.environment}}"
    timeout_seconds: 43200
    max_retries: 3
    retry_on_timeout: true

  - task_key: transform_data
    depends_on:
      - task_key: copy_customer_data
    job_cluster_key: shared_cluster
    notebook_task:
      notebook_path: /Workspace/ETL/transform_customer_data
      base_parameters:
        start_date: "{{job.parameters.start_date}}"
    timeout_seconds: 7200

schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "America/Los_Angeles"
  pause_status: UNPAUSED

max_concurrent_runs: 1

email_notifications:
  on_failure:
    - data-engineering@company.com
  on_success:
    - data-engineering@company.com
```

#### PySpark (Task Implementation)
```python
# Notebook: /Workspace/ETL/copy_customer_data
# Databricks notebook source
from pyspark.sql import SparkSession
from datetime import datetime

# COMMAND ----------
# Get parameters
dbutils.widgets.text("start_date", "2024-01-01")
dbutils.widgets.text("environment", "dev")

start_date = dbutils.widgets.get("start_date")
environment = dbutils.widgets.get("environment")

# COMMAND ----------
# Read from source (replacing ADF Copy Activity)
source_df = (
    spark.read
    .format("delta")
    .load(f"abfss://raw@datalake{environment}.dfs.core.windows.net/customers")
    .filter(f"ingestion_date >= '{start_date}'")
)

# COMMAND ----------
# Write to bronze layer
(
    source_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{environment}_catalog.bronze.customers")
)

# COMMAND ----------
# Return success status
dbutils.notebook.exit("SUCCESS")
```

## 5. Design Patterns

### Pattern 1: Medallion Architecture Mapping
```
ADF Pipeline Stages:
- Raw Copy → Bronze Layer (Auto Loader)
- Transformation → Silver Layer (Materialized Views)
- Aggregation → Gold Layer (Materialized Views)
```

### Pattern 2: Multi-Environment Support
```yaml
# Use job parameters for environment switching
parameters:
  - name: catalog
    default: "dev_catalog"
  - name: schema
    default: "bronze"

tasks:
  - task_key: load_data
    notebook_task:
      base_parameters:
        target_table: "{{job.parameters.catalog}}.{{job.parameters.schema}}.customers"
```

### Pattern 3: Reusable Task Libraries
```yaml
# Define common tasks as notebook libraries
tasks:
  - task_key: data_quality_check
    libraries:
      - pypi:
          package: "great-expectations==0.18.0"
    notebook_task:
      notebook_path: /Shared/Libraries/data_quality_framework
```

### Pattern 4: Conditional Execution
```yaml
tasks:
  - task_key: validate_data
    condition_task:
      op: EQUAL_TO
      left: "{{tasks.load_data.values.row_count}}"
      right: "0"
      when_true:
        - task_key: send_alert
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Direct JSON to YAML Copy
**Problem**: Attempting to directly convert ADF JSON structure to YAML without understanding Databricks concepts.
```yaml
# WRONG: This doesn't work in Databricks
name: pipeline
activities:  # ADF terminology doesn't exist in Lakeflow
  - name: Activity1
    type: Copy  # Databricks doesn't have "Copy" activity type
```

**Solution**: Transform concepts, not syntax.

### ❌ Anti-Pattern 2: Over-Parameterization
**Problem**: Creating too many job parameters for values that should be configuration.
```yaml
# WRONG: Too many parameters
parameters:
  - name: cluster_size
  - name: spark_version
  - name: driver_memory
  # ... 20 more parameters
```

**Solution**: Use cluster policies and job clusters for infrastructure config.

### ❌ Anti-Pattern 3: Monolithic Notebooks
**Problem**: Converting entire ADF pipeline into single massive notebook.
```python
# WRONG: One notebook doing everything
# Cell 1: Copy data
# Cell 2: Transform data
# Cell 3: Validate data
# Cell 4: Load to target
# ... (500 lines later)
```

**Solution**: Break into logical task-based notebooks.

### ❌ Anti-Pattern 4: Ignoring Delta Lake Features
**Problem**: Using ADF patterns that don't leverage Delta capabilities.
```python
# WRONG: Manual deduplication like in ADF
df = spark.read.parquet("source")
df_dedup = df.dropDuplicates(["id"])
```

**Solution**: Use Delta MERGE for upserts.

## 7. Performance Considerations

### Scaling
* **Cluster Sizing**: Right-size clusters based on data volume
  ```yaml
  job_clusters:
    - job_cluster_key: small_cluster
      new_cluster:
        autoscale:
          min_workers: 2
          max_workers: 8
  ```

* **Photon Acceleration**: Enable Photon for SQL-heavy workloads
  ```yaml
  new_cluster:
    runtime_engine: PHOTON
  ```

### Partitioning
* Leverage Delta Lake partitioning
  ```python
  (df.write
     .partitionBy("year", "month")
     .format("delta")
     .saveAsTable("catalog.schema.table"))
  ```

### Optimization
* **Z-Ordering**: Optimize frequently filtered columns
  ```sql
  OPTIMIZE catalog.schema.customers
  ZORDER BY (customer_id, region)
  ```

* **Auto-Optimize**: Enable for streaming writes
  ```python
  spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
  spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
  ```

### Caching Strategy
```yaml
tasks:
  - task_key: load_and_cache
    spark_python_task:
      python_file: /Workspace/ETL/cache_reference_data.py
  
  - task_key: process_data
    depends_on:
      - task_key: load_and_cache
    # Reuse cached data
```

## 8. Governance & Security

### Unity Catalog Usage
```yaml
tasks:
  - task_key: process_data
    notebook_task:
      notebook_path: /Workspace/ETL/process
      base_parameters:
        catalog: "production_catalog"
        schema: "customer_data"
    # Inherits Unity Catalog permissions
```

### Secrets Management
```python
# Replace ADF Key Vault with Databricks Secrets
storage_account_key = dbutils.secrets.get(
    scope="azure-storage",
    key="storage-account-key"
)

spark.conf.set(
    f"fs.azure.account.key.datalake.dfs.core.windows.net",
    storage_account_key
)
```

### Access Control
```yaml
# Job-level permissions
access_control_list:
  - user_name: "data-engineers@company.com"
    permission_level: "CAN_MANAGE"
  - group_name: "data-analysts"
    permission_level: "CAN_VIEW"
```

### Audit Logging
* All job runs automatically logged in Unity Catalog
* Query system tables for audit trails:
  ```sql
  SELECT * FROM system.access.audit
  WHERE action_name = 'runJob'
    AND job_id = 'your_job_id'
  ```

## 9. Automation Hints (IMPORTANT)

### AI Agent Interpretation Rules

```python
class ADFPipelineTransformer:
    """
    AI Agent transformation logic for ADF Pipeline → Lakeflow Job
    """
    
    def parse(self, adf_json: dict) -> dict:
        """Extract metadata from ADF pipeline JSON"""
        return {
            'pipeline_name': adf_json['name'],
            'activities': adf_json['properties']['activities'],
            'parameters': adf_json['properties'].get('parameters', {}),
            'variables': adf_json['properties'].get('variables', {}),
            'concurrency': adf_json['properties'].get('concurrency', 1)
        }
    
    def map(self, metadata: dict) -> dict:
        """Map ADF concepts to Lakeflow"""
        return {
            'job_name': self._sanitize_name(metadata['pipeline_name']),
            'tasks': [self._convert_activity(a) for a in metadata['activities']],
            'parameters': self._convert_parameters(metadata['parameters']),
            'max_concurrent_runs': metadata['concurrency']
        }
    
    def generate(self, mapped_data: dict) -> str:
        """Generate Lakeflow Job YAML"""
        template = """
name: {job_name}
max_concurrent_runs: {max_concurrent_runs}
parameters: {parameters}
tasks: {tasks}
        """
        return template.format(**mapped_data)
    
    def _sanitize_name(self, name: str) -> str:
        """Convert ADF naming to Databricks conventions"""
        return name.lower().replace(' ', '_').replace('-', '_')
```

### Metadata-Driven Approach

```python
# Example: Auto-detect pipeline type and apply appropriate template
def detect_pipeline_pattern(adf_pipeline: dict) -> str:
    """
    Detect common ADF pipeline patterns
    """
    activities = adf_pipeline['properties']['activities']
    
    # Check for batch processing pattern
    if any(a['type'] == 'Copy' for a in activities):
        return 'batch_etl'
    
    # Check for streaming pattern
    if any('streaming' in a.get('name', '').lower() for a in activities):
        return 'streaming'
    
    # Check for orchestration pattern
    if any(a['type'] in ['ExecutePipeline', 'ForEach'] for a in activities):
        return 'orchestration'
    
    return 'generic'

def apply_template(pattern: str) -> str:
    """Return appropriate Lakeflow template for pattern"""
    templates = {
        'batch_etl': 'templates/batch_etl_job.yaml',
        'streaming': 'templates/streaming_job.yaml',
        'orchestration': 'templates/orchestration_job.yaml'
    }
    return templates.get(pattern, 'templates/generic_job.yaml')
```

### Reusable Transformation Rules

```yaml
# transformation_rules.yaml
rules:
  pipeline_name:
    transform: lowercase
    replace:
      " ": "_"
      "-": "_"
  
  timeout:
    source_field: "properties.policy.timeout"
    target_field: "tasks[].timeout_seconds"
    conversion: "parse_iso_duration_to_seconds"
  
  retry:
    source_field: "properties.policy.retry"
    target_field: "tasks[].max_retries"
  
  dependencies:
    source_field: "dependsOn"
    target_field: "depends_on"
    transform: "convert_dependency_format"
```

### Validation Schema

```python
from pydantic import BaseModel, Field
from typing import List, Optional

class LakeflowTask(BaseModel):
    task_key: str = Field(..., pattern=r'^[a-z0-9_]+$')
    depends_on: Optional[List[dict]] = []
    timeout_seconds: Optional[int] = Field(None, gt=0, le=86400)
    max_retries: Optional[int] = Field(0, ge=0, le=10)

class LakeflowJob(BaseModel):
    name: str
    tasks: List[LakeflowTask]
    max_concurrent_runs: int = Field(1, ge=1, le=10)
    
    def validate_job(self):
        """Custom validation logic"""
        task_keys = {task.task_key for task in self.tasks}
        
        # Validate dependencies exist
        for task in self.tasks:
            if task.depends_on:
                for dep in task.depends_on:
                    if dep['task_key'] not in task_keys:
                        raise ValueError(f"Invalid dependency: {dep['task_key']}")
```

## 10. Validation Checklist

### Pre-Migration Checks
- [ ] ADF pipeline JSON is valid and complete
- [ ] All linked services are documented
- [ ] All datasets are cataloged
- [ ] Dependencies are mapped
- [ ] Parameters are identified
- [ ] Timeout and retry policies documented
- [ ] Concurrency requirements understood

### Post-Migration Validation
- [ ] Job YAML syntax is valid
- [ ] All tasks have valid notebook/script paths
- [ ] Parameters are correctly defined
- [ ] Task dependencies are accurate
- [ ] Cluster configurations are appropriate
- [ ] Timeout values are reasonable
- [ ] Retry logic is implemented
- [ ] Email notifications configured
- [ ] Unity Catalog permissions set
- [ ] Job can be triggered manually
- [ ] Scheduled trigger works correctly
- [ ] Job completes successfully end-to-end
- [ ] Output data matches expected schema
- [ ] Performance meets SLA requirements
- [ ] Monitoring and alerting configured

### Data Quality Validation
```python
# Validation notebook
def validate_migration(adf_output_path, lakeflow_table):
    """Compare ADF vs Lakeflow outputs"""
    
    # Read ADF output
    adf_df = spark.read.parquet(adf_output_path)
    
    # Read Lakeflow output
    lakeflow_df = spark.table(lakeflow_table)
    
    # Compare row counts
    assert adf_df.count() == lakeflow_df.count(), "Row count mismatch"
    
    # Compare schemas
    assert set(adf_df.columns) == set(lakeflow_df.columns), "Schema mismatch"
    
    # Sample data comparison
    adf_sample = adf_df.sample(0.01).toPandas()
    lakeflow_sample = lakeflow_df.sample(0.01).toPandas()
    
    # Statistical comparison
    for col in adf_df.columns:
        if adf_df.schema[col].dataType.simpleString() in ['int', 'long', 'double']:
            assert abs(adf_sample[col].mean() - lakeflow_sample[col].mean()) < 0.01
    
    print("✅ Migration validation passed")
```

### Performance Benchmarking
```sql
-- Compare execution times
SELECT 
    'ADF' as platform,
    pipeline_name,
    AVG(duration_seconds) as avg_duration,
    MAX(duration_seconds) as max_duration
FROM adf_monitoring.pipeline_runs
WHERE pipeline_name = 'CustomerDataPipeline'
    AND run_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY pipeline_name

UNION ALL

SELECT 
    'Lakeflow' as platform,
    job_name,
    AVG(execution_duration/1000) as avg_duration,
    MAX(execution_duration/1000) as max_duration
FROM system.lakeflow.job_run_timeline
WHERE job_name = 'customer_data_pipeline'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY job_name
```

---

**Related Skills:**
* [adf_activity_to_task.md](adf_activity_to_task.md) - Detailed activity transformation
* [adf_trigger_to_job_schedule.md](adf_trigger_to_job_schedule.md) - Scheduling migration
* [migration_index.md](migration_index.md) - Complete migration guide
