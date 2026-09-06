# ADF If Condition Activity → Conditional Task Dependency

## 1. Overview

### What is an ADF If Condition Activity?
The If Condition Activity in Azure Data Factory provides conditional branching logic based on the evaluation of an expression. It executes different sets of activities depending on whether the condition evaluates to true or false.

### Where is it used?
* Data validation and quality checks
* Conditional ETL workflows based on row counts or data presence
* Error handling and alternate processing paths
* Environment-specific processing logic
* Business rule enforcement
* Dynamic routing based on metadata

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF If Condition | Lakeflow Equivalent |
------------------|---------------------|
 If Condition Activity | `condition_task` OR `run_if` |
 Expression | Task value comparison |
 ifTrueActivities | `when_true` tasks OR `run_if: SUCCESS` |
 ifFalseActivities | `when_false` tasks OR `run_if: FAILED` |
 Nested conditions | Chained condition tasks |

### Key Differences
* **Expression Language**: ADF expressions vs task value references
* **Complexity**: ADF supports complex expressions vs simpler comparisons in Lakeflow
* **Implementation**: Declarative YAML vs programmatic logic in notebooks
* **Flexibility**: Notebooks offer more complex conditional logic capabilities

## 3. Migration Strategy (Step-by-Step)

### Step 1: Analyze Condition Expression
```python
def analyze_condition(if_activity: dict) -> dict:
    """Extract and analyze If Condition logic"""
    
    type_props = if_activity['typeProperties']
    
    return {
        'expression': type_props['expression']['value'],
        'true_activities': type_props.get('ifTrueActivities', []),
        'false_activities': type_props.get('ifFalseActivities', []),
        'complexity': assess_expression_complexity(type_props['expression']['value'])
    }
```

### Step 2: Determine Migration Approach
```python
def select_conditional_approach(analysis: dict) -> str:
    """Choose appropriate Lakeflow pattern"""
    
    # Simple comparison → condition_task
    if analysis['complexity'] == 'simple':
        return 'condition_task'
    
    # Success/failure branching → run_if
    if is_success_failure_pattern(analysis['expression']):
        return 'run_if'
    
    # Complex logic → notebook with conditional logic
    if analysis['complexity'] == 'complex':
        return 'notebook_conditional'
    
    return 'condition_task'
```

### Step 3: Convert Expression to Task Values
```python
def convert_expression_to_task_value(adf_expression: str) -> dict:
    """Convert ADF expression to Lakeflow task value reference"""
    
    # Example: @greater(activity('GetRowCount').output.count, 0)
    # Converts to: {{tasks.get_row_count.values.count}} > 0
    
    patterns = {
        r"activity\('(\w+)'\)\.output\.(\w+)": r"{{tasks.\1.values.\2}}",
        r"@greater\((.*?),\s*(\d+)\)": r"\1 > \2",
        r"@equals\((.*?),\s*'(.*?)'\)": r"\1 == '\2'",
        r"@less\((.*?),\s*(\d+)\)": r"\1 < \2"
    }
    
    converted = adf_expression
    for pattern, replacement in patterns.items():
        converted = re.sub(pattern, replacement, converted)
    
    return parse_comparison(converted)
```

## 4. Code Conversion

### ADF Example (JSON)

#### Example 1: Simple Row Count Check
```json
{
  "name": "CheckRowCount",
  "type": "IfCondition",
  "dependsOn": [
    {
      "activity": "GetRowCount",
      "dependencyConditions": ["Succeeded"]
    }
  ],
  "typeProperties": {
    "expression": {
      "value": "@greater(activity('GetRowCount').output.firstRow.row_count, 0)",
      "type": "Expression"
    },
    "ifTrueActivities": [
      {
        "name": "ProcessData",
        "type": "DatabricksNotebook",
        "typeProperties": {
          "notebookPath": "/ETL/process_data"
        }
      }
    ],
    "ifFalseActivities": [
      {
        "name": "SendAlert",
        "type": "WebActivity",
        "typeProperties": {
          "url": "https://alerts.company.com/webhook"
        }
      }
    ]
  }
}
```

#### Example 2: Environment-Based Routing
```json
{
  "name": "CheckEnvironment",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@equals(pipeline().parameters.Environment, 'production')",
      "type": "Expression"
    },
    "ifTrueActivities": [
      {
        "name": "ProductionProcessing",
        "type": "ExecutePipeline"
      }
    ],
    "ifFalseActivities": [
      {
        "name": "DevelopmentProcessing",
        "type": "ExecutePipeline"
      }
    ]
  }
}
```

### Databricks Equivalent

#### YAML (Using condition_task)
```yaml
tasks:
  # Get row count
  - task_key: get_row_count
    sql_task:
      query:
        query: |
          SELECT COUNT(*) as row_count
          FROM production_catalog.bronze.customers
          WHERE ingestion_date = CURRENT_DATE()
      warehouse_id: "{{job.parameters.warehouse_id}}"
  
  # Conditional check (replaces If Condition Activity)
  - task_key: check_row_count
    depends_on:
      - task_key: get_row_count
    condition_task:
      op: GREATER_THAN
      left: "{{tasks.get_row_count.values.row_count}}"
      right: "0"
      when_true:
        - task_key: process_data
      when_false:
        - task_key: send_alert
  
  # True branch
  - task_key: process_data
    notebook_task:
      notebook_path: /Workspace/ETL/process_data
    job_cluster_key: shared_cluster
  
  # False branch
  - task_key: send_alert
    notebook_task:
      notebook_path: /Workspace/Alerts/send_alert
      base_parameters:
        message: "No data found for processing"
    job_cluster_key: shared_cluster
```

#### YAML (Using run_if)
```yaml
tasks:
  # Validation task
  - task_key: validate_data
    notebook_task:
      notebook_path: /Workspace/DQ/validate_data
    job_cluster_key: shared_cluster
  
  # Run only if validation succeeds
  - task_key: process_data
    depends_on:
      - task_key: validate_data
    run_if: SUCCESS  # Only run if validate_data succeeded
    notebook_task:
      notebook_path: /Workspace/ETL/process_data
    job_cluster_key: shared_cluster
  
  # Run only if validation fails
  - task_key: handle_validation_failure
    depends_on:
      - task_key: validate_data
    run_if: FAILED  # Only run if validate_data failed
    notebook_task:
      notebook_path: /Workspace/Error/handle_validation_failure
    job_cluster_key: shared_cluster
```

#### PySpark (Notebook for Complex Conditional Logic)
```python
# Databricks notebook: /Workspace/ETL/conditional_processing
# COMMAND ----------
from pyspark.sql.functions import col, count
import json

# COMMAND ----------
# Parameters
dbutils.widgets.text("source_table", "production_catalog.bronze.customers")
dbutils.widgets.text("min_row_threshold", "1000")
dbutils.widgets.text("max_error_pct", "5.0")

source_table = dbutils.widgets.get("source_table")
min_row_threshold = int(dbutils.widgets.get("min_row_threshold"))
max_error_pct = float(dbutils.widgets.get("max_error_pct"))

# COMMAND ----------
# Get data quality metrics
df = spark.table(source_table)

total_rows = df.count()
error_rows = df.filter(col("data_quality_flag") == "ERROR").count()
error_pct = (error_rows / total_rows * 100) if total_rows > 0 else 0

print(f"Total rows: {total_rows}")
print(f"Error rows: {error_rows}")
print(f"Error percentage: {error_pct:.2f}%")

# COMMAND ----------
# Complex conditional logic (replaces nested If Condition activities)
if total_rows == 0:
    # No data scenario
    print("No data found - triggering alert")
    
    # Call alert notebook
    dbutils.notebook.run(
        "/Workspace/Alerts/no_data_alert",
        timeout_seconds=300,
        arguments={"table_name": source_table}
    )
    
    decision = "NO_DATA"
    next_action = "ALERT_SENT"

elif total_rows < min_row_threshold:
    # Insufficient data scenario
    print(f"Insufficient data: {total_rows} < {min_row_threshold}")
    
    # Call partial processing notebook
    dbutils.notebook.run(
        "/Workspace/ETL/partial_processing",
        timeout_seconds=1800,
        arguments={"source_table": source_table}
    )
    
    decision = "PARTIAL_PROCESSING"
    next_action = "COMPLETED"

elif error_pct > max_error_pct:
    # High error rate scenario
    print(f"High error rate: {error_pct:.2f}% > {max_error_pct}%")
    
    # Call quarantine notebook
    dbutils.notebook.run(
        "/Workspace/DQ/quarantine_errors",
        timeout_seconds=1800,
        arguments={
            "source_table": source_table,
            "error_pct": str(error_pct)
        }
    )
    
    decision = "QUARANTINE"
    next_action = "ERRORS_QUARANTINED"

else:
    # Normal processing scenario
    print("Data quality checks passed - proceeding with full processing")
    
    # Call full processing notebook
    dbutils.notebook.run(
        "/Workspace/ETL/full_processing",
        timeout_seconds=3600,
        arguments={"source_table": source_table}
    )
    
    decision = "FULL_PROCESSING"
    next_action = "COMPLETED"

# COMMAND ----------
# Store decision for downstream tasks
dbutils.jobs.taskValues.set(key="decision", value=decision)
dbutils.jobs.taskValues.set(key="total_rows", value=total_rows)
dbutils.jobs.taskValues.set(key="error_pct", value=round(error_pct, 2))

# COMMAND ----------
# Return result
result = {
    "decision": decision,
    "next_action": next_action,
    "total_rows": total_rows,
    "error_rows": error_rows,
    "error_pct": round(error_pct, 2)
}

dbutils.notebook.exit(json.dumps(result))
```

#### YAML (Environment-Based Routing)
```yaml
parameters:
  - name: environment
    default: "dev"

tasks:
  - task_key: check_environment
    condition_task:
      op: EQUAL_TO
      left: "{{job.parameters.environment}}"
      right: "production"
      when_true:
        - task_key: production_processing
      when_false:
        - task_key: development_processing
  
  - task_key: production_processing
    notebook_task:
      notebook_path: /Workspace/ETL/production/process_data
      base_parameters:
        catalog: "production_catalog"
        enable_monitoring: "true"
    job_cluster_key: production_cluster
  
  - task_key: development_processing
    notebook_task:
      notebook_path: /Workspace/ETL/dev/process_data
      base_parameters:
        catalog: "dev_catalog"
        enable_monitoring: "false"
    job_cluster_key: dev_cluster
```

## 5. Design Patterns

### Pattern 1: Data Quality Gates
```yaml
tasks:
  - task_key: check_data_quality
    notebook_task:
      notebook_path: /Workspace/DQ/check_quality
  
  - task_key: quality_gate
    depends_on:
      - task_key: check_data_quality
    condition_task:
      op: GREATER_THAN
      left: "{{tasks.check_data_quality.values.quality_score}}"
      right: "95"
      when_true:
        - task_key: promote_to_production
      when_false:
        - task_key: quarantine_and_alert
```

### Pattern 2: Incremental vs Full Load Decision
```python
# Notebook: Decide load type based on data volume
# COMMAND ----------
# Check data volume
new_records = spark.sql("""
    SELECT COUNT(*) as count
    FROM source
    WHERE modified_date > (SELECT MAX(watermark) FROM control)
""").first()['count']

# COMMAND ----------
# Decide load type
if new_records < 10000:
    # Incremental load
    dbutils.jobs.taskValues.set("load_type", "incremental")
    result = dbutils.notebook.run("/ETL/incremental_load", 3600)
else:
    # Full refresh more efficient
    dbutils.jobs.taskValues.set("load_type", "full")
    result = dbutils.notebook.run("/ETL/full_load", 3600)
```

### Pattern 3: Multi-Level Conditional Routing
```yaml
tasks:
  - task_key: assess_data
    notebook_task:
      notebook_path: /Workspace/Assessment/assess_data
  
  # First level: Check if data exists
  - task_key: check_data_exists
    depends_on:
      - task_key: assess_data
    condition_task:
      op: GREATER_THAN
      left: "{{tasks.assess_data.values.row_count}}"
      right: "0"
      when_true:
        - task_key: check_data_quality  # Go to second level
      when_false:
        - task_key: no_data_alert
  
  # Second level: Check data quality
  - task_key: check_data_quality
    condition_task:
      op: GREATER_THAN
      left: "{{tasks.assess_data.values.quality_score}}"
      right: "90"
      when_true:
        - task_key: production_load
      when_false:
        - task_key: staging_load
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Overly Complex Condition Tasks
**Problem**: Trying to implement complex logic in YAML conditions.
```yaml
# WRONG: Too complex for condition_task
- task_key: complex_check
  condition_task:
    # This gets unmanageable quickly
    op: AND
    conditions:
      - op: GREATER_THAN
        left: "{{x}}"
        right: "0"
      - op: LESS_THAN
        left: "{{y}}"
        right: "100"
      - op: EQUAL_TO
        left: "{{z}}"
        right: "active"
```

**Solution**: Use notebook for complex logic.
```python
# CORRECT: Notebook handles complex conditions
# Evaluate all conditions
result = (x > 0 and y < 100 and z == "active")
dbutils.jobs.taskValues.set("condition_met", result)
```

### ❌ Anti-Pattern 2: Not Handling Both Branches
**Problem**: Only defining true branch, leaving false branch undefined.
```yaml
# WRONG: What happens if condition is false?
- task_key: check
  condition_task:
    op: EQUAL_TO
    left: "{{value}}"
    right: "expected"
    when_true:
      - task_key: process
    # No when_false defined - job just ends
```

**Solution**: Always handle both branches explicitly.
```yaml
# CORRECT: Both branches defined
- task_key: check
  condition_task:
    op: EQUAL_TO
    left: "{{value}}"
    right: "expected"
    when_true:
      - task_key: process
    when_false:
      - task_key: log_unexpected_value
```

## 7. Performance Considerations

### Early Exit Optimization
```python
# Check lightweight condition first
if spark.catalog.tableExists(target_table):
    # Expensive check only if needed
    row_count = spark.table(target_table).count()
    if row_count > 0:
        print("Table already populated, skipping")
        dbutils.notebook.exit('{"status": "SKIPPED"}')

# Proceed with processing
process_data()
```

### Caching Condition Results
```python
# Cache result of expensive condition evaluation
condition_result = evaluate_complex_condition()  # Expensive operation

dbutils.jobs.taskValues.set("condition_met", condition_result)

# Downstream tasks can reference cached result
# No need to re-evaluate
```

## 8. Governance & Security

### Audit Conditional Decisions
```python
# Log all conditional decisions
decision_log = {
    "job_run_id": dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get(),
    "decision_point": "data_quality_check",
    "condition": f"quality_score > {threshold}",
    "result": condition_met,
    "timestamp": datetime.now().isoformat()
}

spark.createDataFrame([decision_log]).write \
    .mode("append") \
    .saveAsTable("production_catalog.audit.conditional_decisions")
```

## 9. Automation Hints (IMPORTANT)

### AI Agent Transformation Logic
```python
class IfConditionTransformer:
    """Transform ADF If Condition to Lakeflow patterns"""
    
    def transform(self, if_activity: dict) -> dict:
        """Main transformation method"""
        
        analysis = self.analyze(if_activity)
        
        if analysis['complexity'] == 'simple':
            return self.generate_condition_task(if_activity)
        else:
            return self.generate_notebook_conditional(if_activity)
    
    def generate_condition_task(self, if_activity: dict) -> dict:
        """Generate condition_task YAML"""
        
        expression = if_activity['typeProperties']['expression']['value']
        comparison = self.parse_expression(expression)
        
        return {
            'task_key': self._sanitize_name(if_activity['name']),
            'condition_task': {
                'op': comparison['operator'],
                'left': comparison['left'],
                'right': comparison['right'],
                'when_true': self._convert_activities(
                    if_activity['typeProperties'].get('ifTrueActivities', [])
                ),
                'when_false': self._convert_activities(
                    if_activity['typeProperties'].get('ifFalseActivities', [])
                )
            }
        }
    
    def parse_expression(self, expression: str) -> dict:
        """Parse ADF expression to comparison components"""
        
        # Example: @greater(activity('GetRowCount').output.count, 0)
        patterns = {
            r'@greater\((.*?),\s*(\d+)\)': ('GREATER_THAN', r'\1', r'\2'),
            r'@equals\((.*?),\s*["\']?(.*?)["\']?\)': ('EQUAL_TO', r'\1', r'\2'),
            r'@less\((.*?),\s*(\d+)\)': ('LESS_THAN', r'\1', r'\2')
        }
        
        for pattern, (op, left, right) in patterns.items():
            match = re.match(pattern, expression)
            if match:
                return {
                    'operator': op,
                    'left': self._convert_to_task_value(match.group(1)),
                    'right': match.group(2)
                }
        
        return {'operator': 'EQUAL_TO', 'left': expression, 'right': 'true'}
    
    def _convert_to_task_value(self, adf_reference: str) -> str:
        """Convert ADF activity output reference to task value"""
        
        # activity('GetRowCount').output.count 
        # → {{tasks.get_row_count.values.count}}
        
        match = re.match(r"activity\('(\w+)'\)\.output\.(\w+)", adf_reference)
        if match:
            activity_name = self._sanitize_name(match.group(1))
            field_name = match.group(2)
            return f"{{{{tasks.{activity_name}.values.{field_name}}}}}"
        
        return adf_reference
```

## 10. Validation Checklist

### Pre-Migration Validation
- [ ] Condition expression documented
- [ ] True branch activities identified
- [ ] False branch activities identified
- [ ] Expression complexity assessed
- [ ] Required task values identified
- [ ] Nested conditions mapped

### Post-Migration Validation
```python
def validate_conditional_logic(
    test_scenarios: list[dict]
):
    """Test conditional logic with various scenarios"""
    
    for scenario in test_scenarios:
        # Set up test conditions
        setup_test_data(scenario['input'])
        
        # Run job
        run_result = trigger_job_run()
        
        # Verify correct branch was taken
        tasks_run = get_executed_tasks(run_result.run_id)
        
        if scenario['expected_branch'] == 'true':
            assert 'process_data' in tasks_run
            assert 'send_alert' not in tasks_run
        else:
            assert 'send_alert' in tasks_run
            assert 'process_data' not in tasks_run
        
        print(f"✅ Scenario '{scenario['name']}' passed")
```

---

**Related Skills:**
* [adf_activity_to_task.md](adf_activity_to_task.md) - Activity transformation
* [adf_lookup_to_spark_sql.md](adf_lookup_to_spark_sql.md) - Condition data sources
* [migration_index.md](migration_index.md) - Complete migration guide
