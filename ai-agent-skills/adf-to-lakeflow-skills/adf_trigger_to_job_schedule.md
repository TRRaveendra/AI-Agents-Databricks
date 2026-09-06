# ADF Trigger → Job Schedule

## 1. Overview

### What is an ADF Trigger?
An ADF Trigger defines when a pipeline execution should be initiated. Triggers control the scheduling and execution of pipelines based on time schedules, events, or manual invocation.

### Where is it used?
* Time-based pipeline scheduling (cron-like schedules)
* Event-based pipeline execution (file arrival, blob creation)
* Manual pipeline invocation
* Tumbling window processing
* Dependency-based execution
* Recurring batch job orchestration

## 2. Lakeflow Equivalent

### Concept Mapping
 ADF Trigger | Lakeflow Equivalent |
-------------|---------------------|
 Schedule Trigger | Job schedule (cron) |
 Tumbling Window Trigger | Scheduled job with parameters |
 Event Trigger | File notification trigger (limited) or continuous job |
 Manual Trigger | Manual job run |
 Trigger dependencies | Job dependencies |

### Key Differences
* **Scheduling**: ADF UI-based vs Databricks cron expressions
* **Event Triggers**: Limited in Databricks vs rich in ADF
* **Tumbling Windows**: Must be implemented with logic vs native ADF support
* **Timezone**: Explicit timezone support in both
* **Dependency**: Cross-job dependencies more explicit in Databricks

## 3. Migration Strategy (Step-by-Step)

### Step 1: Analyze Trigger Types
```python
def analyze_trigger(trigger: dict) -> dict:
    """Extract trigger details from ADF export"""
    
    trigger_type = trigger['properties']['type']
    
    if trigger_type == 'ScheduleTrigger':
        return {
            'type': 'schedule',
            'recurrence': trigger['properties']['typeProperties']['recurrence'],
            'timezone': trigger['properties']['typeProperties'].get('timeZone', 'UTC')
        }
    
    elif trigger_type == 'TumblingWindowTrigger':
        return {
            'type': 'tumbling_window',
            'frequency': trigger['properties']['typeProperties']['frequency'],
            'interval': trigger['properties']['typeProperties']['interval'],
            'start_time': trigger['properties']['typeProperties']['startTime'],
            'end_time': trigger['properties']['typeProperties'].get('endTime')
        }
    
    elif trigger_type == 'BlobEventsTrigger':
        return {
            'type': 'blob_event',
            'scope': trigger['properties']['typeProperties']['scope'],
            'events': trigger['properties']['typeProperties']['events']
        }
    
    return {'type': 'unknown'}
```

### Step 2: Convert to Cron Expression
```python
def convert_to_cron(adf_recurrence: dict) -> str:
    """Convert ADF recurrence to cron expression"""
    
    frequency = adf_recurrence.get('frequency', 'Day')
    interval = adf_recurrence.get('interval', 1)
    schedule = adf_recurrence.get('schedule', {})
    
    if frequency == 'Hour':
        # Every N hours
        minute = schedule.get('minutes', [0])[0]
        return f"0 {minute} */{interval} * * ?"
    
    elif frequency == 'Day':
        # Daily at specific time
        hours = schedule.get('hours', [0])
        minutes = schedule.get('minutes', [0])
        return f"0 {minutes[0]} {hours[0]} * * ?"
    
    elif frequency == 'Week':
        # Weekly on specific days
        week_days = schedule.get('weekDays', ['Monday'])
        day_map = {
            'Monday': '1', 'Tuesday': '2', 'Wednesday': '3',
            'Thursday': '4', 'Friday': '5', 'Saturday': '6', 'Sunday': '0'
        }
        days = ','.join([day_map[d] for d in week_days])
        hours = schedule.get('hours', [0])[0]
        minutes = schedule.get('minutes', [0])[0]
        return f"0 {minutes} {hours} ? * {days}"
    
    elif frequency == 'Month':
        # Monthly on specific days
        month_days = schedule.get('monthDays', [1])
        hours = schedule.get('hours', [0])[0]
        minutes = schedule.get('minutes', [0])[0]
        return f"0 {minutes} {hours} {month_days[0]} * ?"
    
    return "0 0 0 * * ?"  # Default: daily at midnight
```

### Step 3: Generate Job Schedule YAML
```python
def generate_job_schedule(trigger: dict, job_id: str) -> dict:
    """Generate Databricks job schedule configuration"""
    
    analysis = analyze_trigger(trigger)
    
    if analysis['type'] == 'schedule':
        cron_expr = convert_to_cron(analysis['recurrence'])
        
        return {
            'schedule': {
                'quartz_cron_expression': cron_expr,
                'timezone_id': analysis['timezone'],
                'pause_status': 'UNPAUSED'
            }
        }
    
    elif analysis['type'] == 'tumbling_window':
        # Implement as scheduled job with date parameters
        return generate_tumbling_window_schedule(analysis)
    
    return None
```

## 4. Code Conversion

### ADF Example (JSON)

#### Example 1: Daily Schedule Trigger
```json
{
  "name": "DailyTrigger",
  "type": "Microsoft.DataFactory/factories/triggers",
  "properties": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "schedule": {
          "hours": [2],
          "minutes": [30]
        },
        "timeZone": "Pacific Standard Time"
      }
    },
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "CustomerDataPipeline",
          "type": "PipelineReference"
        },
        "parameters": {
          "environment": "production"
        }
      }
    ]
  }
}
```

#### Example 2: Weekly Schedule Trigger
```json
{
  "name": "WeeklyTrigger",
  "type": "Microsoft.DataFactory/factories/triggers",
  "properties": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Week",
        "interval": 1,
        "schedule": {
          "hours": [6],
          "minutes": [0],
          "weekDays": ["Monday", "Wednesday", "Friday"]
        },
        "timeZone": "UTC"
      }
    },
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "WeeklyReportPipeline",
          "type": "PipelineReference"
        }
      }
    ]
  }
}
```

#### Example 3: Tumbling Window Trigger
```json
{
  "name": "HourlyTumblingWindowTrigger",
  "type": "Microsoft.DataFactory/factories/triggers",
  "properties": {
    "type": "TumblingWindowTrigger",
    "typeProperties": {
      "frequency": "Hour",
      "interval": 1,
      "startTime": "2024-01-01T00:00:00Z",
      "delay": "00:05:00",
      "maxConcurrency": 1,
      "retryPolicy": {
        "count": 3,
        "intervalInSeconds": 30
      }
    },
    "pipeline": {
      "pipelineReference": {
        "referenceName": "IncrementalLoadPipeline",
        "type": "PipelineReference"
      },
      "parameters": {
        "windowStart": "@trigger().outputs.windowStartTime",
        "windowEnd": "@trigger().outputs.windowEndTime"
      }
    }
  }
}
```

### Databricks Equivalent

#### YAML (Daily Schedule)
```yaml
name: customer_data_pipeline
tags:
  environment: production
  schedule: daily

schedule:
  quartz_cron_expression: "0 30 2 * * ?"  # Daily at 2:30 AM
  timezone_id: "America/Los_Angeles"  # Pacific Time
  pause_status: UNPAUSED

parameters:
  - name: environment
    default: "production"
  - name: execution_date
    default: "{{date}}"  # Auto-populated by Databricks

tasks:
  - task_key: load_customer_data
    notebook_task:
      notebook_path: /Workspace/ETL/load_customers
      base_parameters:
        environment: "{{job.parameters.environment}}"
        execution_date: "{{job.parameters.execution_date}}"
    job_cluster_key: shared_cluster

email_notifications:
  on_start:
    - data-ops@company.com
  on_success:
    - data-ops@company.com
  on_failure:
    - data-ops@company.com
    - data-engineering-manager@company.com

max_concurrent_runs: 1
timeout_seconds: 7200
```

#### YAML (Weekly Schedule)
```yaml
name: weekly_report_pipeline

schedule:
  quartz_cron_expression: "0 0 6 ? * MON,WED,FRI"  # Mon/Wed/Fri at 6:00 AM
  timezone_id: "UTC"
  pause_status: UNPAUSED

tasks:
  - task_key: generate_weekly_report
    notebook_task:
      notebook_path: /Workspace/Reports/weekly_report
    job_cluster_key: reporting_cluster

max_concurrent_runs: 1
```

#### YAML + PySpark (Tumbling Window Equivalent)
```yaml
name: incremental_load_pipeline

schedule:
  quartz_cron_expression: "0 0 * * * ?"  # Every hour
  timezone_id: "UTC"
  pause_status: UNPAUSED

parameters:
  - name: window_start
    default: ""  # Computed by job
  - name: window_end
    default: ""  # Computed by job

tasks:
  - task_key: compute_time_windows
    notebook_task:
      notebook_path: /Workspace/ETL/compute_windows
    job_cluster_key: shared_cluster
  
  - task_key: incremental_load
    depends_on:
      - task_key: compute_time_windows
    notebook_task:
      notebook_path: /Workspace/ETL/incremental_load
      base_parameters:
        window_start: "{{tasks.compute_time_windows.values.window_start}}"
        window_end: "{{tasks.compute_time_windows.values.window_end}}"
    job_cluster_key: shared_cluster
    max_retries: 3
    retry_on_timeout: true

max_concurrent_runs: 1
```

```python
# Databricks notebook: /Workspace/ETL/compute_windows
# Replaces ADF Tumbling Window automatic window computation
# COMMAND ----------
from datetime import datetime, timedelta
import json

# COMMAND ----------
# Get current execution time
execution_time = datetime.utcnow()

# Compute window boundaries (hourly tumbling window)
window_end = execution_time.replace(minute=0, second=0, microsecond=0)
window_start = window_end - timedelta(hours=1)

# Add delay (equivalent to ADF delay property)
delay_minutes = 5
window_end = window_end - timedelta(minutes=delay_minutes)
window_start = window_start - timedelta(minutes=delay_minutes)

print(f"Window Start: {window_start.isoformat()}")
print(f"Window End: {window_end.isoformat()}")

# COMMAND ----------
# Set task values for downstream tasks
dbutils.jobs.taskValues.set(key="window_start", value=window_start.isoformat())
dbutils.jobs.taskValues.set(key="window_end", value=window_end.isoformat())

# COMMAND ----------
# Return window information
result = {
    "window_start": window_start.isoformat(),
    "window_end": window_end.isoformat(),
    "execution_time": execution_time.isoformat()
}

dbutils.notebook.exit(json.dumps(result))
```

#### Event-Based Processing (Alternative to Event Trigger)
```yaml
# Continuous job that monitors for file arrivals
name: file_arrival_processor

schedule:
  # Run every 5 minutes to check for new files
  quartz_cron_expression: "0 */5 * * * ?"
  timezone_id: "UTC"
  pause_status: UNPAUSED

tasks:
  - task_key: check_and_process_files
    notebook_task:
      notebook_path: /Workspace/ETL/file_arrival_handler
    job_cluster_key: shared_cluster

max_concurrent_runs: 1
```

```python
# Databricks notebook: /Workspace/ETL/file_arrival_handler
# Alternative to ADF Event Trigger
# COMMAND ----------
from datetime import datetime, timedelta

# COMMAND ----------
# Parameters
dbutils.widgets.text("landing_zone", "abfss://landing@storage.dfs.core.windows.net/")
dbutils.widgets.text("checkpoint_table", "production_catalog.control.file_checkpoints")
dbutils.widgets.text("max_file_age_minutes", "10")

landing_zone = dbutils.widgets.get("landing_zone")
checkpoint_table = dbutils.widgets.get("checkpoint_table")
max_age_minutes = int(dbutils.widgets.get("max_file_age_minutes"))

# COMMAND ----------
# Get list of files in landing zone
files = dbutils.fs.ls(landing_zone)

# COMMAND ----------
# Filter for new files
cutoff_time = datetime.now() - timedelta(minutes=max_age_minutes)

new_files = [
    f for f in files 
    if datetime.fromtimestamp(f.modificationTime / 1000) > cutoff_time
]

print(f"Found {len(new_files)} new files")

# COMMAND ----------
# Check which files haven't been processed
processed_files_df = spark.sql(f"""
    SELECT file_path
    FROM {checkpoint_table}
    WHERE status = 'completed'
""")

processed_files = {row['file_path'] for row in processed_files_df.collect()}

files_to_process = [f for f in new_files if f.path not in processed_files]

print(f"Processing {len(files_to_process)} new files")

# COMMAND ----------
# Process each file
for file in files_to_process:
    try:
        # Process file
        result = dbutils.notebook.run(
            "/Workspace/ETL/process_single_file",
            timeout_seconds=3600,
            arguments={"file_path": file.path}
        )
        
        # Mark as processed
        spark.sql(f"""
            INSERT INTO {checkpoint_table}
            VALUES ('{file.path}', 'completed', CURRENT_TIMESTAMP())
        """)
        
        print(f"✅ Processed {file.name}")
    
    except Exception as e:
        print(f"❌ Failed to process {file.name}: {e}")
        
        # Mark as failed
        spark.sql(f"""
            INSERT INTO {checkpoint_table}
            VALUES ('{file.path}', 'failed', CURRENT_TIMESTAMP())
        """)

# COMMAND ----------
dbutils.notebook.exit(f'{{"processed": {len(files_to_process)}}}')
```

## 5. Design Patterns

### Pattern 1: Multi-Schedule Job
```yaml
# Job with multiple schedule definitions
# Note: Databricks supports only one schedule per job
# For multiple schedules, create multiple jobs referencing same tasks

name: customer_data_pipeline_morning
schedule:
  quartz_cron_expression: "0 0 6 * * ?"  # 6 AM
  timezone_id: "UTC"
# ... tasks

---

name: customer_data_pipeline_evening
schedule:
  quartz_cron_expression: "0 0 18 * * ?"  # 6 PM
  timezone_id: "UTC"
# ... same tasks
```

### Pattern 2: Business Day Scheduling
```python
# Notebook to check if today is a business day
# COMMAND ----------
from datetime import datetime
import pandas as pd

# COMMAND ----------
# Get current date
execution_date = datetime.now().date()

# Load business calendar
business_days_df = spark.table("production_catalog.control.business_calendar")
business_days = business_days_df.filter(col("date") == lit(execution_date)).collect()

# COMMAND ----------
if not business_days or not business_days[0]['is_business_day']:
    print(f"{execution_date} is not a business day - skipping processing")
    dbutils.notebook.exit('{"status": "SKIPPED", "reason": "non_business_day"}')

# Continue processing
print(f"{execution_date} is a business day - proceeding")
```

### Pattern 3: Dependency-Based Scheduling
```yaml
# Parent job
name: parent_data_pipeline
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
tasks:
  - task_key: extract_data
    notebook_task:
      notebook_path: /Workspace/ETL/extract

---

# Child job (triggered after parent completes)
name: child_analytics_pipeline
schedule:
  # Scheduled slightly after parent typically completes
  quartz_cron_expression: "0 0 4 * * ?"
tasks:
  - task_key: check_parent_completion
    notebook_task:
      notebook_path: /Workspace/Control/check_parent_job
  
  - task_key: run_analytics
    depends_on:
      - task_key: check_parent_completion
    run_if: SUCCESS
    notebook_task:
      notebook_path: /Workspace/Analytics/run_reports
```

## 6. Anti-Patterns

### ❌ Anti-Pattern 1: Not Setting Timezone
**Problem**: Ambiguous scheduling without explicit timezone.
```yaml
# WRONG: No timezone specified
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  # When does this run? Server timezone? User timezone?
```

**Solution**: Always specify timezone explicitly.
```yaml
# CORRECT: Explicit timezone
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "America/New_York"
```

### ❌ Anti-Pattern 2: Allowing Unlimited Concurrent Runs
**Problem**: Multiple instances running simultaneously.
```yaml
# WRONG: Can cause data inconsistency
name: data_pipeline
schedule:
  quartz_cron_expression: "0 */5 * * * ?"
# No max_concurrent_runs specified (defaults to unlimited)
```

**Solution**: Set appropriate concurrency limit.
```yaml
# CORRECT: Prevent overlapping runs
name: data_pipeline
schedule:
  quartz_cron_expression: "0 */5 * * * ?"
max_concurrent_runs: 1
```

### ❌ Anti-Pattern 3: No Timeout Protection
**Problem**: Jobs can run indefinitely.
```yaml
# WRONG: No timeout
name: long_running_job
schedule:
  quartz_cron_expression: "0 0 0 * * ?"
# No timeout_seconds specified
```

**Solution**: Always set reasonable timeouts.
```yaml
# CORRECT: Timeout protection
name: long_running_job
schedule:
  quartz_cron_expression: "0 0 0 * * ?"
timeout_seconds: 14400  # 4 hours
```

## 7. Performance Considerations

### Cluster Reuse for Scheduled Jobs
```yaml
job_clusters:
  - job_cluster_key: scheduled_cluster
    new_cluster:
      spark_version: 14.3.x-scala2.12
      node_type_id: i3.xlarge
      num_workers: 2
      # Enable cluster reuse
      autotermination_minutes: 15  # Keep alive between runs

tasks:
  - task_key: task1
    job_cluster_key: scheduled_cluster
  
  - task_key: task2
    depends_on:
      - task_key: task1
    job_cluster_key: scheduled_cluster  # Reuses same cluster
```

### Staggered Schedules
```yaml
# Avoid scheduling all jobs at same time
# WRONG: All jobs at midnight
# Job 1: "0 0 0 * * ?"
# Job 2: "0 0 0 * * ?"
# Job 3: "0 0 0 * * ?"

# CORRECT: Staggered schedules
# Job 1: "0 0 0 * * ?"   # 12:00 AM
# Job 2: "0 30 0 * * ?"  # 12:30 AM
# Job 3: "0 0 1 * * ?"   # 1:00 AM
```

## 8. Governance & Security

### Audit Schedule Changes
```sql
-- Query job schedule changes
SELECT 
    event_time,
    user_identity.email,
    request_params.job_id,
    request_params.new_settings.schedule.quartz_cron_expression,
    response.status_code
FROM system.access.audit
WHERE action_name = 'updateJob'
    AND request_params.new_settings.schedule IS NOT NULL
ORDER BY event_time DESC;
```

### Schedule Change Controls
```python
# Implement approval workflow for schedule changes
def validate_schedule_change(job_id: int, new_cron: str) -> bool:
    """Validate schedule change meets business rules"""
    
    # Rule 1: No production jobs during business hours
    if is_production_job(job_id) and is_business_hours_schedule(new_cron):
        raise ValueError("Production jobs cannot run during business hours")
    
    # Rule 2: Require approval for high-frequency schedules
    if get_schedule_frequency(new_cron) < 15:  # Less than 15 minutes
        require_manager_approval(job_id, new_cron)
    
    return True
```

## 9. Automation Hints (IMPORTANT)

### AI Agent Transformation Logic
```python
class TriggerTransformer:
    """Transform ADF Triggers to Databricks Job Schedules"""
    
    def transform(self, trigger: dict, job_name: str) -> dict:
        """Main transformation method"""
        
        trigger_type = trigger['properties']['type']
        
        if trigger_type == 'ScheduleTrigger':
            return self.transform_schedule_trigger(trigger)
        
        elif trigger_type == 'TumblingWindowTrigger':
            return self.transform_tumbling_window(trigger)
        
        elif trigger_type == 'BlobEventsTrigger':
            return self.transform_event_trigger(trigger)
        
        return None
    
    def transform_schedule_trigger(self, trigger: dict) -> dict:
        """Transform Schedule Trigger"""
        
        recurrence = trigger['properties']['typeProperties']['recurrence']
        timezone = trigger['properties']['typeProperties'].get('timeZone', 'UTC')
        
        # Convert to cron
        cron_expr = self.convert_to_cron(recurrence)
        
        # Map timezone
        databricks_timezone = self.map_timezone(timezone)
        
        return {
            'schedule': {
                'quartz_cron_expression': cron_expr,
                'timezone_id': databricks_timezone,
                'pause_status': 'UNPAUSED'
            }
        }
    
    def convert_to_cron(self, recurrence: dict) -> str:
        """Convert ADF recurrence to Quartz cron"""
        
        frequency = recurrence['frequency']
        interval = recurrence.get('interval', 1)
        schedule = recurrence.get('schedule', {})
        
        # Implementation in Step 2 above
        # ... (conversion logic)
        
        return cron_expression
    
    def map_timezone(self, adf_timezone: str) -> str:
        """Map ADF timezone to IANA timezone"""
        
        timezone_mapping = {
            'Pacific Standard Time': 'America/Los_Angeles',
            'Eastern Standard Time': 'America/New_York',
            'Central Standard Time': 'America/Chicago',
            'Mountain Standard Time': 'America/Denver',
            'GMT Standard Time': 'Europe/London',
            'Central European Standard Time': 'Europe/Paris',
            'UTC': 'UTC'
        }
        
        return timezone_mapping.get(adf_timezone, 'UTC')
```

## 10. Validation Checklist

### Pre-Migration Validation
- [ ] All triggers cataloged
- [ ] Schedule expressions documented
- [ ] Timezones recorded
- [ ] Associated pipelines identified
- [ ] Concurrency requirements noted
- [ ] Retry policies documented

### Post-Migration Validation
```python
def validate_schedule_migration(
    adf_trigger_name: str,
    databricks_job_id: int
):
    """Validate trigger migration"""
    
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    job = w.jobs.get(databricks_job_id)
    
    # Verify schedule exists
    assert job.settings.schedule is not None, \
        "Job schedule not configured"
    
    # Verify cron expression
    cron = job.settings.schedule.quartz_cron_expression
    print(f"Cron expression: {cron}")
    
    # Verify timezone
    timezone = job.settings.schedule.timezone_id
    print(f"Timezone: {timezone}")
    
    # Verify pause status
    assert job.settings.schedule.pause_status == 'UNPAUSED', \
        "Job schedule is paused"
    
    # Verify max concurrent runs
    max_concurrent = job.settings.max_concurrent_runs
    print(f"Max concurrent runs: {max_concurrent}")
    
    print(f"✅ Trigger '{adf_trigger_name}' → Job {databricks_job_id} validation passed")
```

### Schedule Testing
```python
# Test schedule is firing correctly
def test_schedule_execution(job_id: int, expected_runs: int = 1):
    """Monitor job runs to verify schedule"""
    
    import time
    from datetime import datetime, timedelta
    
    # Get current runs
    initial_runs = get_recent_runs(job_id, hours=1)
    
    # Wait for next scheduled run
    print("Waiting for scheduled run...")
    time.sleep(3600)  # Wait 1 hour
    
    # Check new runs
    new_runs = get_recent_runs(job_id, hours=1)
    
    actual_new_runs = len(new_runs) - len(initial_runs)
    
    assert actual_new_runs == expected_runs, \
        f"Expected {expected_runs} runs, got {actual_new_runs}"
    
    print(f"✅ Schedule test passed: {actual_new_runs} runs as expected")
```

---

**Related Skills:**
* [adf_pipeline_to_lakeflow_job.md](adf_pipeline_to_lakeflow_job.md) - Pipeline orchestration
* [migration_index.md](migration_index.md) - Complete migration guide
