# Informatica Session / Workflow → Databricks Jobs & Lakeflow

## Session → Job Task

| Informatica Session Property | Databricks Equivalent |
|------------------------------|-----------------------|
| Integration Service | Job cluster or serverless compute |
| Source / Target connections | Unity Catalog external locations + secrets |
| Pre-session / Post-session SQL | Separate notebook or SQL task before/after main task |
| Pre-session / Post-session command | Notebook task with shell or Python |
| Failure on error / Stop on errors | `max_retries`, `timeout_seconds`, task dependency `outcome` |
| Commit interval / Transaction control | Delta auto-commit or explicit MERGE; avoid long transactions |
| Partitioning / Pass-through | Spark partitionBy / repartition; often automatic with AQE |
| Parameter file | Job parameters + `dbutils.widgets` or `params` dict |
| Incremental extraction | Auto Loader checkpoint or high-watermark table |

### Example Job Task (YAML fragment)

```yaml
tasks:
  - task_key: load_orders
    notebook_task:
      notebook_path: /Workspace/Repos/orders/run_m_orders
      base_parameters:
        run_date: "{{job.parameters.run_date}}"
        dry_run: "false"
    job_cluster_key: etl_cluster
    max_retries: 2
    timeout_seconds: 7200
    depends_on: []
```

## Workflow / Worklet → Multi-task Job or Lakeflow Pipeline

- Workflow dependencies become `depends_on` with optional `outcome` (success / failure).
- Worklets become either nested jobs (`run_job_task`) or expanded into the parent job’s task graph.
- Concurrent execution of independent sessions → parallel tasks with no mutual `depends_on`.
- Decision / Event Wait tasks → `condition_task` or `run_if` expressions, or redesign into separate jobs triggered by events.

### Example Multi-task Job Skeleton

```yaml
resources:
  jobs:
    informatica_migrated_orders_wf:
      name: "migrated_orders_workflow"
      parameters:
        - name: run_date
          default: ""
      tasks:
        - task_key: extract_orders
          notebook_task:
            notebook_path: /Repos/orders/extract
        - task_key: transform_orders
          depends_on:
            - task_key: extract_orders
          notebook_task:
            notebook_path: /Repos/orders/transform
        - task_key: load_orders
          depends_on:
            - task_key: transform_orders
          notebook_task:
            notebook_path: /Repos/orders/load
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "Asia/Kolkata"
```

## Lakeflow Declarative Pipelines Alternative

For pure data pipelines (no complex control flow), prefer Lakeflow / DLT:

- Each major target becomes a Streaming Table or Materialized View.
- Expectations replace many data-quality checks that lived in Informatica.
- Apply Changes API replaces classic CDC / Update Strategy patterns.

## Scheduling

| Informatica | Databricks |
|-------------|------------|
| Scheduler / Workflow schedule | Job schedule (cron) or continuous / triggered |
| File watcher / Event wait | Auto Loader + trigger, or File Arrival trigger |
| On-demand / Manual | Job run via UI / API / CLI |

## Error Handling & Notifications

- Session failure e-mail → Job email / webhook / Slack notifications.
- “Fail parent if any child fails” → default Job behavior; override with `run_if` if needed.
- Restartability → design each task to be idempotent (MERGE or partition overwrite).

## Parameter & Variable Handling

- Mapping parameters / variables → Job parameters + notebook widgets or function `params`.
- Persistent mapping variables ($$var) that carry state across runs → store in a control Delta table instead of relying on Informatica’s variable persistence.
