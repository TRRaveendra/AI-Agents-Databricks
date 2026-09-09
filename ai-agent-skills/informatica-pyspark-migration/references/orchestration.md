# Sessions, Workflows and Worklets → Lakeflow Jobs

Once each mapping is a `run_<mapping>()` function, orchestration is the second
half of the migration — and usually the half that surprises people, because
PowerCenter workflows accumulate ten years of operational scar tissue (retry
logic, event waits, decision branches, email tasks) that nobody documented.

## Contents

1. [Component mapping](#1-component-mapping)
2. [Session → job task](#2-session--job-task)
3. [Workflow → multi-task job](#3-workflow--multi-task-job)
4. [Link conditions and decisions](#4-link-conditions-and-decisions)
5. [Worklets](#5-worklets)
6. [Parameters and parameter files](#6-parameters-and-parameter-files)
7. [Passing values between tasks](#7-passing-values-between-tasks)
8. [Scheduling and triggers](#8-scheduling-and-triggers)
9. [When to use a Declarative Pipeline instead](#9-when-to-use-a-declarative-pipeline-instead)
10. [Failure handling, recovery and notification](#10-failure-handling-recovery-and-notification)

---

## 1. Component mapping

| Informatica | Databricks |
|---|---|
| Session | Job **task** (`notebook_task`, `python_wheel_task`, or `spark_python_task`) |
| Workflow | **Job** with multiple tasks and `depends_on` edges |
| Worklet | Nested job invoked via `run_job_task`, or a task group inside the parent job |
| Link condition | `run_if` on the dependent task, or an explicit `condition_task` |
| Decision task | `condition_task` |
| Assignment task | `dbutils.jobs.taskValues.set()` in an upstream task |
| Command task | `notebook_task` running `%sh`, or a Python task shelling out |
| Email task | Job/task **notifications**, or a webhook notification |
| Event Wait (file watch) | **File arrival trigger** on the job, or Auto Loader |
| Event Raise / Event Wait (user-defined) | task dependency, or task values |
| Timer task | job schedule; there is no in-DAG "wait N minutes" — redesign it |
| Session recovery | task-level `max_retries` + idempotent writes |
| Workflow scheduler | job `schedule` (quartz cron) or `continuous` |
| Session log | job run output + your own audit Delta table |
| `pmcmd` invocation from outside | Jobs REST API `/api/2.2/jobs/run-now` |

## 2. Session → job task

A PowerCenter session carries the properties that decide *how* the mapping runs.
Read these five before writing the task, because they change the generated code,
not just the job config:

1. **Treat source rows as** (Insert/Update/Delete/Data driven) — overrides the
   Update Strategy transformation for the whole session.
2. **Target load type** (Normal/Bulk) and **Truncate target table option**.
3. **Enable High Precision** — decides Decimal vs Double (see transformation-patterns §21).
4. **Pre/Post-session SQL** on source and target connections.
5. **Session partitioning** — the original's parallelism; usually replaced by
   Spark's, but a partitioned JDBC read needs the equivalent `numPartitions`.

Pre/post-session SQL becomes explicit steps in the task, not hidden connection
properties:

```python
# s_m_CUSTOMER_DIM_LOAD
def task_customer_dim(spark, params, table_map, dry_run=False):
    # Pre-session SQL (target connection)
    spark.sql(f"DELETE FROM {table_map['dim_customer_stg']} WHERE load_dt = '{params['load_date']}'")

    result = run_m_customer_dim_load(spark, params, table_map, dry_run=dry_run)

    # Post-session SQL
    spark.sql(f"OPTIMIZE {table_map['dim_customer']}")
    return result
```

Cluster sizing: sessions were tuned with DTM buffer sizes and partition counts
that tell you very little about Spark requirements. Start from data volume, not
from the session config. Prefer serverless or a job cluster with autoscaling over
transcribing the old capacity plan.

## 3. Workflow → multi-task job

Convert the workflow link graph directly into `depends_on`. Keep the original
names in the task keys so operators can find their way.

```yaml
# databricks.yml (Databricks Asset Bundles)
resources:
  jobs:
    wf_daily_sales:
      name: wf_DAILY_SALES
      tags:
        source_system: informatica
        legacy_workflow: wf_DAILY_SALES
      parameters:
        - name: load_date
          default: "{{job.start_time.iso_date}}"
        - name: env
          default: dev
      job_clusters:
        - job_cluster_key: etl
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: Standard_D8ds_v5
            autoscale: { min_workers: 2, max_workers: 8 }
      tasks:
        - task_key: s_m_STG_ORDERS
          job_cluster_key: etl
          notebook_task:
            notebook_path: ../src/mappings/m_stg_orders
            base_parameters:
              load_date: "{{job.parameters.load_date}}"
          max_retries: 2
          min_retry_interval_millis: 300000

        - task_key: s_m_CUSTOMER_DIM_LOAD
          depends_on: [{ task_key: s_m_STG_ORDERS }]
          job_cluster_key: etl
          notebook_task:
            notebook_path: ../src/mappings/m_customer_dim_load

        - task_key: s_m_FACT_SALES
          depends_on:
            - { task_key: s_m_CUSTOMER_DIM_LOAD }
            - { task_key: s_m_STG_ORDERS }
          job_cluster_key: etl
          notebook_task:
            notebook_path: ../src/mappings/m_fact_sales

      email_notifications:
        on_failure: ["${var.ops_email}"]
      queue: { enabled: true }
```

One important structural difference: PowerCenter workflows run tasks
sequentially by default unless explicitly linked in parallel. A Databricks job
runs every task whose dependencies are satisfied, **in parallel**. If the
original relied on serial execution for correctness — two sessions writing the
same target, or an implicit ordering nobody wrote down — a faithful DAG
conversion will introduce a race. Check for shared targets before parallelising,
and add artificial dependencies where the ordering was real.

## 4. Link conditions and decisions

A workflow link can carry a condition such as
`$s_m_STG_ORDERS.Status = SUCCEEDED AND $s_m_STG_ORDERS.SrcSuccessRows > 0`.

Split it into two parts:

- **Status conditions** map to `run_if` on the dependent task:

  | Informatica link condition | `run_if` |
  |---|---|
  | `Status = SUCCEEDED` | `ALL_SUCCESS` (the default) |
  | `Status = FAILED` | `AT_LEAST_ONE_FAILED` |
  | run regardless | `ALL_DONE` |
  | all upstream failed | `ALL_FAILED` |

- **Data conditions** (row counts, variables) become task values plus a
  `condition_task`:

```yaml
- task_key: check_rows
  depends_on: [{ task_key: s_m_STG_ORDERS }]
  condition_task:
    op: GREATER_THAN
    left: "{{tasks.s_m_STG_ORDERS.values.src_rows}}"
    right: "0"

- task_key: s_m_FACT_SALES
  depends_on: [{ task_key: check_rows, outcome: "true" }]
  ...
```

A Decision task converts to the same `condition_task` shape — it was already a
branch point, so the translation is honest.

## 5. Worklets

A worklet is a reusable workflow fragment. Two options:

- **`run_job_task`** — the worklet becomes its own job, invoked by the parent.
  Best when the worklet is genuinely shared across workflows or has its own
  schedule/ownership. Costs a little latency per invocation and makes the run
  history two levels deep.
- **Inlined task group** — copy the worklet's tasks into each parent job with a
  name prefix. Best when the worklet was reused twice and is small; avoids
  indirection, at the cost of duplication.

Reusable worklets with parameters map naturally to `run_job_task` with
`job_parameters`, so prefer that when the worklet took parameters.

## 6. Parameters and parameter files

A parameter file looks like:

```
[Global]
$$LOAD_DATE=2024-01-15
[PROJECT.WF:wf_DAILY_SALES.ST:s_m_CUSTOMER_DIM_LOAD]
$$SRC_SCHEMA=SALES_PROD
$DBConnection_SRC=ORA_PROD
```

Convert to three distinct things, because they have different lifecycles:

| Parameter file content | Databricks home |
|---|---|
| Business values (`$$LOAD_DATE`, `$$REGION`) | job parameters → `params` dict |
| Physical names (`$$SRC_SCHEMA`, target table names) | `table_map` dict, per environment |
| Connections/credentials (`$DBConnection_*`) | Unity Catalog connections + secret scopes |

Do not collapse `table_map` into `params`. Keeping them separate is what lets
the same function run against dev and prod without editing business logic, and
it is what makes the differential validation in the checklist possible.

```python
# In the notebook wrapper for each task
dbutils.widgets.text("load_date", "")
dbutils.widgets.text("env", "dev")

env = dbutils.widgets.get("env")
params = {
    "load_date": dbutils.widgets.get("load_date"),
    "run_ts":    datetime.utcnow().isoformat(),
}
table_map = TABLE_MAPS[env]      # loaded from a config module or YAML in the bundle

result = run_m_customer_dim_load(spark, params, table_map, dry_run=(env == "shadow"))
dbutils.jobs.taskValues.set("src_rows", result["row_counts"]["src_customer"])
```

Useful job-parameter defaults that replace common PowerCenter idioms:
`{{job.start_time.iso_date}}` for `$$LOAD_DATE`, `{{job.run_id}}` for the audit
table, `{{task.name}}` for `$PMSessionName`.

## 7. Passing values between tasks

Informatica Assignment tasks set workflow variables that later tasks read.
Databricks task values are the direct equivalent:

```python
dbutils.jobs.taskValues.set(key="src_rows", value=result["row_counts"]["src_customer"])
# downstream task
src_rows = dbutils.jobs.taskValues.get(taskKey="s_m_STG_ORDERS", key="src_rows", default=0)
```

Task values are small JSON-serialisable values, scoped to a single job run. For
anything that must survive across runs — watermarks, sequence counters,
`SETMAXVARIABLE` state — use a Delta control table instead (see
expression-function-map §10). Getting this distinction wrong is a common source
of "works in test, loses data in prod after a restart".

## 8. Scheduling and triggers

- **Time-based** workflow schedules → job `schedule` with a quartz cron
  expression and an explicit `timezone_id`. PowerCenter schedules ran in the
  Integration Service's timezone; state which timezone you assumed.
- **File watch / Event Wait** → job **file arrival trigger** on the volume or
  external location, or an Auto Loader stream if the arrival rate is high enough
  that per-file job runs are wasteful.
- **`pmcmd startworkflow` from an external scheduler** (Control-M, Autosys, Tivoli)
  → the scheduler stays, and calls the Jobs REST API `run-now` endpoint instead.
  This is often the lowest-risk cutover path, because the enterprise scheduler's
  calendar and dependency rules do not have to move on day one.
- **Continuously running workflows** → a job with `continuous: { pause_status: UNPAUSED }`,
  or a Structured Streaming job — decide based on whether the mapping is
  genuinely incremental or just polls.

## 9. When to use a Declarative Pipeline instead

Lakeflow Declarative Pipelines (formerly Delta Live Tables) fit when the
workflow is essentially "build these tables in dependency order, with data
quality rules". They fit poorly when the workflow does branching, external
commands, or non-table side effects.

Use a Declarative Pipeline when most of these hold:

- The workflow is a DAG of table builds with no decision branches.
- Targets are Delta tables in Unity Catalog, mostly append or CDC-style.
- The mapping's reject handling maps naturally to expectations.
- You want the dependency graph inferred rather than hand-maintained.

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(name="stg_orders", comment="From Informatica m_STG_ORDERS")
@dlt.expect_or_drop("valid_amount", "amount >= 0")        # was ERROR() in EXP_VALIDATE
def stg_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv").load(LANDING)

@dlt.table(name="dim_customer")
def dim_customer():
    return dlt.read("stg_orders").transform(mplt_address_standardize)
```

Use a plain multi-task Job when the workflow has Decision tasks, Command tasks,
per-run parameter files, or targets outside Delta. Do not force a branching
workflow into a pipeline — the resulting `if` statements at pipeline-definition
time evaluate once, not per run, which is a subtle and nasty bug.

`APPLY CHANGES INTO` (change data capture) in a Declarative Pipeline is a strong
replacement for the Lookup + Update Strategy + Sequence Generator SCD pattern,
including SCD Type 2, and is worth proposing when several dimension mappings
share that shape.

## 10. Failure handling, recovery and notification

| PowerCenter behaviour | Databricks equivalent |
|---|---|
| Session "Recovery Strategy: Resume from last checkpoint" | make the task idempotent (MERGE, or overwrite a partition) and set `max_retries` — Spark has no row-level checkpoint for batch |
| "Stop on errors: N" | count rejects, compare against a threshold, raise if exceeded |
| "Fail parent if this task fails" | default behaviour; use `run_if: ALL_DONE` to opt out |
| Suspend workflow on error | no direct equivalent — the job fails and the run history holds state; use `run_if` branches for cleanup tasks |
| Email task on failure | `email_notifications.on_failure` or a webhook to the incident tool |
| Session log retention | job run history + your audit Delta table (job logs have a retention window; the audit table is what you keep) |

Idempotency deserves emphasis. PowerCenter recovery worked because the
Integration Service tracked committed rows. A retried Databricks task re-runs the
whole thing, so an appending task that half-succeeded will double-load on retry.
Every converted task should either `MERGE`, or overwrite a deterministic
partition, or be guarded by a run-id check against the audit table. Say this
explicitly when you emit a task with `max_retries > 0` and an append write —
that combination is a data-duplication bug waiting for its first outage.
