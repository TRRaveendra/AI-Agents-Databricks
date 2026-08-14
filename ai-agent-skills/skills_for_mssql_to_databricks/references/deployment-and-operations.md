# Deployment and Operations

This reference covers how to operationalize converted MSSQL procedures in Databricks.

---

## 1. Project Structure

Recommended repository layout:

```text
mssql-to-databricks/
├── pyproject.toml
├── databricks.yml
├── src/
│   └── etl/
│       ├── __init__.py
│       ├── common/
│       │   ├── sql_runner.py
│       │   ├── identifiers.py
│       │   ├── logging.py
│       │   └── validation.py
│       ├── procedures/
│       │   ├── load_customer.py
│       │   ├── load_order.py
│       │   └── reconcile_inventory.py
│       └── tests/
│           ├── test_load_customer.py
│           └── test_load_order.py
├── notebooks/
│   ├── load_customer_interactive.py
│   └── reconcile_inventory_interactive.py
└── resources/
    ├── sql/
    └── config/
```

---

## 2. Databricks Asset Bundles

Use Databricks Asset Bundles for repeatable deployment.

Conceptual `databricks.yml`:

```yaml
bundle:
  name: mssql-migration

targets:
  dev:
    default: true
    workspace:
      host: https://example.cloud.databricks.com

  prod:
    workspace:
      host: https://prod.cloud.databricks.com

resources:
  jobs:
    customer_load_job:
      name: customer_load_job
      tasks:
        - task_key: load_customer
          notebook_task:
            notebook_path: ../notebooks/load_customer_interactive.py
```

Actual bundle syntax should be adapted to current Databricks CLI and bundle schema.

---

## 3. Job Orchestration

Map SQL Agent jobs to Databricks Workflows.

### MSSQL Agent Job

```text
Job: Nightly Sales Load
Step 1: EXEC dbo.LoadCustomer
Step 2: EXEC dbo.LoadOrder
Step 3: EXEC dbo.BuildSalesSummary
```

### Databricks Workflow

```text
Job: nightly_sales_load
Task 1: load_customer
Task 2: load_order
Task 3: build_sales_summary
```

Use task dependencies:

```text
load_customer -> load_order -> build_sales_summary
```

Add:

- Notifications
- Retries
- Timeouts
- Repair/re-run strategy
- Environment configuration
- Service principal permissions

---

## 4. Notebook vs Library Task

Use notebook tasks for:

- Interactive conversions
- Small jobs
- Simple orchestration
- Demonstrations

Use Python wheel/library tasks for:

- Reusable code
- Unit tests
- CI/CD
- Versioned releases
- Complex dependencies
- Production-grade structure

Preferred production pattern:

```text
Databricks Workflow
  -> Python task
    -> run_load_customer(spark, params, table_map, dry_run=False)
```

---

## 5. Parameterization

Use Databricks jobs widgets or job parameters.

Example job parameter:

```json
{
  "load_date": "2026-08-14"
}
```

In notebook:

```python
dbutils.widgets.text("load_date", "")
load_date = dbutils.widgets.get("load_date")
```

In Python task, pass parameters through task configuration or configuration files.

---

## 6. Environment Strategy

Use separate environments:

- Dev
- QA
- UAT
- Prod

Use `table_map` to avoid hardcoding environment-specific table names.

```python
table_map = {
    "customer_target": "dev.sales.customer"
}
```

or:

```python
table_map = {
    "customer_target": "prod.sales.customer"
}
```

---

## 7. Idempotency

Every converted procedure should be re-runnable.

Techniques:

- `MERGE` by business key
- `INSERT OVERWRITE` by partition
- Delete-insert with audit key
- Watermark tracking
- Unique load identifiers
- Conditional inserts

---

## 8. Monitoring

Track:

- Job success/failure
- Duration
- Input row count
- Output row count
- Inserted/updated/deleted counts
- Validation failures
- Data quality exceptions
- Drift from expected volume
- Slow tasks
- Shuffle spill
- Skew

Use:

- Databricks Workflows alerts
- Unity Catalog audit logs
- Cluster metrics
- Custom audit tables
- Logging to Delta tables

---

## 9. Logging Pattern

```python
from datetime import datetime

def log_run(spark, procedure_name, status, details=None):
    spark.createDataFrame(
        [
            (
                procedure_name,
                status,
                details,
                datetime.utcnow()
            )
        ],
        ["ProcedureName", "Status", "Details", "RunAt"]
    ).write.mode("append").saveAsTable("analytics.audit.pipeline_run_log")
```

Do not log secrets or excessive personal data.

---

## 10. Rollback Strategy

Databricks does not behave like SQL Server transaction rollback across arbitrary statements.

Rollback options:

### Delta Time Travel

```sql
RESTORE TABLE analytics.sales.customer
TO VERSION AS OF 123;
```

or:

```sql
RESTORE TABLE analytics.sales.customer
TO TIMESTAMP AS OF '2026-08-14 01:00:00';
```

Use with governance approval.

### Backup Table Pattern

Before destructive operation:

```sql
CREATE OR REPLACE TABLE analytics.sales.customer_backup_20260814 AS
SELECT * FROM analytics.sales.customer;
```

Then restore if needed.

### Compensation Logic

For multi-step workflows:

- Track step status
- Reverse completed steps if safe
- Rerun from checkpoint
- Alert for manual review if not reversible

---

## 11. Performance Tuning

Common optimizations:

- Partition large date-based tables
- Use liquid clustering
- Use `OPTIMIZE`
- Use `ZORDER` for frequent filters
- Broadcast small dimension tables
- Reduce shuffles
- Deduplicate before joins
- Avoid Python UDFs where built-ins work
- Cache only if reused multiple times
- Avoid collecting large datasets to driver
- Use AQE where enabled
- Monitor skew

Example broadcast join:

```python
from pyspark.sql.functions import broadcast

result_df = large_df.join(broadcast(small_df), "key")
```

Example SQL hint:

```sql
SELECT /*+ BROADCAST(dim) */
    f.*,
    dim.Name
FROM fact_table f
JOIN dim_table dim
  ON f.DimId = dim.Id;
```

---

## 12. Maintenance Jobs

Recommended scheduled maintenance:

```text
Daily:
  - Data quality checks
  - Audit reconciliation

Weekly:
  - OPTIMIZE key tables
  - VACUUM with approved retention

Monthly:
  - Access review
  - Table ownership review
  - Cost review
  - Performance review
```

---

## 13. CI/CD

Recommended pipeline:

1. Lint Python code
2. Run unit tests
3. Run synthetic Spark tests
4. Run SQL syntax validation
5. Run security scan
6. Deploy bundle to dev
7. Run integration tests
8. Deploy to QA/UAT
9. Run differential validation
10. Approve production deployment
11. Deploy bundle to prod
12. Run smoke tests

---

## 14. Operational Checklist

Before production cutover:

- [ ] Stored procedure logic reviewed
- [ ] Table mapping approved
- [ ] Data types approved
- [ ] Security mapping approved
- [ ] Dry-run tested
- [ ] Synthetic tests passed
- [ ] Differential validation passed
- [ ] Performance acceptable
- [ ] Rollback plan documented
- [ ] Monitoring configured
- [ ] Alerting configured
- [ ] Permissions configured
- [ ] Workflow dependencies tested
- [ ] Failure retry behavior tested
- [ ] Documentation complete