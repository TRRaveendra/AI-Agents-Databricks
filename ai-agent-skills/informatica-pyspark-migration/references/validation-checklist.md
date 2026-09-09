# Validation & Cutover Checklist

## Static Checks (must pass before any execution)

- [ ] Every physical table / path comes from `table_map`
- [ ] No hard-coded credentials or connection strings
- [ ] All writes guarded by `if not dry_run:`
- [ ] No `eval`, `exec`, unrestricted `dbutils`, or network calls outside approved JDBC
- [ ] Imports are limited to `pyspark.sql`, `pyspark.sql.functions`, `delta.tables`, standard library
- [ ] Function signature is exactly `run_<name>(spark, params, table_map, dry_run=True)`
- [ ] Semantic Notes section documents Sequence Generator, Update Strategy, unconnected Lookup, and any intentional deviations

## Synthetic Dry-Run

- [ ] Notebook / function executes with `dry_run=True` without writing
- [ ] Returned DataFrames have expected schema (column names + types)
- [ ] Row counts on sample data are non-zero where expected
- [ ] No Spark analysis exceptions

## Differential Validation (Informatica vs Databricks)

For a representative date range or key set:

- [ ] Source row counts match
- [ ] Target row counts match (or documented intentional differences)
- [ ] Key business columns (amounts, statuses, dates) match within tolerance
- [ ] Null counts per critical column match
- [ ] Aggregate checksums (sum of amount, count of distinct keys) match
- [ ] SCD Type 2 history (if applicable) produces equivalent current + historical rows
- [ ] Reject / error rows handled equivalently (or intentionally improved)

## Performance & Operability

- [ ] Job completes within acceptable SLA (compare to Informatica baseline)
- [ ] Cluster size / Photon / AQE settings documented
- [ ] Auto Loader checkpoints (if used) are durable and recoverable
- [ ] Monitoring & alerts configured (Job success/failure, data quality expectations)
- [ ] Unity Catalog grants applied; no over-privileged principals

## Cutover Readiness

- [ ] Parallel run period completed with sign-off
- [ ] Informatica jobs disabled or decommissioned in controlled order
- [ ] Rollback plan exists (re-enable Informatica + restore from Delta time travel if needed)
- [ ] Documentation and runbooks updated
- [ ] Team trained on the new Databricks jobs / pipelines
