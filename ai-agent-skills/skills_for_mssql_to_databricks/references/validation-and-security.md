# Validation and Security

Use three validation gates for every converted stored procedure.

---

## Gate 1: Static Validation

Static validation checks the converted code without necessarily running it against production data.

Check:

- Python syntax
- Spark SQL syntax
- Table names exist
- Column names exist
- Data types are compatible
- Identifier whitelist is used
- No secrets in code
- No hardcoded credentials
- No unsafe dynamic SQL
- Dry-run support exists
- Return structure matches contract
- Logging does not expose sensitive data

Static checks can include:

```python
import ast

with open("converted_proc.py") as f:
    ast.parse(f.read())
```

For Spark SQL syntax:

```python
spark.sql("EXPLAIN SELECT 1")
```

Use `EXPLAIN` on generated queries where safe.

---

## Gate 2: Synthetic Validation

Create controlled test data.

### Required Test Cases

1. Empty source
2. Empty target
3. Single row insert
4. Update existing row
5. Delete row
6. Upsert mixed changes
7. Duplicate source keys
8. NULL keys
9. NULL values in comparison columns
10. Empty strings
11. Unicode strings
12. Boundary dates
13. Time zone-sensitive timestamps
14. Decimal precision and scale
15. Large integers
16. Case-sensitive values
17. Whitespace differences
18. Duplicate target rows
19. Constraint violations
20. Error handling path

### Example Synthetic Test

```python
from pyspark.sql import functions as F

source_df = spark.createDataFrame(
    [
        (1, "Alice", True),
        (2, "Bob", False),
        (2, "Bobby", False),
        (3, None, True)
    ],
    ["CustomerId", "CustomerName", "IsActive"]
)

target_df = spark.createDataFrame(
    [
        (1, "Old Alice", True),
        (4, "Dave", True)
    ],
    ["CustomerId", "CustomerName", "IsActive"]
)

source_df.createOrReplaceTempView("customer_source")
target_df.createOrReplaceTempView("customer_target")
```

Then run conversion against temp views and assert expected rows.

---

## Gate 3: Differential Validation

Compare MSSQL output with Databricks output.

### Recommended Strategy

1. Extract source input snapshot.
2. Run MSSQL stored procedure against snapshot.
3. Run PySpark conversion against same snapshot.
4. Normalize outputs:
   - Column order
   - Data types
   - Timestamp precision
   - Decimal scale
   - NULL representation
   - Sorting
5. Compare row counts.
6. Compare checksums.
7. Compare column-level differences.
8. Store exception report.

---

### Row Count Check

```python
mssql_count = ...
spark_count = spark.table("converted_output").count()

assert mssql_count == spark_count
```

### Hash Comparison

```python
from pyspark.sql import functions as F

spark_hash = (
    spark.table("converted_output")
         .withColumn(
             "row_hash",
             F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]))
         )
         .groupBy("row_hash")
         .count()
)
```

Compare with equivalent hash generated from MSSQL export.

---

### Column-Level Difference Pattern

```python
diff = (
    expected_df.alias("e")
    .join(actual_df.alias("a"), on=key_columns, how="full_outer")
    .filter(
        F.col("e.CustomerId").isNull() |
        F.col("a.CustomerId").isNull() |
        (F.col("e.CustomerName") != F.col("a.CustomerName"))
    )
)
```

Use null-safe comparison where necessary:

```python
F.col("e.CustomerName").eqNullSafe(F.col("a.CustomerName"))
```

---

## Data Quality Checks

Add checks for:

- Unique keys
- Referential integrity expectations
- Not null columns
- Valid ranges
- Valid dates
- Valid enums
- Amount precision
- No orphan rows
- No duplicate business keys
- Freshness
- Volume anomalies

---

## Security Rules

### 1. No credentials in code

Never include:

- SQL Server passwords
- PAT tokens
- OAuth secrets
- Storage account keys
- Personal connection strings

Use Databricks secrets or platform secret management.

---

### 2. Least privilege

The conversion process and runtime service principal should have minimum required permissions.

Examples:

- Read source schema
- Write target schema
- Execute workflow
- Read secret scope

Avoid workspace admin unless required.

---

### 3. Safe dynamic SQL

Allowed:

```python
ALLOWED_TABLES = {"customer": "analytics.sales.customer"}
table = ALLOWED_TABLES[logical_name]
```

Not allowed:

```python
table = user_input
spark.sql(f"SELECT * FROM {table}")
```

---

### 4. Parameter handling

Use parameter views, DataFrame filters, or runtime-supported SQL parameter binding.

Do not concatenate parameter values.

---

### 5. Dry-run mode

Destructive operations must support dry run.

```python
if dry_run:
    return {
        "status": "dry_run",
        "message": "No mutations executed"
    }
```

---

### 6. Sensitive data logging

Do not log:

- Passwords
- Tokens
- Connection strings
- Personal data unless approved
- Full error payloads containing secrets

---

### 7. External access

Do not introduce external calls unless required and approved.

---

## Validation Output Template

```json
{
  "static_validation": {
    "syntax": "PASS",
    "identifier_whitelist": "PASS",
    "secret_scan": "PASS"
  },
  "synthetic_validation": {
    "empty_source": "PASS",
    "duplicate_keys": "PASS",
    "null_handling": "PASS"
  },
  "differential_validation": {
    "row_count_match": true,
    "hash_match": false,
    "difference_count": 3,
    "report_location": "/Volumes/.../validation_report.csv"
  },
  "security_validation": {
    "no_credentials": true,
    "safe_dynamic_sql": true,
    "dry_run_supported": true
  }
}
```