---
name: mssql-to-pyspark-migration
description: Use when asked to convert MSSQL T-SQL stored procedures to Databricks PySpark or Spark SQL. Covers DML, DDL, DRL/permissions, MERGE, INSERT, UPDATE, DELETE, SELECT, JOINs, CTEs, subqueries, set operators, temp tables, cursors, transactions, error handling, dynamic SQL, and deployment.
---

# MSSQL Stored Procedure to Databricks PySpark Migration Skill

## Purpose

Act as a dual expert in:

1. **Microsoft SQL Server T-SQL stored procedures**
   - Stored procedures
   - DML: `INSERT`, `UPDATE`, `DELETE`, `MERGE`
   - DDL: `CREATE`, `ALTER`, `DROP`
   - DRL / security: `GRANT`, `DENY`, `REVOKE`
   - Control-of-flow: `IF`, `WHILE`, `TRY/CATCH`, `RETURN`
   - Temp tables, table variables, cursors
   - Dynamic SQL and nested procedures

2. **Databricks PySpark / Spark SQL**
   - `spark.sql`
   - `%sql` notebook magic
   - DataFrame API
   - Delta Lake `MERGE`
   - Unity Catalog
   - Databricks Workflows
   - Asset Bundles
   - Production-grade notebook and job design

This skill converts MSSQL stored procedures into maintainable, secure, production-ready Databricks PySpark code while preserving business semantics.

---

## When to Activate

Activate when the user asks for any of:

- Convert MSSQL stored procedure to PySpark
- Convert T-SQL to Spark SQL
- Migrate SQL Server ETL to Databricks
- Convert `MERGE` T-SQL to Delta `MERGE`
- Convert `UPDATE FROM` / `DELETE FROM` joins to PySpark
- Convert temp tables, cursors, transactions, error handling
- Convert MSSQL DDL to Unity Catalog DDL
- Map SQL Server permissions to Unity Catalog privileges
- Refactor T-SQL procedural logic into PySpark functions
- Validate converted Spark SQL against original MSSQL logic

---

## CRITICAL: Security-First Conversion Rules

### SQL Injection Prevention (MANDATORY)

**⚠️ NEVER use f-strings or string concatenation with user input in SQL queries**

This is the #1 security vulnerability in MSSQL-to-Databricks conversions.

#### ❌ DANGEROUS - DO NOT DO THIS:

```python
# DANGEROUS - SQL injection vulnerability
customer_id = dbutils.widgets.get("CustomerID")
start_date = dbutils.widgets.get("StartDate")

# BAD - Direct f-string interpolation
df = spark.sql(f"""
    SELECT * FROM Orders
    WHERE CustomerID = {customer_id}
      AND OrderDate >= '{start_date}'
""")

# BAD - String concatenation
sql = "SELECT * FROM Orders WHERE CustomerID = " + str(customer_id)
df = spark.sql(sql)
```

#### ✅ SAFE - ALWAYS DO THIS:

**Method 1: DataFrame API with col() comparisons (PREFERRED)**

```python
from pyspark.sql.functions import col, lit

# Safe - DataFrame filter with column expressions
customer_id = int(dbutils.widgets.get("CustomerID"))
start_date = dbutils.widgets.get("StartDate")
end_date = dbutils.widgets.get("EndDate")
min_amount = float(dbutils.widgets.get("MinAmount"))

df = spark.table("Orders").filter(
    (col("CustomerID") == customer_id) &
    (col("OrderDate") >= start_date) &
    ((lit(end_date) == "") | (col("OrderDate") <= end_date)) &
    (col("TotalAmount") >= min_amount)
)
```

**Method 2: Typed DataFrames with Temp Views for Complex SQL**

```python
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

# Create strongly-typed source DataFrame
source_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("OrderCount", LongType(), False),
    StructField("TotalAmount", DoubleType(), False)
])

source_data = [(customer_id, total_orders, total_amount)]
source_df = spark.createDataFrame(source_data, source_schema)
source_df = source_df.withColumn("ProcessedDate", current_timestamp())
source_df.createOrReplaceTempView("MergeSource")

# Safe - No string interpolation needed
spark.sql("""
    MERGE INTO CustomerSummary AS target
    USING MergeSource AS source
    ON target.CustomerID = source.CustomerID
    WHEN MATCHED THEN UPDATE SET
        OrderCount = target.OrderCount + source.OrderCount,
        TotalAmount = target.TotalAmount + source.TotalAmount,
        LastProcessed = source.ProcessedDate
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Method 3: Safe Parameterized Updates via MERGE**

```python
# Get order IDs safely using DataFrame operations
order_ids = [row.OrderID for row in filtered_df.select("OrderID").collect()]

if order_ids:
    # Create temp view with IDs to update
    update_ids_df = spark.createDataFrame(
        [(oid,) for oid in order_ids], 
        ["OrderID"]
    )
    update_ids_df.createOrReplaceTempView("UpdateOrderIDs")
    
    # Safe parameterized update
    spark.sql("""
        MERGE INTO Orders AS target
        USING UpdateOrderIDs AS source
        ON target.OrderID = source.OrderID
        WHEN MATCHED THEN UPDATE SET
            Status = 'Processed',
            ProcessedDate = current_timestamp()
    """)
```

### Date/Timestamp Handling (MANDATORY)

**⚠️ NEVER use Python datetime objects in Spark SQL**

#### ❌ DANGEROUS:

```python
from datetime import datetime

# BAD - Python datetime incompatible with Spark SQL
processed_date = datetime.now()
spark.sql(f"""
    UPDATE orders 
    SET processed_date = '{processed_date}'
    WHERE ...
""")
```

#### ✅ SAFE:

```python
from pyspark.sql.functions import current_timestamp, current_date, to_date, lit

# Method 1: Use Spark SQL native functions
df = df.withColumn("processed_date", current_timestamp())
df = df.withColumn("report_date", current_date())

# Method 2: In SQL queries
spark.sql("""
    UPDATE orders 
    SET processed_date = current_timestamp(),
        report_date = current_date()
    WHERE ...
""")

# Method 3: For specific dates, use to_date with lit
df = df.withColumn("cutoff_date", to_date(lit("2024-01-01")))
```

### Type Safety and Validation

**Always validate and cast user input:**

```python
# Type validation
try:
    customer_id = int(dbutils.widgets.get("CustomerID"))
    min_amount = float(dbutils.widgets.get("MinAmount"))
except ValueError as e:
    raise ValueError(f"Invalid parameter type: {e}")

# Range validation
if min_amount < 0:
    raise ValueError("MinAmount must be non-negative")

# Define schemas explicitly
result_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("ProcessedOrders", LongType(), False),
    StructField("ProcessedAmount", DoubleType(), False),
    StructField("ProcessedDate", TimestampType(), False)
])
```

### Seven Mandatory Security Rules

1. **NEVER use f-strings or + concatenation with user input in SQL**
2. **Use DataFrame API `.filter()` instead of WHERE clauses with variables**
3. **Use `current_timestamp()` and `current_date()` instead of Python datetime**
4. **Create typed DataFrames and temp views for parameterized SQL**
5. **Always validate and cast user input to expected types**
6. **Use `lit()` for literal values in DataFrame operations**
7. **Prefer DataFrame API over spark.sql() when dealing with user input**

---

## Core Conversion Principles

### 1. Prefer set-based Spark SQL over row-by-row logic

MSSQL stored procedures often use procedural constructs. In Databricks, always prefer:

- Set-based SQL
- Delta `MERGE`
- DataFrame transformations
- Window functions
- Joins instead of cursors

Avoid row-by-row processing unless there is no alternative.

---

### 2. Prefer Spark SQL for SQL-like operations

Use Spark SQL for:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`
- CTEs
- Joins
- Set operators
- Window functions

Use PySpark DataFrame API when:

- Dynamic logic is needed
- Conditional branching is complex
- Parameters must be safely injected
- Reusable functions are required
- Unit testing is easier through Python functions

---

### 3. Use Delta Lake for mutation

`UPDATE`, `DELETE`, and `MERGE` require Delta tables or another transactional table format.

Do not assume plain Parquet, CSV, or external non-transactional tables support ACID updates.

---

### 4. Preserve semantics, not just syntax

A direct translation may be syntactically valid but semantically wrong.

Always check:

- NULL handling
- Join cardinality
- Duplicate rows
- Integer division
- Date/time boundaries
- Collation and case sensitivity
- Empty string versus NULL
- Ordering with `TOP`, `OFFSET`, `FETCH`
- Transaction scope
- Error-handling behavior

---

### 5. Treat SQL Server objects as Lakehouse objects

Typical mapping:

| MSSQL | Databricks |
|---|---|
| Server | Workspace / catalog boundary |
| Database | Catalog or schema |
| Schema | Schema |
| Table | Unity Catalog Delta table |
| View | Unity Catalog view |
| Stored procedure | Python function / notebook / workflow task |
| Function | Python function, SQL UDF, or Spark SQL expression |
| Temp table `#temp` | Temporary view |
| Global temp table `##temp` | Global temporary view |
| SQL Agent job | Databricks Workflow |
| Login/user/permission | Unity Catalog principal/privilege |

---

## Mandatory Output Contract

When converting a stored procedure, generate a reusable Python function unless the user explicitly asks for notebook-only SQL.

Use this contract:

```python
def run_<procedure_name>(
    spark,
    params: dict | None = None,
    table_map: dict | None = None,
    dry_run: bool = True
) -> dict:
    """
    Converted from MSSQL stored procedure: <procedure_name>

    Args:
        spark: Active SparkSession.
        params: Procedure parameters.
        table_map: Logical-to-physical table mapping.
        dry_run: If True, do not mutate target tables.

    Returns:
        dict containing status, executed statements, metrics, and validation info.
    """
```

Rules:

- Default `dry_run=True` for safety.
- Do not hard-code credentials.
- Do not hard-code environment-specific catalog/schema/table names unless explicitly provided.
- Use `table_map` for table name substitution.
- Validate identifiers before interpolation.
- Prefer temporary views and parameterized SQL over string concatenation.
- Return useful metadata:

```python
{
    "status": "success",
    "procedure": "<proc_name>",
    "dry_run": True,
    "statements": [],
    "metrics": {},
    "warnings": [],
    "validation": {}
}
```

---

## Conversion Workflow

Use this workflow for every stored procedure conversion.

### Step 1: Inventory the MSSQL procedure

Identify:

- Procedure name
- Parameters
- Temp tables
- Table variables
- Cursors
- Transactions
- Error handling
- Dynamic SQL
- DML statements
- DDL statements
- Permission statements
- Dependencies
- Side effects
- Return codes
- Output parameters

---

### Step 2: Classify statements

Classify every block:

| Type | Examples |
|---|---|
| DRL/Security | `GRANT`, `DENY`, `REVOKE` |
| DDL | `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE VIEW` |
| DML | `INSERT`, `UPDATE`, `DELETE`, `MERGE` |
| Query | `SELECT`, joins, CTEs, subqueries, set operators |
| Control flow | `IF`, `WHILE`, `TRY/CATCH`, `RETURN` |
| Temp objects | `#temp`, `##temp`, table variables |
| Procedural | Cursors, dynamic SQL, nested proc calls |
| Transactional | `BEGIN TRAN`, `COMMIT`, `ROLLBACK` |
| Diagnostics | `@@ROWCOUNT`, `@@ERROR`, `RAISERROR`, `THROW` |

---

### Step 3: Map to Databricks equivalents

Use the quick mapping table below and the reference files.

---

### Step 4: Generate PySpark/Spark SQL

Generate one of:

1. Pure Python function using `spark.sql`
2. Notebook with `%sql` blocks for interactive use
3. Hybrid Python + `%sql` notebook
4. Databricks Asset Bundle task
5. Unit-testable module with helper functions

Default to reusable Python function unless asked otherwise.

---

### Step 5: Add validation

For every conversion, include:

- Static validation
- Synthetic test plan
- Differential validation plan against MSSQL output
- Edge cases
- Data quality checks

---

### Step 6: Document risks

Call out:

- Semantic differences
- Performance risks
- Unsupported constructs
- Required redesign
- Data type mismatches
- Security mapping gaps

---

## Quick Mapping Table

### DML

| MSSQL T-SQL | Databricks PySpark / Spark SQL |
|---|---|
| `INSERT INTO ... SELECT` | `INSERT INTO ... SELECT` or DataFrame `write.mode("append")` |
| `INSERT INTO ... VALUES` | DataFrame with literal rows or `INSERT INTO ... VALUES` |
| `INSERT OVERWRITE` | `INSERT OVERWRITE` or `write.mode("overwrite")` |
| `SELECT INTO` | `CREATE TABLE ... AS SELECT` |
| `UPDATE` | Delta `UPDATE` or `MERGE` |
| `UPDATE FROM JOIN` | Delta `MERGE INTO ... USING ...` |
| `DELETE` | Delta `DELETE` or `MERGE` |
| `DELETE FROM JOIN` | Delta `MERGE INTO ... USING ...` |
| `TRUNCATE TABLE` | `TRUNCATE TABLE` for Delta tables |
| `MERGE` | Delta `MERGE INTO` |
| `OUTPUT` clause | Capture via CDF, audit table, or before/after join |
| `@@ROWCOUNT` | DataFrame count, Delta operation metrics, or audit logic |

---

### SELECT and Query Constructs

| MSSQL T-SQL | Databricks Spark SQL |
|---|---|
| `SELECT` | `SELECT` |
| `TOP` | `LIMIT` |
| `OFFSET/FETCH` | `OFFSET/FETCH` or `LIMIT` with window |
| `INNER JOIN` | `INNER JOIN` |
| `LEFT JOIN` | `LEFT JOIN` |
| `RIGHT JOIN` | `RIGHT JOIN` |
| `FULL JOIN` | `FULL JOIN` |
| `CROSS JOIN` | `CROSS JOIN` |
| `CROSS APPLY` | `LATERAL JOIN`, `LATERAL VIEW explode`, or inline rewrite |
| `OUTER APPLY` | `LEFT LATERAL JOIN` or inline rewrite |
| CTE | `WITH ... AS` |
| Recursive CTE | Recursive CTE if supported; otherwise iterative PySpark |
| Scalar subquery | Scalar subquery, join, or window function |
| Correlated subquery | Join, window function, or aggregation |
| `EXISTS` | `LEFT SEMI JOIN` or `EXISTS` |
| `NOT EXISTS` | `LEFT ANTI JOIN` or `NOT EXISTS` |
| `IN` | `IN`, join, or array function |
| `UNION` | `UNION` |
| `UNION ALL` | `UNION ALL` |
| `INTERSECT` | `INTERSECT` |
| `EXCEPT` | `EXCEPT` |
| `ROW_NUMBER()` | `ROW_NUMBER()` |
| `RANK()` | `RANK()` |
| `DENSE_RANK()` | `DENSE_RANK()` |
| `LEAD()` | `LEAD()` |
| `LAG()` | `LAG()` |
| `NTILE()` | `NTILE()` |

---

### DDL

| MSSQL T-SQL | Databricks |
|---|---|
| `CREATE TABLE` | `CREATE TABLE` Delta/Unity Catalog |
| `ALTER TABLE ADD` | `ALTER TABLE ADD COLUMN` |
| `ALTER TABLE DROP COLUMN` | `ALTER TABLE DROP COLUMN` |
| `ALTER TABLE ALTER COLUMN` | Type change with constraints and compatibility checks |
| `CREATE VIEW` | `CREATE VIEW` |
| `CREATE OR ALTER VIEW` | `CREATE OR REPLACE VIEW` |
| `DROP TABLE` | `DROP TABLE` |
| `DROP VIEW` | `DROP VIEW` |
| Index | Use partitioning, liquid clustering, `OPTIMIZE`, `ZORDER` |
| Statistics | Managed by Delta/Photon; use `ANALYZE TABLE` where needed |
| Constraints | `NOT NULL`, `CHECK`, primary/foreign key where supported/informational |
| Identity | `GENERATED ALWAYS AS IDENTITY` or surrogate key logic |
| Sequence | Identity-like column, sequence table, or key-generation function |

---

### DRL / Security

| MSSQL | Databricks Unity Catalog |
|---|---|
| `GRANT SELECT` | `GRANT SELECT` |
| `GRANT INSERT` | `GRANT MODIFY` or table-level write privilege |
| `GRANT UPDATE` | `GRANT MODIFY` |
| `GRANT DELETE` | `GRANT MODIFY` |
| `GRANT EXECUTE` | No direct equivalent; control notebook/job/workspace access |
| `GRANT CONTROL` | Catalog/schema-level admin privileges |
| `DENY` | Use least privilege; Unity Catalog does not use DENY the same way |
| `REVOKE` | `REVOKE` |
| Database roles | Unity Catalog groups/account groups |
| Row-level security | Row filters |
| Column masking | Column masks |

---

### Control Flow and Procedural Logic

| MSSQL | Databricks |
|---|---|
| `DECLARE @var` | Python variable or temporary view parameter |
| `SET @var = ...` | Python assignment or Spark SQL assignment into view |
| `IF` | Python `if` or SQL `CASE WHEN` |
| `WHILE` | Python loop, iterative DataFrame logic, or avoid |
| `BEGIN TRY/CATCH` | Python `try/except` |
| `THROW` / `RAISERROR` | `raise` Python exception |
| `RETURN` | Python `return` |
| Output parameter | Return value from Python function |
| Cursor | Set-based SQL or controlled iterative loop |
| Temp table `#temp` | Temporary view |
| Global temp table `##temp` | Global temporary view |
| Table variable | Temporary view or DataFrame |
| Dynamic SQL | Safe Python SQL builder with whitelisted identifiers |
| Nested procedure call | Python function call |
| SQL Agent job | Databricks Workflow |
| `BEGIN TRAN` | Delta atomic operation / orchestration transaction design |
| `COMMIT` | Successful job/task completion |
| `ROLLBACK` | Compensation logic / restore / idempotent retry |

---

## Preferred Code Style

### Python wrapper with Spark SQL (SECURE TEMPLATE)

```python
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
import re

def run_upsert_customer(
    spark,
    params: dict | None = None,
    table_map: dict | None = None,
    dry_run: bool = True
) -> dict:
    """
    Secure production template for MSSQL stored procedure conversion.
    
    Security features:
    - No f-string interpolation with user data
    - Parameterized queries via temp views
    - Type validation
    - Identifier whitelist validation
    """
    params = params or {}
    table_map = table_map or {}

    # Validate identifiers (whitelist pattern)
    def validate_identifier(name: str) -> str:
        if not re.match(r'^[a-zA-Z0-9_\.]+

---

## Notebook `%sql` Usage

Use `%sql` for interactive exploration or notebook demonstrations.

Example:

```sql
CREATE OR REPLACE TEMP VIEW customer_source AS
SELECT *
FROM dev.sales.customer_source
WHERE is_active = 1;
```

Then:

```sql
MERGE INTO dev.sales.customer AS tgt
USING customer_source AS src
    ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;
```

For production jobs, prefer Python functions with `spark.sql`, because:

- Parameters are easier to manage
- Error handling is clearer
- Unit testing is easier
- Dynamic logic is safer
- Workflows are easier to orchestrate

---

## High-Risk Constructs

Flag these explicitly:

| MSSQL Construct | Risk | Recommended Databricks Approach |
|---|---|---|
| Cursor | Row-by-row performance issue | Rewrite as set-based SQL |
| Dynamic SQL | Injection and maintainability risk | Whitelist identifiers, avoid dynamic values |
| `sp_executesql` | Dynamic SQL risk | Safe SQL builder |
| `WHILE` loops | Poor scalability | Set-based rewrite or bounded iteration |
| Multi-statement transactions | No exact equivalent across tables | Design idempotent tasks and compensation logic |
| Temp tables | Session/state dependence | Temporary views or DataFrames |
| Global temp tables | Shared state risk | Global temp views only if necessary |
| Linked servers | Cross-system dependency | Lakehouse Federation, JDBC, external locations |
| CLR procedures | Not portable | Rewrite in Python/Scala/SQL |
| `xp_cmdshell` | Security risk | Do not migrate; redesign securely |
| Service Broker | Messaging system | Use event-driven architecture or messaging service |
| SQL Agent | Scheduler | Databricks Workflows |
| `OUTPUT` clause | Not always direct | Use audit table or Change Data Feed |
| `@@ROWCOUNT` | Implicit state | Capture counts explicitly |
| `SET` options | Session behavior differences | Validate semantics case by case |
| Row-level triggers | Not Delta-equivalent | Redesign with MERGE, CDF, or orchestration |

---

## Required Response Format for Conversions

When asked to convert a stored procedure, respond with:

1. **Source Procedure Summary**
   - Procedure name
   - Parameters
   - Business intent
   - Dependencies

2. **Conversion Strategy**
   - What can be directly mapped
   - What needs redesign
   - What is unsupported or risky

3. **Converted PySpark Code**
   - Production-ready Python function
   - Optional `%sql` version if useful

4. **Validation Plan**
   - Static checks
   - Synthetic tests
   - Differential validation

5. **Risks and Recommendations**
   - Semantic differences
   - Performance concerns
   - Security mapping
   - Operational notes

---

## Reference Files

Use these reference files for detailed patterns:

- `references/dml-patterns.md`
  - `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `OUTPUT`, `@@ROWCOUNT`

- `references/select-joins-cte.md`
  - `SELECT`, joins, CTEs, subqueries, set operators, window functions

- `references/ddl-drl.md`
  - Tables, views, identity, constraints, permissions, Unity Catalog

- `references/control-flow-temp.md`
  - Variables, temp tables, cursors, transactions, error handling, dynamic SQL

- `references/conversion-rules.md`
  - Mandatory rules, semantic differences, data type mapping, anti-patterns

- `references/validation-and-security.md`
  - Validation gates, differential testing, security rules

- `references/deployment-and-operations.md`
  - Databricks Asset Bundles, jobs, monitoring, rollback, optimization, name):
            raise ValueError(f"Invalid identifier: {name}")
        return name

    source_table = validate_identifier(
        table_map.get("source_table", "dev.sales.customer_source")
    )
    target_table = validate_identifier(
        table_map.get("target_table", "dev.sales.customer")
    )

    # Validate and cast parameters
    customer_id = int(params.get("customer_id", 0))
    min_amount = float(params.get("min_amount", 0.0))
    
    # Use DataFrame API for filtering (safe from SQL injection)
    source_df = spark.table(source_table).filter(
        (col("customer_id") == customer_id) &
        (col("amount") >= min_amount) &
        (col("is_active") == True)
    )
    
    # Add timestamp using Spark function (not Python datetime)
    source_df = source_df.withColumn("processed_date", current_timestamp())
    
    # Create temp view for parameterized MERGE
    source_df.createOrReplaceTempView("customer_source_filtered")

    # Safe SQL - no user input interpolation
    if not dry_run:
        spark.sql(f"""
            MERGE INTO {target_table} AS tgt
            USING customer_source_filtered AS src
                ON tgt.customer_id = src.customer_id
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        status = "success"
    else:
        status = "dry_run_success"

    return {
        "status": status,
        "dry_run": dry_run,
        "statements": [f"MERGE {target_table}"],
        "metrics": {
            "source_rows": source_df.count(),
            "filtered_rows": source_df.count()
        },
        "warnings": [],
        "validation": {
            "customer_id": customer_id,
            "min_amount": min_amount
        }
    }
```

### Key Security Features in Template:

1. **Identifier validation** - Whitelist pattern for table names
2. **Type casting** - Explicit int/float conversions with error handling
3. **DataFrame API filtering** - No SQL string interpolation for user data
4. **Temp views** - Safe parameterization for complex SQL
5. **Spark SQL functions** - `current_timestamp()` not Python datetime
6. **Dry-run support** - Safe testing without mutations
7. **Metrics and validation** - Return structured results

For production, add:
- Enhanced logging
- Audit trail
- Error recovery
- Performance metrics
- Unit tests

---

## Notebook `%sql` Usage

Use `%sql` for interactive exploration or notebook demonstrations.

Example:

```sql
CREATE OR REPLACE TEMP VIEW customer_source AS
SELECT *
FROM dev.sales.customer_source
WHERE is_active = 1;
```

Then:

```sql
MERGE INTO dev.sales.customer AS tgt
USING customer_source AS src
    ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;
```

For production jobs, prefer Python functions with `spark.sql`, because:

- Parameters are easier to manage
- Error handling is clearer
- Unit testing is easier
- Dynamic logic is safer
- Workflows are easier to orchestrate

---

## High-Risk Constructs

Flag these explicitly:

| MSSQL Construct | Risk | Recommended Databricks Approach |
|---|---|---|
| Cursor | Row-by-row performance issue | Rewrite as set-based SQL |
| Dynamic SQL | Injection and maintainability risk | Whitelist identifiers, avoid dynamic values |
| `sp_executesql` | Dynamic SQL risk | Safe SQL builder |
| `WHILE` loops | Poor scalability | Set-based rewrite or bounded iteration |
| Multi-statement transactions | No exact equivalent across tables | Design idempotent tasks and compensation logic |
| Temp tables | Session/state dependence | Temporary views or DataFrames |
| Global temp tables | Shared state risk | Global temp views only if necessary |
| Linked servers | Cross-system dependency | Lakehouse Federation, JDBC, external locations |
| CLR procedures | Not portable | Rewrite in Python/Scala/SQL |
| `xp_cmdshell` | Security risk | Do not migrate; redesign securely |
| Service Broker | Messaging system | Use event-driven architecture or messaging service |
| SQL Agent | Scheduler | Databricks Workflows |
| `OUTPUT` clause | Not always direct | Use audit table or Change Data Feed |
| `@@ROWCOUNT` | Implicit state | Capture counts explicitly |
| `SET` options | Session behavior differences | Validate semantics case by case |
| Row-level triggers | Not Delta-equivalent | Redesign with MERGE, CDF, or orchestration |

---

## Required Response Format for Conversions

When asked to convert a stored procedure, respond with:

1. **Source Procedure Summary**
   - Procedure name
   - Parameters
   - Business intent
   - Dependencies

2. **Conversion Strategy**
   - What can be directly mapped
   - What needs redesign
   - What is unsupported or risky

3. **Converted PySpark Code**
   - Production-ready Python function
   - Optional `%sql` version if useful

4. **Validation Plan**
   - Static checks
   - Synthetic tests
   - Differential validation

5. **Risks and Recommendations**
   - Semantic differences
   - Performance concerns
   - Security mapping
   - Operational notes

---

## Reference Files

Use these reference files for detailed patterns:

- `references/dml-patterns.md`
  - `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `OUTPUT`, `@@ROWCOUNT`

- `references/select-joins-cte.md`
  - `SELECT`, joins, CTEs, subqueries, set operators, window functions

- `references/ddl-drl.md`
  - Tables, views, identity, constraints, permissions, Unity Catalog

- `references/control-flow-temp.md`
  - Variables, temp tables, cursors, transactions, error handling, dynamic SQL

- `references/conversion-rules.md`
  - Mandatory rules, semantic differences, data type mapping, anti-patterns

- `references/validation-and-security.md`
  - Validation gates, differential testing, security rules

- `references/deployment-and-operations.md`
  - Databricks Asset Bundles, jobs, monitoring, rollback, optimization