---
name: mssql-to-databricks-migration
description: Complete migration of MS SQL Server (T-SQL) stored procedures, scripts, and queries to Databricks PySpark and Spark SQL. Load this skill when migrating T-SQL code to Databricks, converting stored procedures to notebooks, or translating any T-SQL construct (DDL, DML, DQL, cursors, temp tables, dynamic SQL, transactions, procedural logic) to Spark equivalents.
---

# MSSQL to Databricks Migration Guide

## Overview

This skill provides a complete, production-ready framework for migrating MS SQL Server stored
procedures to Databricks. The migration converts T-SQL stored procedures into Databricks
notebooks that use **Spark SQL** for set-based operations and **PySpark** for procedural logic.

### Migration Philosophy

**Prefer Spark SQL for data operations, PySpark for orchestration:**
* Use Spark SQL (`%sql` cells or `spark.sql()`) for: SELECT, JOIN, MERGE, INSERT, UPDATE, DELETE,
  CTEs, window functions, aggregations — anything fundamentally set-based
* Use PySpark/Python for: parameters, variables, control flow (IF/WHILE), error handling
  (TRY/CATCH), dynamic SQL generation, orchestration between SQL statements

**Preserve business logic, not syntax:**
* Understand what the T-SQL accomplishes (e.g., SCD Type 2, incremental load, data quality checks)
* Use idiomatic Databricks patterns for the same outcome
* Don't do token-by-token translation — that produces brittle code

## How to Use This Skill

### Step 1: Analyze the Source Procedure

1. **Identify components:**
   * Parameters (`@StartDate DATETIME`, `@Year INT = 2024`)
   * Variables (`DECLARE @MinAmount DECIMAL(18,2) = 0.00`)
   * Temp tables (`#CustomerSales`, `@TableVariable`)
   * Control flow (`IF`, `WHILE`, `TRY/CATCH`)
   * DML statements (INSERT, UPDATE, DELETE, MERGE)
   * Cursors and dynamic SQL
   * Transaction boundaries (`BEGIN TRAN`, `COMMIT`, `ROLLBACK`)

2. **Classify each statement** using the reference guides

3. **Flag blockers** — features with no direct Spark equivalent (cursors doing row-by-row
   operations, sp_send_dbmail, linked server queries)

### Step 2: Load Reference Guides

Load only the reference files you need for the specific procedure. All references are in the
`references/` folder. Read the entire reference file to access detailed mappings and examples.

| Source T-SQL Component | Reference File |
|---|---|
| CREATE/ALTER/DROP TABLE, indexes, constraints | [references/ddl-mapping.md](references/ddl-mapping.md) |
| INSERT, UPDATE, DELETE, MERGE | [references/dml-mapping.md](references/dml-mapping.md) |
| SELECT, JOINs, CTEs, UNION, window functions | [references/dql-select-joins.md](references/dql-select-joins.md) |
| Variables, IF/WHILE, TRY/CATCH, cursors, temp tables, dynamic SQL | [references/control-flow-procedural.md](references/control-flow-procedural.md) |
| Built-in functions (date, string, conversion, aggregation) | [references/functions-mapping.md](references/functions-mapping.md) |
| Data type conversions (money, bit, datetime, varchar) | [references/data-type-mapping.md](references/data-type-mapping.md) |
| Connection patterns, JDBC, linked servers | [references/connection-mapping.md](references/connection-mapping.md) |
| SSIS components, medallion architecture patterns | [references/component-mapping.md](references/component-mapping.md) |
| Databricks design patterns and best practices | [references/databricks-patterns.md](references/databricks-patterns.md) |

### Step 3: Create the Databricks Notebook

**Output structure:** A complete Databricks notebook with:
* Parameter widgets for T-SQL input parameters
* Python variables for T-SQL declared variables
* Temp views for T-SQL temp tables
* Spark SQL cells for data operations
* Python cells for control flow and error handling
* Audit logging and error handling

## Quick Reference: Common Conversions

**⚠️ SECURITY WARNING:** Never use f-strings or string concatenation with user input in SQL queries. See the "Security Best Practices" section below for safe patterns.

### Data Types
* `money`, `decimal(p,s)` → `DECIMAL(p,s)` (NEVER `DOUBLE` — breaks financial reconciliation)
* `bit` → `BOOLEAN`
* `datetime`, `datetime2` → `TIMESTAMP`
* `varchar(n)`, `nvarchar(n)` → `STRING`
* `int` → `INT`, `bigint` → `BIGINT`

### Functions
| T-SQL | Spark SQL | Notes |
|---|---|---|
| `GETDATE()` | `current_timestamp()` | ⚠️ Use Spark function, NOT Python `datetime.now()` |
| `DATEPART(year, col)` | `year(col)` | Also: `month()`, `day()`, `hour()` |
| `DATEDIFF(day, a, b)` | `datediff(b, a)` | Note: reversed parameter order |
| `ISNULL(col, 'x')` | `coalesce(col, 'x')` | |
| `LEN(col)` | `length(col)` | |
| `STRING_AGG(col, ',')` | `array_join(collect_list(col), ',')` | Requires groupBy |

### Transactions
* **No multi-statement transactions** in Spark/Delta
* Each MERGE/INSERT/UPDATE/DELETE is independently atomic
* Design idempotent, re-runnable statements instead
* Document this explicitly in migrated code comments

### Temp Tables
* `#TempTable` → `createOrReplaceTempView("temp_table")` (session-scoped)
* `##GlobalTemp` → Delta table in `staging` schema (if must survive restarts)
* `@TableVariable` → `createOrReplaceTempView()` or Python list/dict

### Parameters and Variables
* T-SQL parameters (`@StartDate DATETIME`) → `dbutils.widgets.text("start_date", "")`
* T-SQL variables (`DECLARE @x INT = 0`) → Python variables (`x = 0`)

## Migration Workflow

### Phase 1: Discovery
1. Inventory all stored procedures
2. Identify dependencies (what calls what)
3. Classify by complexity (Simple/Moderate/Complex)

### Phase 2: Pattern Mapping
1. Identify data flow patterns (incremental load, SCD Type 2, CDC, etc.)
2. Map to Databricks patterns (MERGE, streaming, DLT)

### Phase 3: Conversion
1. Create notebook for each procedure
2. Migrate in dependency order (bottom-up)
3. Test with sample data

### Phase 4: Orchestration
1. Create Databricks Workflows (Jobs)
2. Define task dependencies (DAG)
3. Set up schedules and notifications

### Phase 5: Validation
1. Parallel run and compare outputs
2. Performance tuning (OPTIMIZE, Z-ORDER)
3. UAT and signoff

## CRITICAL: Security Best Practices for MSSQL Conversion

### 🔴 SQL Injection Prevention (MANDATORY)

**The #1 security vulnerability in MSSQL-to-Databricks conversions is SQL injection through string interpolation.**

#### ❌ DANGEROUS - NEVER DO THIS:

```python
# DANGEROUS - SQL injection vulnerability
customer_id = dbutils.widgets.get("CustomerID")
start_date = dbutils.widgets.get("StartDate")
end_date = dbutils.widgets.get("EndDate")
min_amount = dbutils.widgets.get("MinAmount")

# BAD - Direct f-string interpolation with user input
temp_orders_df = spark.sql(f"""
    SELECT * 
    FROM Orders
    WHERE CustomerID = {customer_id}
      AND OrderDate >= '{start_date}'
      AND OrderDate <= '{end_date}'
      AND TotalAmount >= {min_amount}
""")

# BAD - String concatenation
sql = "UPDATE Orders SET Status = 'Processed' WHERE OrderDate = '" + processed_date + "'"
spark.sql(sql)
```

#### ✅ SAFE - ALWAYS DO THIS:

**Method 1: DataFrame API with col() comparisons (PREFERRED)**

```python
from pyspark.sql.functions import col, lit

# Safe - DataFrame API prevents SQL injection
customer_id = int(dbutils.widgets.get("CustomerID"))
start_date = dbutils.widgets.get("StartDate")
end_date = dbutils.widgets.get("EndDate")
min_amount = float(dbutils.widgets.get("MinAmount"))

temp_orders_df = spark.table("Orders").filter(
    (col("CustomerID") == customer_id) &
    (col("OrderDate") >= start_date) &
    ((lit(end_date) == "") | (col("OrderDate") <= end_date)) &
    (col("TotalAmount") >= min_amount)
)
```

**Method 2: Typed DataFrames with Temp Views**

```python
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType
from pyspark.sql.functions import current_timestamp

# Create strongly-typed DataFrame
source_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("OrderCount", LongType(), False),
    StructField("TotalAmount", DoubleType(), False)
])

source_data = [(customer_id, total_orders, total_amount)]
source_df = spark.createDataFrame(source_data, source_schema)
source_df = source_df.withColumn("ProcessedDate", current_timestamp())
source_df.createOrReplaceTempView("MergeSource")

# Safe - No string interpolation of user data
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

**Method 3: Safe Parameterized Updates**

```python
# Collect order IDs using DataFrame operations
order_ids = [row.OrderID for row in temp_orders_df.select("OrderID").collect()]

if order_ids:
    # Create temp view with IDs to update
    update_ids_df = spark.createDataFrame(
        [(oid,) for oid in order_ids], 
        ["OrderID"]
    )
    update_ids_df.createOrReplaceTempView("UpdateOrderIDs")
    
    # Safe parameterized update via MERGE
    spark.sql("""
        MERGE INTO Orders AS target
        USING UpdateOrderIDs AS source
        ON target.OrderID = source.OrderID
        WHEN MATCHED THEN UPDATE SET
            Status = 'Processed',
            ProcessedDate = current_timestamp()
    """)
```

### 🔴 Date/Timestamp Handling (MANDATORY)

**Never use Python datetime objects in Spark SQL - they cause type mismatches and wrong results.**

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

# Method 1: Use Spark SQL native functions in DataFrame operations
df = df.withColumn("processed_date", current_timestamp())
df = df.withColumn("report_date", current_date())

# Method 2: Use Spark functions in SQL queries
spark.sql("""
    UPDATE orders 
    SET processed_date = current_timestamp(),
        report_date = current_date()
    WHERE ...
""")

# Method 3: For specific dates, use to_date with lit
df = df.withColumn("cutoff_date", to_date(lit("2024-01-01")))
```

### 🔴 Type Safety and Validation

**Always validate and cast user input to prevent type errors and injection:**

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# Type validation with error handling
try:
    customer_id = int(dbutils.widgets.get("CustomerID"))
    min_amount = float(dbutils.widgets.get("MinAmount"))
except ValueError as e:
    raise ValueError(f"Invalid parameter type: {e}")

# Range validation
if min_amount < 0:
    raise ValueError("MinAmount must be non-negative")

# Define explicit schemas
result_schema = StructType([
    StructField("CustomerID", IntegerType(), False),
    StructField("ProcessedOrders", LongType(), False),
    StructField("ProcessedAmount", DoubleType(), False),
    StructField("ProcessedDate", TimestampType(), False)
])

result_df = spark.createDataFrame(result_data, result_schema)
```

### Seven Mandatory Security Rules

1. **NEVER use f-strings or + concatenation with user input in SQL queries**
2. **Use DataFrame API `.filter()` instead of WHERE clauses with string interpolation**
3. **Use `current_timestamp()` and `current_date()` instead of Python datetime objects**
4. **Create typed DataFrames and temp views for complex parameterized SQL**
5. **Always validate and cast user input to expected types**
6. **Use `lit()` for literal values in DataFrame operations**
7. **Prefer DataFrame API over spark.sql() when dealing with user input**

---

## Common Migration Patterns

### Pattern: Cursor → Set-Based Rewrite
Cursors doing row-by-row processing should be rewritten as single set-based operations.
Prefer set-based MERGE/UPDATE/DELETE over iterative loops whenever possible.

**Example:**
```sql
-- T-SQL: Cursor updating row-by-row
DECLARE @ID INT;
DECLARE cursor1 CURSOR FOR SELECT CustomerID FROM Customer;
OPEN cursor1;
FETCH NEXT FROM cursor1 INTO @ID;
WHILE @@FETCH_STATUS = 0 BEGIN
    UPDATE Orders SET Status = 'Processed' WHERE CustomerID = @ID;
    FETCH NEXT FROM cursor1 INTO @ID;
END
CLOSE cursor1;
```

```sql
-- Spark SQL: Single set-based operation
UPDATE Orders SET Status = 'Processed' 
WHERE CustomerID IN (SELECT CustomerID FROM Customer);
```

### Pattern: Dynamic SQL (SECURE)
T-SQL dynamic SQL with `sp_executesql` → **Validated identifiers + DataFrame API**

**Example:**
```sql
-- T-SQL
DECLARE @sql NVARCHAR(MAX);
SET @sql = N'SELECT * FROM ' + @TableName + ' WHERE Year = ' + CAST(@Year AS VARCHAR);
EXEC sp_executesql @sql;
```

```python
# PySpark - SECURE approach
import re

def validate_identifier(name: str) -> str:
    """Whitelist validation for table/column names"""
    if not re.match(r'^[a-zA-Z0-9_\.]+

### Pattern: Temp Tables
`SELECT ... INTO #Temp` → `CREATE OR REPLACE TEMP VIEW` or DataFrame operations

**Example:**
```sql
-- T-SQL
SELECT * INTO #TempOrders FROM Orders WHERE OrderDate >= @StartDate;
```

```python
# PySpark - Option 1: Temp View
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW temp_orders AS
    SELECT * FROM Orders WHERE OrderDate >= '{start_date}'
""")

# Option 2: DataFrame
df_temp = spark.table("Orders").filter(f"OrderDate >= '{start_date}'")
```

## What This Skill Does NOT Cover

* **Linked Server queries** — replace with JDBC connections or Delta Sharing
* **CLR procedures** — rewrite in PySpark/Python
* **SQL Mail** (sp_send_dbmail) — use Databricks Workflow notifications
* **Service Broker** — redesign as Delta Live Tables or streaming
* **Full-text search** — use external search engine or LIKE patterns
* **SQL Agent Jobs** — replace with Databricks Workflows

## When to Use Each Reference

Use the reference table above to determine which files to load. For comprehensive migrations:

1. **Start with data types** (`data-type-mapping.md`) to understand type conversions
2. **Load DDL** (`ddl-mapping.md`) for table structure migrations
3. **Load DML** (`dml-mapping.md`) for INSERT/UPDATE/DELETE/MERGE patterns
4. **Load DQL** (`dql-select-joins.md`) for complex SELECT queries
5. **Load control flow** (`control-flow-procedural.md`) for procedural logic
6. **Load functions** (`functions-mapping.md`) for specific function conversions

---

**Ready to migrate? Load the specific reference files you need from the `references/` folder.**
, name):
        raise ValueError(f"Invalid identifier: {name}")
    return name

# Validate table name (prevent SQL injection)
table_name = validate_identifier(dbutils.widgets.get("table_name"))
year = int(dbutils.widgets.get("year"))  # Cast to int

# Safe - use DataFrame API for filtering
df = spark.table(table_name).filter(col("Year") == year)

# Alternative: If you must use spark.sql, validate identifiers first
table_name_safe = validate_identifier(table_name)
df = spark.sql(f"SELECT * FROM {table_name_safe}").filter(col("Year") == year)
```

### Pattern: Temp Tables (SECURE)
`SELECT ... INTO #Temp` → `CREATE OR REPLACE TEMP VIEW` or DataFrame operations

**Example:**
```sql
-- T-SQL
SELECT * INTO #TempOrders FROM Orders WHERE OrderDate >= @StartDate;
```

```python
# PySpark - SECURE Option 1: DataFrame API (PREFERRED)
start_date = dbutils.widgets.get("start_date")

df_temp = spark.table("Orders").filter(col("OrderDate") >= start_date)
df_temp.createOrReplaceTempView("temp_orders")

# SECURE Option 2: Temp View with safe SQL (no user input)
spark.sql("""
    CREATE OR REPLACE TEMP VIEW temp_orders AS
    SELECT * FROM Orders WHERE OrderDate >= current_date()
""")

# ❌ INSECURE - DO NOT DO THIS:
# df_temp = spark.table("Orders").filter(f"OrderDate >= '{start_date}'")
# spark.sql(f"CREATE OR REPLACE TEMP VIEW temp_orders AS SELECT * FROM Orders WHERE OrderDate >= '{start_date}'")
```

## What This Skill Does NOT Cover

* **Linked Server queries** — replace with JDBC connections or Delta Sharing
* **CLR procedures** — rewrite in PySpark/Python
* **SQL Mail** (sp_send_dbmail) — use Databricks Workflow notifications
* **Service Broker** — redesign as Delta Live Tables or streaming
* **Full-text search** — use external search engine or LIKE patterns
* **SQL Agent Jobs** — replace with Databricks Workflows

## When to Use Each Reference

Use the reference table above to determine which files to load. For comprehensive migrations:

1. **Start with data types** (`data-type-mapping.md`) to understand type conversions
2. **Load DDL** (`ddl-mapping.md`) for table structure migrations
3. **Load DML** (`dml-mapping.md`) for INSERT/UPDATE/DELETE/MERGE patterns
4. **Load DQL** (`dql-select-joins.md`) for complex SELECT queries
5. **Load control flow** (`control-flow-procedural.md`) for procedural logic
6. **Load functions** (`functions-mapping.md`) for specific function conversions

---

**Ready to migrate? Load the specific reference files you need from the `references/` folder.**
