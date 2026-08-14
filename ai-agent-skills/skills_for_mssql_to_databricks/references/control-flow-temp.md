# Control Flow, Temp Objects, Transactions, Error Handling, Cursors, Dynamic SQL

This reference converts procedural MSSQL constructs into Databricks PySpark patterns.

---

## 1. Variables

### MSSQL

```sql
DECLARE @CustomerId INT = 1001;
DECLARE @StartDate DATE = '2026-01-01';

SELECT *
FROM dbo.[Order]
WHERE CustomerId = @CustomerId
  AND OrderDate >= @StartDate;
```

### PySpark

```python
params = {
    "customer_id": 1001,
    "start_date": "2026-01-01"
}

customer_id = params["customer_id"]
start_date = params["start_date"]

order_df = (
    spark.table("analytics.sales.order")
         .filter(F.col("CustomerId") == F.lit(customer_id))
         .filter(F.col("OrderDate") >= F.lit(start_date))
)
```

Or create a parameter view:

```python
spark.createDataFrame(
    [(customer_id, start_date)],
    ["CustomerId", "StartDate"]
).createOrReplaceTempView("proc_params")
```

Then:

```sql
SELECT o.*
FROM analytics.sales.order o
JOIN proc_params p
  ON o.CustomerId = p.CustomerId
 AND o.OrderDate >= p.StartDate;
```

This avoids direct value interpolation.

---

## 2. SET Variable from Query

### MSSQL

```sql
DECLARE @CustomerCount INT;

SELECT @CustomerCount = COUNT(*)
FROM dbo.Customer;
```

### PySpark

```python
customer_count = spark.table("analytics.sales.customer").count()
```

Or:

```python
customer_count = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM analytics.sales.customer
""").collect()[0]["cnt"]
```

---

## 3. IF / ELSE

### MSSQL

```sql
IF @CustomerCount > 0
BEGIN
    INSERT INTO dbo.AuditLog (Message)
    VALUES ('Customers exist');
END
ELSE
BEGIN
    INSERT INTO dbo.AuditLog (Message)
    VALUES ('No customers');
END
```

### PySpark

```python
if customer_count > 0:
    spark.sql("""
        INSERT INTO analytics.audit.audit_log (Message)
        VALUES ('Customers exist')
    """)
else:
    spark.sql("""
        INSERT INTO analytics.audit.audit_log (Message)
        VALUES ('No customers')
    """)
```

---

## 4. WHILE

### MSSQL

```sql
DECLARE @i INT = 1;

WHILE @i <= 10
BEGIN
    INSERT INTO dbo.BatchLog (BatchNumber)
    VALUES (@i);

    SET @i = @i + 1;
END
```

### PySpark

```python
for i in range(1, 11):
    spark.sql(f"""
        INSERT INTO analytics.audit.batch_log (BatchNumber)
        VALUES ({i})
    """)
```

Better set-based version:

```python
batch_df = spark.range(1, 11).withColumnRenamed("id", "BatchNumber")
batch_df.write.mode("append").saveAsTable("analytics.audit.batch_log")
```

Always prefer set-based generation.

---

## 5. Temp Tables

### MSSQL

```sql
SELECT *
INTO #ActiveCustomer
FROM dbo.Customer
WHERE IsActive = 1;

SELECT *
FROM #ActiveCustomer;
```

### Spark SQL Temporary View

```python
spark.sql("""
    CREATE OR REPLACE TEMP VIEW active_customer AS
    SELECT *
    FROM analytics.sales.customer
    WHERE IsActive = TRUE
""")

spark.sql("""
    SELECT *
    FROM active_customer
""")
```

Temporary views are session-scoped and notebook/job-scoped depending on usage.

---

## 6. Global Temp Tables

### MSSQL

```sql
SELECT *
INTO ##GlobalCustomer
FROM dbo.Customer;
```

### Databricks Global Temp View

```python
spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW global_customer AS
    SELECT *
    FROM analytics.sales.customer
""")
```

Query with:

```sql
SELECT * FROM global_temp.global_customer;
```

Use global temp views carefully. They can create shared-state dependencies.

---

## 7. Table Variables

### MSSQL

```sql
DECLARE @CustomerIds TABLE (
    CustomerId INT
);

INSERT INTO @CustomerIds (CustomerId)
SELECT CustomerId
FROM dbo.Customer
WHERE IsActive = 1;
```

### Spark

Use temporary view or DataFrame:

```python
customer_ids_df = (
    spark.table("analytics.sales.customer")
         .filter("IsActive = TRUE")
         .select("CustomerId")
)

customer_ids_df.createOrReplaceTempView("customer_ids")
```

---

## 8. TRY/CATCH

### MSSQL

```sql
BEGIN TRY
    INSERT INTO dbo.Customer (CustomerId, CustomerName)
    VALUES (1, 'Test');
END TRY
BEGIN CATCH
    INSERT INTO dbo.ErrorLog (ErrorMessage)
    VALUES (ERROR_MESSAGE());

    THROW;
END CATCH
```

### PySpark

```python
def run_insert_customer(spark):
    try:
        spark.sql("""
            INSERT INTO analytics.sales.customer (CustomerId, CustomerName)
            VALUES (1, 'Test')
        """)

        return {"status": "success"}

    except Exception as exc:
        spark.sql(f"""
            INSERT INTO analytics.audit.error_log (ErrorMessage)
            VALUES ('{str(exc).replace("'", "''")}')
        """)

        raise
```

Better:

```python
def run_insert_customer(spark):
    try:
        spark.sql("""
            INSERT INTO analytics.sales.customer (CustomerId, CustomerName)
            VALUES (1, 'Test')
        """)

        return {"status": "success"}

    except Exception as exc:
        error_df = spark.createDataFrame([(str(exc))], ["ErrorMessage"])
        error_df.write.mode("append").saveAsTable("analytics.audit.error_log")
        raise
```

---

## 9. THROW / RAISERROR

### MSSQL

```sql
RAISERROR('Customer not found', 16, 1);
THROW;
```

### PySpark

```python
raise ValueError("Customer not found")
```

Use custom exceptions for domain errors:

```python
class MigrationValidationError(Exception):
    pass

raise MigrationValidationError("Customer not found")
```

---

## 10. Transactions

### MSSQL

```sql
BEGIN TRANSACTION;

BEGIN TRY
    UPDATE dbo.Customer
    SET IsActive = 0
    WHERE CustomerId = @CustomerId;

    INSERT INTO dbo.AuditLog (Message)
    VALUES ('Customer deactivated');

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;

    THROW;
END CATCH
```

### Databricks Pattern

Delta provides ACID guarantees per operation, but Databricks does not provide the same multi-statement transaction scope as SQL Server.

Recommended redesign:

```python
def run_deactivate_customer(spark, params, dry_run=True):
    customer_id = params["customer_id"]

    if dry_run:
        return {
            "status": "dry_run",
            "message": "No changes applied"
        }

    try:
        spark.sql(f"""
            UPDATE analytics.sales.customer
            SET IsActive = FALSE
            WHERE CustomerId = {int(customer_id)}
        """)

        spark.sql(f"""
            INSERT INTO analytics.audit.audit_log (Message)
            VALUES ('Customer deactivated')
        """)

        return {"status": "success"}

    except Exception:
        # Compensation logic or rethrow for orchestrator retry
        raise
```

For multi-table atomicity, use:

- Idempotent tasks
- Checkpoints
- Audit markers
- Compensation logic
- Workflow failure handling
- Snapshot/restore strategy if required

---

## 11. Cursors

### MSSQL

```sql
DECLARE @CustomerId INT;

DECLARE customer_cursor CURSOR FOR
SELECT CustomerId
FROM dbo.Customer
WHERE IsActive = 1;

OPEN customer_cursor;

FETCH NEXT FROM customer_cursor INTO @CustomerId;

WHILE @@FETCH_STATUS = 0
BEGIN
    UPDATE dbo.CustomerSummary
    SET TotalOrders = 0
    WHERE CustomerId = @CustomerId;

    FETCH NEXT FROM customer_cursor INTO @CustomerId;
END

CLOSE customer_cursor;
DEALLOCATE customer_cursor;
```

### Spark Set-Based Replacement

```sql
MERGE INTO analytics.sales.customer_summary AS tgt
USING (
    SELECT CustomerId
    FROM analytics.sales.customer
    WHERE IsActive = TRUE
) AS src
    ON tgt.CustomerId = src.CustomerId
WHEN MATCHED THEN
    UPDATE SET TotalOrders = 0;
```

If iteration is absolutely required:

```python
customer_ids = [
    row.CustomerId
    for row in spark.table("analytics.sales.customer")
                    .filter("IsActive = TRUE")
                    .select("CustomerId")
                    .collect()
]

for customer_id in customer_ids:
    spark.sql(f"""
        UPDATE analytics.sales.customer_summary
        SET TotalOrders = 0
        WHERE CustomerId = {int(customer_id)}
    """)
```

Only do this when:

- Dataset is small.
- Side effects require row-by-row calls.
- No set-based alternative exists.

Always document the reason.

---

## 12. Dynamic SQL

### MSSQL

```sql
DECLARE @TableName NVARCHAR(200) = N'dbo.Customer';
DECLARE @SQL NVARCHAR(MAX);

SET @SQL = N'SELECT COUNT(*) FROM ' + @TableName;

EXEC sp_executesql @SQL;
```

### Safe PySpark Pattern

```python
ALLOWED_TABLES = {
    "customer": "analytics.sales.customer",
    "order": "analytics.sales.order"
}

def run_dynamic_count(spark, logical_table_name):
    table_name = ALLOWED_TABLES.get(logical_table_name)

    if not table_name:
        raise ValueError(f"Table not allowed: {logical_table_name}")

    return spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {table_name}
    """).collect()[0]["cnt"]
```

Never do this with untrusted input:

```python
spark.sql(f"SELECT * FROM {user_input}")
```

Use:

- Whitelists
- Identifier validation
- Parameter views for values
- Avoid dynamic values inside SQL strings

---

## 13. sp_executesql with Parameters

### MSSQL

```sql
EXEC sp_executesql
    N'SELECT * FROM dbo.Customer WHERE CustomerId = @CustomerId',
    N'@CustomerId INT',
    @CustomerId = 1001;
```

### PySpark Parameter View

```python
spark.createDataFrame([(1001,)], ["CustomerId"]) \
     .createOrReplaceTempView("proc_params")

spark.sql("""
    SELECT c.*
    FROM analytics.sales.customer c
    JOIN proc_params p
      ON c.CustomerId = p.CustomerId
""")
```

If runtime supports SQL parameter binding, use it. Otherwise, parameter views are safer than string interpolation.

---

## 14. Nested Stored Procedure Calls

### MSSQL

```sql
EXEC dbo.LoadCustomer;
EXEC dbo.LoadOrder;
```

### Databricks

Use Python functions:

```python
def run_load_customer(spark, params=None, table_map=None, dry_run=True):
    ...

def run_load_order(spark, params=None, table_map=None, dry_run=True):
    ...

def run_master_load(spark, params=None, table_map=None, dry_run=True):
    run_load_customer(spark, params, table_map, dry_run)
    run_load_order(spark, params, table_map, dry_run)
```

Or orchestrate as Databricks Workflow tasks.

---

## 15. RETURN Codes and Output Parameters

### MSSQL

```sql
CREATE PROCEDURE dbo.GetCustomerCount
    @CustomerCount INT OUTPUT
AS
BEGIN
    SELECT @CustomerCount = COUNT(*)
    FROM dbo.Customer;

    RETURN 0;
END
```

### PySpark

```python
def run_get_customer_count(spark, params=None, table_map=None, dry_run=True):
    customer_count = spark.table("analytics.sales.customer").count()

    return {
        "status": "success",
        "return_code": 0,
        "output": {
            "customer_count": customer_count
        }
    }
```

---

## 16. SET Options

MSSQL options like:

```sql
SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
```

Usually have no direct equivalent.

Handle semantics explicitly:

- Null comparisons
- Transaction failure behavior
- Row count messaging
- ANSI padding and string behavior

---

## 17. DBCC and Maintenance

| MSSQL | Databricks |
|---|---|
| `DBCC CHECKDB` | Delta table verification / data quality checks |
| `UPDATE STATISTICS` | `ANALYZE TABLE` / automatic statistics |
| Index rebuild | `OPTIMIZE`, clustering, compaction |
| Log management | Delta/VACUUM with retention policies |
| Shrink | Usually avoid; use `VACUUM` carefully |

Example:

```sql
OPTIMIZE analytics.sales.customer;
VACUUM analytics.sales.customer RETAIN 168 HOURS;
```

Use `VACUUM` carefully and only with governance approval.

---

## Procedural Conversion Checklist

- [ ] Replace temp tables with temp views or DataFrames.
- [ ] Replace cursors with set-based logic.
- [ ] Replace `TRY/CATCH` with Python `try/except`.
- [ ] Replace `THROW/RAISERROR` with Python exceptions.
- [ ] Replace transactions with idempotent task design.
- [ ] Replace dynamic SQL with whitelisted builders.
- [ ] Replace `EXEC` with Python function calls or workflow tasks.
- [ ] Replace output parameters with return dictionaries.
- [ ] Document any required manual intervention.