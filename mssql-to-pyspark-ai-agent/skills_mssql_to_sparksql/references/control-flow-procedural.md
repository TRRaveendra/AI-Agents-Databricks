# Control Flow and Procedural Constructs

This reference covers T-SQL procedural elements that have no direct SQL equivalent in Spark:
variables, IF/WHILE, TRY/CATCH, cursors, temp tables, dynamic SQL, and transactions.

## Variables

### Declaring and Setting Variables

**T-SQL:**
```sql
DECLARE @MinAmount DECIMAL(18,2) = 0.00;
DECLARE @EndDate DATETIME;
SET @EndDate = GETDATE();
```

**PySpark:**
```python
min_amount = 0.00
end_date = None

from datetime import datetime
if end_date is None:
    end_date = datetime.now()
```

### @@ROWCOUNT

**T-SQL:**
```sql
INSERT INTO t SELECT * FROM s;
SET @RowCount = @@ROWCOUNT;
```

**PySpark:**
```python
spark.sql("INSERT INTO t SELECT * FROM s")
row_count = spark.table("t").count()
```

## IF / ELSE

**T-SQL:**
```sql
IF @Year IS NULL
    SET @Year = 2024;

IF EXISTS (SELECT 1 FROM dbo.Customer WHERE Status = 'Active')
    INSERT INTO dbo.AuditLog VALUES ('Active', GETDATE());
```

**PySpark:**
```python
if year is None:
    year = 2024

if spark.table("gold.customer").filter("status = 'Active'").count() > 0:
    spark.sql("""INSERT INTO gold.audit_log VALUES ('Active', current_timestamp())""")
```

## WHILE Loops

**T-SQL:**
```sql
DECLARE @Counter INT = 0;
WHILE @Counter < 10
BEGIN
    INSERT INTO dbo.Numbers VALUES (@Counter);
    SET @Counter = @Counter + 1;
END
```

**PySpark:**
```python
counter = 0
while counter < 10:
    spark.sql(f"INSERT INTO gold.numbers VALUES ({counter})")
    counter += 1

# Better: Use set-based operations
spark.range(10).write.saveAsTable("gold.numbers")
```

**Critical:** WHILE loops often indicate row-by-row processing that should be rewritten 
as set-based operations.

## TRY / CATCH

**T-SQL:**
```sql
BEGIN TRY
    MERGE INTO fact.Orders ... ;
    INSERT INTO audit.Log VALUES ('Success', @@ROWCOUNT, GETDATE());
END TRY
BEGIN CATCH
    INSERT INTO audit.Log VALUES (ERROR_MESSAGE(), 0, GETDATE());
    THROW;
END CATCH
```

**PySpark:**
```python
try:
    spark.sql("""MERGE INTO fact.orders ...""")
    rows = spark.table("fact.orders").count()
    spark.sql(f"""INSERT INTO audit.log VALUES ('Success', {rows}, current_timestamp())""")
except Exception as e:
    error_msg = str(e)[:500]
    spark.sql(f"""INSERT INTO audit.log VALUES ('{error_msg}', 0, current_timestamp())""")
    raise
```

### Error Functions

| T-SQL | PySpark |
|---|---|
| ERROR_MESSAGE() | str(e) |
| ERROR_NUMBER() | type(e).__name__ |
| THROW | raise |
| RAISERROR | raise Exception("msg") |

## Temp Tables

### Local Temp Tables (#TempTable)

**T-SQL:**
```sql
SELECT CustomerID, SUM(Amount) AS TotalAmount
INTO #CustomerTotals
FROM dbo.Orders
GROUP BY CustomerID;

SELECT * FROM #CustomerTotals WHERE TotalAmount > 1000;
```

**PySpark:**
```python
spark.sql("""
    CREATE OR REPLACE TEMP VIEW customer_totals AS
    SELECT customer_id, SUM(amount) AS total_amount
    FROM gold.orders
    GROUP BY customer_id
""")

df = spark.sql("SELECT * FROM customer_totals WHERE total_amount > 1000")
```

### Global Temp Tables (##TempTable)

**T-SQL:**
```sql
SELECT * INTO ##GlobalTemp FROM dbo.LargeTable;
```

**PySpark:**
```python
spark.sql("""
    CREATE OR REPLACE TABLE staging.global_temp AS
    SELECT * FROM gold.large_table
""")
```

### Table Variables (@TableName)

**T-SQL:**
```sql
DECLARE @CustomerIDs TABLE (CustomerID INT, CustomerName VARCHAR(200));
INSERT INTO @CustomerIDs SELECT CustomerID, CustomerName FROM dbo.Customer;
```

**PySpark:**
```python
spark.sql("""
    CREATE OR REPLACE TEMP VIEW customer_ids AS
    SELECT customer_id, customer_name FROM gold.customer
""")
```

## Cursors

**IMPORTANT:** Cursors are an anti-pattern (RBAR - Row By Agonizing Row). Always prefer 
set-based operations.

### Example: Cursor to Set-Based Rewrite

**T-SQL (ANTI-PATTERN):**
```sql
DECLARE @pid INT;
DECLARE inv_cursor CURSOR FOR
    SELECT ProductID FROM dbo.Inventory WHERE QuantityOnHand < 0;

OPEN inv_cursor;
FETCH NEXT FROM inv_cursor INTO @pid;

WHILE @@FETCH_STATUS = 0
BEGIN
    UPDATE dbo.Inventory SET QuantityOnHand = 0 WHERE ProductID = @pid;
    FETCH NEXT FROM inv_cursor INTO @pid;
END

CLOSE inv_cursor;
DEALLOCATE inv_cursor;
```

**PySpark (BEST PRACTICE):**
```python
# Single set-based UPDATE
spark.sql("""
    UPDATE gold.inventory
    SET quantity_on_hand = 0
    WHERE quantity_on_hand < 0
""")
```

**PySpark (If row-by-row truly needed - rare):**
```python
for row in spark.sql("SELECT product_id FROM gold.inventory WHERE quantity_on_hand < 0").collect():
    product_id = row["product_id"]
    spark.sql(f"UPDATE gold.inventory SET quantity_on_hand = 0 WHERE product_id = {product_id}")
```

## Dynamic SQL

### Basic Dynamic SQL

**T-SQL:**
```sql
DECLARE @TableName NVARCHAR(200) = 'dbo.Customer';
DECLARE @sql NVARCHAR(MAX) = N'SELECT * FROM ' + @TableName;
EXEC sp_executesql @sql;
```

**PySpark:**
```python
table_name = "gold.customer"
sql = f"SELECT * FROM {table_name}"
df = spark.sql(sql)
```

### Security: SQL Injection Prevention

**BAD:**
```python
user_input = dbutils.widgets.get("table_name")
spark.sql(f"SELECT * FROM {user_input}")  # DANGEROUS!
```

**GOOD:**
```python
allowed_tables = ["gold.customer", "gold.orders", "gold.products"]
user_input = dbutils.widgets.get("table_name")
if user_input in allowed_tables:
    spark.sql(f"SELECT * FROM {user_input}")
else:
    raise ValueError(f"Invalid table name: {user_input}")
```

## Transactions

### Critical Difference

**T-SQL:**
```sql
BEGIN TRANSACTION;
    UPDATE dbo.Account SET Balance = Balance - 100 WHERE AccountID = 1;
    UPDATE dbo.Account SET Balance = Balance + 100 WHERE AccountID = 2;
COMMIT TRANSACTION;
```

**Spark/Delta:** NO multi-statement transactions. Each statement is independently atomic.

**PySpark:**
```python
# These are TWO separate atomic transactions:
spark.sql("UPDATE gold.account SET balance = balance - 100 WHERE account_id = 1")
spark.sql("UPDATE gold.account SET balance = balance + 100 WHERE account_id = 2")
# If second fails, first is already committed!
```

### Pattern: Single MERGE (Preferred)

```python
spark.sql("""
    MERGE INTO gold.account t
    USING (
        SELECT 1 AS account_id, -100 AS amount
        UNION ALL
        SELECT 2 AS account_id, 100 AS amount
    ) s ON t.account_id = s.account_id
    WHEN MATCHED THEN UPDATE SET balance = t.balance + s.amount
""")
# Now atomic - single transaction
```

### Pattern: Idempotent + Re-runnable

```python
run_id = datetime.now().strftime('%Y%m%d_%H%M%S')

spark.sql(f"""INSERT INTO audit.batch_log VALUES ('{run_id}', 'started', current_timestamp())""")

try:
    spark.sql("""
        MERGE INTO gold.target t USING staging.source s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.col = s.col
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql(f"""
        UPDATE audit.batch_log SET status = 'completed', end_time = current_timestamp()
        WHERE run_id = '{run_id}'
    """)
except Exception as e:
    spark.sql(f"""
        UPDATE audit.batch_log SET status = 'failed', end_time = current_timestamp()
        WHERE run_id = '{run_id}'
    """)
    raise
```

### ROLLBACK Equivalent

**Option 1: Delta Time Travel**
```python
spark.sql("RESTORE TABLE gold.orders TO VERSION AS OF 42")
# Or by timestamp:
spark.sql("RESTORE TABLE gold.orders TO TIMESTAMP AS OF '2024-01-15 10:00:00'")
```

**Option 2: Delete + Re-load**
```python
spark.sql(f"DELETE FROM gold.orders WHERE batch_id = '{run_id}'")
```

## Best Practices Summary

1. **Variables:** Use Python variables for procedural logic
2. **Control Flow:** Use Python if/while, but prefer set-based SQL
3. **Error Handling:** Use try/except, preserve audit logging
4. **Temp Tables:** Use temp views or staging Delta tables
5. **Cursors:** Rewrite as set-based operations (almost always)
6. **Dynamic SQL:** Use f-strings, validate inputs
7. **Transactions:** Design idempotent single-statement operations

## Example: inventory_procedure.sql Migration

**Original (T-SQL):**
```sql
MERGE INTO dbo.Inventory AS tgt USING ... ;

DECLARE inv_cursor CURSOR FOR SELECT ProductID FROM dbo.Inventory WHERE QuantityOnHand < 0;
-- ... cursor cleanup logic ...
```

**Migrated (PySpark):**
```python
spark.sql("""
    MERGE INTO gold.inventory AS tgt
    USING (
        SELECT product_id, SUM(order_qty) AS sold_qty
        FROM gold.sales_order_detail
        GROUP BY product_id
    ) AS src ON tgt.product_id = src.product_id
    WHEN MATCHED THEN
        UPDATE SET 
            tgt.quantity_on_hand = GREATEST(tgt.quantity_on_hand - src.sold_qty, 0),
            tgt.last_updated = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (product_id, quantity_on_hand, last_updated)
        VALUES (src.product_id, 0, current_timestamp())
""")
# No cursor needed - GREATEST() clamps to 0
```

---

**Remember:** Preserve business logic while using idiomatic Databricks patterns.
