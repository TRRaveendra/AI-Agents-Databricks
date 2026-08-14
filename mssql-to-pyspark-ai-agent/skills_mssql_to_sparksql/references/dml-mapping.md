# DML Mapping — INSERT / UPDATE / DELETE / MERGE

## INSERT

### Literal values
```sql
-- T-SQL
INSERT INTO dbo.Region (RegionCode, RegionName) VALUES ('APAC','Asia Pacific');
```
```sql
-- Spark SQL — identical
INSERT INTO gold.region (region_code, region_name) VALUES ('APAC','Asia Pacific');
```

### INSERT ... SELECT
```sql
-- T-SQL
INSERT INTO dbo.CustomerArchive (CustomerID, CustomerName)
SELECT CustomerID, CustomerName FROM dbo.Customer WHERE IsActive = 0;
```
```sql
-- Spark SQL — identical syntax
INSERT INTO gold.customer_archive (customer_id, customer_name)
SELECT customer_id, customer_name FROM gold.customer WHERE is_active = false;
```
Or in PySpark, when the source is a DataFrame rather than a table:
```python
df.write.format("delta").mode("append").saveAsTable("gold.customer_archive")
# or, to also match the explicit column list behavior of T-SQL INSERT:
df.select("customer_id", "customer_name").write.format("delta").mode("append") \
  .saveAsTable("gold.customer_archive")
```

### INSERT with IDENTITY_INSERT / explicit keys
T-SQL's `SET IDENTITY_INSERT dbo.X ON` has no Spark equivalent (identity columns are
generated, not both generated-and-overridable in the same session in most configs) — if you
must insert explicit surrogate keys, either write directly to a non-identity column, or drop
identity generation for the migrated table and manage keys entirely in code (see
ddl-mapping.md "Sequences / IDENTITY").

### Multi-row VALUES
```sql
INSERT INTO t (a,b) VALUES (1,'x'), (2,'y'), (3,'z');
```
```sql
-- identical in Spark SQL
INSERT INTO t (a,b) VALUES (1,'x'), (2,'y'), (3,'z');
```

## UPDATE

### Simple UPDATE
```sql
UPDATE dbo.Customer SET IsActive = 0 WHERE LastOrderDate < '2023-01-01';
```
```sql
-- Delta supports UPDATE directly
UPDATE gold.customer SET is_active = false WHERE last_order_date < '2023-01-01';
```

### UPDATE ... FROM ... JOIN (the tricky one)
```sql
-- T-SQL
UPDATE c
SET c.CreditLimit = t.NewLimit, c.ModifiedDate = GETDATE()
FROM dbo.Customer c
INNER JOIN staging.CreditLimitChanges t ON c.CustomerID = t.CustomerID
WHERE t.EffectiveDate <= GETDATE();
```
Spark SQL's `UPDATE` does **not** support a `FROM`/join clause directly. Use one of:

**Option A — correlated subquery (works for 1:1 lookups):**
```sql
UPDATE gold.customer
SET credit_limit = (
        SELECT t.new_limit FROM staging.credit_limit_changes t
        WHERE t.customer_id = gold.customer.customer_id AND t.effective_date <= current_date()
    ),
    modified_date = current_timestamp()
WHERE EXISTS (
    SELECT 1 FROM staging.credit_limit_changes t
    WHERE t.customer_id = gold.customer.customer_id AND t.effective_date <= current_date()
);
```

**Option B — MERGE (preferred, especially for >1 matching column or when it's cleaner):**
```sql
MERGE INTO gold.customer AS target
USING (
    SELECT customer_id, new_limit FROM staging.credit_limit_changes WHERE effective_date <= current_date()
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    target.credit_limit = source.new_limit,
    target.modified_date = current_timestamp()
```
**Rule of thumb: any T-SQL `UPDATE ... FROM <join>` should become a Delta `MERGE` with only
a `WHEN MATCHED THEN UPDATE` clause** (no `WHEN NOT MATCHED`) — it's more readable and
correct than the correlated-subquery form once more than one join column is involved.

## DELETE

### Simple DELETE
```sql
DELETE FROM dbo.StagingOrders WHERE ProcessedFlag = 1;
```
```sql
DELETE FROM staging.orders WHERE processed_flag = true;   -- identical
```

### DELETE ... FROM ... JOIN
```sql
-- T-SQL
DELETE s
FROM staging.Orders s
INNER JOIN dbo.CancelledOrders c ON s.OrderID = c.OrderID;
```
```sql
-- Spark SQL: use EXISTS, not a join in the DELETE clause
DELETE FROM staging.orders s
WHERE EXISTS (
    SELECT 1 FROM gold.cancelled_orders c WHERE c.order_id = s.order_id
);
```
For anti-joins ("delete rows NOT in another table"), use `NOT EXISTS`:
```sql
DELETE FROM staging.orders s
WHERE NOT EXISTS (SELECT 1 FROM gold.customer c WHERE c.customer_id = s.customer_id);
```

## MERGE (the most important pattern for T-SQL ELT procedures)

T-SQL and Spark SQL `MERGE` syntax are close cousins — this usually ports almost directly.

```sql
-- T-SQL
MERGE fact.Orders AS target
USING (
    SELECT s.OrderID, dc.CustomerKey, s.Status, s.TotalAmount
    FROM staging.Orders s
    INNER JOIN dim.Customer dc ON s.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
) AS source
ON target.OrderID = source.OrderID
WHEN MATCHED AND target.Status != source.Status THEN
    UPDATE SET target.Status = source.Status, target.TotalAmount = source.TotalAmount
WHEN NOT MATCHED THEN
    INSERT (OrderID, CustomerKey, Status, TotalAmount)
    VALUES (source.OrderID, source.CustomerKey, source.Status, source.TotalAmount)
WHEN NOT MATCHED BY SOURCE AND target.Status = 'Draft' THEN
    DELETE;
```

```python
# PySpark — build the source as a DataFrame or temp view, then MERGE
source_df = (
    spark.table("staging.orders").alias("s")
    .join(
        spark.table("dim.customer").filter("is_current = true").alias("dc"),
        on=[F.col("s.customer_id") == F.col("dc.customer_id")],
        how="inner",
    )
    .select("s.order_id", "dc.customer_key", "s.status", "s.total_amount")
)
source_df.createOrReplaceTempView("orders_source")

spark.sql("""
    MERGE INTO fact.orders AS target
    USING orders_source AS source
    ON target.order_id = source.order_id
    WHEN MATCHED AND target.status != source.status THEN
        UPDATE SET target.status = source.status, target.total_amount = source.total_amount
    WHEN NOT MATCHED THEN
        INSERT (order_id, customer_key, status, total_amount)
        VALUES (source.order_id, source.customer_key, source.status, source.total_amount)
    WHEN NOT MATCHED BY SOURCE AND target.status = 'Draft' THEN
        DELETE
""")
```
Or equivalently with the DeltaTable Python API:
```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "fact.orders")
(target.alias("target")
    .merge(source_df.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdate(
        condition="target.status != source.status",
        set={"status": "source.status", "total_amount": "source.total_amount"})
    .whenNotMatchedInsert(
        values={"order_id": "source.order_id", "customer_key": "source.customer_key",
                "status": "source.status", "total_amount": "source.total_amount"})
    .whenNotMatchedBySourceDelete(condition="target.status = 'Draft'")
    .execute())
```

MERGE clause mapping:

| T-SQL MERGE clause | Spark SQL / Delta MERGE |
|---|---|
| `WHEN MATCHED THEN UPDATE SET ...` | identical |
| `WHEN MATCHED AND <cond> THEN UPDATE ...` | identical |
| `WHEN NOT MATCHED THEN INSERT ...` | identical (Spark also accepts `WHEN NOT MATCHED BY TARGET`) |
| `WHEN NOT MATCHED BY SOURCE THEN DELETE/UPDATE` | supported since Delta Lake 2.3 / DBR 12.2+; confirm runtime version |
| `MERGE ... OUTPUT $action, INSERTED.*, DELETED.*` | not supported directly — see "OUTPUT clause" below |
| Multiple `WHEN MATCHED` with different conditions (max 2 in T-SQL) | Spark allows unlimited ordered `WHEN MATCHED` clauses — more flexible |

### Getting @@ROWCOUNT after MERGE/INSERT/UPDATE/DELETE
T-SQL procedures constantly do `SET @Rows = @@ROWCOUNT` after a DML statement for audit
logging. Spark SQL statements executed via `spark.sql()` don't return a row count directly
in older APIs; capture it like this:
```python
result = spark.sql("MERGE INTO ... ")   # DBR 11+: MERGE returns operation metrics
metrics = result.first().asDict() if result.columns else {}
rows_inserted = metrics.get("num_inserted_rows", 0)
rows_updated  = metrics.get("num_updated_rows", 0)
rows_deleted  = metrics.get("num_deleted_rows", 0)
# Or, for any table: DESCRIBE HISTORY t LIMIT 1 -> operationMetrics column
hist = spark.sql("DESCRIBE HISTORY fact.orders LIMIT 1").collect()[0]
op_metrics = hist["operationMetrics"]
```

### OUTPUT clause (capturing changed rows)
T-SQL:
```sql
MERGE fact.Orders AS target USING source ON ...
WHEN MATCHED THEN UPDATE SET ...
OUTPUT $action, INSERTED.OrderID, DELETED.Status AS OldStatus, INSERTED.Status AS NewStatus
INTO audit.OrderChanges;
```
Spark has no `OUTPUT` clause. Reproduce the intent with **Change Data Feed (CDF)**:
```sql
ALTER TABLE fact.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
-- after the MERGE, read what changed:
SELECT * FROM table_changes('fact.orders', <version_before>, <version_after>)
WHERE _change_type IN ('update_postimage','insert');
```
or diff the source/target before-and-after as a fallback if CDF isn't enabled.

## Full worked example — SCD Type 2 dimension merge

T-SQL (`sp_MergeCustomerDim` pattern — hash-compare, expire, insert):
```sql
-- Expire changed
UPDATE d SET d.IsCurrent = 0, d.EffectiveEndDate = @MergeDate
FROM dim.Customer d
INNER JOIN staging.Customer s ON d.CustomerID = s.CustomerID
WHERE d.IsCurrent = 1 AND d.RecordHash != HASHBYTES('SHA2_256', CONCAT(s.Name,'|',s.Email));

-- Insert new + changed versions
INSERT INTO dim.Customer (...)
SELECT ..., HASHBYTES('SHA2_256', CONCAT(s.Name,'|',s.Email))
FROM staging.Customer s
WHERE NOT EXISTS (SELECT 1 FROM dim.Customer d WHERE d.CustomerID = s.CustomerID AND d.IsCurrent = 1);
```

PySpark / Spark SQL equivalent:
```python
from pyspark.sql import functions as F

merge_date_expr = "current_timestamp()"

# Build the hashed source once — reused for both the expire-check and the insert
src = spark.table("staging.customer").withColumn(
    "record_hash",
    F.sha2(F.concat_ws("|", F.col("customer_name"), F.col("email")), 256)
)
src.createOrReplaceTempView("customer_source")

# Step 1: expire changed current records
spark.sql(f"""
    UPDATE gold.dim_customer d
    SET d.is_current = false, d.effective_end_date = {merge_date_expr}
    WHERE d.is_current = true
      AND EXISTS (
          SELECT 1 FROM customer_source s
          WHERE s.customer_id = d.customer_id AND s.record_hash != d.record_hash
      )
""")

# Step 2: insert new + changed versions (new customers AND newly-expired ones)
spark.sql(f"""
    INSERT INTO gold.dim_customer
    SELECT s.*, {merge_date_expr} AS effective_start_date, DATE'9999-12-31' AS effective_end_date,
           true AS is_current
    FROM customer_source s
    WHERE NOT EXISTS (
        SELECT 1 FROM gold.dim_customer d
        WHERE d.customer_id = s.customer_id AND d.is_current = true
    )
""")
```
Note this is done as **two statements (expire, then insert)** exactly like the T-SQL
original — a single `MERGE` cannot both expire an old row and insert a brand-new versioned
row for the *same business key* in one pass, so the two-step pattern is the correct,
idiomatic translation, not a simplification to avoid.
