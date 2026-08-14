# DML Patterns: MSSQL to PySpark / Spark SQL

This reference covers `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`, `OUTPUT`, and row-count handling.

---

## General DML Rules

1. Target tables should usually be Delta tables.
2. Use `MERGE` when updates or deletes depend on joins.
3. Deduplicate source data before `MERGE` if source can contain duplicate keys.
4. Preserve NULL semantics.
5. Use explicit column lists instead of `INSERT *` where possible.
6. Prefer idempotent operations.
7. For destructive operations, support `dry_run`.

---

## 1. INSERT INTO SELECT

### MSSQL

```sql
INSERT INTO dbo.CustomerSummary (
    CustomerId,
    CustomerName,
    TotalOrders
)
SELECT
    CustomerId,
    CustomerName,
    TotalOrders
FROM dbo.CustomerStaging
WHERE IsActive = 1;
```

### Spark SQL

```sql
INSERT INTO analytics.sales.customer_summary (
    CustomerId,
    CustomerName,
    TotalOrders
)
SELECT
    CustomerId,
    CustomerName,
    TotalOrders
FROM analytics.sales.customer_staging
WHERE IsActive = 1;
```

### PySpark

```python
spark.sql("""
    INSERT INTO analytics.sales.customer_summary (
        CustomerId,
        CustomerName,
        TotalOrders
    )
    SELECT
        CustomerId,
        CustomerName,
        TotalOrders
    FROM analytics.sales.customer_staging
    WHERE IsActive = 1
""")
```

### DataFrame Alternative

```python
df = spark.table("analytics.sales.customer_staging").filter("IsActive = 1")

(
    df.select("CustomerId", "CustomerName", "TotalOrders")
      .write
      .mode("append")
      .saveAsTable("analytics.sales.customer_summary")
)
```

---

## 2. INSERT OVERWRITE

### MSSQL Equivalent Pattern

SQL Server often uses:

```sql
DELETE FROM dbo.CustomerSummary;

INSERT INTO dbo.CustomerSummary (...)
SELECT ...;
```

or truncate plus insert.

### Spark SQL

```sql
INSERT OVERWRITE analytics.sales.customer_summary
SELECT *
FROM analytics.sales.customer_staging;
```

### PySpark

```python
(
    spark.table("analytics.sales.customer_staging")
         .write
         .mode("overwrite")
         .saveAsTable("analytics.sales.customer_summary")
)
```

Use overwrite only when replacing the full table or partition is intended.

---

## 3. SELECT INTO

### MSSQL

```sql
SELECT
    CustomerId,
    CustomerName
INTO dbo.CustomerBackup
FROM dbo.Customer;
```

### Spark SQL

```sql
CREATE TABLE analytics.sales.customer_backup AS
SELECT
    CustomerId,
    CustomerName
FROM analytics.sales.customer;
```

Important:

- Does not copy indexes.
- Does not copy constraints.
- Does not copy identity behavior automatically.
- Does not copy permissions.

---

## 4. INSERT VALUES

### MSSQL

```sql
INSERT INTO dbo.AuditLog (
    ProcessName,
    Status,
    CreatedAt
)
VALUES (
    'LoadCustomer',
    'Success',
    GETDATE()
);
```

### Spark SQL

```sql
INSERT INTO analytics.audit.audit_log (
    ProcessName,
    Status,
    CreatedAt
)
VALUES (
    'LoadCustomer',
    'Success',
    CURRENT_TIMESTAMP()
);
```

### PySpark

```python
from pyspark.sql import functions as F

audit_df = spark.createDataFrame(
    [
        ("LoadCustomer", "Success")
    ],
    ["ProcessName", "Status"]
).withColumn("CreatedAt", F.current_timestamp())

audit_df.write.mode("append").saveAsTable("analytics.audit.audit_log")
```

---

## 5. Simple UPDATE

### MSSQL

```sql
UPDATE dbo.Customer
SET IsActive = 0
WHERE LastOrderDate < '2024-01-01';
```

### Spark SQL

```sql
UPDATE analytics.sales.customer
SET IsActive = 0
WHERE LastOrderDate < DATE('2024-01-01');
```

### PySpark DataFrame API

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "tables/analytics/sales/customer")

delta_table.update(
    condition="LastOrderDate < DATE('2024-01-01')",
    set={"IsActive": "0"}
)
```

If table is registered in Unity Catalog:

```python
spark.sql("""
    UPDATE analytics.sales.customer
    SET IsActive = 0
    WHERE LastOrderDate < DATE('2024-01-01')
""")
```

---

## 6. UPDATE with JOIN / UPDATE FROM

### MSSQL

```sql
UPDATE c
SET c.TotalOrders = s.TotalOrders
FROM dbo.Customer c
INNER JOIN dbo.CustomerSummary s
    ON c.CustomerId = s.CustomerId
WHERE s.TotalOrders IS NOT NULL;
```

### Preferred Spark SQL: MERGE

```sql
MERGE INTO analytics.sales.customer AS c
USING (
    SELECT
        CustomerId,
        TotalOrders
    FROM analytics.sales.customer_summary
    WHERE TotalOrders IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CustomerId
        ORDER BY TotalOrders DESC
    ) = 1
) AS s
    ON c.CustomerId = s.CustomerId
WHEN MATCHED THEN
    UPDATE SET c.TotalOrders = s.TotalOrders;
```

Notes:

- `QUALIFY` is useful for deduplication in Databricks SQL.
- If the source can match the target multiple times, Delta `MERGE` can fail unless handled.
- Explicit deduplication is often safer.

Alternative dedupe:

```sql
MERGE INTO analytics.sales.customer AS c
USING (
    SELECT
        CustomerId,
        TotalOrders
    FROM (
        SELECT
            CustomerId,
            TotalOrders,
            ROW_NUMBER() OVER (
                PARTITION BY CustomerId
                ORDER BY TotalOrders DESC
            ) AS rn
        FROM analytics.sales.customer_summary
        WHERE TotalOrders IS NOT NULL
    )
    WHERE rn = 1
) AS s
    ON c.CustomerId = s.CustomerId
WHEN MATCHED THEN
    UPDATE SET c.TotalOrders = s.TotalOrders;
```

---

## 7. Simple DELETE

### MSSQL

```sql
DELETE FROM dbo.CustomerStaging
WHERE LoadDate < '2024-01-01';
```

### Spark SQL

```sql
DELETE FROM analytics.sales.customer_staging
WHERE LoadDate < DATE('2024-01-01');
```

### DeltaTable API

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "tables/analytics/sales/customer_staging")

delta_table.delete("LoadDate < DATE('2024-01-01')")
```

---

## 8. DELETE with JOIN or EXISTS

### MSSQL

```sql
DELETE c
FROM dbo.Customer c
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.Order o
    WHERE o.CustomerId = c.CustomerId
);
```

### Spark SQL

```sql
DELETE FROM analytics.sales.customer AS c
WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.sales.order AS o
    WHERE o.CustomerId = c.CustomerId
);
```

### MERGE Alternative

```sql
MERGE INTO analytics.sales.customer AS c
USING (
    SELECT DISTINCT CustomerId
    FROM analytics.sales.order
) AS o
    ON c.CustomerId = o.CustomerId
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
```

Use caution with `WHEN NOT MATCHED BY SOURCE`. It can delete many rows if the source is incomplete.

---

## 9. TRUNCATE TABLE

### MSSQL

```sql
TRUNCATE TABLE dbo.CustomerStaging;
```

### Spark SQL

```sql
TRUNCATE TABLE analytics.sales.customer_staging;
```

For partition truncate:

```sql
TRUNCATE TABLE analytics.sales.customer_staging PARTITION (LoadDate = '2026-01-01');
```

---

## 10. MERGE: Basic Upsert

### MSSQL

```sql
MERGE INTO dbo.Customer AS tgt
USING dbo.CustomerStaging AS src
    ON tgt.CustomerId = src.CustomerId
WHEN MATCHED THEN
    UPDATE SET
        CustomerName = src.CustomerName,
        Email = src.Email,
        UpdatedAt = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (
        CustomerId,
        CustomerName,
        Email,
        CreatedAt,
        UpdatedAt
    )
    VALUES (
        src.CustomerId,
        src.CustomerName,
        src.Email,
        GETDATE(),
        GETDATE()
    );
```

### Spark SQL / Delta

```sql
MERGE INTO analytics.sales.customer AS tgt
USING analytics.sales.customer_staging AS src
    ON tgt.CustomerId = src.CustomerId
WHEN MATCHED THEN
    UPDATE SET
        CustomerName = src.CustomerName,
        Email = src.Email,
        UpdatedAt = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        CustomerId,
        CustomerName,
        Email,
        CreatedAt,
        UpdatedAt
    )
    VALUES (
        src.CustomerId,
        src.CustomerName,
        src.Email,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );
```

---

## 11. MERGE: Upsert with Delete Flag

### MSSQL

```sql
MERGE INTO dbo.Customer AS tgt
USING dbo.CustomerStaging AS src
    ON tgt.CustomerId = src.CustomerId
WHEN MATCHED AND src.IsDeleted = 1 THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET
        CustomerName = src.CustomerName,
        Email = src.Email
WHEN NOT MATCHED AND src.IsDeleted = 0 THEN
    INSERT (
        CustomerId,
        CustomerName,
        Email
    )
    VALUES (
        src.CustomerId,
        src.CustomerName,
        src.Email
    );
```

### Spark SQL

```sql
MERGE INTO analytics.sales.customer AS tgt
USING analytics.sales.customer_staging AS src
    ON tgt.CustomerId = src.CustomerId
WHEN MATCHED AND src.IsDeleted = 1 THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET
        CustomerName = src.CustomerName,
        Email = src.Email
WHEN NOT MATCHED AND src.IsDeleted = 0 THEN
    INSERT (
        CustomerId,
        CustomerName,
        Email
    )
    VALUES (
        src.CustomerId,
        src.CustomerName,
        src.Email
    );
```

---

## 12. MERGE: SCD Type 2

### Spark SQL Pattern

```sql
MERGE INTO analytics.sales.customer_scd2 AS tgt
USING (
    SELECT
        CustomerId,
        CustomerName,
        Email,
        EffectiveStartDate,
        EffectiveEndDate,
        IsCurrent
    FROM analytics.sales.customer_stage
) AS src
    ON tgt.CustomerId = src.CustomerId
   AND tgt.IsCurrent = 1
WHEN MATCHED AND (
        tgt.CustomerName <> src.CustomerName
     OR tgt.Email <> src.Email
) THEN
    UPDATE SET
        tgt.IsCurrent = 0,
        tgt.EffectiveEndDate = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        CustomerId,
        CustomerName,
        Email,
        EffectiveStartDate,
        EffectiveEndDate,
        IsCurrent
    )
    VALUES (
        src.CustomerId,
        src.CustomerName,
        src.Email,
        CURRENT_TIMESTAMP(),
        NULL,
        1
    );
```

For a full SCD2 pattern with expiring old rows and inserting new current rows, use a two-phase approach or a source that includes action flags.

---

## 13. MERGE with `WHEN NOT MATCHED BY SOURCE`

### Spark SQL

```sql
MERGE INTO analytics.sales.customer AS tgt
USING (
    SELECT DISTINCT CustomerId
    FROM analytics.sales.active_customer_source
) AS src
    ON tgt.CustomerId = src.CustomerId
WHEN NOT MATCHED BY SOURCE AND tgt.IsActive = 1 THEN
    UPDATE SET IsActive = 0;
```

Use this carefully. Make sure source is complete and filtered correctly.

---

## 14. OUTPUT Clause Equivalent

### MSSQL

```sql
INSERT INTO dbo.AuditLog (CustomerId, Action, ChangedAt)
OUTPUT inserted.CustomerId, 'INSERT', GETDATE()
SELECT CustomerId
FROM dbo.CustomerStaging;
```

Spark SQL does not provide a universal `OUTPUT` clause equivalent.

Recommended patterns:

### Option 1: Explicit audit insert

```sql
INSERT INTO analytics.audit.audit_log
SELECT
    CustomerId,
    'INSERT' AS Action,
    CURRENT_TIMESTAMP() AS ChangedAt
FROM analytics.sales.customer_staging;

INSERT INTO analytics.sales.customer
SELECT *
FROM analytics.sales.customer_staging;
```

### Option 2: Use Change Data Feed

Enable CDF:

```sql
ALTER TABLE analytics.sales.customer
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

Then query:

```sql
SELECT *
FROM table_changes('analytics.sales.customer', 1);
```

---

## 15. @@ROWCOUNT Equivalent

### MSSQL

```sql
UPDATE dbo.Customer
SET IsActive = 0
WHERE LastOrderDate < '2024-01-01';

SELECT @@ROWCOUNT AS RowsAffected;
```

### Databricks Options

1. Count before update:

```python
affected_count = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM analytics.sales.customer
    WHERE LastOrderDate < DATE('2024-01-01')
""").collect()[0]["cnt"]
```

2. Run update.

3. Return count in result dictionary.

For `MERGE`, use Delta operation metrics where available:

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "analytics.sales.customer")

merge_result = (
    dt.alias("tgt")
    .merge(
        spark.table("analytics.sales.customer_staging").alias("src"),
        "tgt.CustomerId = src.CustomerId"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

Then collect metrics if needed from Delta table history or explicit counts.

---

## DML Anti-Patterns

Avoid:

- Updating non-Delta tables expecting transactional behavior.
- Running `MERGE` with duplicate source keys.
- Using `INSERT OVERWRITE` when only a subset should change.
- Relying on nondeterministic join order for updates.
- Ignoring NULL-safe comparison.
- Using string interpolation for user-provided values.
- Assuming `TRUNCATE` is reversible without backup.