# DDL and DRL: MSSQL to Databricks Unity Catalog

This reference covers DDL objects and permission/security mapping.

---

## 1. CREATE TABLE

### MSSQL

```sql
CREATE TABLE dbo.Customer (
    CustomerId INT NOT NULL PRIMARY KEY,
    CustomerName NVARCHAR(200) NOT NULL,
    Email NVARCHAR(255) NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT GETDATE(),
    IsActive BIT NOT NULL DEFAULT 1
);
```

### Databricks Delta / Unity Catalog

```sql
CREATE TABLE IF NOT EXISTS analytics.sales.customer (
    CustomerId BIGINT NOT NULL,
    CustomerName STRING NOT NULL,
    Email STRING,
    CreatedAt TIMESTAMP NOT NULL,
    IsActive BOOLEAN NOT NULL
)
USING DELTA
COMMENT 'Customer master table';
```

Notes:

- Primary keys may be informational depending on workspace/runtime capabilities.
- Foreign keys may not enforce referential integrity like SQL Server.
- Default constraints may require explicit insert logic or generated columns.
- Use Delta table properties for optimization.

---

## 2. CREATE TABLE AS SELECT

```sql
CREATE TABLE analytics.sales.customer_backup AS
SELECT *
FROM analytics.sales.customer;
```

---

## 3. ALTER TABLE ADD COLUMN

### MSSQL

```sql
ALTER TABLE dbo.Customer
ADD PhoneNumber NVARCHAR(50) NULL;
```

### Spark SQL

```sql
ALTER TABLE analytics.sales.customer
ADD COLUMN PhoneNumber STRING;
```

---

## 4. ALTER TABLE DROP COLUMN

```sql
ALTER TABLE analytics.sales.customer
DROP COLUMN PhoneNumber;
```

---

## 5. ALTER TABLE RENAME COLUMN

```sql
ALTER TABLE analytics.sales.customer
RENAME COLUMN PhoneNumber TO Phone;
```

---

## 6. ALTER TABLE CHANGE COLUMN TYPE

Use caution. Type changes must be compatible.

```sql
ALTER TABLE analytics.sales.customer
ALTER COLUMN CustomerName TYPE STRING;
```

For incompatible changes, use:

1. Add new column.
2. Backfill.
3. Drop/rename old column.

---

## 7. DROP TABLE

### MSSQL

```sql
DROP TABLE IF EXISTS dbo.CustomerBackup;
```

### Spark SQL

```sql
DROP TABLE IF EXISTS analytics.sales.customer_backup;
```

---

## 8. CREATE VIEW

### MSSQL

```sql
CREATE VIEW dbo.ActiveCustomer
AS
SELECT
    CustomerId,
    CustomerName,
    Email
FROM dbo.Customer
WHERE IsActive = 1;
```

### Spark SQL

```sql
CREATE OR REPLACE VIEW analytics.sales.active_customer AS
SELECT
    CustomerId,
    CustomerName,
    Email
FROM analytics.sales.customer
WHERE IsActive = TRUE;
```

---

## 9. CREATE OR ALTER VIEW

Use:

```sql
CREATE OR REPLACE VIEW analytics.sales.active_customer AS
SELECT ...;
```

---

## 10. DROP VIEW

```sql
DROP VIEW IF EXISTS analytics.sales.active_customer;
```

---

## 11. Indexes

MSSQL:

```sql
CREATE INDEX IX_Customer_Email
ON dbo.Customer (Email);
```

Databricks does not use traditional indexes the same way.

Use:

- Partitioning
- Liquid clustering
- `OPTIMIZE`
- `ZORDER`
- Statistics
- Photon-optimized layouts

Example clustering:

```sql
ALTER TABLE analytics.sales.customer
SET CLUSTER KEY (Email);
```

Or optimize:

```sql
OPTIMIZE analytics.sales.customer;
```

With Z-Order:

```sql
OPTIMIZE analytics.sales.customer
ZORDER BY (Email);
```

---

## 12. Statistics

MSSQL statistics are managed differently in Databricks.

You can use:

```sql
ANALYZE TABLE analytics.sales.customer COMPUTE STATISTICS;
```

and column statistics where supported.

---

## 13. Constraints

### NOT NULL

```sql
CREATE TABLE analytics.sales.customer (
    CustomerId BIGINT NOT NULL,
    CustomerName STRING NOT NULL
);
```

### CHECK Constraint

```sql
ALTER TABLE analytics.sales.customer
ADD CONSTRAINT customer_email_valid
CHECK (Email IS NULL OR Email LIKE '%@%');
```

Use constraints where supported. Otherwise enforce in pipeline code.

---

## 14. Primary Key and Foreign Key

Where supported, use informational constraints:

```sql
ALTER TABLE analytics.sales.customer
ADD CONSTRAINT pk_customer PRIMARY KEY (CustomerId);
```

However, do not assume SQL Server-like enforcement.

Enforce business keys using:

- Deduplication
- MERGE logic
- Data quality rules
- Expectation frameworks
- Pipeline tests

---

## 15. Identity / Sequence

### MSSQL

```sql
CREATE TABLE dbo.AuditLog (
    AuditLogId BIGINT IDENTITY(1,1) PRIMARY KEY,
    Message NVARCHAR(1000)
);
```

### Databricks Identity Column

```sql
CREATE TABLE analytics.audit.audit_log (
    AuditLogId BIGINT GENERATED ALWAYS AS IDENTITY,
    Message STRING
);
```

If identity columns are unavailable or insufficient:

```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY CreatedAt) AS AuditLogId,
    Message
FROM analytics.audit.audit_log_staging;
```

or:

```python
from pyspark.sql import functions as F

df = df.withColumn("AuditLogId", F.monotonically_increasing_id())
```

Important:

- `monotonically_increasing_id()` is not guaranteed sequential or gapless.
- Use deterministic ordering if business requires exact sequence.
- For high-volume key generation, consider sequence tables or key management patterns.

---

## 16. Computed Columns

### MSSQL

```sql
ALTER TABLE dbo.Customer
ADD FullName AS FirstName + ' ' + LastName;
```

### Databricks

Use generated columns where supported:

```sql
ALTER TABLE analytics.sales.customer
ADD COLUMN FullName STRING GENERATED ALWAYS AS (FirstName || ' ' || LastName);
```

If not supported, compute in pipeline.

---

## 17. Partitioning

```sql
CREATE TABLE analytics.sales.order (
    OrderId BIGINT,
    CustomerId BIGINT,
    OrderDate DATE,
    OrderAmount DECIMAL(18,2)
)
USING DELTA
PARTITIONED BY (OrderDate);
```

Use partitioning for high-cardinality, query-filtered date columns.

Do not over-partition small tables.

---

## 18. Liquid Clustering

```sql
CREATE TABLE analytics.sales.order (
    OrderId BIGINT,
    CustomerId BIGINT,
    OrderDate DATE,
    OrderAmount DECIMAL(18,2)
)
USING DELTA
CLUSTER BY (CustomerId);
```

Prefer liquid clustering for new tables where appropriate.

---

## 19. Stored Procedures

MSSQL stored procedures do not have a direct Databricks SQL object equivalent.

Map to:

- Python function
- Notebook
- Databricks Workflow task
- SQL warehouse script if simple
- Reusable library module

Example:

```python
def run_load_customer(spark, params=None, table_map=None, dry_run=True):
    ...
```

---

## 20. Functions

### Scalar UDF

Use PySpark UDFs cautiously because they can reduce performance.

Prefer built-in Spark SQL functions.

If needed:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def format_name(first_name, last_name):
    if first_name is None and last_name is None:
        return None
    return f"{first_name or ''} {last_name or ''}".strip()
```

### SQL UDF

Databricks supports SQL UDFs in many environments:

```sql
CREATE FUNCTION analytics.udf.format_name(first_name STRING, last_name STRING)
RETURNS STRING
RETURN CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''));
```

Use SQL UDFs where supported and performance is acceptable.

---

## 21. DRL / Permissions

### MSSQL

```sql
GRANT SELECT ON dbo.Customer TO AppReadOnly;
GRANT INSERT ON dbo.Customer TO AppWriter;
GRANT UPDATE ON dbo.Customer TO AppWriter;
GRANT DELETE ON dbo.Customer TO AppWriter;
GRANT EXECUTE ON dbo.LoadCustomer TO AppETL;
```

### Unity Catalog

```sql
GRANT SELECT ON TABLE analytics.sales.customer TO `app_readers`;
GRANT MODIFY ON TABLE analytics.sales.customer TO `app_writers`;
GRANT USAGE ON SCHEMA analytics.sales TO `app_writers`;
GRANT USAGE ON CATALOG analytics TO `app_writers`;
```

For views:

```sql
GRANT SELECT ON VIEW analytics.sales.active_customer TO `app_readers`;
```

For functions:

```sql
GRANT EXECUTE ON FUNCTION analytics.udf.format_name TO `app_writers`;
```

---

## 22. DENY Semantics

MSSQL:

```sql
DENY SELECT ON dbo.Customer TO AppUser;
```

Unity Catalog generally uses positive grants and least privilege.

Instead of `DENY`, use:

- Separate schemas/catalogs
- Groups
- Narrow grants
- Row filters
- Column masks
- No broad `SELECT` grants

---

## 23. Row-Level Security

Unity Catalog row filters can restrict rows by group or condition.

Conceptual pattern:

```sql
ALTER TABLE analytics.sales.customer
SET ROW FILTER analytics.security.customer_row_filter ON (
    Country = CURRENT_USER_COUNTRY()
);
```

Implementation depends on workspace capabilities and security model.

---

## 24. Column Masking

Use column masks for sensitive fields.

Conceptual pattern:

```sql
ALTER TABLE analytics.sales.customer
ALTER COLUMN Email
SET MASK analytics.security.mask_email;
```

Use masking functions approved by your governance team.

---

## DDL/DRL Conversion Checklist

- [ ] Map SQL Server database to catalog/schema strategy.
- [ ] Convert data types safely.
- [ ] Confirm identity/sequence replacement.
- [ ] Confirm constraint enforcement differences.
- [ ] Replace indexes with clustering/optimize strategy.
- [ ] Replace stored procedures with Python/workflow functions.
- [ ] Map logins/users to Unity Catalog groups/principals.
- [ ] Replace `GRANT EXECUTE` with job/notebook/workspace access.
- [ ] Validate row-level security and column masking needs.
- [ ] Confirm ownership and service principal permissions.