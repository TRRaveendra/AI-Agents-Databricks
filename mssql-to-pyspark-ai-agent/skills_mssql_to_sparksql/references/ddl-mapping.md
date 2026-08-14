# DDL Mapping — T-SQL → Spark SQL / Delta Lake

## CREATE TABLE

```sql
-- T-SQL
CREATE TABLE dbo.Customer (
    CustomerKey     INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID      INT NOT NULL,
    CustomerName    NVARCHAR(200) NOT NULL,
    Email           VARCHAR(255) NULL,
    CreditLimit     DECIMAL(18,2) DEFAULT 0,
    IsCurrent       BIT NOT NULL DEFAULT 1,
    CreatedDate     DATETIME2 DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_Customer_ID UNIQUE (CustomerID, IsCurrent)
);
```

```sql
-- Spark SQL / Delta
CREATE TABLE gold.customer (
    customer_key    BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id     INT NOT NULL,
    customer_name   STRING NOT NULL,
    email           STRING,
    credit_limit    DECIMAL(18,2) DEFAULT 0,
    is_current      BOOLEAN NOT NULL DEFAULT true,
    created_date    TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('delta.feature.identityColumns' = 'supported');
```

Notes:
- `IDENTITY(1,1)` → `GENERATED ALWAYS AS IDENTITY` (Delta Lake identity columns, DBR 10.4+)
  or, more commonly in lakehouse designs, use `F.monotonically_increasing_id()` /
  a surrogate-key sequence table if identity semantics must match exactly across merges.
- Delta has **no PRIMARY KEY / UNIQUE / FOREIGN KEY enforcement** at the storage engine
  level. Unity Catalog supports declaring PK/FK as **informational constraints** (not
  enforced) for lineage/BI tools:
  ```sql
  ALTER TABLE gold.customer ADD CONSTRAINT pk_customer PRIMARY KEY (customer_key);
  ALTER TABLE fact.orders ADD CONSTRAINT fk_orders_customer
      FOREIGN KEY (customer_key) REFERENCES gold.customer(customer_key) NOT ENFORCED;
  ```
  Enforce uniqueness/integrity in your MERGE logic instead — the constraint won't reject bad data.
- `CHECK` constraints ARE enforced in Delta (`ALTER TABLE t ADD CONSTRAINT chk CHECK (col > 0)`).
- Column name convention: T-SQL PascalCase is commonly converted to `snake_case` for Spark/
  Unity Catalog tables (not required, but is the prevailing lakehouse convention) — confirm
  with the user's existing naming standard before renaming.

### CREATE TABLE ... AS SELECT (CTAS)

```sql
-- T-SQL: SELECT ... INTO
SELECT * INTO dbo.CustomerSnapshot FROM dbo.Customer WHERE IsCurrent = 1;
```
```python
# PySpark
spark.sql("""
    CREATE OR REPLACE TABLE gold.customer_snapshot
    USING DELTA AS
    SELECT * FROM gold.customer WHERE is_current = true
""")
```

## ALTER TABLE

| T-SQL | Spark SQL |
|---|---|
| `ALTER TABLE t ADD col INT NULL` | `ALTER TABLE t ADD COLUMNS (col INT)` |
| `ALTER TABLE t DROP COLUMN col` | `ALTER TABLE t DROP COLUMN col` (requires `delta.columnMapping.mode = 'name'`) |
| `ALTER TABLE t ALTER COLUMN col BIGINT` | `ALTER TABLE t ALTER COLUMN col TYPE BIGINT` (widening only) |
| `EXEC sp_rename 'old','new','COLUMN'` | `ALTER TABLE t RENAME COLUMN old TO new` |
| `ALTER TABLE t ADD CONSTRAINT ...` | `ALTER TABLE t ADD CONSTRAINT ...` (CHECK enforced; PK/FK informational only) |
| `ALTER TABLE t WITH CHECK ADD CONSTRAINT fk ...` | Unity Catalog `NOT ENFORCED` FK, or validate in code |

Column type narrowing (e.g. `BIGINT` → `INT`) is **not supported** in either engine without
a rebuild — script a `CREATE OR REPLACE TABLE AS SELECT CAST(...)`.

## DROP / TRUNCATE

| T-SQL | Spark SQL |
|---|---|
| `DROP TABLE dbo.X` | `DROP TABLE catalog.schema.x` |
| `DROP TABLE IF EXISTS dbo.X` | `DROP TABLE IF EXISTS catalog.schema.x` |
| `TRUNCATE TABLE dbo.X` | `TRUNCATE TABLE catalog.schema.x` (Delta supports this; keeps history/metadata) |
| `DROP INDEX ix_x ON t` | N/A — see Indexes below |

## Indexes → Delta equivalents

Delta Lake has no B-tree secondary indexes. Map intent, not syntax:

| T-SQL Index Purpose | Delta Lake Equivalent |
|---|---|
| Clustered index for range scans on a date/id column | `CLUSTER BY (col)` (Liquid Clustering) or `PARTITIONED BY (col)` for low-cardinality date parts |
| Nonclustered index for point lookups on a filter column | `OPTIMIZE t ZORDER BY (col)` or Liquid Clustering on that column |
| Covering index | Not applicable — Delta reads full columns needed; rely on column pruning + file skipping (Z-order/stats) |
| Unique index enforcing dedup | Enforce via MERGE logic or a `CHECK`/informational constraint; not physically enforced |
| Full-text index | Databricks doesn't have native full-text search; consider a search index product or `LIKE`/regex on Spark |

```sql
-- T-SQL
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID ON fact.Orders(CustomerID);
CREATE CLUSTERED INDEX IX_Orders_OrderDate ON fact.Orders(OrderDate);
```
```sql
-- Spark SQL (Delta Lake 3.x Liquid Clustering — preferred over legacy partitioning for most cases)
CREATE TABLE fact.orders (...)
USING DELTA
CLUSTER BY (customer_id, order_date);

-- or, legacy partitioning for coarse, low-cardinality date partitioning
CREATE TABLE fact.orders (...)
USING DELTA
PARTITIONED BY (order_year, order_month);

-- periodic maintenance (replaces index rebuild/reorg jobs)
OPTIMIZE fact.orders ZORDER BY (customer_id);
VACUUM fact.orders RETAIN 168 HOURS;
```

## Schemas / Namespaces

| T-SQL | Spark SQL / Unity Catalog |
|---|---|
| `CREATE SCHEMA dbo` | `CREATE SCHEMA catalog.schema_name` |
| `dbo.TableName` (2-part name) | `catalog.schema.table` (3-part name in Unity Catalog) |
| `USE DatabaseName` | `USE CATALOG catalog_name; USE SCHEMA schema_name;` |

## Views

```sql
-- T-SQL
CREATE OR ALTER VIEW dbo.vw_ActiveCustomers AS
SELECT CustomerID, CustomerName FROM dbo.Customer WHERE IsActive = 1;
```
```sql
-- Spark SQL
CREATE OR REPLACE VIEW gold.vw_active_customers AS
SELECT customer_id, customer_name FROM gold.customer WHERE is_active = true;
```
Materialized views: T-SQL indexed views map to Databricks **materialized views**
(`CREATE MATERIALIZED VIEW ...`, DLT-backed, auto-refreshing) rather than plain views when
the original view was indexed for performance.

## Sequences / IDENTITY

```sql
-- T-SQL
CREATE SEQUENCE dbo.OrderIDSeq START WITH 1 INCREMENT BY 1;
NEXT VALUE FOR dbo.OrderIDSeq
```
```python
# Spark SQL has IDENTITY columns but no standalone CREATE SEQUENCE object.
# Use an identity column on the target table, or generate keys with monotonically_increasing_id()
# combined with a max-key lookup for merge-safe surrogate key assignment:
from pyspark.sql import functions as F
max_key = spark.sql("SELECT COALESCE(MAX(customer_key),0) AS m FROM gold.customer").collect()[0]["m"]
df_new = df_new.withColumn("customer_key", F.monotonically_increasing_id() + F.lit(max_key) + 1)
```

## Table types with no direct Delta equivalent

- **Temporal tables** (`SYSTEM_VERSIONING = ON`) → Delta's built-in transaction log gives
  you `DESCRIBE HISTORY` / `VERSION AS OF` / `TIMESTAMP AS OF` (time travel) largely for
  free — no explicit history table needed. For SCD-style "as of" business queries, still
  build an explicit SCD Type 2 dimension (see dml-mapping.md), since Delta time travel is
  about *table state*, not *business-effective-dated* records.
- **Partitioned views / partitioned tables with `$PARTITION`** → Delta `PARTITIONED BY` or
  Liquid Clustering; no `$PARTITION` function equivalent — use `input_file_name()` or the
  partition columns directly.
- **Filegroups / file placement** → not applicable; Delta relies on cloud object storage,
  managed via table location and `OPTIMIZE`/Z-order instead of physical file placement.
