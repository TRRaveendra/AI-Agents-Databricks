# Conversion Rules and Anti-Patterns

These rules are mandatory for MSSQL to PySpark migration.

---

## Mandatory Rules

### Rule 1: Do not blindly translate procedural code

Rewrite for distributed set-based execution.

Bad:

- Cursor loop converted to Python loop over millions of rows.

Good:

- Join, aggregation, window function, or `MERGE`.

---

### Rule 2: Preserve business semantics

Do not assume syntactic equivalence equals semantic equivalence.

Check:

- NULL behavior
- Join duplication
- Ordering
- Type coercion
- Date boundaries
- Integer division
- Empty strings

---

### Rule 3: Use Delta for mutable tables

If the procedure contains `UPDATE`, `DELETE`, or `MERGE`, target tables should be Delta tables.

---

### Rule 4: Deduplicate MERGE sources

If source can contain multiple rows for the same target key, deduplicate first.

```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC) = 1
```

or:

```sql
WHERE rn = 1
```

---

### Rule 5: Avoid SQL injection

Never interpolate untrusted values into SQL.

Bad:

```python
spark.sql(f"SELECT * FROM customer WHERE name = '{user_input}'")
```

Good:

- Use DataFrame filters with literals.
- Use parameter views.
- Use whitelists for identifiers.
- Use runtime-supported parameter binding if available.

---

### Rule 6: Prefer explicit column lists

Avoid `INSERT *` or `SELECT *` in production mutations.

---

### Rule 7: Make conversions idempotent

Design so re-running the job does not duplicate data.

Use:

- MERGE
- Overwrite by partition
- Unique keys
- Idempotent keys
- Watermarks
- Delete-insert patterns with transaction boundaries

---

### Rule 8: Document unsupported features

Examples:

- CLR
- `xp_cmdshell`
- Service Broker
- Linked servers
- File system operations
- Windows authentication assumptions
- SQL Agent-specific features

---

## Semantic Differences

### NULL Handling

MSSQL and Spark SQL both use three-valued logic, but functions differ.

Use:

```sql
COALESCE(column, default)
```

Instead of only:

```sql
ISNULL(column, default)
```

For null-safe equality:

```sql
column1 <=> column2
```

---

### Empty String vs NULL

MSSQL treats empty string as a string, not NULL.

Spark treats `''` and `NULL` differently.

If source uses empty strings meaning NULL, add:

```sql
NULLIF(TRIM(column), '')
```

---

### Integer Division

MSSQL:

```sql
SELECT 1 / 2
```

returns `0`.

Spark SQL may return floating result depending on types.

Use explicit casts:

```sql
CAST(1 AS INT) / CAST(2 AS INT)
```

or:

```sql
FLOOR(a / b)
```

when integer division semantics are required.

---

### Date Boundaries

MSSQL:

```sql
WHERE CreatedDate >= '2026-01-01'
  AND CreatedDate < '2026-02-01'
```

Keep the same boundaries in Spark SQL.

Avoid:

```sql
WHERE DATE(CreatedDate) = '2026-01-01'
```

unless filtering by date is intended.

---

### Collation and Case Sensitivity

MSSQL collations can be case-insensitive or accent-insensitive.

Spark SQL string comparisons may behave differently depending on configuration.

If needed, normalize:

```sql
UPPER(column)
LOWER(column)
```

or apply explicit matching rules.

---

### TOP Requires Deterministic ORDER BY

MSSQL:

```sql
SELECT TOP 1 *
FROM Customer
```

without `ORDER BY` is nondeterministic.

Spark:

```sql
LIMIT 1
```

is also nondeterministic unless ordered.

Always add deterministic ordering.

---

### UPDATE FROM with Multiple Matches

MSSQL may choose one matching row nondeterministically.

Delta `MERGE` may fail when multiple source rows match one target row.

Deduplicate source.

---

## Data Type Mapping

| MSSQL Type | Recommended Databricks Type |
|---|---|
| `BIT` | `BOOLEAN` |
| `TINYINT` | `BYTE` or `SMALLINT` |
| `SMALLINT` | `SMALLINT` |
| `INT` | `INT` or `BIGINT` |
| `BIGINT` | `BIGINT` |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` |
| `NUMERIC(p,s)` | `DECIMAL(p,s)` |
| `MONEY` | `DECIMAL(19,4)` |
| `SMALLMONEY` | `DECIMAL(10,4)` |
| `FLOAT` | `DOUBLE` |
| `REAL` | `FLOAT` |
| `CHAR(n)` | `STRING` |
| `NCHAR(n)` | `STRING` |
| `VARCHAR(n)` | `STRING` |
| `NVARCHAR(n)` | `STRING` |
| `VARCHAR(MAX)` | `STRING` |
| `NVARCHAR(MAX)` | `STRING` |
| `TEXT` | `STRING` |
| `NTEXT` | `STRING` |
| `DATE` | `DATE` |
| `TIME` | `STRING` or `TIMESTAMP` depending on use |
| `DATETIME` | `TIMESTAMP` |
| `DATETIME2` | `TIMESTAMP` |
| `SMALLDATETIME` | `TIMESTAMP` |
| `DATETIMEOFFSET` | `TIMESTAMP` with timezone strategy |
| `UNIQUEIDENTIFIER` | `STRING` |
| `BINARY` | `BINARY` |
| `VARBINARY` | `BINARY` |
| `IMAGE` | `BINARY` |
| `XML` | `STRING` or structured type after parsing |
| `SQL_VARIANT` | `STRING` or explicit typed redesign |
| `HIERARCHYID` | `STRING` or struct redesign |
| `GEOGRAPHY` | `STRING`, WKT/WKB, or geospatial library |
| `GEOMETRY` | `STRING`, WKT/WKB, or geospatial library |

---

## Function Mapping

| MSSQL Function | Spark SQL Equivalent |
|---|---|
| `GETDATE()` | `CURRENT_TIMESTAMP()` |
| `SYSDATETIME()` | `CURRENT_TIMESTAMP()` |
| `GETUTCDATE()` | `CURRENT_TIMESTAMP()` with UTC strategy |
| `DATEADD(day, n, date)` | `date_add(date, n)` for days |
| `DATEADD(month, n, date)` | `add_months(date, n)` |
| `DATEADD(year, n, date)` | `add_months(date, n * 12)` |
| `DATEDIFF(day, a, b)` | `datediff(b, a)` |
| `DATEDIFF(month, a, b)` | `months_between(b, a)` with care |
| `DATEDIFF(year, a, b)` | custom expression |
| `DATEFROMPARTS` | `make_date` |
| `YEAR(date)` | `year(date)` |
| `MONTH(date)` | `month(date)` |
| `DAY(date)` | `day(date)` |
| `EOMONTH(date)` | `last_day(date)` |
| `CONVERT(type, value)` | `CAST(value AS type)` |
| `TRY_CONVERT` | `TRY_CAST` |
| `FORMAT(date, format)` | `date_format(date, format)` |
| `ISNULL(a,b)` | `COALESCE(a,b)` |
| `COALESCE` | `COALESCE` |
| `IIF(cond,a,b)` | `CASE WHEN cond THEN a ELSE b END` |
| `LEN(x)` | `LENGTH(x)` |
| `DATALENGTH(x)` | approximate with `OCTET_LENGTH(x)` depending on type |
| `CHARINDEX(sub,x)` | `LOCATE(sub, x)` or `POSITION(sub IN x)` |
| `PATINDEX` | `regexp_extract` or `rlike` |
| `REPLACE` | `REPLACE` |
| `STUFF` | `overlay` or string concatenation |
| `SUBSTRING` | `SUBSTRING` |
| `LEFT(x,n)` | `LEFT(x,n)` |
| `RIGHT(x,n)` | `RIGHT(x,n)` |
| `LTRIM/RTRIM` | `LTRIM/RTRIM` |
| `UPPER/LOWER` | `UPPER/LOWER` |
| `NEWID()` | `uuid()` |
| `CHECKSUM` | `hash()` or `crc32()` depending on requirement |
| `ROWCOUNT` | explicit count or metrics |
| `SCOPE_IDENTITY()` | identity column, sequence pattern, or generated key |

---

## Anti-Patterns

### 1. Cursor-to-loop conversion without redesign

Do not convert a cursor processing millions of rows into a Python loop.

---

### 2. String interpolation for values

Do not inject user values into SQL strings.

---

### 3. Relying on temp tables across tasks

Temporary views do not persist across cluster restarts or separate tasks unless recreated.

---

### 4. Assuming global temp tables behave like SQL Server

Global temp views have different lifecycle and visibility semantics.

---

### 5. Using `INSERT OVERWRITE` accidentally

Ensure overwrite scope is intended: table, partition, or path.

---

### 6. Ignoring MERGE cardinality

Duplicate source rows can fail or corrupt data.

---

### 7. Converting indexes directly

There is no direct index object. Use clustering and optimization.

---

### 8. Assuming constraints are enforced the same way

Validate keys and references in pipeline logic.

---

### 9. Ignoring time zones

MSSQL `DATETIMEOFFSET` and Spark timestamp semantics require explicit strategy.

---

### 10. Forgetting permissions

Stored procedure `EXECUTE` permissions do not map directly. Use workspace/job/Unity Catalog security.

---

## Final Quality Gate

Before marking a conversion complete, verify:

- [ ] All DML operations are Delta-compatible.
- [ ] All procedural logic is replaced or documented.
- [ ] All parameters are handled safely.
- [ ] All temp objects are converted.
- [ ] All transactions are redesigned.
- [ ] All error handling is converted.
- [ ] All permissions are mapped.
- [ ] All high-risk constructs are documented.
- [ ] Differential validation plan exists.