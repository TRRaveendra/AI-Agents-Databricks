# T-SQL to Spark SQL Functions Mapping

Complete reference for T-SQL built-in functions and their Spark SQL equivalents, organized
by category. Based on patterns found in real-world stored procedure migrations.

## Date and Time Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| GETDATE() | current_timestamp() | Returns current timestamp |
| SYSDATETIME() | current_timestamp() | Same as GETDATE in Spark |
| GETUTCDATE() | current_timestamp() | Spark uses UTC by default |
| CURRENT_TIMESTAMP | current_timestamp() | Identical |
| DATEPART(year, col) | year(col) | Extract year |
| DATEPART(month, col) | month(col) | Extract month (1-12) |
| DATEPART(day, col) | day(col) or dayofmonth(col) | Extract day |
| DATEPART(hour, col) | hour(col) | Extract hour |
| DATEPART(minute, col) | minute(col) | Extract minute |
| DATEPART(second, col) | second(col) | Extract second |
| DATEPART(week, col) | weekofyear(col) | Week number |
| DATEPART(quarter, col) | quarter(col) | Quarter (1-4) |
| DATEPART(dayofyear, col) | dayofyear(col) | Day of year (1-366) |
| DATEPART(weekday, col) | dayofweek(col) | Day of week (1=Sun to 7=Sat) |
| YEAR(col) | year(col) | Identical |
| MONTH(col) | month(col) | Identical |
| DAY(col) | day(col) or dayofmonth(col) | Identical |
| DATEDIFF(day, a, b) | datediff(b, a) | **Note: arguments reversed!** Returns b - a in days |
| DATEDIFF(month, a, b) | months_between(b, a) | Returns months difference |
| DATEDIFF(year, a, b) | year(b) - year(a) | No direct function |
| DATEADD(day, n, col) | date_add(col, n) | Add n days |
| DATEADD(month, n, col) | add_months(col, n) | Add n months |
| DATEADD(year, n, col) | add_months(col, n*12) | Add years via months |
| EOMONTH(col) | last_day(col) | Last day of month |
| DATEFROMPARTS(y,m,d) | make_date(y, m, d) | Create date from parts |
| CONVERT(VARCHAR(10), d, 120) | date_format(d, 'yyyy-MM-dd') | Format date as string |
| CONVERT(VARCHAR(23), d, 121) | date_format(d, 'yyyy-MM-dd HH:mm:ss.SSS') | ISO format with ms |
| CAST(col AS DATE) | to_date(col) or cast(col AS DATE) | Convert to date |
| CAST(col AS DATETIME) | to_timestamp(col) or cast(col AS TIMESTAMP) | Convert to timestamp |

### Example from history_procedure.sql

**T-SQL:**
```sql
CONVERT(VARCHAR(10), soh.OrderDate, 120) AS OrderDateText
```

**Spark SQL:**
```sql
date_format(soh.order_date, 'yyyy-MM-dd') AS order_date_text
```

### DATEDIFF Argument Order - CRITICAL!

**T-SQL:**
```sql
DATEDIFF(day, LastOrderDate, @EndDate)  -- Returns @EndDate - LastOrderDate
```

**Spark SQL:**
```sql
datediff(end_date, last_order_date)  -- Returns end_date - last_order_date
-- Note: arguments are REVERSED!
```

## String Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| LEN(col) | length(col) | String length |
| LTRIM(col) | ltrim(col) | Trim left spaces |
| RTRIM(col) | rtrim(col) | Trim right spaces |
| TRIM(col) | trim(col) | Trim both sides |
| UPPER(col) | upper(col) | Uppercase |
| LOWER(col) | lower(col) | Lowercase |
| SUBSTRING(col, start, len) | substring(col, start, len) | Extract substring |
| LEFT(col, n) | left(col, n) or substring(col, 1, n) | First n chars |
| RIGHT(col, n) | right(col, n) | Last n chars |
| CHARINDEX(find, col) | instr(col, find) | Position of substring (1-based) |
| REPLACE(col, old, new) | replace(col, old, new) | Replace text |
| CONCAT(a, b, c) | concat(a, b, c) | Concatenate strings |
| a + b (string concat) | concat(a, b) or a \|\| b | Spark + is arithmetic only |
| REPLICATE(col, n) | repeat(col, n) | Repeat string n times |
| REVERSE(col) | reverse(col) | Reverse string |
| STRING_AGG(col, ',') | array_join(collect_list(col), ',') | Aggregate to delimited string |
| STRING_AGG(col, ',') WITHIN GROUP (ORDER BY x) | array_join(collect_list(col ORDER BY x), ',') | Ordered aggregation |
| FORMAT(num, 'N2') | format_number(num, 2) | Format number with decimals |
| STUFF(col, start, len, new) | overlay(col PLACING new FROM start FOR len) | Replace substring |

### Example from sales_procedure.sql

**T-SQL:**
```sql
STRING_AGG(p.ProductName, ', ') AS Products
```

**Spark SQL:**
```sql
array_join(collect_list(p.product_name), ', ') AS products
```

### String Concatenation

**T-SQL:**
```sql
SELECT FirstName + ' ' + LastName AS FullName
```

**Spark SQL:**
```sql
-- Option 1: concat function
SELECT concat(first_name, ' ', last_name) AS full_name

-- Option 2: || operator
SELECT first_name || ' ' || last_name AS full_name
```

## NULL Handling Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| ISNULL(col, default) | coalesce(col, default) or nvl(col, default) | Return default if NULL |
| COALESCE(a, b, c) | coalesce(a, b, c) | Return first non-NULL |
| NULLIF(a, b) | nullif(a, b) | Return NULL if a=b, else a |

### Example from customer_procedure.sql

**T-SQL:**
```sql
ISNULL(CompanyName, 'UNKNOWN') AS DisplayName
```

**Spark SQL:**
```sql
coalesce(company_name, 'UNKNOWN') AS display_name
-- Or:
nvl(company_name, 'UNKNOWN') AS display_name
```

## Aggregate Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| COUNT(*) | count(*) | Identical |
| COUNT(col) | count(col) | Count non-NULL values |
| COUNT(DISTINCT col) | count(distinct col) | Count unique |
| SUM(col) | sum(col) | Identical |
| AVG(col) | avg(col) or mean(col) | Identical |
| MIN(col) | min(col) | Identical |
| MAX(col) | max(col) | Identical |
| STDEV(col) | stddev(col) | Standard deviation |
| VAR(col) | variance(col) | Variance |
| STRING_AGG | array_join(collect_list(col), delim) | See String Functions |

## Conversion and Type Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| CAST(col AS INT) | cast(col AS INT) or col::INT | Identical |
| CONVERT(INT, col) | cast(col AS INT) | Use CAST in Spark |
| TRY_CAST(col AS INT) | try_cast(col AS INT) | Returns NULL on failure (Spark 3.2+) |
| CONVERT(VARCHAR(10), d, 120) | date_format(d, 'yyyy-MM-dd') | Date format conversion |
| STR(num) | cast(num AS STRING) | Number to string |
| PARSE(col AS INT) | cast(col AS INT) | Use CAST |

## Mathematical Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| ABS(col) | abs(col) | Absolute value |
| ROUND(col, n) | round(col, n) | Round to n decimals |
| CEILING(col) | ceil(col) or ceiling(col) | Round up |
| FLOOR(col) | floor(col) | Round down |
| POWER(base, exp) | power(base, exp) or pow(base, exp) | Exponentiation |
| SQRT(col) | sqrt(col) | Square root |
| SIGN(col) | sign(col) | Sign of number (-1, 0, 1) |
| MOD(a, b) | a % b or mod(a, b) | Modulo |
| RAND() | rand() | Random number 0-1 |
| RAND(seed) | rand(seed) | Random with seed |

## Conditional Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| CASE WHEN ... END | CASE WHEN ... END | Identical |
| IIF(condition, true, false) | if(condition, true, false) | Ternary conditional |
| CHOOSE(index, val1, val2, ...) | No direct equivalent | Use CASE WHEN |

### Example from order_procedure.sql

**T-SQL:**
```sql
CASE WHEN SUM(NetLineTotal) >= 10000 THEN 'Large'
     WHEN SUM(NetLineTotal) >= 1000  THEN 'Medium'
     ELSE 'Small'
END AS RevenueBand
```

**Spark SQL:**
```sql
CASE WHEN SUM(net_line_total) >= 10000 THEN 'Large'
     WHEN SUM(net_line_total) >= 1000  THEN 'Medium'
     ELSE 'Small'
END AS revenue_band
```

## Window Functions

**Note:** Window functions are nearly identical in T-SQL and Spark SQL.

| T-SQL | Spark SQL | Notes |
|---|---|---|
| ROW_NUMBER() OVER (...) | ROW_NUMBER() OVER (...) | Identical |
| RANK() OVER (...) | RANK() OVER (...) | Identical |
| DENSE_RANK() OVER (...) | DENSE_RANK() OVER (...) | Identical |
| NTILE(n) OVER (...) | NTILE(n) OVER (...) | Identical |
| LAG(col, n) OVER (...) | LAG(col, n) OVER (...) | Identical |
| LEAD(col, n) OVER (...) | LEAD(col, n) OVER (...) | Identical |
| FIRST_VALUE(col) OVER (...) | FIRST_VALUE(col) OVER (...) | Identical |
| LAST_VALUE(col) OVER (...) | LAST_VALUE(col) OVER (...) | Identical |
| SUM(col) OVER (...) | SUM(col) OVER (...) | Running totals |
| AVG(col) OVER (...) | AVG(col) OVER (...) | Moving averages |

### Example from order_procedure.sql

**T-SQL:**
```sql
ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderRevenue DESC, SalesOrderID) AS RevenueRank
```

**Spark SQL:**
```sql
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_revenue DESC, sales_order_id) AS revenue_rank
```

## Hash and Encoding Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| HASHBYTES('SHA2_256', col) | sha2(col, 256) | SHA-256 hash |
| HASHBYTES('MD5', col) | md5(col) | MD5 hash |
| CHECKSUM(col) | hash(col) | Murmurhash3 |

## JSON Functions (T-SQL 2016+)

| T-SQL | Spark SQL | Notes |
|---|---|---|
| OPENJSON(json) | from_json(json, schema) | Parse JSON |
| JSON_VALUE(json, '$.path') | get_json_object(json, '$.path') | Extract scalar |
| JSON_QUERY(json, '$.path') | get_json_object(json, '$.path') | Extract object/array |
| FOR JSON PATH | to_json(struct(...)) | Generate JSON |

## System Functions

| T-SQL | Spark SQL | Notes |
|---|---|---|
| @@ROWCOUNT | See control-flow-procedural.md | Row count after DML |
| @@IDENTITY | Not applicable | Use IDENTITY columns or sequences |
| NEWID() | uuid() | Generate UUID |
| HOST_NAME() | No direct equivalent | Use Spark config |
| USER_NAME() | current_user() | Current user |
| DB_NAME() | current_database() or current_catalog() | Current schema/catalog |

## Functions with No Direct Equivalent

### PIVOT / UNPIVOT

**T-SQL:**
```sql
SELECT * FROM 
    (SELECT Category, Year, Amount FROM Sales) AS src
PIVOT (SUM(Amount) FOR Year IN ([2022], [2023], [2024])) AS pvt;
```

**Spark SQL (DBR 14+):**
```sql
SELECT * FROM
    (SELECT category, year, amount FROM sales)
PIVOT (SUM(amount) FOR year IN (2022, 2023, 2024));
```

**PySpark (older runtimes):**
```python
df.groupBy("category").pivot("year").agg(F.sum("amount"))
```

### FORMAT (date/number formatting)

**T-SQL:**
```sql
FORMAT(OrderDate, 'yyyy-MM-dd')
FORMAT(Price, 'C', 'en-US')  -- Currency
```

**Spark SQL:**
```sql
-- Dates: use date_format
date_format(order_date, 'yyyy-MM-dd')

-- Numbers: use format_number or concat
concat('$', format_number(price, 2))
```

### TRY_CONVERT / TRY_PARSE

**T-SQL:**
```sql
TRY_CONVERT(INT, StringCol)  -- Returns NULL if conversion fails
```

**Spark SQL (3.2+):**
```sql
try_cast(string_col AS INT)
```

**Spark SQL (older):**
```sql
-- Use CASE to handle conversion errors
CASE WHEN string_col RLIKE '^[0-9]+$' THEN cast(string_col AS INT) ELSE NULL END
```

## Performance Notes

### collect_list vs collect_set

* `collect_list(col)` - keeps duplicates, like T-SQL STRING_AGG
* `collect_set(col)` - removes duplicates, like DISTINCT STRING_AGG

**Example:**
```sql
-- T-SQL
STRING_AGG(DISTINCT ProductName, ', ')

-- Spark SQL
array_join(collect_set(product_name), ', ')
```

### GREATEST / LEAST

Not T-SQL functions, but very useful in Spark for rewriting cursor logic:

```sql
-- Clamp negative values to 0 (replaces cursor pattern)
UPDATE gold.inventory
SET quantity_on_hand = GREATEST(quantity_on_hand - sold_qty, 0)
```

## Migration Patterns

### Pattern: CONVERT Date Formats

**T-SQL style codes:**
* 120 = yyyy-mm-dd
* 121 = yyyy-mm-dd hh:mi:ss.mmm
* 101 = mm/dd/yyyy
* 103 = dd/mm/yyyy

**Spark SQL equivalents:**
* 'yyyy-MM-dd'
* 'yyyy-MM-dd HH:mm:ss.SSS'
* 'MM/dd/yyyy'
* 'dd/MM/yyyy'

### Pattern: String Aggregation with Ordering

**T-SQL:**
```sql
STRING_AGG(ProductName, ', ') WITHIN GROUP (ORDER BY Price DESC)
```

**Spark SQL:**
```sql
array_join(collect_list(product_name ORDER BY price DESC), ', ')
```

### Pattern: Multi-Column String Concatenation

**T-SQL:**
```sql
FirstName + ' ' + MiddleName + ' ' + LastName
```

**Spark SQL:**
```sql
concat_ws(' ', first_name, middle_name, last_name)
-- concat_ws automatically handles NULLs
```

---

**Tip:** When migrating, search for function names in the source T-SQL and replace systematically using this reference. Pay special attention to argument order (DATEDIFF) and NULL handling (ISNULL → coalesce).
