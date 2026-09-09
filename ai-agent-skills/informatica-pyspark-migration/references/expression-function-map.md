# Informatica Expression Language → Spark SQL / PySpark Function Map

Use this map when translating Expression Transformation ports or SQL overrides.

## Conditional & Null Handling

| Informatica | Spark SQL | PySpark |
|-------------|-----------|---------|
| IIF(c, a, b) | `CASE WHEN c THEN a ELSE b END` | `F.when(c, a).otherwise(b)` |
| DECODE(v, s1,r1, s2,r2, def) | nested CASE | nested `when` |
| ISNULL(x) / IS_NULL(x) | `x IS NULL` | `F.isnull(x)` / `F.col("x").isNull()` |
| NVL(a,b) | `COALESCE(a,b)` | `F.coalesce(a,b)` |
| NVL2(a,b,c) | `CASE WHEN a IS NOT NULL THEN b ELSE c END` | `F.when(a.isNotNull(), b).otherwise(c)` |
| NULLIF(a,b) | `NULLIF(a,b)` | `F.when(a == b, None).otherwise(a)` |
| COALESCE(...) | `COALESCE(...)` | `F.coalesce(...)` |

## String Functions

| Informatica | Spark SQL / PySpark |
|-------------|---------------------|
| LENGTH / LEN | `length` / `F.length` |
| LOWER / UPPER | `lower` / `upper` |
| LTRIM / RTRIM / TRIM | `ltrim` / `rtrim` / `trim` |
| SUBSTR(s, start, len) | `substring(s, start, len)` (1-based in both) |
| INSTR(s, sub) | `instr(s, sub)` / `locate` |
| REPLACESTR(1, s, old, new) | `replace(s, old, new)` or `regexp_replace` |
| LPAD / RPAD | `lpad` / `rpad` |
| CONCAT / \|\| | `concat` / `||` |
| INITCAP | `initcap` |
| REVERSE | `reverse` |
| SOUNDEX | `soundex` |
| CHR / ASCII | `chr` / `ascii` |
| REG_MATCH / REG_EXTRACT | `rlike` / `regexp_extract` |

## Date & Time

| Informatica | Spark SQL / PySpark |
|-------------|---------------------|
| TO_DATE(s, fmt) | `to_date(s, fmt)` |
| TO_CHAR(d, fmt) / TO_CHAR(n, fmt) | `date_format` / `format_number` |
| GETDATE / SYSDATE | `current_timestamp()` / `current_date()` |
| ADD_TO_DATE(d, 'MM', n) | `add_months(d, n)` or `date_add` |
| DATE_DIFF(d1, d2, 'DD') | `datediff(d1, d2)` |
| TRUNC(d, 'MM') | `trunc(d, 'MM')` / `date_trunc` |
| LAST_DAY | `last_day` |
| MONTHS_BETWEEN | `months_between` |
| ROUND(d, 'YYYY') etc. | `date_trunc` or custom |

Note: Informatica format strings (`MM/DD/YYYY`) must be converted to Spark format (`MM/dd/yyyy`).

## Numeric & Math

| Informatica | Spark |
|-------------|-------|
| ROUND / TRUNC / FLOOR / CEIL | same names |
| ABS / MOD / POWER / SQRT | same |
| TO_DECIMAL / TO_INTEGER / TO_FLOAT | `cast(... as decimal)` / `cast as int` / `cast as double` |
| ISNULL numeric default | `COALESCE(col, 0)` |

## Conversion & Casting

Prefer explicit `cast` in Spark. Informatica’s implicit conversions can be lossy; surface them in the Semantic Notes.

## Aggregate Functions (inside Aggregator)

| Informatica | Spark |
|-------------|-------|
| SUM / AVG / COUNT / MIN / MAX | same |
| FIRST / LAST | `first` / `last` (with `ignorenulls`) |
| MEDIAN | `percentile_approx` or `median` (Spark 3.4+) |
| STDDEV / VARIANCE | `stddev` / `variance` |

## Lookup-related in Expressions

`:LKP.LOOKUP_NAME(args)` → rewrite as join or UDF; never leave as-is.

## Variables & Parameters

- `$$MappingVariable` / `$SessionParameter` → function argument from `params` dict.
- `ABORT('msg')` → raise exception or use `assert` + controlled error handling.
- `ERROR('msg')` → same.

## Best Practice

When an expression is complex, emit both:

1. The Spark SQL version (readable for analysts).
2. The equivalent DataFrame `withColumn` chain (for composition).

Always unit-test a sample of rows that exercise every branch of IIF / DECODE.
