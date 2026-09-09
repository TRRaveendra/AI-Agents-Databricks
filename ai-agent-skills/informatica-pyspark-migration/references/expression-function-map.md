# Informatica Expression Language → Spark

Do not translate these from memory. The null-handling and rounding differences
below are where migrations quietly break.

## Contents

1. [The five differences that cause most bugs](#1-the-five-differences-that-cause-most-bugs)
2. [Conditional and null functions](#2-conditional-and-null-functions)
3. [String functions](#3-string-functions)
4. [Numeric functions](#4-numeric-functions)
5. [Date functions](#5-date-functions)
6. [Date format tokens](#6-date-format-tokens)
7. [Conversion and test functions](#7-conversion-and-test-functions)
8. [Aggregate and window functions](#8-aggregate-and-window-functions)
9. [Encoding, hashing, encryption](#9-encoding-hashing-encryption)
10. [Variable, error and system functions](#10-variable-error-and-system-functions)
11. [Operators](#11-operators)

---

## 1. The five differences that cause most bugs

| # | Informatica | Spark | Fix |
|---|---|---|---|
| 1 | `DECODE` matches **NULL to NULL** | `CASE WHEN x = v` yields NULL for NULL x | use `eqNullSafe` / `<=>` |
| 2 | `\|\|` and `CONCAT` treat NULL as **empty string** | `concat` returns NULL if any arg is NULL | use `concat_ws("")` or `coalesce` each arg |
| 3 | `DATE_DIFF` returns a **fractional** number | `datediff` returns whole days | compute from `unix_timestamp` or `months_between` |
| 4 | `SYSDATE` is evaluated **per row** | `current_timestamp()` is constant per query | usually harmless — but say so if timing matters |
| 5 | Decimal handling depends on the **session's high-precision flag** | `DecimalType` is exact | check the session before choosing Decimal vs Double |

---

## 2. Conditional and null functions

| Informatica | PySpark |
|---|---|
| `IIF(cond, t, f)` | `F.when(cond, t).otherwise(f)` |
| `IIF(cond, t)` (two-arg form) | returns the **port datatype's default** (0 for numeric, empty string for string, NULL for date) — not NULL. Reproduce the default explicitly. |
| `DECODE(x, v1, r1, v2, r2, default)` | `F.when(F.col("x").eqNullSafe(v1), r1).when(...).otherwise(default)` |
| `ISNULL(x)` | `F.col("x").isNull()` |
| `NOT ISNULL(x)` | `F.col("x").isNotNull()` |
| `IIF(ISNULL(x), y, x)` (the NVL idiom) | `F.coalesce(F.col("x"), y)` |
| `IN(x, v1, v2, ..., caseflag)` | `F.col("x").isin(v1, v2, ...)`; for `caseflag=0` (case-insensitive) compare on `F.lower` |
| `CHOOSE(idx, v1, v2, ...)` | `F.element_at(F.array(v1, v2, ...), F.col("idx"))` (both 1-based) |
| `GREATEST(a, b, ...)` / `LEAST(...)` | `F.greatest(...)` / `F.least(...)` — **Spark skips NULLs**; if the Informatica version returns NULL when any argument is NULL, wrap: `F.when(a.isNull() \| b.isNull(), None).otherwise(F.greatest(a, b))` |
| `ABORT('msg')` | check the condition, then `raise RuntimeError("msg")` after the DataFrame action |
| `ERROR('msg')` | route the row to a reject DataFrame with `_reject_reason = "msg"` |

`DECODE` deserves care. This mapping expression:

```
DECODE(TRUE,
       SALES > 1000, 'HIGH',
       SALES > 500,  'MED',
       'LOW')
```

is a searched CASE, not a value match, and converts cleanly:

```python
F.when(F.col("sales") > 1000, "HIGH").when(F.col("sales") > 500, "MED").otherwise("LOW")
```

But the value-match form `DECODE(STATUS, NULL, 'UNKNOWN', 'A', 'ACTIVE', 'OTHER')`
relies on NULL matching NULL:

```python
(F.when(F.col("status").eqNullSafe(F.lit(None)), "UNKNOWN")
  .when(F.col("status").eqNullSafe(F.lit("A")), "ACTIVE")
  .otherwise("OTHER"))
```

---

## 3. String functions

| Informatica | PySpark |
|---|---|
| `LTRIM(s)` / `RTRIM(s)` / trim both | `F.ltrim` / `F.rtrim` / `F.trim` |
| `LTRIM(s, trim_set)` | `F.regexp_replace(s, "^[" + re.escape(trim_set) + "]+", "")` — the trim_set is a *character set*, not a substring |
| `SUBSTR(s, start, len)` | `F.substring(s, start, len)` — both 1-based; both treat start 0 as 1; both count negative start from the end |
| `SUBSTR(s, start)` (no length) | `F.expr("substring(s, start)")` |
| `INSTR(s, search)` | `F.instr(s, search)` (1-based, 0 = not found) |
| `INSTR(s, search, start, occurrence)` | no direct equivalent — use `F.expr("locate(search, s, start)")` for the start form; for the Nth occurrence use `F.expr("...")` with `regexp_instr` where available, or a split-based count. Flag it if the occurrence argument is > 1. |
| `LENGTH(s)` | `F.length(s)` — both count trailing spaces |
| `LPAD(s, n, pad)` / `RPAD` | `F.lpad(s, n, pad)` / `F.rpad(s, n, pad)` |
| `UPPER` / `LOWER` / `INITCAP` | `F.upper` / `F.lower` / `F.initcap` |
| `REVERSE(s)` | `F.reverse(s)` |
| `CONCAT(a, b)` and `a \|\| b` | `F.concat_ws("", a, b)` — **not** `F.concat`, which nulls out |
| `REPLACECHR(caseflag, s, old_chars, new_char)` | `F.translate(s, old_chars, new_char * len(old_chars))`; if `new_char` is NULL the characters are deleted → `F.regexp_replace(s, "[chars]", "")` |
| `REPLACESTR(caseflag, s, old1, old2, ..., new)` | chained `F.regexp_replace`; use `(?i)` in the pattern for `caseflag = 0` |
| `REG_MATCH(s, pattern)` | `F.col("s").rlike("^" + pattern + "$")` — REG_MATCH requires the **whole** string to match, `rlike` does not |
| `REG_EXTRACT(s, pattern, n)` | `F.regexp_extract(s, pattern, n)` |
| `REG_REPLACE(s, pattern, repl, n)` | `F.regexp_replace(s, pattern, repl)` — Spark replaces **all** occurrences; if `n` limits replacements, note the difference |
| `SOUNDEX(s)` | `F.soundex(s)` |
| `METAPHONE(s)` | no equivalent — flag it |
| `CHR(n)` / `ASCII(s)` / `CHRCODE(s)` | `F.expr("chr(n)")` / `F.ascii(s)` |
| `INDEXOF(s, str1, str2, ...)` | `F.expr("array_position(array(...), s)")` |
| `IS_SPACES(s)` | `F.trim(F.col("s")) == F.lit("")` and `F.col("s").isNotNull()` |

---

## 4. Numeric functions

| Informatica | PySpark |
|---|---|
| `ABS`, `CEIL`, `FLOOR`, `EXP`, `SQRT`, `SIGN` | `F.abs`, `F.ceil`, `F.floor`, `F.exp`, `F.sqrt`, `F.signum` |
| `POWER(base, exp)` | `F.pow(base, exp)` |
| `LOG(base, exponent)` | `F.log(base, exponent)` — same argument order |
| `LN(x)` | `F.log(x)` |
| `MOD(a, b)` | `a % b` — both take the sign of the dividend. `F.pmod` does **not** (always non-negative) |
| `ROUND(x, n)` | `F.round(x, n)` — both round half away from zero. `F.bround` is half-to-even; do not substitute it |
| `TRUNC(x, n)` | truncation toward zero: `F.expr(f"trunc_num(x, {n})")` is not a Spark builtin — use `F.when(x < 0, F.ceil(x * 10**n)).otherwise(F.floor(x * 10**n)) / 10**n` |
| `CUME(x)` | window: `F.sum("x").over(w.rowsBetween(Window.unboundedPreceding, 0))` |
| `MOVINGSUM(x, n)` | `F.sum("x").over(w.rowsBetween(-(n-1), 0))` |
| `MOVINGAVG(x, n)` | `F.avg("x").over(w.rowsBetween(-(n-1), 0))` |
| `RAND(seed)` | `F.rand(seed)` — values will differ from the original run; never reconcile on a RAND column |
| `CONVERT_BASE(v, from, to)` | `F.conv(v, from, to)` |

`TRUNC` on numbers and `TRUNC` on dates are different functions sharing a name.
Check the argument type before converting.

---

## 5. Date functions

Informatica dates carry sub-second precision and are best mapped to
`TimestampType`, even when the mapping only ever used the date part.

| Informatica | PySpark |
|---|---|
| `SYSDATE` | `F.current_timestamp()` (constant per query, not per row) |
| `SESSSTARTTIME`, `$$$SessStartTime` | `F.lit(params["run_ts"])` — pass the job start time in explicitly |
| `ADD_TO_DATE(d, 'DD', n)` | `F.date_add(d, n)` or `d + F.expr("INTERVAL n DAYS")` |
| `ADD_TO_DATE(d, 'MM', n)` | `F.add_months(d, n)` |
| `ADD_TO_DATE(d, 'YY', n)` | `F.add_months(d, 12 * n)` |
| `ADD_TO_DATE(d, 'HH'/'MI'/'SS', n)` | `d + F.expr(f"INTERVAL {n} HOURS")` etc. |
| `DATE_DIFF(d1, d2, 'DD')` | `(F.unix_timestamp(d1) - F.unix_timestamp(d2)) / 86400.0` — keep the fraction; `F.datediff` truncates to whole days |
| `DATE_DIFF(d1, d2, 'MM')` | `F.months_between(d1, d2)` (also fractional — this one matches) |
| `DATE_COMPARE(d1, d2)` | `F.when(d1 < d2, -1).when(d1 > d2, 1).otherwise(0)` |
| `LAST_DAY(d)` | `F.last_day(d)` |
| `GET_DATE_PART(d, 'YYYY')` | `F.year(d)` (also `F.month`, `F.dayofmonth`, `F.hour`, `F.minute`, `F.second`) |
| `SET_DATE_PART(d, 'MM', 6)` | rebuild with `F.make_timestamp(...)` or `F.trunc` + interval arithmetic |
| `TRUNC(d, 'MM')` | `F.trunc(d, "MM")` (date) / `F.date_trunc("month", d)` (timestamp) |
| `MAKE_DATE_TIME(y, m, d, h, mi, s)` | `F.make_timestamp(y, m, d, h, mi, s)` |
| `TO_DATE(s, fmt)` | `F.to_timestamp(s, spark_fmt)` — convert the format string (§6) |
| `TO_CHAR(d, fmt)` | `F.date_format(d, spark_fmt)` |

`DATE_DIFF` returning a fraction is the trap. `DATE_DIFF(SYSDATE, HIRE_DT, 'YY')`
in a mapping that then applied `TRUNC` produced whole years; if you convert to
`datediff(...)/365` you will disagree at the boundaries.

---

## 6. Date format tokens

Informatica format strings are Oracle-like; Spark uses Java `DateTimeFormatter`
patterns. They are not interchangeable — `MM/DD/YYYY` means nothing sensible to
Spark (`DD` is day-of-year, `YYYY` is week-based-year).

| Informatica | Spark | Meaning |
|---|---|---|
| `YYYY` | `yyyy` | 4-digit year |
| `YY` | `yy` | 2-digit year |
| `MM` | `MM` | month number |
| `MON` | `MMM` | abbreviated month |
| `MONTH` | `MMMM` | full month name |
| `DD` | `dd` | day of month |
| `DDD` | `DDD` | day of year |
| `DY` | `EEE` | abbreviated day name |
| `DAY` | `EEEE` | full day name |
| `HH`, `HH12` | `hh` | hour 1–12 |
| `HH24` | `HH` | hour 0–23 |
| `MI` | `mm` | minutes |
| `SS` | `ss` | seconds |
| `MS` / `US` / `NS` | `SSS` / `SSSSSS` / `SSSSSSSSS` | fractional seconds |
| `AM` / `PM` | `a` | meridiem |
| `Q` | — | quarter: use `F.quarter(d)` separately |
| `J` | — | Julian day: no equivalent |
| `SSSSS` | — | seconds since midnight: compute arithmetically |

Two Spark-side notes worth stating in the output:

- Spark 3+ uses a strict datetime parser. A source value that PowerCenter
  accepted leniently may now throw. Decide deliberately between fixing the data
  and setting `spark.sql.legacy.timeParserPolicy=LEGACY`; prefer fixing the data,
  but note that the legacy setting exists so the user is not stuck at cutover.
- `yyyy` (year-of-era) versus `uuuu` (proleptic year) matters only for BC dates —
  irrelevant for almost all warehouses, but say so if the data has them.

---

## 7. Conversion and test functions

| Informatica | PySpark |
|---|---|
| `TO_CHAR(numeric)` | `F.col("x").cast("string")` — check formatting of decimals; Informatica may emit a different number of trailing zeros |
| `TO_DECIMAL(s, scale)` | `F.col("s").cast(DecimalType(p, scale))` |
| `TO_FLOAT(s)` | `.cast("double")` |
| `TO_INTEGER(s, flag)` | flag controls truncate vs round — `F.floor`/`F.round` then `.cast("int")`. Confirm which the mapping relied on rather than assuming |
| `TO_BIGINT(s, flag)` | same, `.cast("bigint")` |
| `IS_DATE(s, fmt)` | `F.to_timestamp(s, fmt).isNotNull()` |
| `IS_NUMBER(s)` | `F.col("s").cast("double").isNotNull()` — note Informatica accepts leading/trailing spaces and `+`/`-`; Spark's cast is close but test edge cases |
| `IS_SPACES(s)` | see string section |

Invalid casts differ in behaviour: Informatica raises a transformation error and
rejects the row; Spark returns NULL by default (or throws under
`spark.sql.ansi.enabled=true`). If the mapping counted on rejects, add an
explicit validity check and route failures, rather than letting nulls through.

---

## 8. Aggregate and window functions

Used inside an Aggregator transformation:

| Informatica | PySpark |
|---|---|
| `SUM`, `AVG`, `MIN`, `MAX`, `COUNT` | `F.sum`, `F.avg`, `F.min`, `F.max`, `F.count` |
| `COUNT(*)` vs `COUNT(port)` | `F.count("*")` vs `F.count("port")` — same null semantics |
| `FIRST(port)` / `LAST(port)` | `F.first` / `F.last` **with an explicit ordering** — undefined otherwise |
| `MEDIAN(port)` | `F.percentile_approx("port", 0.5)` or `F.expr("percentile(port, 0.5)")` for exact |
| `PERCENTILE(port, p)` | `F.expr(f"percentile(port, {p/100})")` — Informatica takes 0–100, Spark takes 0–1 |
| `STDDEV(port)` | `F.stddev` (sample) — confirm sample vs population |
| `VARIANCE(port)` | `F.variance` (sample) |
| `SUM(x, filter_cond)` (conditional aggregate) | `F.sum(F.when(cond, F.col("x")))` |

The conditional-aggregate form is common and easy to miss: Informatica aggregate
functions take an optional second filter argument.

---

## 9. Encoding, hashing, encryption

| Informatica | PySpark |
|---|---|
| `MD5(s)` | `F.md5(s)` (both 32-char lowercase hex) |
| `CRC32(s)` | `F.crc32(s)` |
| `ENC_BASE64(s)` / `DEC_BASE64(s)` | `F.base64` / `F.unbase64` |
| `AES_ENCRYPT` / `AES_DECRYPT` | `F.expr("aes_encrypt(...)")` — key management moves to Databricks secrets; **do not** carry the literal key from the mapping into code |
| `COMPRESS` / `DECOMPRESS` | no direct equivalent — flag it |

Hash values must match exactly if any downstream table stores them. `MD5` does;
`CRC32` returns a signed bigint in Spark, so compare types before assuming.

---

## 10. Variable, error and system functions

| Informatica | Databricks approach |
|---|---|
| `$$ParameterName` | `params["parameter_name"]` |
| `$PMSessionName`, `$PMWorkflowName` | job/task name from `dbutils.notebook.entry_point` context, or passed in `params` |
| `SETVARIABLE(x, v)` | write to a Delta control table at end of run |
| `SETMAXVARIABLE(x, v)` | `MERGE` into the control table keeping the max — **high risk**, this is often the incremental-extract watermark |
| `SETMINVARIABLE`, `SETCOUNTVARIABLE` | same control-table pattern |
| `ERROR('msg')` | reject row + reason column |
| `ABORT('msg')` | raise an exception |

The watermark pattern deserves an explicit implementation, since it is the most
common persistent variable:

```python
# $$LAST_EXTRACT_DT, previously maintained by SETMAXVARIABLE
wm = (spark.table(table_map["control"])
      .filter(F.col("mapping") == "m_ORDERS_INCR")
      .select("watermark").collect())
last_wm = wm[0]["watermark"] if wm else params["initial_watermark"]

new_rows = src.filter(F.col("order_dt") > F.lit(last_wm))
new_wm   = new_rows.agg(F.max("order_dt")).collect()[0][0] or last_wm

if not dry_run and new_wm is not None:
    ctl = DeltaTable.forName(spark, table_map["control"])
    (ctl.alias("t")
        .merge(spark.createDataFrame([("m_ORDERS_INCR", new_wm)], ["mapping", "watermark"]).alias("s"),
               "t.mapping = s.mapping")
        .whenMatchedUpdate(condition="s.watermark > t.watermark",
                           set={"watermark": "s.watermark"})
        .whenNotMatchedInsertAll()
        .execute())
```

Update the watermark **after** the target write succeeds, not before — otherwise
a failed run silently skips data on the retry. PowerCenter had the same hazard;
do not reproduce the bug if the user is aware of it.

---

## 11. Operators

| Informatica | Spark | Note |
|---|---|---|
| `=` | `==` | on NULL both yield NULL |
| `!=`, `^=`, `<>` | `!=` | |
| `AND`, `OR`, `NOT` | `&`, `\|`, `~` | parentheses are mandatory in PySpark: `(a == 1) & (b == 2)` |
| `\|\|` | `F.concat_ws("")` | NULL handling, see §1 |
| `+ - * /` | same | integer division differs: Informatica `/` on integers returns a decimal; Spark `/` returns double, `div` floors |
| `%` | `%` | |
| `TRUE` / `FALSE` | `F.lit(True)` / `F.lit(False)` | Informatica also accepts 1/0 as boolean |
