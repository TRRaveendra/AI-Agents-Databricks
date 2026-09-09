# Transformation Patterns: Informatica → PySpark

Before/after for every major PowerCenter transformation. Each section states the
Informatica semantics first, because the semantics — not the code — are what
migrations get wrong.

## Contents

1. [Sources and Source Qualifier](#1-sources-and-source-qualifier)
2. [Targets](#2-targets)
3. [Expression](#3-expression)
4. [Filter](#4-filter)
5. [Router](#5-router)
6. [Joiner](#6-joiner)
7. [Lookup — connected](#7-lookup--connected)
8. [Lookup — unconnected](#8-lookup--unconnected)
9. [Aggregator](#9-aggregator)
10. [Sorter](#10-sorter)
11. [Rank](#11-rank)
12. [Normalizer](#12-normalizer)
13. [Union](#13-union)
14. [Sequence Generator](#14-sequence-generator)
15. [Update Strategy](#15-update-strategy)
16. [SCD Type 2](#16-scd-type-2)
17. [Mapplet](#17-mapplet)
18. [Transaction Control](#18-transaction-control)
19. [Stored Procedure / SQL / Java transformations](#19-stored-procedure--sql--java-transformations)
20. [Reject and error handling](#20-reject-and-error-handling)
21. [Datatype mapping](#21-datatype-mapping)

---

## 1. Sources and Source Qualifier

The Source Qualifier is where PowerCenter decides what SQL actually hits the
database. Check four properties before writing any read:

- **SQL Query** (override) — if present, this replaces the generated SQL entirely.
- **Source Filter** — appended as a WHERE clause.
- **Number of Sorted Ports** — adds an ORDER BY.
- **Select Distinct** — adds DISTINCT.
- **User Defined Join** — joins homogeneous sources inside the SQ, before any Joiner.

### Relational source, no override

```python
# SQ_CUSTOMER over Oracle CUSTOMER, Source Filter: STATUS = 'A'
src = (spark.table(table_map["src_customer"])
             .filter(F.col("status") == F.lit("A")))
```

Prefer a Unity Catalog table (already ingested, or federated via Lakehouse
Federation) over a raw JDBC read. Use JDBC only when the source genuinely still
lives on-prem and there is no landing layer:

```python
src = (spark.read.format("jdbc")
       .option("url", params["src_jdbc_url"])
       .option("dbtable", f"({sql_override}) q")
       .option("user", dbutils.secrets.get(params["secret_scope"], "src_user"))
       .option("password", dbutils.secrets.get(params["secret_scope"], "src_pwd"))
       .option("partitionColumn", "cust_id")
       .option("lowerBound", params["lb"]).option("upperBound", params["ub"])
       .option("numPartitions", 8)
       .load())
```

Without `partitionColumn`, a JDBC read is single-threaded — this is the most
common reason a converted pipeline is slower than the PowerCenter original,
which used session partitioning.

### SQL override

Keep hand-tuned override SQL verbatim rather than re-deriving it. It usually
encodes years of tuning and business rules that appear nowhere else.

```python
# SQ_ORDERS SQL Override preserved as-is; $$LOAD_DATE bound as a parameter.
sql_override = f"""
  SELECT o.order_id, o.cust_id, o.amount, o.order_dt
  FROM   orders o
  WHERE  o.order_dt >= DATE '{params['load_date']}'
"""
```

If the override contains `$$` variables, substitute from `params` — and validate
the values rather than string-concatenating whatever arrives, so a parameter
file cannot turn into SQL injection.

### Flat file source

Informatica flat-file sources define delimiter, quote character, header rows,
null character, and *fixed-width* offsets. Map them explicitly; do not rely on
`inferSchema`, which will not reproduce the mapping's declared precision.

```python
src = (spark.readStream.format("cloudFiles")          # Auto Loader for incremental
       .option("cloudFiles.format", "csv")
       .option("cloudFiles.schemaLocation", table_map["schema_loc_orders"])
       .option("header", True).option("sep", "|").option("quote", '"')
       .schema(orders_schema)                          # declared, not inferred
       .load(table_map["landing_orders"]))
```

Use `spark.read` (batch) when the session processed a single fixed file per run;
Auto Loader when it processed "whatever is in the directory", which is the case
PowerCenter handled with indirect file lists.

### Indirect file list

An Informatica indirect source reads a file that contains a list of file paths.
In Spark, read the manifest, then pass the paths:

```python
paths = [r.path for r in spark.read.text(table_map["manifest"]).collect()]
src = spark.read.schema(orders_schema).option("header", True).csv(paths)
```

This is one of the few legitimate uses of `collect()` — the manifest is tiny.

---

## 2. Targets

| Informatica target load type | Databricks write |
|---|---|
| Insert only, truncate target | `.mode("overwrite")` |
| Insert only, append | `.mode("append")` |
| Update else Insert / Update Strategy | `MERGE` |
| Delete | `MERGE ... whenMatchedDelete()` or `DELETE FROM` |
| Bulk load | `.mode("append")` — Delta has no separate bulk mode |

```python
(target_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "false")     # deliberate: Informatica is strictly typed
    .saveAsTable(table_map["dim_customer"]))
```

Two Informatica target properties that people forget to carry over:

- **Truncate target table option** — if set, the converted job must overwrite,
  and any incremental logic you inferred is wrong.
- **Target update override** — a hand-written UPDATE statement; read it, because
  it often updates a *different* set of columns than the ports suggest.

---

## 3. Expression

An Expression transformation contains input, **variable**, and output ports.
Variable ports evaluate top-to-bottom and can reference previously-evaluated
variables in the same row — and, critically, retain their value across rows,
which is how PowerCenter implements running totals and change detection.

### Ordinary ports → `withColumns`

```python
# EXP_CLEANSE ports:
#   v_name    (variable) = LTRIM(RTRIM(CUST_NAME))
#   o_name    (output)   = UPPER(v_name)
#   o_status  (output)   = IIF(ISNULL(STATUS), 'A', STATUS)
#   o_full    (output)   = FIRST_NAME || ' ' || LAST_NAME
cleansed = df.withColumns({
    "o_name":   F.upper(F.trim(F.col("cust_name"))),
    "o_status": F.coalesce(F.col("status"), F.lit("A")),
    # Informatica || treats NULL as empty string; Spark concat returns NULL.
    "o_full":   F.concat_ws("", F.col("first_name"), F.lit(" "), F.col("last_name")),
})
```

Inline variable ports that are used once. Keep them as intermediate columns
(prefixed `v_`) when reused several times, then `.drop()` them at the end — this
keeps the code diffable against the original mapping.

### Row-carrying variable ports — high risk

```
v_prev_id = v_curr_id        -- reads the PREVIOUS row's value
v_curr_id = CUST_ID
o_is_new  = IIF(v_prev_id != v_curr_id, 1, 0)
```

This is a lag over an ordered stream. It only has meaning if the session had
sorted input; without a defined order, the original mapping was already
non-deterministic. Convert with a window and **state the ordering assumption**:

```python
# RISK: original relied on row order from SRT_CUST (cust_id ASC). Verified against
# session property "Sorted Input" = Yes.
w = Window.orderBy("cust_id")          # add partitionBy if the mapping grouped
out = df.withColumn("v_prev_id", F.lag("cust_id").over(w)) \
        .withColumn("o_is_new",
                    F.when(F.col("v_prev_id").eqNullSafe(F.col("cust_id")), 0).otherwise(1))
```

A `Window.orderBy` with no `partitionBy` funnels all data to one partition. If
the volume is large, this is a design problem to raise with the user, not
something to bury.

---

## 4. Filter

```python
# FIL_ACTIVE, Filter Condition: STATUS = 'A' AND NOT ISNULL(EMAIL)
filtered = df.filter((F.col("status") == F.lit("A")) & F.col("email").isNotNull())
```

Semantics to preserve: PowerCenter drops rows where the condition evaluates to
FALSE **or NULL**. Spark `.filter()` does the same, so the default translation
is correct. But if the original condition used `IIF` with a NULL-safe default,
reproduce that explicitly rather than relying on the coincidence.

---

## 5. Router

A Router has one input group and N output groups plus a DEFAULT group. Group
conditions are evaluated independently, so **a row can go to more than one
group** — this is the difference from a chain of filters, and it is easy to get
wrong.

```python
# RTR_REGION: groups NA (region='NA'), EU (region='EU'), DEFAULT
src = df.cache()   # legitimate: consumed by three branches

grp_na  = src.filter(F.col("region") == F.lit("NA"))
grp_eu  = src.filter(F.col("region") == F.lit("EU"))
# DEFAULT = rows matching no group condition
grp_def = src.filter(~((F.col("region") == F.lit("NA")) |
                       (F.col("region") == F.lit("EU"))).eqNullSafe(F.lit(True)))
```

The `eqNullSafe(True)` wrapper on the DEFAULT branch matters: a row with
`region IS NULL` matches no group and must land in DEFAULT, but a plain `~(...)`
of a NULL condition yields NULL and drops the row.

---

## 6. Joiner

The naming is counter-intuitive and is the single most common conversion bug.
In PowerCenter, the **master** source is cached in memory and the **detail**
source is streamed:

| Informatica join type | Rows kept | PySpark |
|---|---|---|
| Normal Join | matching only | `detail.join(master, cond, "inner")` |
| **Master Outer Join** | all **detail** rows | `detail.join(master, cond, "left")` |
| **Detail Outer Join** | all **master** rows | `detail.join(master, cond, "right")` |
| Full Outer Join | all rows both sides | `detail.join(master, cond, "full")` |

Read the type as "the *master* is the outer/optional side" for Master Outer.

```python
# JNR_CUST_ORDER: master = customer (small), detail = orders, Master Outer Join
joined = orders.join(F.broadcast(customer),
                     orders.cust_id == customer.cust_id,
                     "left")
```

Other semantics:

- A Normal Join drops rows where the join key is NULL on either side (NULL never
  equals NULL). Spark `==` behaves identically. Only use `eqNullSafe` if the
  mapping deliberately joined on a NULL-substituted expression.
- PowerCenter's Joiner supports only equality conditions joined by AND. If you
  see a non-equi join, it came from a Source Qualifier user-defined join or a
  Lookup override, not a Joiner.
- Broadcasting the master mirrors the original design (master was cached), so it
  is usually the right call — but check the actual size, not the label.

---

## 7. Lookup — connected

A connected Lookup is a left join with a **multiple-match policy** that decides
what happens when the lookup source has more than one matching row:

| Lookup policy on multiple match | PySpark |
|---|---|
| Use First Value | dedupe with `row_number()` ordered by the lookup's declared order |
| Use Last Value | same, ordered descending |
| Use Any Value | `dropDuplicates(keys)` — non-deterministic, note it |
| Report Error | join, then assert no key has count > 1, and route violators to rejects |
| Use All Values | plain join — this is the only policy that fans out rows |

```python
# LKP_COUNTRY: condition COUNTRY_CODE = IN_COUNTRY_CODE, policy = Use First Value
w = Window.partitionBy("country_code").orderBy(F.col("country_id").asc())
lkp = (spark.table(table_map["src_country"])
       .withColumn("_rn", F.row_number().over(w))
       .filter(F.col("_rn") == 1).drop("_rn")
       .select("country_code", "country_name", "region"))

out = df.join(F.broadcast(lkp), "country_code", "left")
```

Also check:

- **Lookup SQL Override** — a full SQL statement, possibly filtering the lookup
  set. Reproduce it as a filter/projection on the lookup DataFrame.
- **Lookup Cache** (static/dynamic/persistent). A **dynamic** cache means the
  lookup is updated as rows flow through and later rows see earlier inserts —
  this is a self-referencing loop with no clean Spark equivalent. It is nearly
  always implementing "insert if not exists, reuse key if it does", which maps
  to a `MERGE` plus a re-read, or to SCD Type 2 logic. Flag it and design it
  deliberately.
- **"Return unconnected/first row"** for uncached lookups on huge tables — the
  original may have been doing millions of point lookups; the join version will
  be far faster, which is a win worth pointing out.

---

## 8. Lookup — unconnected — HIGH RISK

Called from inside an expression: `IIF(ISNULL(:LKP.lkp_RATE(CURRENCY, DT)), 1, :LKP.lkp_RATE(CURRENCY, DT))`.
There is no row-scoped function call in Spark, so it must become a join.

```python
# RISK: unconnected lookup :LKP.lkp_RATE rewritten as a broadcast join.
# Verified lkp_RATE returns at most one row per (currency, rate_dt) - if this is
# not true in production, this join changes the row count. See validation checklist.
rates = (spark.table(table_map["fx_rate"])
         .select("currency", "rate_dt", F.col("rate").alias("_lkp_rate")))

out = (df.join(F.broadcast(rates),
               (df.currency == rates.currency) & (df.txn_dt == rates.rate_dt),
               "left")
         .withColumn("conv_rate", F.coalesce(F.col("_lkp_rate"), F.lit(1.0)))
         .drop("_lkp_rate"))
```

Three things to verify with the user before accepting this rewrite:

1. **Uniqueness** of the lookup key. If not unique, the join fans out and the
   row count changes; the original silently returned one row.
2. **Call count.** The same `:LKP` is often called several times per row with
   *different* arguments — each distinct argument set is a separate join.
3. **Conditional invocation.** `IIF(cond, :LKP.x(a), 0)` only looks up when
   `cond` is true. The join is unconditional; that is fine for correctness of the
   result but changes cost, and matters if the lookup source is huge.

If the lookup source is too large to broadcast and the call is genuinely
conditional and rare, splitting the DataFrame (`cond` true → join, `cond` false
→ literal) then `unionByName` is faster than joining everything.

---

## 9. Aggregator

Semantics that differ from `groupBy`:

- **Aggregate functions ignore NULLs** (as does Spark) — but `COUNT(port)` counts
  non-nulls while `COUNT(*)` counts rows, same as Spark. This one is safe.
- **Ungrouped, non-aggregated ports return the value from the *last row* of the
  group** as the group was read. Spark has no "last row" without an ordering, so
  this must become an explicit window or `last(col, ignoreNulls=False)` with a
  stated ordering assumption.
- **No group-by ports at all** → a single output row over the whole dataset.
- **Nested aggregate functions** like `MAX(SUM(sales))` are legal in an
  Informatica Aggregator and require two Spark stages.

```python
# AGG_ORDER_TOTALS: group by CUST_ID; SUM(AMOUNT), COUNT(ORDER_ID),
#                   plus passthrough port LAST_ORDER_DT (last row of group)
w_last = Window.partitionBy("cust_id").orderBy(F.col("order_seq").asc()) \
               .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

agg = (df.withColumn("last_order_dt", F.last("order_dt").over(w_last))
         .groupBy("cust_id")
         .agg(F.sum("amount").alias("total_amount"),
              F.count("order_id").alias("order_cnt"),
              F.first("last_order_dt").alias("last_order_dt")))
```

Two Aggregator properties to check:

- **Sorted Input** — a performance flag in PowerCenter. Drop it; `groupBy` does
  not need it. Do *not* also drop the upstream Sorter if that Sorter's order is
  relied on by a "last row" port.
- **Incremental Aggregation** — the session merges results into a cache from the
  previous run. This is stateful and has no direct equivalent: it becomes either
  a full recompute (simplest, verify it is affordable) or a `MERGE` of deltas
  into an aggregate table. Flag it.

---

## 10. Sorter

```python
# SRT_CUST: keys CUST_ID ASC, ORDER_DT DESC; Distinct = Yes
sorted_df = df.orderBy(F.col("cust_id").asc(), F.col("order_dt").desc())
# Distinct applies to ALL ports, and in Informatica happens after the sort
distinct_df = sorted_df.dropDuplicates()
```

Most Sorters exist only to feed an Aggregator or Joiner and can be deleted
entirely. Keep the sort only when: it feeds a "last row" Aggregator port, a
row-carrying variable port, a Rank, or the target load order genuinely matters
(rare, and usually it does not — Delta does not preserve insertion order in a
way anything should depend on).

---

## 11. Rank

Rank returns the top or bottom N rows per group, and exposes a RANKINDEX port.

```python
# RNK_TOP5_ORDERS: group by CUST_ID, rank port AMOUNT, Top 5
w = Window.partitionBy("cust_id").orderBy(F.col("amount").desc())
ranked = (df.withColumn("rankindex", F.row_number().over(w))
            .filter(F.col("rankindex") <= 5))
```

Tie behaviour differs between `row_number()` (exactly N rows, ties broken
arbitrarily) and `rank()`/`dense_rank()` (may return more than N). PowerCenter's
Rank returns exactly N and breaks ties by arrival order, so `row_number()` is
the closer match — but the *choice among tied rows* is non-deterministic in both
systems. If the data has ties on the rank port, say so, and suggest adding a
deterministic tie-breaker to the `orderBy` so reruns are reproducible.

---

## 12. Normalizer

Two distinct uses:

**Pivoting occurs (repeated columns → rows):**

```python
# NRM_QTR: occurs=4 over Q1_SALES..Q4_SALES, GCID gives the occurrence number
out = df.select(
    "cust_id",
    F.explode(F.arrays_zip(
        F.array(F.lit(1), F.lit(2), F.lit(3), F.lit(4)).alias("gcid"),
        F.array("q1_sales", "q2_sales", "q3_sales", "q4_sales").alias("sales"),
    )).alias("_n")
).select("cust_id", F.col("_n.gcid").alias("gcid_quarter"),
                    F.col("_n.sales").alias("sales"))
```

Or more readably with `stack`:

```python
out = df.selectExpr(
    "cust_id",
    "stack(4, 1, q1_sales, 2, q2_sales, 3, q3_sales, 4, q4_sales) as (gcid_quarter, sales)")
```

**VSAM/COBOL Normalizer** (parsing mainframe copybooks with OCCURS/REDEFINES)
has no Spark equivalent and needs a dedicated reader. Flag it as a scoped
work item, not a translation.

Note the GCID and GK ports: GCID is the occurrence index (reproduced above); GK
is a generated key with the same problems as Sequence Generator (§14).

---

## 13. Union

```python
# UN_ALL_REGIONS: three input groups with identical port lists
out = df_na.unionByName(df_eu).unionByName(df_apac)
```

`unionByName` rather than `union`, because Informatica unions by port name while
`union` matches by position — a column-order difference would produce silently
corrupted data. Informatica's Union does **not** deduplicate; do not add
`distinct()` unless a downstream Sorter had Distinct set.

If groups have differing columns (allowed in some IDMC versions), use
`unionByName(other, allowMissingColumns=True)` and confirm the nulls are intended.

---

## 14. Sequence Generator — HIGH RISK

PowerCenter's Sequence Generator emits contiguous integers from a counter held
in the repository, persisting across sessions (with Start Value, Increment By,
End Value, Cycle, Reset, and Number of Cached Values). Downstream systems
sometimes depend on the *actual values* — gap-free ranges, or key ordering that
matches load order.

Choose deliberately among three options and tell the user what each gives up:

**Option A — Delta IDENTITY (default recommendation)**

```sql
CREATE TABLE main.dw.dim_customer (
  cust_key BIGINT GENERATED ALWAYS AS IDENTITY,
  cust_id  STRING,
  ...
) USING DELTA;
```

Values are unique and increasing. They are **not guaranteed contiguous** —
concurrent or restarted writes leave gaps. Use `GENERATED BY DEFAULT AS IDENTITY`
when you must also insert pre-existing keys during the historical migration.
Note that `MERGE` and `INSERT` work with IDENTITY, but writes from a DataFrame
must omit the identity column.

**Option B — window offset from current max (contiguous, single-writer only)**

```python
max_key = (spark.table(table_map["dim_customer"])
           .agg(F.coalesce(F.max("cust_key"), F.lit(0))).collect()[0][0])
w = Window.orderBy(F.col("cust_id").asc())     # deterministic ordering required
out = df.withColumn("cust_key", F.lit(max_key) + F.row_number().over(w))
```

Contiguous and reproducible, but serializes through one partition and breaks
under concurrent writers. Acceptable for nightly single-writer dimension loads,
which is what most Sequence Generators actually did.

**Option C — `monotonically_increasing_id()`**

Fast, but values are neither contiguous, nor small, nor stable across runs.
Only acceptable when the key is genuinely opaque and never persisted anywhere
that reruns must match. Never use it silently.

If the Sequence Generator is **reusable/shared** across mappings, none of the
above is sufficient on its own — the counter is a global resource. Model it as a
control table with a `MERGE`-based allocator, and say plainly that this is a
design decision requiring the user's input.

---

## 15. Update Strategy

Flags: `DD_INSERT` (0), `DD_UPDATE` (1), `DD_DELETE` (2), `DD_REJECT` (3).

```python
# UPD_STRATEGY expression:
#   IIF(ISNULL(LKP_CUST_KEY), DD_INSERT, IIF(CHANGED_FLAG=1, DD_UPDATE, DD_REJECT))
flagged = df.withColumn("_dd",
    F.when(F.col("lkp_cust_key").isNull(), F.lit(0))
     .when(F.col("changed_flag") == 1,      F.lit(1))
     .otherwise(F.lit(3)))

tgt = DeltaTable.forName(spark, table_map["dim_customer"])
(tgt.alias("t")
    .merge(flagged.filter(F.col("_dd").isin(0, 1, 2)).alias("s"),
           "t.cust_id = s.cust_id")
    .whenMatchedDelete(condition="s._dd = 2")
    .whenMatchedUpdateAll(condition="s._dd = 1")
    .whenNotMatchedInsertAll(condition="s._dd = 0")
    .execute())

rejects = flagged.filter(F.col("_dd") == 3)
```

Semantic differences to state:

- **Duplicate source keys.** Delta `MERGE` raises an error when multiple source
  rows match one target row; PowerCenter applied them sequentially, last write
  winning. If the source can contain duplicates, dedupe first (keeping the last
  by the original order) and say that you did.
- **DD_UPDATE on a non-existent row** is a no-op in `MERGE`. In PowerCenter it
  produced a rejected row that appeared in the reject file. If the reject file
  was monitored, add a `whenNotMatched` branch routing those rows to a quarantine
  table instead of dropping them.
- **Multiple targets** from one Update Strategy become multiple `MERGE`s, which
  are separately atomic. If the mapping relied on all-or-nothing behaviour across
  targets, flag it — Delta has no cross-table transaction.
- **"Treat source rows as" session property** overrides the transformation for
  the whole session. Read the session before trusting the mapping.

---

## 16. SCD Type 2

Not an Informatica transformation, but almost every dimension mapping is one,
usually built from a Lookup + Expression (change detect) + Router + two Update
Strategies + Sequence Generator. Recognise the shape and convert it as a unit.

```python
def scd2_merge(spark, src: DataFrame, table_map, params, dry_run=True):
    """Close out changed rows and insert new versions. Business key: cust_id."""
    tgt_name = table_map["dim_customer"]
    hash_cols = ["cust_name", "address", "segment"]        # tracked attributes
    src2 = src.withColumn("row_hash", F.sha2(F.concat_ws("||",
                          *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in hash_cols]), 256))

    current = spark.table(tgt_name).filter(F.col("is_current"))
    changed = (src2.join(current.select("cust_id", F.col("row_hash").alias("t_hash")),
                         "cust_id", "left")
                   .filter(F.col("t_hash").isNull() | (F.col("t_hash") != F.col("row_hash"))))
    if dry_run:
        return {"target_df": changed, "row_counts": {"changed": changed.count()}, "rejects": None}

    tgt = DeltaTable.forName(spark, tgt_name)
    # 1) close the existing current row
    (tgt.alias("t").merge(changed.alias("s"),
        "t.cust_id = s.cust_id AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": F.lit(False),
                                "end_dt":     F.lit(params["load_date"])})
        .execute())
    # 2) insert the new version
    (changed.withColumn("is_current", F.lit(True))
            .withColumn("start_dt", F.lit(params["load_date"]))
            .withColumn("end_dt", F.lit(None).cast("date"))
            .write.format("delta").mode("append").saveAsTable(tgt_name))
    return {"target_df": changed, "row_counts": {"changed": changed.count()}, "rejects": None}
```

The `coalesce(..., "~")` inside the hash matters: without it, a NULL anywhere
makes the whole hash NULL and every row looks changed. Use a sentinel that
cannot appear in the data.

---

## 17. Mapplet

A mapplet is a reusable subgraph. Convert it to a function taking and returning
DataFrames, in its own module, so multiple converted mappings import the same
implementation — this preserves the reuse that motivated the mapplet.

```python
# mplt_address_standardize.py
def mplt_address_standardize(df: DataFrame) -> DataFrame:
    """Mapplet mplt_ADDRESS_STANDARDIZE. Ports in: addr1, addr2, city, zip.
       Ports out: std_addr, std_city, std_zip."""
    return df.withColumns({
        "std_addr": F.upper(F.trim(F.concat_ws(" ", "addr1", "addr2"))),
        "std_city": F.initcap(F.trim(F.col("city"))),
        "std_zip":  F.lpad(F.regexp_replace(F.col("zip"), "[^0-9]", ""), 5, "0"),
    })
```

Mapplet input/output *groups* map to multiple DataFrame arguments/returns. If
the mapplet contained an active transformation (Aggregator, Filter), the row
count changes — document that in the docstring, since callers will assume
passthrough.

---

## 18. Transaction Control

`TC_COMMIT_BEFORE` / `TC_COMMIT_AFTER` / `TC_ROLLBACK_BEFORE` / `TC_ROLLBACK_AFTER`
control commit points mid-stream. Delta commits per write operation, so there is
no equivalent to "commit every time the group key changes".

Usually the transformation exists for one of two reasons:

- **Memory/rollback management** in a row-based engine — irrelevant in Spark;
  delete it.
- **Genuine per-group atomicity**, e.g. one file or partition per group. Convert
  to a loop over groups with one write each, or `partitionBy` on the group
  column, and note the loss of a single atomic commit across groups.

---

## 19. Stored Procedure / SQL / Java transformations

These execute code that does not live in the mapping. You cannot convert what
you cannot see — request the source first, then:

- **Stored Procedure (unconnected, per-row)** — the row-by-row call pattern is
  the problem, not the SQL. Rewrite the procedure's logic as set-based Spark, or
  keep the procedure in the source database and call it once per batch.
- **Stored Procedure (pre/post session)** — becomes a `spark.sql()` call or a SQL
  task in the job before/after the main task. Straightforward.
- **SQL transformation (query mode)** — becomes `spark.sql()` with the query
  rewritten to Databricks SQL dialect; check for vendor-specific functions.
- **Java / Custom / External Procedure** — port by hand. Estimate generously and
  do not present a machine translation of Java as production-ready.

---

## 20. Reject and error handling

PowerCenter writes rejected rows to a `.bad` file and `ERROR()`/`ABORT()` calls
to the session log. Reproduce this explicitly — the operations team relies on it.

```python
# ERROR('Invalid amount') in an expression -> quarantine, don't fail the job
valid   = df.filter(F.col("amount") >= 0)
rejects = (df.filter(~(F.col("amount") >= 0).eqNullSafe(F.lit(True)))
             .withColumn("_reject_reason", F.lit("Invalid amount"))
             .withColumn("_mapping",   F.lit("m_CUSTOMER_DIM_LOAD"))
             .withColumn("_run_ts",    F.current_timestamp()))

if not dry_run:
    rejects.write.format("delta").mode("append").saveAsTable(table_map["reject_log"])
```

`ABORT()` is different from `ERROR()`: it stops the session. Translate it to
raising an exception after the check, so the job fails rather than silently
producing partial output.

---

## 21. Datatype mapping

| Informatica transformation datatype | Spark / Delta |
|---|---|
| string, nstring, text, ntext | `StringType` |
| integer | `IntegerType` |
| bigint | `LongType` |
| small integer | `ShortType` |
| decimal(p,s) — **high precision ON** | `DecimalType(p, s)` (p ≤ 38) |
| decimal(p,s) — **high precision OFF** | `DoubleType` — this is what the session actually did |
| double, real | `DoubleType` |
| date/time | `TimestampType` |
| binary, raw | `BinaryType` |

Three traps:

1. **Enable High Precision** is a *session* property. With it off, PowerCenter
   handled Decimal as Double, so a "faithful" `DecimalType` conversion produces
   different values than production did. Check the session, then decide with the
   user whether to reproduce the legacy behaviour or fix it (usually fix it, but
   deliberately, with a note in the cutover plan).
2. **Precision above 28 digits** was ignored by PowerCenter unless high precision
   was on. Delta supports up to 38.
3. **Trailing spaces.** Informatica CHAR ports were often space-padded and
   compared with padding. If a converted join or filter starts matching rows the
   original missed, this is usually why — check whether the mapping trimmed
   explicitly or relied on the port datatype.
