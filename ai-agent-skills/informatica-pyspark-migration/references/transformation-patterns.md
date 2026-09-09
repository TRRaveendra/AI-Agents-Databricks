# Informatica Transformation → PySpark / Spark SQL Patterns

## 1. Source Qualifier

**Informatica**: Source Qualifier with optional SQL Override, Filter, Sorter, Pre-SQL, Post-SQL.

**Pattern**:
```python
def read_source(spark, table_map, params, source_key="src_orders"):
    src = table_map[source_key]
    if src.get("type") == "jdbc":
        df = (spark.read.format("jdbc")
              .option("url", src["url"])
              .option("dbtable", src.get("dbtable") or src["query"])
              .option("user", src["user"])
              .option("password", src["password"])  # prefer secrets
              .load())
    elif src.get("type") == "delta":
        df = spark.table(src["table"])
    elif src.get("type") == "files":
        df = (spark.readStream.format("cloudFiles")  # Auto Loader preferred
              .option("cloudFiles.format", src.get("format", "parquet"))
              .option("cloudFiles.schemaLocation", src["schema_location"])
              .load(src["path"]))
    else:
        df = spark.read.format(src.get("format", "parquet")).load(src["path"])

    if "filter" in params:
        df = df.filter(params["filter"])
    return df
```

- Push Filter and simple projections into the source read when possible.
- SQL Override → use as the JDBC query or as a Spark SQL view.

## 2. Expression Transformation

Map each port expression. Common function translations:

| Informatica | Spark SQL / PySpark |
|-------------|---------------------|
| IIF(cond, a, b) | `CASE WHEN cond THEN a ELSE b END` / `F.when(cond, a).otherwise(b)` |
| DECODE(val, s1, r1, s2, r2, def) | nested `when` or `CASE` |
| ISNULL / IS_NULL | `IS NULL` / `F.isnull` |
| NVL / NVL2 | `COALESCE` / `F.coalesce` |
| TO_DATE / TO_CHAR | `to_date`, `date_format` |
| SUBSTR / INSTR | `substring`, `instr` / `locate` |
| LTRIM / RTRIM / TRIM | `ltrim`, `rtrim`, `trim` |
| ROUND / TRUNC | `round`, `trunc` |
| ADD_TO_DATE / DATE_DIFF | `date_add`, `datediff` |
| REPLACESTR | `regexp_replace` or `replace` |

Always keep the original port name as the column name unless renaming is required for target.

## 3. Filter

```python
df = df.filter(F.col("status") == "ACTIVE")  # or Spark SQL WHERE
```

Push down to source when the filter is simple and source supports it.

## 4. Joiner

```python
# Master = left, Detail = right (common convention)
joined = master_df.join(
    F.broadcast(detail_df) if is_small else detail_df,
    on=join_keys,
    how=join_type  # "inner", "left", "right", "full"
)
```

- Sorted Input option in Informatica is rarely needed in Spark; omit unless proven beneficial.
- Heterogeneous joins (different sources) become ordinary DataFrame joins.

## 5. Lookup (Connected)

Preferred: rewrite as join.

```python
lookup_df = spark.table(table_map["lkp_product"]).select("product_id", "product_name", "category")
result = source_df.join(
    F.broadcast(lookup_df),  # if lookup is small / cached in Informatica
    on="product_id",
    how="left"
).withColumn("product_name", F.coalesce(F.col("product_name"), F.lit("Unknown")))
```

- Lookup cache (static / dynamic) → broadcast or normal join.
- Multiple return ports → select the needed columns.
- Lookup condition with inequality → still a join; may need range join hints for large data.

## 6. Lookup (Unconnected)

High-risk. Prefer rewrite to a join or a map-side lookup:

```python
# Option A – join rewrite (preferred)
# Option B – collect small lookup as dict (driver) and use UDF only if unavoidable
lookup_map = {r.product_id: r.product_name for r in lookup_df.collect()}
broadcast_map = spark.sparkContext.broadcast(lookup_map)

@F.udf("string")
def lkp(pid):
    return broadcast_map.value.get(pid, "Unknown")

df = df.withColumn("product_name", lkp(F.col("product_id")))
```

Flag for manual review when the unconnected lookup is called inside complex expressions or in Aggregator.

## 7. Router

```python
active_df = df.filter(F.col("status") == "A")
inactive_df = df.filter(F.col("status") == "I")
default_df = df.filter(~F.col("status").isin("A", "I"))
# Each output group becomes a separate DataFrame that may feed different targets
```

## 8. Aggregator

```python
agg_df = (df.groupBy("customer_id", "order_month")
          .agg(
              F.sum("amount").alias("total_amount"),
              F.count("*").alias("order_cnt"),
              F.max("order_date").alias("last_order")
          ))
```

- Sorted Input is not required.
- Watch for very high cardinality group keys → may need repartition or AQE.

## 9. Sorter

```python
df = df.orderBy(F.col("order_date").desc(), F.col("customer_id"))
```

Often the Sorter can be removed if the only purpose was to feed a Joiner or Aggregator that no longer needs global order. Keep only when order is contractually required (e.g., downstream file order).

## 10. Rank

```python
from pyspark.sql.window import Window
w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
ranked = (df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") <= 5))
```

## 11. Sequence Generator

Informatica CURRVAL / NEXTVAL has session-scoped, restartable semantics that differ from Spark.

Preferred replacements (document the choice):

- Surrogate key: Delta `GENERATED ALWAYS AS IDENTITY` or `monotonically_increasing_id()` (not gap-free).
- Dense sequential within partition: `row_number()` over a window.
- Global unique: `uuid()` or hash of business keys.

Never assume the generated values will match Informatica numbers.

## 12. Update Strategy + Multi-target

```python
# Classic DD_INSERT / DD_UPDATE / DD_DELETE pattern → Delta MERGE
from delta.tables import DeltaTable

delta_tgt = DeltaTable.forName(spark, table_map["tgt_orders"]["table"])

(delta_tgt.alias("t")
 .merge(source_df.alias("s"), "t.order_id = s.order_id")
 .whenMatchedUpdateAll(condition="s.dd_op = 1")   # DD_UPDATE
 .whenMatchedDelete(condition="s.dd_op = 2")      # DD_DELETE
 .whenNotMatchedInsertAll(condition="s.dd_op = 0") # DD_INSERT
 .execute())
```

If the original mapping wrote to multiple targets with different Update Strategies, emit separate MERGE or write statements, one per target.

## 13. Union

```python
result = df1.unionByName(df2, allowMissingColumns=True)
```

## 14. Mapplet

Extract the mapplet logic into a reusable Python function:

```python
def mapplet_customer_enrichment(df, lookup_df):
    # ... expression + lookup logic ...
    return enriched_df
```

Call it from every mapping that used the mapplet.

## 15. Incremental / CDC Patterns

- File landing → Auto Loader (`cloudFiles`) + checkpoint.
- Database CDC → Lakeflow Apply Changes or MERGE on change tables.
- Informatica CDC mappings usually become a streaming or micro-batch job with MERGE.
