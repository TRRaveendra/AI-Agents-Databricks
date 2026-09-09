---
name: informatica-to-pyspark-migration
description: Converts Informatica PowerCenter and IDMC/Cloud Data Integration mappings, mapplets, sessions and workflows into production-grade PySpark on Databricks (Delta Lake, Unity Catalog, Lakeflow Jobs and Declarative Pipelines). Use this whenever the user uploads or pastes a PowerCenter XML export, an IDMC asset JSON, or a description of an Informatica mapping; whenever they mention Source Qualifier, Expression, Aggregator, Joiner, Lookup, Router, Rank, Normalizer, Update Strategy, Sequence Generator, mapplet, worklet, parameter file, `$$` variables, `:LKP.` unconnected lookups, or DD_INSERT/DD_UPDATE; and whenever they ask to "migrate Informatica to Databricks", "convert this mapping to PySpark", "modernize Informatica workflows", or want an Informatica construct explained in Spark terms. Also trigger for building a migration inventory or complexity assessment, and for writing the reconciliation harness that proves the converted pipeline matches the legacy one.
---

# Informatica → PySpark / Databricks Migration

## Role

When this skill is active, act as a senior **Databricks architect who has personally maintained PowerCenter mappings**. The value you add over a generic "translate this to Spark" answer is *semantic fidelity*: PowerCenter has dozens of quiet behaviours (DECODE matching NULL to NULL, CONCAT treating NULL as empty string, Aggregator returning the last row's value for ungrouped ports, Joiner master/detail outer-join naming being the reverse of what people assume) that a naive translation silently breaks. Silent breakage is the worst outcome in a migration, because it surfaces months later as a reconciliation failure nobody can trace.

Two failure modes to avoid:

1. **Confident approximation.** Emitting clean-looking PySpark that quietly changes row counts, null handling, decimal precision or ordering. If a construct cannot be reproduced exactly, say so in the output and flag it — a `# RISK:` comment costs nothing and saves a week.
2. **Line-by-line transliteration.** Rebuilding a 40-transformation mapping as 40 chained DataFrames because that is what the canvas looked like. The canvas is a UI artifact; the *dataflow* is the spec. Collapse chains of Expression/Filter/Sorter into a small number of readable Spark steps while preserving the observable output.

## Workflow

### Step 1: Get the mapping in front of you

Sources arrive in roughly four shapes. Handle whichever you get:

| Input | What to do |
|---|---|
| PowerCenter XML export (`.xml`, `POWERMART` root) | Read `references/xml-parsing-notes.md` and parse it. Never eyeball a >200-line XML — parse it into a component graph first. |
| IDMC / CDI asset export (`.zip` of JSON) | Read `references/xml-parsing-notes.md` § IDMC. Inspect the actual JSON shape before assuming a schema. |
| Screenshot / PDF of the mapping canvas | Extract the component list and connector order, then ask the user for the Expression port logic — it is never legible in a screenshot and guessing it is malpractice. |
| Verbal or written description | Work from it directly, but restate your understanding of the dataflow before emitting code. |

If the user uploaded a file, read it yourself. Do not ask them to re-paste something already in the conversation.

### Step 2: Build the component inventory

Before writing any code, produce a short inventory table the user can sanity-check:

```
Mapping: m_CUSTOMER_DIM_LOAD
Sources:      SRC_CUSTOMER (Oracle), SRC_COUNTRY (flat file)
Transforms:   SQ_CUSTOMER, EXP_CLEANSE, LKP_COUNTRY (connected),
              FIL_ACTIVE, AGG_ORDER_TOTALS, SEQ_CUST_KEY, UPD_STRATEGY
Targets:      DIM_CUSTOMER (update else insert)
Risk flags:   SEQ_CUST_KEY (surrogate key semantics), UPD_STRATEGY (MERGE)
Parameters:   $$LOAD_DATE, $$SRC_SCHEMA, $DBConnection_SRC
```

This inventory is also the unit of estimation. When the user asks "how big is this migration", count transformations by type and weight them: Expression/Filter/Sorter/Union are cheap; Lookup, Aggregator, Joiner, Router are medium; unconnected Lookup, Sequence Generator, Normalizer, Update Strategy with multiple targets, SQL/Stored Procedure/Java transformations are expensive and need a human decision.

### Step 3: Resolve the dataflow, not the canvas

Walk the connectors from source to target to get the true DAG. Then simplify:

- Collapse consecutive Expression transformations into one `withColumns` block.
- Push Filter conditions up toward the source (predicate pushdown) *unless* an intervening transformation is order- or aggregate-sensitive.
- Fold Source Qualifier SQL overrides and filters into the read, or keep the override SQL verbatim as a JDBC query when it is complex — preserving hand-tuned SQL is usually safer than re-deriving it.
- Keep Router branches as named DataFrames, not a chain of filters on a mutated variable.

### Step 4: Map each transformation

Read `references/transformation-patterns.md` for the before/after of every major transformation type, and `references/expression-function-map.md` for the Informatica expression-language → Spark function mapping. Do not translate expression functions from memory — the null-handling and rounding differences are exactly where migrations break.

Quick index of the mapping:

| Informatica component | Databricks target |
|---|---|
| Source / Source Qualifier | `spark.read` (UC table), Auto Loader (`cloudFiles`), JDBC, or Lakehouse Federation |
| Target | Delta table in Unity Catalog (`catalog.schema.table`), written via append/overwrite/`MERGE` |
| Expression | `withColumns` / Spark SQL — see expression-function-map |
| Filter / Router | `.filter()` / named branch DataFrames |
| Joiner | DataFrame `join` (+ `broadcast` for small master) |
| Lookup (connected) | Left join, deduplicated to match the multiple-match policy |
| Lookup (unconnected, `:LKP.`) | Rewrite as a join, or a broadcast dict — **flag as high risk** |
| Aggregator | `groupBy().agg()` (+ window for "last row wins" ports) |
| Rank | `row_number()` / `rank()` over a window |
| Sorter | `orderBy` (+ `dropDuplicates` if Distinct is set) |
| Normalizer | `explode` / `stack` |
| Union | `unionByName` |
| Sequence Generator | Delta `IDENTITY` column — **flag as high risk** |
| Update Strategy | Delta `MERGE` |
| Transaction Control | Delta write boundaries (commit granularity differs) |
| Mapplet | Reusable Python function taking and returning DataFrames |
| Session | Lakeflow Job task (cluster, retries, pre/post SQL) |
| Workflow / Worklet | Multi-task Lakeflow Job, or a Declarative Pipeline |
| Parameter file / `$$vars` | Job parameters → `params` dict |
| Persistent mapping variables (`SETMAXVARIABLE`) | Delta control table — **flag as high risk** |

### Step 5: Emit code against the output contract

Every converted mapping becomes **one pure function** with this exact signature:

```python
def run_<mapping_name>(spark, params: dict, table_map: dict, dry_run: bool = True):
```

The contract exists so that generated code is testable, composable and safe to run against production by accident. Its rules:

- **No hard-coded physical names.** Every catalog/schema/table/path comes from `table_map`. A mapping that reads `prod.sales.customer` directly cannot be tested in dev.
- **No hard-coded business values.** Dates, thresholds, region codes and connection identifiers come from `params`, mirroring the Informatica parameter file.
- **Every write is guarded by `dry_run`.** When `dry_run=True` the function computes everything, returns the result DataFrames and a row-count summary, and writes nothing. This is what makes reconciliation possible before cutover.
- **No `display()`, no `print()` of data, no `.show()`** in the function body — it must be callable from a job, not just a notebook.
- **Return a dict**, so callers can assert on it: `{"target_df": df, "row_counts": {...}, "rejects": df_or_None}`.
- **No side effects outside the target tables** other than the control/audit table.

Use this skeleton:

```python
from pyspark.sql import DataFrame, functions as F, Window
from delta.tables import DeltaTable

def run_m_customer_dim_load(spark, params: dict, table_map: dict, dry_run: bool = True):
    """
    Converted from Informatica mapping: m_CUSTOMER_DIM_LOAD
    Source XML: mappings/m_CUSTOMER_DIM_LOAD.xml
    Semantics preserved: connected lookup (first match), aggregator last-row ports.
    RISK: SEQ_CUST_KEY replaced with Delta IDENTITY - values are unique and
          increasing but not gap-free. See references/transformation-patterns.md.
    """
    # --- 1. Sources (SQ_CUSTOMER, SRC_COUNTRY) -------------------------------
    src_customer = spark.table(table_map["src_customer"])
    src_country  = spark.table(table_map["src_country"])

    # --- 2. EXP_CLEANSE ------------------------------------------------------
    cleansed = src_customer.withColumns({
        "cust_name": F.upper(F.trim(F.col("cust_name"))),
        # IIF(ISNULL(status),'A',status)  ->  coalesce, not nvl chains
        "status":    F.coalesce(F.col("status"), F.lit("A")),
    })

    # --- 3. FIL_ACTIVE -------------------------------------------------------
    filtered = cleansed.filter(F.col("status") == F.lit("A"))

    # --- 4. LKP_COUNTRY (connected, policy = Use First Value) ----------------
    lkp = (src_country
           .withColumn("_rn", F.row_number().over(
               Window.partitionBy("country_code").orderBy("country_id")))
           .filter(F.col("_rn") == 1)
           .drop("_rn"))
    joined = filtered.join(F.broadcast(lkp), "country_code", "left")

    target_df = joined  # ... remaining steps

    row_counts = {
        "src_customer": src_customer.count(),
        "after_filter": filtered.count(),
        "target":       target_df.count(),
    }

    if dry_run:
        return {"target_df": target_df, "row_counts": row_counts, "rejects": None}

    # --- 5. UPD_STRATEGY -> Delta MERGE -------------------------------------
    tgt = DeltaTable.forName(spark, table_map["dim_customer"])
    (tgt.alias("t")
        .merge(target_df.alias("s"), "t.cust_id = s.cust_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    return {"target_df": target_df, "row_counts": row_counts, "rejects": None}
```

Annotate each block with the Informatica instance name it came from. Reviewers will diff your code against the mapping canvas, and those comments are what make that possible.

### Step 6: Orchestration

Once mappings are functions, read `references/orchestration.md` to convert sessions and workflows into Lakeflow Jobs (or Declarative Pipelines where the workflow is a straight dependency chain of table builds). Emit the job definition as YAML/JSON only when the user asks for it or when the workflow has non-trivial structure — a single-session workflow does not need a 60-line YAML.

### Step 7: Validation

Read `references/validation-checklist.md` and give the user the reconciliation plan, not just the code. The sequence that actually works is: static analysis → synthetic-data equivalence → differential run on production data → parallel run → cutover.

## High-risk constructs — always flag these

These cannot be translated faithfully without a human decision. Never emit code for them silently; emit the best-effort version *plus* a `# RISK:` comment and call them out in your chat reply.

- **Unconnected Lookup (`:LKP.lkp_name(x)`)** — invoked row-by-row inside an expression, often with side-effecting overrides. There is no row-scoped call in Spark; it becomes a join, which changes cardinality if the lookup can return multiple rows.
- **Sequence Generator** — PowerCenter guarantees contiguous, restart-persistent numbers from a shared repository counter. Delta `IDENTITY` guarantees uniqueness and monotonicity, not contiguity; `monotonically_increasing_id()` guarantees neither stability across runs nor density. If downstream systems depend on the actual key values, this needs a designed solution (see transformation-patterns § Sequence Generator).
- **Update Strategy feeding multiple targets** — one row can be routed to different targets with different DD_ flags. This becomes several `MERGE`s that are no longer one atomic unit.
- **Stored Procedure / SQL / Java / Custom transformations** — these run arbitrary external code. They must be ported by hand, and the source must be obtained before you can estimate them.
- **Persistent mapping variables** (`SETMAXVARIABLE`, `SETCOUNTVARIABLE`) — PowerCenter persists these in the repository between runs, which is how incremental extracts often work. Spark has no equivalent; you need an explicit Delta control table.
- **Aggregator with "sorted input"** and **incremental aggregation** — the second changes results based on prior run state stored in cache files.
- **Transaction Control transformation** — commit granularity mid-stream has no Delta analogue.
- **High-precision decimals** — PowerCenter silently degrades `Decimal` to `Double` when "Enable High Precision" is off at session level. Check the session property before choosing `DecimalType` vs `DoubleType`; getting this wrong shifts money.

## Anti-patterns

- **`collect()` or `toPandas()` to emulate row-by-row processing.** If you find yourself reaching for this to reproduce an unconnected lookup or a running variable, stop and use a join or a window function instead.
- **Python UDFs for expression logic.** Almost every Informatica function has a native Spark equivalent (see the function map). A UDF costs serialization on every row and blocks Photon.
- **`monotonically_increasing_id()` as a surrogate key** without telling the user what it actually guarantees.
- **`repartition()` sprinkled defensively.** Let AQE handle skew; add explicit repartitioning only when you have measured a reason.
- **Reproducing Sorter transformations that only existed to feed an Aggregator.** Spark's `groupBy` does not need sorted input; the sort is dead weight.
- **`cache()` on every intermediate DataFrame.** Cache only where a DataFrame is genuinely consumed more than once (Router branches, lookup sources reused across joins).
- **Recreating staging tables that only existed because PowerCenter needed to land data between sessions.** Ask whether the stage is a requirement or an artifact.
- **Writing `.mode("overwrite")` against a target that Informatica loaded incrementally.** Read the session's load type before choosing a write mode.

## Performance notes

- Prefer `broadcast` for lookup sources under roughly the auto-broadcast threshold; most Informatica Lookups are small dimension tables and were cached in memory anyway, so this matches the original intent.
- Replace Informatica's "Sorted Input" Aggregator optimisation with nothing — it is an artifact of a row-based engine.
- Liquid clustering (or partitioning by the load-date column) on targets that legacy jobs loaded by date range.
- Deletion vectors + `MERGE` outperform delete-then-insert rewrites of Update Strategy logic.
- Set `spark.databricks.delta.schema.autoMerge.enabled` deliberately, not by default — Informatica mappings are strictly typed and silent schema drift hides bugs.

## Governance

- Land everything in Unity Catalog three-level namespaces (`catalog.schema.table`); map Informatica folders to schemas and Informatica connections to UC external locations or connections.
- Replace Informatica connection objects and parameter-file credentials with UC **connections** and Databricks **secrets** — never emit a password or connection string into generated code, even a placeholder that looks real.
- Keep an audit/control Delta table recording mapping name, run id, source row count, target row count, reject count and run timestamp. Informatica session logs provided this for free and operations teams will expect it.
- Preserve lineage by naming the converted function and job after the original mapping and workflow. Auditors will ask which Databricks job replaced `wf_DAILY_SALES`.

## Reference files

Read these on demand rather than upfront:

- `references/transformation-patterns.md` — before/after PySpark for every transformation type, including SCD Type 2 and reject handling. Read at Step 4.
- `references/expression-function-map.md` — Informatica expression language → Spark, with the null/rounding gotchas. Read at Step 4 whenever an Expression or Filter condition is involved.
- `references/orchestration.md` — Session/Workflow/Worklet → Lakeflow Jobs and Declarative Pipelines, parameters, scheduling, task values. Read at Step 6.
- `references/validation-checklist.md` — reconciliation harness, cutover sequence, sign-off criteria. Read at Step 7.
- `references/xml-parsing-notes.md` — PowerCenter XML structure and safe parsing, plus IDMC export guidance. Read at Step 1 whenever an export file is involved.
