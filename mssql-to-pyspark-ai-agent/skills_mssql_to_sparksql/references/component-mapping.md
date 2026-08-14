# SSIS → Databricks Component Mapping

## Control Flow Tasks

| SSIS Task | Databricks Equivalent | Notes |
|---|---|---|
| **Execute SQL Task** | Notebook cell (`%sql` or `spark.sql()`) | Direct SQL execution. Parameters become widget values. |
| **Data Flow Task** | Spark DataFrame pipeline | Each source/transform/dest becomes DataFrame operations. |
| **Execute Process Task** | `%sh` magic command or `subprocess` | Shell commands within notebook. |
| **Script Task (C#)** | Python cell in notebook | Rewrite logic in PySpark/Python. |
| **For Each Loop Container** | Python `for` loop + `dbutils.notebook.run()` | Iterate over files, tables, or parameter lists. |
| **For Loop Container** | Python `for`/`while` loop | Counter-based iteration. |
| **Sequence Container** | Notebook section (markdown + code cells) | Logical grouping of related steps. |
| **Send Mail Task** | Databricks notification or webhook | Use workflow notifications or custom Python `smtplib`. |
| **File System Task** | `dbutils.fs` commands | Copy/move/delete files in DBFS or cloud storage. |
| **FTP Task** | Python `ftplib` or `paramiko` | Rewrite as Python in notebook. |
| **Expression Task** | Python variable assignment | `variable = expression` |
| **Execute Package Task** | `dbutils.notebook.run()` | Call child notebooks with parameters. |

## Data Flow Transformations

| SSIS Transform | PySpark Equivalent | Example |
|---|---|---|
| **OLE DB Source** | `spark.read.format("jdbc").option("url", ...).load()` | See connection-mapping.md |
| **Flat File Source** | `spark.read.csv(path, header=True)` | Options: `inferSchema`, `delimiter`, `quote` |
| **Excel Source** | `spark.read.format("com.crealytics.spark.excel")` | Requires library install |
| **Lookup** | `df.join(lookup_df, on="key", how="left")` | Use broadcast for small lookups: `F.broadcast(lookup_df)` |
| **Conditional Split** | `df.filter(condition)` | Create separate DataFrames per branch |
| **Derived Column** | `df.withColumn("new_col", expr)` | Use `F.when()`, `F.concat()`, `F.upper()`, etc. |
| **Data Conversion** | `df.withColumn("col", df["col"].cast("decimal(18,4)"))` | Use `.cast()` for type changes |
| **Aggregate** | `df.groupBy("col").agg(F.sum("amt"), F.count("*"))` | All aggregate functions in `pyspark.sql.functions` |
| **Sort** | `df.orderBy("col")` or `df.sort(F.desc("col"))` | Avoid in distributed workloads unless necessary |
| **Merge Join** | `df1.join(df2, on="key", how="inner")` | Pre-sorting not needed in Spark |
| **Union All** | `df1.unionByName(df2, allowMissingColumns=True)` | Column order doesn't matter with `unionByName` |
| **Multicast** | Reuse same DataFrame variable | Spark DAG handles it — no need for explicit multicast |
| **Row Count** | `df.count()` | Store in variable for audit |
| **OLE DB Destination** | `df.write.format("delta").mode("append").saveAsTable()` | Use `mergeSchema` option if schema evolves |
| **Flat File Destination** | `df.write.csv(path, header=True)` | Or `.parquet()`, `.json()` |
| **Error Output** | Try/except + separate DataFrame | Redirect bad rows to error table |

## Precedence Constraints

| SSIS Constraint | Databricks Equivalent |
|---|---|
| **On Success** (green arrow) | Sequential notebook cells (natural order) |
| **On Failure** (red arrow) | `try`/`except` block |
| **On Completion** | `try`/`except`/`finally` |
| **Expression-based** | Python `if`/`else` |

## Event Handlers

| SSIS Event | Databricks Equivalent |
|---|---|
| **OnError** | `try`/`except` + logging |
| **OnPreExecute** | First cell in notebook |
| **OnPostExecute** | Last cell in notebook |
| **OnWarning** | Python `logging.warning()` |
| **OnTaskFailed** | Workflow task failure notification |

## Variables and Parameters

| SSIS Concept | Databricks Equivalent | Example |
|---|---|---|
| **Package Variable** | Python variable | `batch_date = "2024-01-01"` |
| **Package Parameter** | `dbutils.widgets` | `dbutils.widgets.text("batch_date", "2024-01-01")` |
| **Project Parameter** | Workflow job parameter | Defined in job JSON config |
| **System Variable** | Built-in Spark configs | `spark.conf.get("spark.databricks.clusterUsageTags.clusterName")` |
| **Expression** | Python f-string or `F.expr()` | `f"SELECT * FROM t WHERE date > '{batch_date}'"` |

## Containers → Notebook Patterns

```
SSIS:
┌─────────────────────────────────────┐
│ Sequence Container: Pre-Processing  │
│  ├── Execute SQL: Truncate Staging  │
│  └── Execute SQL: Get Max ID       │
├─────────────────────────────────────┤
│ Data Flow: Load Data                │
├─────────────────────────────────────┤
│ Execute SQL: Merge to Dimension     │
└─────────────────────────────────────┘

Databricks Notebook:
┌─────────────────────────────────────┐
│ # Cell 1: Pre-Processing           │
│ spark.sql("TRUNCATE TABLE ...")     │
│ max_id = spark.sql("SELECT MAX..") │
├─────────────────────────────────────┤
│ # Cell 2-4: Load Data (DataFrame)  │
│ df = spark.read.jdbc(...)           │
│ df_transformed = df.withColumn(...) │
│ df_transformed.write.saveAsTable()  │
├─────────────────────────────────────┤
│ # Cell 5: Merge to Dimension       │
│ spark.sql("MERGE INTO dim.X ...")   │
└─────────────────────────────────────┘
```
