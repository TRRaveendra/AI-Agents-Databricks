# DQL Mapping — SELECT, JOINs, Subqueries, CTEs, Set Operators, Window Functions

Most of T-SQL's query grammar (`SELECT`/`FROM`/`WHERE`/`GROUP BY`/`HAVING`/`ORDER BY`,
joins, CTEs, window functions, set operators) is near-identical in Spark SQL — it's all
ANSI-SQL-derived. The differences that matter are called out below; everything else you can
port statement-for-statement.

## SELECT basics

| T-SQL | Spark SQL | Note |
|---|---|---|
| `SELECT TOP 10 * FROM t ORDER BY x DESC` | `SELECT * FROM t ORDER BY x DESC LIMIT 10` | `TOP` → `LIMIT`, and `LIMIT` goes at the end |
| `SELECT TOP 10 PERCENT * FROM t` | no direct equivalent | compute `LIMIT CEIL(COUNT(*)*0.1)` via a variable, or `.limit(int(df.count()*0.1))` |
| `SELECT DISTINCT a,b FROM t` | identical | |
| `SELECT a, b INTO #tmp FROM t` | `df.createOrReplaceTempView("tmp")` or `CREATE TEMP VIEW tmp AS SELECT ...` | see control-flow-procedural.md |
| `SELECT * FROM t (NOLOCK)` | remove — no equivalent, and unnecessary (Delta MVCC handles concurrent reads) | |
| `ISNULL(col, default)` | `coalesce(col, default)` (Spark's `isnull()` is a predicate, not this function — don't confuse them) | |
| `COALESCE(a,b,c)` | identical | |
| String concatenation `a + b` | `concat(a, b)` or `a || b` — `+` is arithmetic-only in Spark | |
| `CASE WHEN ... THEN ... ELSE ... END` | identical | |
| `SELECT @@ROWCOUNT` | see dml-mapping.md "Getting @@ROWCOUNT" | |

## JOINs — all types

Syntax is identical; only the keyword forms below are worth noting.

| T-SQL | Spark SQL |
|---|---|
| `INNER JOIN` | `INNER JOIN` / `JOIN` |
| `LEFT [OUTER] JOIN` | `LEFT [OUTER] JOIN` |
| `RIGHT [OUTER] JOIN` | `RIGHT [OUTER] JOIN` |
| `FULL [OUTER] JOIN` | `FULL [OUTER] JOIN` |
| `CROSS JOIN` | `CROSS JOIN` |
| `CROSS APPLY` | correlated `LATERAL` join (Spark SQL supports `LATERAL VIEW` / `LATERAL (subquery)` — DBR 12.2+) or rewrite as a `LEFT SEMI`/regular join if not truly row-dependent |
| `OUTER APPLY` | `LEFT JOIN LATERAL (...)` equivalent — same DBR version note |
| Self-join | identical |
| `LEFT JOIN ... WHERE right.key IS NULL` (anti-join idiom) | prefer `LEFT ANTI JOIN` explicitly — clearer and often faster |
| — | Spark also exposes `LEFT SEMI JOIN` directly (T-SQL emulates this with `WHERE EXISTS`) |

```sql
-- T-SQL CROSS APPLY example
SELECT c.CustomerID, o.OrderID
FROM dbo.Customer c
CROSS APPLY (
    SELECT TOP 3 OrderID FROM dbo.Orders o WHERE o.CustomerID = c.CustomerID ORDER BY OrderDate DESC
) o;
```
```sql
-- Spark SQL: usually cleanest rewritten with a window function instead of LATERAL
SELECT customer_id, order_id FROM (
    SELECT c.customer_id, o.order_id,
           ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_date DESC) AS rn
    FROM gold.customer c JOIN gold.orders o ON c.customer_id = o.customer_id
) WHERE rn <= 3;
```

```sql
-- T-SQL anti-join idiom
SELECT s.* FROM staging.Orders s LEFT JOIN dim.Customer d ON s.CustomerID = d.CustomerID
WHERE d.CustomerID IS NULL;
```
```sql
-- Spark SQL: prefer explicit LEFT ANTI JOIN
SELECT s.* FROM staging.orders s LEFT ANTI JOIN dim.customer d ON s.customer_id = d.customer_id;
```

## CTEs (Common Table Expressions)

Non-recursive CTEs are **identical syntax**:
```sql
WITH RecentOrders AS (
    SELECT * FROM fact.Orders WHERE OrderDate > DATEADD(month,-3,GETDATE())
),
CustomerTotals AS (
    SELECT CustomerKey, SUM(TotalAmount) AS Total FROM RecentOrders GROUP BY CustomerKey
)
SELECT * FROM CustomerTotals WHERE Total > 1000;
```
```sql
-- Spark SQL: identical, multiple CTEs supported the same way
WITH recent_orders AS (
    SELECT * FROM fact.orders WHERE order_date > add_months(current_date(), -3)
),
customer_totals AS (
    SELECT customer_key, SUM(total_amount) AS total FROM recent_orders GROUP BY customer_key
)
SELECT * FROM customer_totals WHERE total > 1000;
```

### Recursive CTEs
T-SQL:
```sql
WITH OrgChart AS (
    SELECT EmployeeID, ManagerID, 0 AS Level FROM dbo.Employee WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.EmployeeID, e.ManagerID, oc.Level + 1
    FROM dbo.Employee e INNER JOIN OrgChart oc ON e.ManagerID = oc.EmployeeID
)
SELECT * FROM OrgChart;
```
Spark SQL has **no native recursive CTE** (as of DBR 14.x; check current runtime — this is
an active area of development, verify support before assuming it's unavailable). Two
idiomatic replacements:

**Option A — iterative Python loop (most common, works everywhere):**
```python
from pyspark.sql import functions as F

base = spark.sql("SELECT employee_id, manager_id, 0 AS level FROM dbo.employee WHERE manager_id IS NULL")
result = base
frontier = base
level = 0
max_levels = 20  # guard against runaway/cyclic data — T-SQL uses MAXRECURSION for the same reason

while frontier.count() > 0 and level < max_levels:
    level += 1
    frontier = (
        spark.table("dbo.employee").alias("e")
        .join(frontier.alias("oc"), F.col("e.manager_id") == F.col("oc.employee_id"))
        .select("e.employee_id", "e.manager_id", F.lit(level).alias("level"))
    )
    result = result.unionByName(frontier)

result.write.format("delta").mode("overwrite").saveAsTable("gold.org_chart")
```

**Option B — GraphFrames** (for genuinely graph-shaped problems: shortest path, connected
components, hierarchy traversal at scale) — use `graphframes` library's `bfs()` or
`connectedComponents()` rather than hand-rolling recursion.

Flag recursive CTEs explicitly when converting — they need a deliberate rewrite, not a
line-for-line port.

## Set Operators

| T-SQL | Spark SQL |
|---|---|
| `UNION` | `UNION` (dedups, identical) |
| `UNION ALL` | `UNION ALL` (identical) |
| `INTERSECT` | `INTERSECT` (identical) |
| `EXCEPT` | `EXCEPT` (identical) |

Caveat: T-SQL set operators match columns **positionally**; Spark's `UNION`/`UNION ALL` do
too by default. If source column orders differ, use PySpark's `unionByName(allowMissingColumns=True)`
instead of raw SQL `UNION` to match by name rather than position — this is a common source of
silent bugs when porting T-SQL UNIONs where column order isn't guaranteed to match.
```python
df1.unionByName(df2, allowMissingColumns=True)
```

## Subqueries

### Scalar subquery
```sql
SELECT CustomerID, (SELECT MAX(OrderDate) FROM fact.Orders o WHERE o.CustomerID = c.CustomerID) AS LastOrder
FROM dim.Customer c;
```
Identical in Spark SQL. For performance, prefer rewriting as a `LEFT JOIN` to a
pre-aggregated subquery when the correlated scalar subquery runs per-row over a large table:
```sql
SELECT c.customer_id, o.last_order
FROM dim.customer c
LEFT JOIN (SELECT customer_id, MAX(order_date) AS last_order FROM fact.orders GROUP BY customer_id) o
  ON c.customer_id = o.customer_id;
```

### IN / EXISTS / NOT EXISTS subqueries
All supported identically in Spark SQL:
```sql
SELECT * FROM dim.Customer WHERE CustomerID IN (SELECT CustomerID FROM staging.Orders);
SELECT * FROM dim.Customer c WHERE EXISTS (SELECT 1 FROM staging.Orders s WHERE s.CustomerID = c.CustomerID);
SELECT * FROM dim.Customer c WHERE NOT EXISTS (SELECT 1 FROM staging.Orders s WHERE s.CustomerID = c.CustomerID);
```
Prefer `EXISTS`/`NOT EXISTS`/semi-anti joins over `IN`/`NOT IN` when the subquery can return
NULLs — `NOT IN` with NULLs is a classic T-SQL footgun that behaves the same (surprisingly)
in Spark: `NOT IN (SELECT ... )` returns **no rows** if the subquery result contains any
NULL. Preserve this behavior faithfully (don't "fix" it silently) unless the user confirms
it was already a latent bug in the original procedure.

## Window Functions

Identical syntax and semantics — this is one of the cleanest 1:1 ports:
```sql
-- T-SQL and Spark SQL — same code
SELECT
    OrderID, CustomerID, OrderDate, TotalAmount,
    ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC) AS rn,
    RANK()       OVER (PARTITION BY CustomerID ORDER BY TotalAmount DESC) AS rnk,
    DENSE_RANK() OVER (PARTITION BY CustomerID ORDER BY TotalAmount DESC) AS drnk,
    LAG(TotalAmount)  OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS prev_amount,
    LEAD(TotalAmount) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS next_amount,
    SUM(TotalAmount)  OVER (PARTITION BY CustomerID ORDER BY OrderDate
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    NTILE(4) OVER (ORDER BY TotalAmount) AS quartile
FROM fact.Orders;
```
PySpark equivalent (when composing programmatically rather than via `%sql`):
```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("customer_id").orderBy("order_date")
w_running = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = (df
    .withColumn("rn", F.row_number().over(w))
    .withColumn("prev_amount", F.lag("total_amount").over(w))
    .withColumn("next_amount", F.lead("total_amount").over(w))
    .withColumn("running_total", F.sum("total_amount").over(w_running))
)
```

## GROUP BY / HAVING / ROLLUP / CUBE / GROUPING SETS

Identical syntax:
```sql
SELECT RegionKey, DateKey, SUM(TotalAmount) AS Revenue
FROM fact.Orders
GROUP BY ROLLUP(RegionKey, DateKey)
HAVING SUM(TotalAmount) > 1000;
```
Ports directly:
```sql
SELECT region_key, date_key, SUM(total_amount) AS revenue
FROM fact.orders
GROUP BY ROLLUP(region_key, date_key)
HAVING SUM(total_amount) > 1000;
```
`CUBE(...)` and `GROUPING SETS ((a,b),(a),())` are also supported identically in Spark SQL.
`GROUPING(col)` / `GROUPING_ID()` also both exist and behave the same.

## PIVOT / UNPIVOT

```sql
-- T-SQL
SELECT * FROM (
    SELECT RegionCode, OrderMonth, TotalAmount FROM fact.Orders
) src
PIVOT (SUM(TotalAmount) FOR OrderMonth IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12])) pvt;
```
Spark SQL supports `PIVOT` natively (since Spark 2.4) with very similar syntax:
```sql
SELECT * FROM (
    SELECT region_code, order_month, total_amount FROM fact.orders
) src
PIVOT (SUM(total_amount) FOR order_month IN (1,2,3,4,5,6,7,8,9,10,11,12));
```
Or with the DataFrame API (handles dynamic/unknown pivot values more gracefully than SQL,
which needs the `IN (...)` list known up front — same limitation T-SQL PIVOT has):
```python
df.groupBy("region_code").pivot("order_month").sum("total_amount")
```

UNPIVOT:
```sql
-- T-SQL
SELECT RegionCode, Month, Amount FROM t UNPIVOT (Amount FOR Month IN ([Jan],[Feb],[Mar])) u;
```
Spark SQL supports `UNPIVOT` too (DBR 13+); if targeting an older runtime, use `stack()`:
```sql
SELECT region_code, stack(3, 'Jan', Jan, 'Feb', Feb, 'Mar', Mar) AS (month, amount) FROM t;
```

## ORDER BY / OFFSET-FETCH (pagination)

```sql
-- T-SQL
SELECT * FROM t ORDER BY OrderDate OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;
```
```sql
-- Spark SQL — identical OFFSET/FETCH support (DBR 12+), or use LIMIT/OFFSET
SELECT * FROM t ORDER BY order_date OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;
-- equivalently
SELECT * FROM t ORDER BY order_date LIMIT 10 OFFSET 20;
```

## STRING_AGG / FOR XML PATH concatenation

```sql
-- T-SQL modern
SELECT CustomerID, STRING_AGG(ProductName, ', ') WITHIN GROUP (ORDER BY ProductName) AS Products
FROM t GROUP BY CustomerID;

-- T-SQL legacy trick
SELECT CustomerID, STUFF((SELECT ',' + ProductName FROM t t2 WHERE t2.CustomerID = t.CustomerID FOR XML PATH('')),1,1,'')
FROM t;
```
```sql
-- Spark SQL
SELECT customer_id, array_join(sort_array(collect_list(product_name)), ', ') AS products
FROM t GROUP BY customer_id;
-- or, if order doesn't matter:
SELECT customer_id, concat_ws(', ', collect_list(product_name)) AS products
FROM t GROUP BY customer_id;
```
