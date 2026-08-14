# SELECT, JOINs, CTEs, Subqueries, Set Operators, and Window Functions

This reference covers query conversion from MSSQL T-SQL to Databricks Spark SQL.

---

## 1. SELECT Basics

### MSSQL

```sql
SELECT
    CustomerId,
    CustomerName,
    Email
FROM dbo.Customer
WHERE IsActive = 1;
```

### Spark SQL

```sql
SELECT
    CustomerId,
    CustomerName,
    Email
FROM analytics.sales.customer
WHERE IsActive = 1;
```

---

## 2. TOP vs LIMIT

### MSSQL

```sql
SELECT TOP 100 *
FROM dbo.Customer
ORDER BY CreatedAt DESC;
```

### Spark SQL

```sql
SELECT *
FROM analytics.sales.customer
ORDER BY CreatedAt DESC
LIMIT 100;
```

---

## 3. OFFSET / FETCH

### MSSQL

```sql
SELECT *
FROM dbo.Customer
ORDER BY CustomerId
OFFSET 100 ROWS
FETCH NEXT 50 ROWS ONLY;
```

### Spark SQL

```sql
SELECT *
FROM analytics.sales.customer
ORDER BY CustomerId
OFFSET 100
FETCH NEXT 50 ROWS ONLY;
```

If compatibility is required:

```sql
SELECT *
FROM analytics.sales.customer
ORDER BY CustomerId
LIMIT 50 OFFSET 100;
```

---

## 4. DISTINCT

### MSSQL

```sql
SELECT DISTINCT Country
FROM dbo.Customer;
```

### Spark SQL

```sql
SELECT DISTINCT Country
FROM analytics.sales.customer;
```

---

## 5. GROUP BY and HAVING

### MSSQL

```sql
SELECT
    Country,
    COUNT(*) AS CustomerCount
FROM dbo.Customer
GROUP BY Country
HAVING COUNT(*) > 100;
```

### Spark SQL

```sql
SELECT
    Country,
    COUNT(*) AS CustomerCount
FROM analytics.sales.customer
GROUP BY Country
HAVING COUNT(*) > 100;
```

---

## 6. INNER JOIN

### MSSQL

```sql
SELECT
    c.CustomerName,
    o.OrderId
FROM dbo.Customer c
INNER JOIN dbo.[Order] o
    ON c.CustomerId = o.CustomerId;
```

### Spark SQL

```sql
SELECT
    c.CustomerName,
    o.OrderId
FROM analytics.sales.customer c
INNER JOIN analytics.sales.order o
    ON c.CustomerId = o.CustomerId;
```

---

## 7. LEFT JOIN

```sql
SELECT
    c.CustomerName,
    o.OrderId
FROM analytics.sales.customer c
LEFT JOIN analytics.sales.order o
    ON c.CustomerId = o.CustomerId;
```

---

## 8. RIGHT JOIN

```sql
SELECT
    c.CustomerName,
    o.OrderId
FROM analytics.sales.customer c
RIGHT JOIN analytics.sales.order o
    ON c.CustomerId = o.CustomerId;
```

Prefer `LEFT JOIN` by swapping table order when possible for readability.

---

## 9. FULL JOIN

```sql
SELECT
    c.CustomerName,
    o.OrderId
FROM analytics.sales.customer c
FULL JOIN analytics.sales.order o
    ON c.CustomerId = o.CustomerId;
```

---

## 10. CROSS JOIN

### MSSQL

```sql
SELECT
    d.CalendarDate,
    p.ProductId
FROM dbo.Calendar d
CROSS JOIN dbo.Product p;
```

### Spark SQL

```sql
SELECT
    d.CalendarDate,
    p.ProductId
FROM analytics.ref.calendar d
CROSS JOIN analytics.ref.product p;
```

---

## 11. Self Join

```sql
SELECT
    e.EmployeeName,
    m.EmployeeName AS ManagerName
FROM analytics.hr.employee e
LEFT JOIN analytics.hr.employee m
    ON e.ManagerId = m.EmployeeId;
```

---

## 12. EXISTS / NOT EXISTS

### MSSQL

```sql
SELECT c.CustomerId
FROM dbo.Customer c
WHERE EXISTS (
    SELECT 1
    FROM dbo.[Order] o
    WHERE o.CustomerId = c.CustomerId
);
```

### Spark SQL

```sql
SELECT c.CustomerId
FROM analytics.sales.customer c
WHERE EXISTS (
    SELECT 1
    FROM analytics.sales.order o
    WHERE o.CustomerId = c.CustomerId
);
```

### Join Alternative

```sql
SELECT c.CustomerId
FROM analytics.sales.customer c
LEFT SEMI JOIN analytics.sales.order o
    ON c.CustomerId = o.CustomerId;
```

For `NOT EXISTS`:

```sql
SELECT c.CustomerId
FROM analytics.sales.customer c
LEFT ANTI JOIN analytics.sales.order o
    ON c.CustomerId = o.CustomerId;
```

---

## 13. IN and NOT IN

### MSSQL

```sql
SELECT *
FROM dbo.Customer
WHERE Country IN ('US', 'CA');
```

### Spark SQL

```sql
SELECT *
FROM analytics.sales.customer
WHERE Country IN ('US', 'CA');
```

Warning:

`NOT IN` with NULLs can behave unexpectedly:

```sql
WHERE Country NOT IN (SELECT Country FROM ExcludedCountries)
```

If subquery returns NULL, result can be empty. Prefer `NOT EXISTS` or null-safe anti join.

---

## 14. Derived Tables

### MSSQL

```sql
SELECT x.Country, x.CustomerCount
FROM (
    SELECT Country, COUNT(*) AS CustomerCount
    FROM dbo.Customer
    GROUP BY Country
) x
WHERE x.CustomerCount > 10;
```

### Spark SQL

```sql
SELECT x.Country, x.CustomerCount
FROM (
    SELECT Country, COUNT(*) AS CustomerCount
    FROM analytics.sales.customer
    GROUP BY Country
) x
WHERE x.CustomerCount > 10;
```

---

## 15. Common Table Expressions

### MSSQL

```sql
WITH CustomerCounts AS (
    SELECT
        Country,
        COUNT(*) AS CustomerCount
    FROM dbo.Customer
    GROUP BY Country
)
SELECT *
FROM CustomerCounts
WHERE CustomerCount > 10;
```

### Spark SQL

```sql
WITH CustomerCounts AS (
    SELECT
        Country,
        COUNT(*) AS CustomerCount
    FROM analytics.sales.customer
    GROUP BY Country
)
SELECT *
FROM CustomerCounts
WHERE CustomerCount > 10;
```

---

## 16. Multiple CTEs

```sql
WITH CustomerCounts AS (
    SELECT Country, COUNT(*) AS CustomerCount
    FROM analytics.sales.customer
    GROUP BY Country
),
HighValueCountries AS (
    SELECT Country
    FROM CustomerCounts
    WHERE CustomerCount > 100
)
SELECT *
FROM HighValueCountries;
```

---

## 17. Recursive CTE

MSSQL recursive CTEs are often used for hierarchies.

### MSSQL

```sql
WITH OrgHierarchy AS (
    SELECT
        EmployeeId,
        ManagerId,
        EmployeeName,
        0 AS Level
    FROM dbo.Employee
    WHERE ManagerId IS NULL

    UNION ALL

    SELECT
        e.EmployeeId,
        e.ManagerId,
        e.EmployeeName,
        h.Level + 1
    FROM dbo.Employee e
    INNER JOIN OrgHierarchy h
        ON e.ManagerId = h.EmployeeId
)
SELECT * FROM OrgHierarchy;
```

### Spark SQL

Use recursive CTE only if supported by the target Databricks runtime.

If unsupported or behavior is complex, convert to iterative PySpark:

```python
employee_df = spark.table("analytics.hr.employee")

current_level = employee_df.filter("ManagerId IS NULL").withColumn("Level", F.lit(0))
result_df = current_level

max_depth = 10

for _ in range(max_depth):
    next_level = (
        employee_df.alias("e")
        .join(current_level.alias("h"), F.col("e.ManagerId") == F.col("h.EmployeeId"), "inner")
        .select(
            F.col("e.EmployeeId"),
            F.col("e.ManagerId"),
            F.col("e.EmployeeName"),
            (F.col("h.Level") + 1).alias("Level")
        )
    )

    if next_level.limit(1).count() == 0:
        break

    result_df = result_df.unionByName(next_level)
    current_level = next_level
```

Recommended:

- Prefer recursive CTE if runtime supports it and hierarchy depth is manageable.
- Otherwise use iterative loops with bounded depth.
- Consider graph processing for very deep or complex hierarchies.

---

## 18. Scalar Subqueries

### MSSQL

```sql
SELECT
    CustomerId,
    (
        SELECT COUNT(*)
        FROM dbo.[Order] o
        WHERE o.CustomerId = c.CustomerId
    ) AS OrderCount
FROM dbo.Customer c;
```

### Spark SQL

```sql
SELECT
    CustomerId,
    (
        SELECT COUNT(*)
        FROM analytics.sales.order o
        WHERE o.CustomerId = c.CustomerId
    ) AS OrderCount
FROM analytics.sales.customer c;
```

### Join Alternative

```sql
SELECT
    c.CustomerId,
    COUNT(o.OrderId) AS OrderCount
FROM analytics.sales.customer c
LEFT JOIN analytics.sales.order o
    ON c.CustomerId = o.CustomerId
GROUP BY c.CustomerId;
```

Join or window functions often perform better.

---

## 19. Correlated Subqueries

Prefer joins or window functions.

### MSSQL

```sql
SELECT *
FROM dbo.Customer c
WHERE EXISTS (
    SELECT 1
    FROM dbo.[Order] o
    WHERE o.CustomerId = c.CustomerId
      AND o.OrderAmount > 1000
);
```

### Spark SQL

```sql
SELECT *
FROM analytics.sales.customer c
WHERE EXISTS (
    SELECT 1
    FROM analytics.sales.order o
    WHERE o.CustomerId = c.CustomerId
      AND o.OrderAmount > 1000
);
```

### Semi Join

```sql
SELECT c.*
FROM analytics.sales.customer c
LEFT SEMI JOIN analytics.sales.order o
    ON c.CustomerId = o.CustomerId
   AND o.OrderAmount > 1000;
```

---

## 20. Set Operators

### UNION ALL

```sql
SELECT CustomerId, CustomerName FROM analytics.sales.customer_usa
UNION ALL
SELECT CustomerId, CustomerName FROM analytics.sales.customer_canada;
```

### UNION

```sql
SELECT CustomerId, CustomerName FROM analytics.sales.customer_usa
UNION
SELECT CustomerId, CustomerName FROM analytics.sales.customer_canada;
```

### INTERSECT

```sql
SELECT CustomerId FROM analytics.sales.customer
INTERSECT
SELECT CustomerId FROM analytics.sales.active_customer;
```

### EXCEPT

```sql
SELECT CustomerId FROM analytics.sales.customer
EXCEPT
SELECT CustomerId FROM analytics.sales.churned_customer;
```

MSSQL `EXCEPT` maps to Spark SQL `EXCEPT`.

---

## 21. CROSS APPLY and OUTER APPLY

### MSSQL

```sql
SELECT
    c.CustomerId,
    latest_order.OrderId
FROM dbo.Customer c
CROSS APPLY (
    SELECT TOP 1 o.OrderId
    FROM dbo.[Order] o
    WHERE o.CustomerId = c.CustomerId
    ORDER BY o.OrderDate DESC
) latest_order;
```

### Spark SQL Rewrite

```sql
SELECT
    c.CustomerId,
    latest_order.OrderId
FROM analytics.sales.customer c
INNER JOIN (
    SELECT
        CustomerId,
        OrderId,
        ROW_NUMBER() OVER (
            PARTITION BY CustomerId
            ORDER BY OrderDate DESC
        ) AS rn
    FROM analytics.sales.order
) latest_order
    ON c.CustomerId = latest_order.CustomerId
   AND latest_order.rn = 1;
```

For array or map transformations, use:

```sql
LATERAL VIEW explode(array_column) AS exploded_value
```

For supported runtimes, `LATERAL JOIN` may be available.

---

## 22. Window Functions

### Row Number

```sql
SELECT
    CustomerId,
    OrderId,
    ROW_NUMBER() OVER (
        PARTITION BY CustomerId
        ORDER BY OrderDate DESC
    ) AS rn
FROM analytics.sales.order;
```

### Rank

```sql
SELECT
    ProductId,
    SalesAmount,
    RANK() OVER (
        PARTITION BY CategoryId
        ORDER BY SalesAmount DESC
    ) AS SalesRank
FROM analytics.sales.product_sales;
```

### Lead and Lag

```sql
SELECT
    CustomerId,
    OrderDate,
    LAG(OrderDate) OVER (
        PARTITION BY CustomerId
        ORDER BY OrderDate
    ) AS PreviousOrderDate,
    LEAD(OrderDate) OVER (
        PARTITION BY CustomerId
        ORDER BY OrderDate
    ) AS NextOrderDate
FROM analytics.sales.order;
```

### Running Total

```sql
SELECT
    CustomerId,
    OrderDate,
    OrderAmount,
    SUM(OrderAmount) OVER (
        PARTITION BY CustomerId
        ORDER BY OrderDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS RunningTotal
FROM analytics.sales.order;
```

---

## Query Conversion Checklist

Before finalizing a query conversion, check:

- [ ] Column names exist in target schema.
- [ ] NULL behavior is preserved.
- [ ] Join cardinality is understood.
- [ ] Duplicates are handled.
- [ ] `TOP` is converted with deterministic `ORDER BY`.
- [ ] Recursive CTE behavior is verified.
- [ ] Set operator semantics match.
- [ ] Window frame boundaries match.
- [ ] Date/time functions are semantically equivalent.
- [ ] Case sensitivity/collation differences are handled.