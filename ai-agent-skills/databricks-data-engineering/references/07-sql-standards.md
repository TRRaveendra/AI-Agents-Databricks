# SQL Coding Standards

## General SQL Style

### Formatting and naming
\```sql
-- Keywords: UPPERCASE
-- Table/column names: lowercase or snake_case  
-- Always use explicit JOIN syntax (never implicit)
-- Indent for readability

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    o.order_id,
    o.order_amount,
    o.order_date
FROM silver.customers c
INNER JOIN silver.orders o
    ON c.customer_id = o.customer_id
WHERE o.order_date >= '2024-01-01'
    AND o.order_amount > 100
ORDER BY o.order_date DESC;
\```

### Use CTEs (Common Table Expressions) for readability

**Best practices**: CTEs make complex queries readable and maintainable. Always prefer CTEs over nested subqueries.

---

**Remember**: Write SQL that is readable, maintainable, and performant. Use CTEs, window functions, and MERGE for efficient operations.
