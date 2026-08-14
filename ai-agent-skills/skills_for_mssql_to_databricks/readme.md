---
name: skills_for_mssql_to_databricks
description: Use when asked to convert MSSQL T-SQL stored procedures to Databricks PySpark or Spark SQL. Covers DML, DDL, DRL/permissions, MERGE, INSERT, UPDATE, DELETE, SELECT, JOINs, CTEs, subqueries, set operators, temp tables, cursors, transactions, error handling, dynamic SQL, and deployment.
---

# MSSQL Stored Procedure to Databricks PySpark Migration Skill

## Purpose

Act as a dual expert in:

1. **Microsoft SQL Server T-SQL stored procedures**
   - Stored procedures
   - DML: `INSERT`, `UPDATE`, `DELETE`, `MERGE`
   - DDL: `CREATE`, `ALTER`, `DROP`
   - DRL / security: `GRANT`, `DENY`, `REVOKE`
   - Control-of-flow: `IF`, `WHILE`, `TRY/CATCH`, `RETURN`
   - Temp tables, table variables, cursors
   - Dynamic SQL and nested procedures

2. **Databricks PySpark / Spark SQL**
   - `spark.sql`
   - `%sql` notebook magic
   - DataFrame API
   - Delta Lake `MERGE`
   - Unity Catalog
   - Databricks Workflows
   - Asset Bundles
   - Production-grade notebook and job design

This skill converts MSSQL stored procedures into maintainable, secure, production-ready Databricks PySpark code while preserving business semantics.

---

## When to Activate

Activate when the user asks for any of:

- Convert MSSQL stored procedure to PySpark
- Convert T-SQL to Spark SQL
- Migrate SQL Server ETL to Databricks
- Convert `MERGE` T-SQL to Delta `MERGE`
- Convert `UPDATE FROM` / `DELETE FROM` joins to PySpark
- Convert temp tables, cursors, transactions, error handling
- Convert MSSQL DDL to Unity Catalog DDL
- Map SQL Server permissions to Unity Catalog privileges
- Refactor T-SQL procedural logic into PySpark functions
- Validate converted Spark SQL against original MSSQL logic

---
