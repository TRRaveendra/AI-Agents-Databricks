# MSSQL to Databricks Migration Skill

## Overview

This skill provides comprehensive guidance for migrating Microsoft SQL Server (T-SQL) code to Databricks, including stored procedures, functions, scripts, and ad-hoc queries. The skill converts T-SQL into idiomatic Databricks notebooks using **Spark SQL** for set-based operations and **PySpark** for procedural logic.

## Skill Structure

```
skills_mssql_to_sparksql/
├── SKILL.md                    # Main skill entry point
├── README.md                   # This file
└── references/                 # Detailed reference documentation
    ├── ddl-mapping.md          # CREATE/ALTER/DROP TABLE, indexes, constraints
    ├── dml-mapping.md          # INSERT, UPDATE, DELETE, MERGE
    ├── dql-select-joins.md     # SELECT, JOINs, CTEs, window functions
    ├── control-flow-procedural.md  # Variables, IF/WHILE, TRY/CATCH, cursors
    ├── functions-mapping.md    # Built-in function conversions
    ├── data-type-mapping.md    # Data type conversions
    ├── connection-mapping.md   # Connection patterns, JDBC, linked servers
    ├── component-mapping.md    # SSIS components, patterns
    └── databricks-patterns.md  # Design patterns and best practices
```

## When to Use This Skill

Load this skill when you need to:

* **Migrate stored procedures** from SQL Server to Databricks
* **Convert T-SQL scripts** to PySpark/Spark SQL notebooks
* **Translate specific T-SQL constructs** (MERGE, cursors, CTEs, PIVOT, etc.)
* **Understand equivalents** for T-SQL features in Databricks
* **Modernize legacy SQL Server workloads** to lakehouse architecture

## Reference Files Guide

### Core SQL Mappings

| File | Purpose | Load When... |
|------|---------|-------------|
| **ddl-mapping.md** | Table structure, indexes, constraints | Creating/altering tables, managing schemas |
| **dml-mapping.md** | Data modification operations | Using INSERT, UPDATE, DELETE, MERGE |
| **dql-select-joins.md** | Query patterns | Writing SELECT queries, JOINs, CTEs |

### Procedural Logic

| File | Purpose | Load When... |
|------|---------|-------------|
| **control-flow-procedural.md** | Variables, loops, error handling | Converting IF/WHILE, TRY/CATCH, cursors, temp tables |
| **functions-mapping.md** | Built-in functions | Using date, string, conversion, aggregate functions |

### Data & Patterns

| File | Purpose | Load When... |
|------|---------|-------------|
| **data-type-mapping.md** | Type conversions | Understanding data type mappings |
| **connection-mapping.md** | Connectivity | Setting up JDBC, replacing linked servers |
| **component-mapping.md** | ETL patterns | Migrating SSIS packages, ETL workflows |
| **databricks-patterns.md** | Best practices | Implementing medallion architecture, optimization |

## Quick Start

### 1. Basic Stored Procedure Migration

```python
# Load the skill
# The agent will automatically load SKILL.md

# Then load specific references as needed
# - control-flow-procedural.md for variables and IF/WHILE
# - dml-mapping.md for INSERT/UPDATE/DELETE
# - functions-mapping.md for function conversions
```

### 2. Typical Migration Flow

1. **Load SKILL.md** — Understand migration philosophy and workflow
2. **Analyze source** — Identify T-SQL constructs in your procedure
3. **Load references** — Load only the reference files you need
4. **Create notebook** — Build Databricks notebook with proper structure
5. **Test & validate** — Compare outputs with original procedure

## Key Migration Principles

### ✅ DO

* **Use Spark SQL for data operations** — SELECT, JOIN, MERGE, INSERT, UPDATE, DELETE
* **Use PySpark for orchestration** — Variables, control flow, error handling
* **Preserve business logic** — Understand intent, not just syntax
* **Design idempotent operations** — Make notebooks re-runnable
* **Use set-based operations** — Replace cursors with MERGE/UPDATE

### ❌ DON'T

* **Token-by-token translation** — Produces brittle code
* **Use DOUBLE for money** — Always use DECIMAL(p,s)
* **Expect multi-statement transactions** — Each DML is independently atomic
* **Keep cursor patterns** — Rewrite as set-based operations

## Common Conversions Reference

### Data Types
```sql
-- T-SQL → Spark SQL
money             → DECIMAL(19,4)
bit               → BOOLEAN
datetime/datetime2 → TIMESTAMP
varchar/nvarchar  → STRING
```

### Functions
```sql
-- T-SQL → Spark SQL
GETDATE()         → current_timestamp()
ISNULL(x, y)      → coalesce(x, y)
LEN(x)            → length(x)
DATEPART(year, x) → year(x)
```

### Temp Tables
```sql
-- T-SQL
SELECT * INTO #Temp FROM Orders;

-- Spark SQL
CREATE OR REPLACE TEMP VIEW temp_orders AS
SELECT * FROM Orders;
```

### Parameters
```sql
-- T-SQL
@StartDate DATETIME

-- PySpark
dbutils.widgets.text("start_date", "")
start_date = dbutils.widgets.get("start_date")
```

## File Sizes & Complexity

| Reference File | Size | Lines | Complexity |
|----------------|------|-------|------------|
| dql-select-joins.md | 12K | ~400 | High |
| functions-mapping.md | 12K | ~400 | High |
| dml-mapping.md | 11K | ~350 | Medium-High |
| control-flow-procedural.md | 8.7K | ~280 | Medium |
| ddl-mapping.md | 7.3K | ~230 | Medium |
| databricks-patterns.md | 5.6K | ~180 | Medium |
| component-mapping.md | 5.9K | ~190 | Medium |
| data-type-mapping.md | 4.5K | ~145 | Low-Medium |
| connection-mapping.md | 4.4K | ~140 | Low-Medium |

## Support & Coverage

### ✅ Fully Supported
* DDL (CREATE/ALTER/DROP TABLE, views, schemas)
* DML (INSERT, UPDATE, DELETE, MERGE)
* DQL (SELECT, all JOIN types, CTEs, window functions)
* Variables, parameters, control flow
* Temp tables and table variables
* Built-in functions (date, string, conversion, aggregation)
* Dynamic SQL

### ⚠️  Requires Redesign
* Cursors → Set-based operations
* Multi-statement transactions → Idempotent DML
* Linked servers → JDBC/Delta Sharing
* SQL Agent jobs → Databricks Workflows

### ❌ Not Supported
* CLR procedures → Rewrite in PySpark
* SQL Mail → Workflow notifications
* Service Broker → Streaming/DLT
* Full-text search → External search engine

## Maintenance

### Last Updated
August 14, 2026

### Version
2.0.0 (Restructured with references/ folder)

### Contributors
Data Engineering Team

## Related Skills

* **migration-orchestrator** — File/folder migration automation
* **databricks-cli-public** — CLI commands for workspace management
* **writing-sql** — Databricks SQL best practices

---

**Need help?** Load SKILL.md and specify which T-SQL constructs you're migrating.
