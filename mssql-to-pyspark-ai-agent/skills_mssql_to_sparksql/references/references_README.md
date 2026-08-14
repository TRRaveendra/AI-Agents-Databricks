# MSSQL to SparkSQL Migration References

This directory contains the domain-specific reference material used by the `skills_mssql_to_sparksql` Agent Skill.

The references are organized by migration concern so an AI agent can select the relevant guidance when converting Microsoft SQL Server workloads to Databricks Spark SQL and PySpark.

## 📚 Reference Files

| Reference | Purpose |
|---|---|
| [`component-mapping.md`](component-mapping.md) | Maps SQL Server components and objects to Databricks/Spark equivalents |
| [`connection-mapping.md`](connection-mapping.md) | Covers SQL Server connectivity and target connection patterns |
| [`control-flow-procedural.md`](control-flow-procedural.md) | Covers stored-procedure logic, variables, conditions, loops, cursors and procedural constructs |
| [`data-type-mapping.md`](data-type-mapping.md) | Maps SQL Server data types to Spark/Databricks data types |
| [`databricks-patterns.md`](databricks-patterns.md) | Provides Databricks-native implementation patterns |
| [`ddl-mapping.md`](ddl-mapping.md) | Covers SQL Server DDL to Databricks/Delta DDL migration |
| [`dml-mapping.md`](dml-mapping.md) | Covers `INSERT`, `UPDATE`, `DELETE`, and `MERGE` migration |
| [`dql-select-joins.md`](dql-select-joins.md) | Covers `SELECT`, filters, aggregations, subqueries and JOIN migration |
| [`functions-mapping.md`](functions-mapping.md) | Covers SQL Server function to Spark SQL/PySpark function mapping |

## 🗂️ Reference Organization

```text
references/
│
├── README.md
│
├── component-mapping.md
├── connection-mapping.md
├── control-flow-procedural.md
├── data-type-mapping.md
├── databricks-patterns.md
├── ddl-mapping.md
├── dml-mapping.md
├── dql-select-joins.md
└── functions-mapping.md
```

## 🔄 How the References Are Used

The AI agent should not necessarily load every reference for every migration.

A recommended approach is:

```text
SQL Server Source
       │
       ▼
   Analyze Code
       │
       ├── Components ───────► component-mapping.md
       ├── Connections ───────► connection-mapping.md
       ├── Procedures ────────► control-flow-procedural.md
       ├── Data Types ────────► data-type-mapping.md
       ├── Databricks Target ─► databricks-patterns.md
       ├── DDL ───────────────► ddl-mapping.md
       ├── DML ───────────────► dml-mapping.md
       ├── SELECT/JOIN ───────► dql-select-joins.md
       └── Functions ─────────► functions-mapping.md
       │
       ▼
PySpark / Spark SQL
       │
       ▼
Validation
```

## 🎯 Migration Philosophy

These references are intended to support **semantic migration**, not simple text substitution.

The agent should:

1. Analyze the SQL Server source.
2. Identify the SQL constructs and business logic involved.
3. Select the relevant reference material.
4. Apply the appropriate Databricks migration pattern.
5. Generate PySpark and/or Spark SQL.
6. Validate syntax and semantics.
7. Flag areas requiring human review.

### Example

A stored procedure containing:

```text
Parameters
    +
Variables
    +
Temporary Tables
    +
SELECT / JOIN
    +
MERGE
    +
SQL Functions
```

may require:

```text
control-flow-procedural.md
data-type-mapping.md
dql-select-joins.md
dml-mapping.md
functions-mapping.md
databricks-patterns.md
```

The agent should use the combination of references rather than treating the procedure as a simple SQL-to-SQL translation.

## 🧩 Relationship to SKILL.md

The parent skill is:

```text
skills_mssql_to_sparksql/
│
├── SKILL.md
│
└── references/
    ├── README.md
    └── *.md
```

`SKILL.md` provides the primary instructions and migration behavior for the AI agent.

The files in this directory provide the detailed reference knowledge that supports those instructions.

```text
SKILL.md
   │
   ├── defines migration approach
   │
   └── uses references/
          │
          ├── component mapping
          ├── connection mapping
          ├── procedural mapping
          ├── data type mapping
          ├── Databricks patterns
          ├── DDL mapping
          ├── DML mapping
          ├── DQL / JOIN mapping
          └── function mapping
```

## ⚠️ Important

Reference documents provide migration guidance. They do not guarantee that every SQL Server construct has a direct one-to-one Databricks equivalent.

Generated code should be validated for:

- Business logic
- Data type compatibility
- NULL semantics
- Date/time behavior
- JOIN behavior
- Aggregation results
- `MERGE` behavior
- `INSERT` / `UPDATE` / `DELETE` behavior
- Procedural behavior
- Performance
- Idempotency
- Target Databricks/Delta semantics

## 🤖 Designed for AI-Agent Use

The directory is intentionally modular so the skill can evolve without putting every migration rule into a single large instruction file.

Additional migration references can be added as new patterns are identified, for example:

```text
references/
├── error-handling-mapping.md
├── transaction-mapping.md
├── temp-table-mapping.md
├── dynamic-sql-mapping.md
├── cursor-migration.md
├── performance-optimization.md
└── testing-validation.md
```

Any new reference should have a focused purpose and be linked from this README.

---

**Part of the `skills_mssql_to_sparksql` Agent Skill — Microsoft SQL Server to Databricks PySpark / Spark SQL migration.**
