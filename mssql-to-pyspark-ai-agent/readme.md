# MSSQL to PySpark Migration Agent

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> 🤖 AI-powered Microsoft SQL Server → Databricks PySpark / Spark SQL migration accelerator using Databricks Agent Skills.

## Overview

This repository contains a reusable `skills_mssql_to_sparksql` Agent Skill designed to help an AI coding agent analyze Microsoft SQL Server workloads and generate Databricks-oriented PySpark and Spark SQL implementations.

![MSSQL to PySpark Migration Agent](https://github.com/TRRaveendra/AI-Agents-Databricks/blob/main/images/ai-agent-sqlserver-databricks.png?raw=true)

The skill is organized into a main `SKILL.md` plus focused migration references covering:

- SQL Server component mapping
- Connection mapping
- Procedural/control-flow migration
- Data-type mapping
- Databricks implementation patterns
- DDL migration
- DML migration
- DQL, SELECT and JOIN migration
- SQL function mapping

The goal is **migration-aware code generation**, not simple line-by-line SQL translation.

---

## 🎯 Objective

The skill helps an AI agent reason through SQL Server code before generating Databricks code.

Typical migration flow:

    SQL Server T-SQL
          |
          v
    AI Migration Agent
          |
          +-- Component mapping
          +-- Connection mapping
          +-- Procedural/control-flow mapping
          +-- Data-type mapping
          +-- Databricks patterns
          +-- DDL mapping
          +-- DML mapping
          +-- DQL / JOIN mapping
          +-- Function mapping
          |
          v
    PySpark / Spark SQL
          |
          v
    Validation -> Review -> Test -> Deploy

---

# 📁 Skill Structure

The current skill structure is:

    skills_mssql_to_sparksql/
    |
    +-- SKILL.md
    +-- README.md
    |
    +-- references/
        |
        +-- component-mapping.md
        +-- connection-mapping.md
        +-- control-flow-procedural.md
        +-- data-type-mapping.md
        +-- databricks-patterns.md
        +-- ddl-mapping.md
        +-- dml-mapping.md
        +-- dql-select-joins.md
        +-- functions-mapping.md

`SKILL.md` is the main instruction file for the AI agent. The files under `references/` provide focused migration knowledge that the agent can use when a particular migration problem requires deeper guidance.

---

# 🧠 Reference Knowledge

## 1. Component Mapping

**Reference:** `references/component-mapping.md`

Used for mapping SQL Server objects and components to their Databricks/Lakehouse equivalents.

Examples of concepts that may require mapping:

    SQL Server Database
          -> Databricks Catalog

    SQL Server Schema
          -> Databricks Schema

    SQL Server Table
          -> Delta Table

    SQL Server View
          -> Databricks View

The agent should select the target architecture based on the migration context rather than performing a blind textual replacement.

---

## 2. Connection Mapping

**Reference:** `references/connection-mapping.md`

Used when migration logic depends on SQL Server connectivity, source systems, JDBC access, or target Databricks connectivity.

The generated implementation should separate:

- Connection configuration
- Authentication
- Source access
- Target access
- Credentials/secrets

Credentials must never be embedded in generated source code.

---

## 3. Control Flow & Procedural Logic

**Reference:** `references/control-flow-procedural.md`

This reference is particularly important for stored-procedure migration.

SQL Server procedures can contain:

- Parameters
- Variables
- `IF / ELSE`
- `WHILE`
- Cursors
- Temporary objects
- Procedural calculations
- Error handling
- Nested logic

The agent should decide whether each construct belongs in:

- PySpark/Python control flow
- Spark SQL
- DataFrame transformations
- Set-based SQL
- Databricks workflow/task orchestration

The objective is to preserve business behavior while adopting Spark-native execution patterns.

---

## 4. Data Type Mapping

**Reference:** `references/data-type-mapping.md`

Used for SQL Server → Spark/Databricks type conversion.

Important considerations include:

- Precision
- Scale
- Nullability
- Date/time semantics
- Numeric ranges
- String representation
- Binary data
- Casting behavior

A SQL Server type should not automatically be assumed to have an identical Spark semantic equivalent.

---

## 5. Databricks Patterns

**Reference:** `references/databricks-patterns.md`

Provides Databricks-oriented implementation guidance.

This reference is critical because a migration should produce code that fits the target Lakehouse rather than reproducing SQL Server behavior inefficiently.

The agent should consider appropriate use of:

- Spark SQL
- PySpark
- DataFrames
- Delta Lake
- Unity Catalog
- Temporary views
- Databricks Jobs/Workflows
- Set-based processing

---

## 6. DDL Mapping

**Reference:** `references/ddl-mapping.md`

Covers Data Definition Language migration.

Key areas include:

- `CREATE TABLE`
- `ALTER TABLE`
- `DROP TABLE`
- Views
- Object definitions
- Schema/object changes

The agent should account for Databricks and Delta semantics when generating target DDL.

---

## 7. DML Mapping

**Reference:** `references/dml-mapping.md`

Covers:

- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`

A common target pattern is:

    SQL Server MERGE
          |
          v
    Delta Lake MERGE INTO

However, the agent should analyze match conditions, duplicate source records, update/insert behavior, delete conditions and idempotency before generating a target implementation.

---

## 8. DQL — SELECT & JOINs

**Reference:** `references/dql-select-joins.md`

Covers query-level migration such as:

- `SELECT`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `ORDER BY`
- `DISTINCT`
- JOINs
- Subqueries
- Aggregations

The agent should preserve SQL semantics while choosing Spark SQL or PySpark DataFrame syntax appropriately.

---

## 9. Functions Mapping

**Reference:** `references/functions-mapping.md`

Provides guidance for SQL Server function conversion.

Migration areas include:

- Date functions
- String functions
- Numeric functions
- Conditional functions
- NULL handling
- Aggregation functions
- Casting/conversion functions

Function names should not be translated blindly. The agent should consider differences in SQL and Spark semantics.

---

# 🔄 End-to-End Agent Workflow

    1. RECEIVE
       |
       +-- Stored procedure
       +-- SQL query
       +-- DDL
       +-- DML
       +-- SQL Server object
       |
       v
    2. ANALYZE
       |
       +-- Components
       +-- Data types
       +-- Control flow
       +-- DDL
       +-- DML
       +-- DQL
       +-- Functions
       +-- Dependencies
       |
       v
    3. SELECT RELEVANT REFERENCES
       |
       v
    4. GENERATE
       |
       +-- PySpark
       +-- Spark SQL
       +-- Databricks-native patterns
       |
       v
    5. VALIDATE
       |
       +-- Syntax
       +-- Semantics
       +-- Data types
       +-- Business logic
       +-- Edge cases
       |
       v
    6. HUMAN REVIEW
       |
       v
    7. TEST
       |
       v
    8. DEPLOY

---

# 🤖 Why Agent Skills?

Generic LLM prompting can produce syntactically valid Spark code while missing migration-specific behavior.

This skill gives the agent a structured migration knowledge base:

    Generic LLM
        |
        +-- "Convert SQL Server to PySpark"
        |
        v
    Generic translation

versus:

    Migration Agent
        |
        +-- SKILL.md
        +-- Component Mapping
        +-- Connection Mapping
        +-- Procedural Mapping
        +-- Data Type Mapping
        +-- Databricks Patterns
        +-- DDL
        +-- DML
        +-- DQL / JOINs
        +-- Functions
        |
        v
    Migration-aware conversion

This makes the migration process more consistent and maintainable.

---

# 🏗️ Recommended GitHub Repository

    mssql-to-pyspark-ai-agent/
    |
    +-- README.md
    |
    +-- skills_mssql_to_sparksql/
    |   |
    |   +-- SKILL.md
    |   +-- README.md
    |   |
    |   +-- references/
    |       +-- component-mapping.md
    |       +-- connection-mapping.md
    |       +-- control-flow-procedural.md
    |       +-- data-type-mapping.md
    |       +-- databricks-patterns.md
    |       +-- ddl-mapping.md
    |       +-- dml-mapping.md
    |       +-- dql-select-joins.md
    |       +-- functions-mapping.md
    |
    +-- examples/
    |   +-- input/
    |   +-- output/
    |
    +-- tests/
    |   +-- input/
    |   +-- expected/
    |   +-- validation/
    |
    +-- docs/
        +-- architecture.md
        +-- migration-guide.md
        +-- validation.md

---

# 🚀 Using the Skill

Place the skill in the Databricks Agent Skills location supported by your workspace:

    .assistant/
    |
    +-- skills/
        |
        +-- skills_mssql_to_sparksql/
            |
            +-- SKILL.md
            +-- references/
                +-- component-mapping.md
                +-- connection-mapping.md
                +-- control-flow-procedural.md
                +-- data-type-mapping.md
                +-- databricks-patterns.md
                +-- ddl-mapping.md
                +-- dml-mapping.md
                +-- dql-select-joins.md
                +-- functions-mapping.md

The agent can then use `SKILL.md` and the supporting references when performing MSSQL migration tasks.

---

# 🧪 Example Migration Pattern

A stored procedure containing:

- Input parameters
- Variables
- Temporary tables
- Aggregations
- `MERGE`
- `UPDATE`
- Business rules

can be analyzed as:

    T-SQL Procedure
          |
          +-- Parameters
          +-- Variables
          +-- Temp tables
          +-- SELECT/JOIN
          +-- Aggregation
          +-- MERGE
          +-- UPDATE
          |
          v
    Relevant Skill References
          |
          +-- control-flow-procedural.md
          +-- data-type-mapping.md
          +-- dml-mapping.md
          +-- dql-select-joins.md
          +-- functions-mapping.md
          +-- databricks-patterns.md
          |
          v
    Databricks Implementation
          |
          +-- PySpark orchestration
          +-- Spark SQL transformations
          +-- Delta MERGE
          +-- Unity Catalog objects

The important principle is that the agent should **analyze the procedure first and then generate the target implementation**.

---

# 🔐 Security

Do not commit:

- SQL Server passwords
- Databricks tokens
- OAuth secrets
- Client secrets
- Production connection strings
- Personal credentials

Use approved authentication and secret-management mechanisms.

The skill itself should contain migration knowledge and instructions, not production credentials.

---

# ⚠️ Validation Requirements

AI-generated migration code must be reviewed and tested before production deployment.

Recommended validation:

    ✓ SQL syntax
    ✓ Spark SQL syntax
    ✓ PySpark syntax
    ✓ Data type compatibility
    ✓ NULL behavior
    ✓ Date/time semantics
    ✓ JOIN behavior
    ✓ Aggregation results
    ✓ MERGE semantics
    ✓ INSERT / UPDATE / DELETE behavior
    ✓ Temporary-table behavior
    ✓ Procedural logic
    ✓ Error handling
    ✓ Idempotency
    ✓ Performance
    ✓ Source vs target row counts
    ✓ Source vs target business results

For critical workloads, perform source-to-target data reconciliation and representative functional testing.

---

# 📊 Enterprise Migration Architecture

    +---------------------+
    |    SQL Server       |
    |  Stored Procedures  |
    +----------+----------+
               |
               v
    +---------------------+
    | Discovery /         |
    | Complexity Analysis |
    +----------+----------+
               |
               v
    +---------------------+
    | MSSQL Migration     |
    | Agent Skill         |
    +----------+----------+
               |
       +-------+-------+
       |       |       |
       v       v       v
    Mapping  Logic   Types
       |       |       |
       +-------+-------+
               |
               v
    +---------------------+
    | Databricks Patterns |
    +----------+----------+
               |
               v
    +---------------------+
    | PySpark / Spark SQL |
    +----------+----------+
               |
               v
    +---------------------+
    | Validation + Human  |
    | Review               |
    +----------+----------+
               |
               v
    +---------------------+
    | Databricks Lakehouse|
    +---------------------+

---

# 📈 Future Multi-Agent Extension

The current skill structure can become the foundation for a larger migration-agent platform:

    MSSQL Discovery Agent
             |
             v
    Dependency Analysis Agent
             |
             v
    Complexity Classification Agent
             |
             v
    MSSQL -> PySpark Conversion Agent
             |
             v
    SQL Validation Agent
             |
             v
    Data Reconciliation Agent
             |
             v
    Performance Optimization Agent
             |
             v
    Human Approval
             |
             v
    Deployment Agent

Possible future capabilities:

- Automatic procedure inventory
- Dependency graph generation
- Complexity scoring
- Conversion confidence scoring
- Automated test generation
- Source/target reconciliation
- Performance optimization
- Migration audit reports
- Human approval gates
- CI/CD integration
- Migration dashboards

---

# 📚 Reference Index

| Reference | Purpose |
|---|---|
| `component-mapping.md` | SQL Server component/object mapping |
| `connection-mapping.md` | Connection and connectivity mapping |
| `control-flow-procedural.md` | Procedural and control-flow migration |
| `data-type-mapping.md` | SQL Server → Spark data types |
| `databricks-patterns.md` | Databricks-native implementation patterns |
| `ddl-mapping.md` | DDL migration |
| `dml-mapping.md` | INSERT / UPDATE / DELETE / MERGE |
| `dql-select-joins.md` | SELECT, JOIN and query migration |
| `functions-mapping.md` | SQL Server function mapping |

---

# ⭐ Core Migration Principle

> **Do not translate SQL Server code line-by-line. Analyze the business logic, identify the relevant migration patterns, generate Databricks-native PySpark/Spark SQL, and validate the result.**

---

## Disclaimer

This repository is an AI-assisted migration accelerator and Agent Skill implementation.

Generated code should be reviewed, tested, and approved before production deployment. Migration behavior can vary based on SQL Server features, source data, target architecture, Databricks Runtime, and workload characteristics.

The README describes the skill structure visible in the supplied project. The detailed migration rules themselves remain defined by `SKILL.md` and the individual files under `references/`.

---

# 📄 License

This project is released as **open-source software under the MIT License**.

You are free to:

- ✅ Use the project commercially or personally
- ✅ Modify the source code and Agent Skill
- ✅ Copy and redistribute the project
- ✅ Create derivative works
- ✅ Use the migration references in your own projects

The MIT License requires preservation of the copyright and license notice.

See the [`LICENSE`](LICENSE) file for the complete license text.

> **Important:** This license applies to the original source code and migration-skill materials in this repository. It does not grant rights to third-party products, trademarks, documentation, or proprietary content referenced by the project, including Databricks, Microsoft SQL Server, Apache Spark, or other third-party technologies.
