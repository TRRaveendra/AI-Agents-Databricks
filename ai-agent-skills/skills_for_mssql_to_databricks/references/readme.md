# 📚 SQL Agent References

The `references` folder contains reusable **SQL knowledge, patterns, validation rules, and operational guidance** used by the AI SQL Agent.

These reference documents provide the agent with structured domain knowledge for generating, validating, explaining, and safely executing SQL.

---

## 📁 Folder Structure

```text
references/
│
├── control-flow-temp.md
├── conversion-rules.md
├── ddl-drl.md
├── deployment-and-operations.md
├── dml-patterns.md
├── select-joins-cte.md
└── validation-and-security.md
```

---

## 📖 Reference Documents

| File                           | Purpose                                                                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `control-flow-temp.md`         | Control-flow logic, temporary objects, variables, procedural patterns, and execution flow             |
| `conversion-rules.md`          | Data-type conversions, casting rules, implicit/explicit conversions, and compatibility considerations |
| `ddl-drl.md`                   | DDL and database/schema object creation and modification patterns                                     |
| `deployment-and-operations.md` | SQL deployment, execution, operational practices, troubleshooting, and production considerations      |
| `dml-patterns.md`              | INSERT, UPDATE, DELETE, MERGE, and other data-manipulation patterns                                   |
| `select-joins-cte.md`          | SELECT statements, filtering, joins, subqueries, CTEs, aggregations, and query composition            |
| `validation-and-security.md`   | SQL validation, security controls, safe execution, authorization, and injection prevention            |

---

## 🎯 Purpose

These references are designed to help an AI agent:

```text
User Request
     │
     ▼
┌─────────────────────┐
│    SQL AI Agent     │
└──────────┬──────────┘
           │
           ▼
   Identify SQL Task
           │
           ▼
┌─────────────────────────────┐
│     Reference Selection     │
├─────────────────────────────┤
│ SELECT / JOIN / CTE         │
│ DML Patterns                │
│ DDL / DRL                   │
│ Conversion Rules            │
│ Control Flow / TEMP         │
│ Deployment / Operations     │
│ Validation / Security       │
└──────────────┬──────────────┘
               │
               ▼
        Generate SQL
               │
               ▼
     Validate & Secure
               │
               ▼
       Final SQL / Action
```

---

## 🧠 How the Agent Should Use References

The agent should retrieve only the reference material relevant to the current task.

### Example: SELECT Query

For a request such as:

> Find the top 10 customers by revenue.

The agent should primarily use:

```text
select-joins-cte.md
```

### Example: MERGE

For:

> Merge daily customer updates into the target table.

Use:

```text
dml-patterns.md
```

### Example: Table Creation

For:

> Create a customer dimension table.

Use:

```text
ddl-drl.md
```

### Example: Type Conversion

For:

> Convert a VARCHAR date into DATE.

Use:

```text
conversion-rules.md
```

### Example: Temporary Processing

For:

> Create a temporary table and process intermediate results.

Use:

```text
control-flow-temp.md
```

### Example: Production Deployment

For:

> Deploy this SQL change safely to production.

Use:

```text
deployment-and-operations.md
```

### Example: Security Validation

For:

> Validate whether this dynamically generated SQL is safe.

Use:

```text
validation-and-security.md
```

---

## 🔐 Security First

`validation-and-security.md` should be considered a **mandatory reference for SQL generated from untrusted or user-provided input**.

The agent should validate:

* SQL syntax
* Allowed operations
* Object access
* User permissions
* Dynamic SQL
* Input parameters
* Potential SQL injection
* Destructive operations
* Production restrictions
* Sensitive data access

The agent should **not execute generated SQL solely because it is syntactically valid**.

```text
Generated SQL
      │
      ▼
Syntax Validation
      │
      ▼
Security Validation
      │
      ▼
Authorization Check
      │
      ├── ❌ Reject
      │
      └── ✅ Continue
               │
               ▼
         Execution Policy
               │
               ▼
            Execute
```

---

## 🔄 Recommended Retrieval Strategy

For a RAG-enabled SQL Agent:

```text
1. Understand user request
        ↓
2. Identify SQL operation
        ↓
3. Select relevant reference
        ↓
4. Retrieve relevant sections
        ↓
5. Generate SQL
        ↓
6. Validate SQL
        ↓
7. Apply security rules
        ↓
8. Execute only when authorized
        ↓
9. Explain result
```

Avoid retrieving the entire `references` folder for every request. Use **targeted retrieval** based on the SQL operation and execution context.

---

## 🧩 Reference Mapping

```text
                    SQL AI AGENT
                         │
        ┌────────────────┼─────────────────┐
        │                │                 │
        ▼                ▼                 ▼
     Query            Mutation          Schema
        │                │                 │
        ▼                ▼                 ▼
select-joins-cte    dml-patterns       ddl-drl
        │                │                 │
        └────────────────┼─────────────────┘
                         │
                         ▼
                  Supporting Rules
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
       conversion    control-flow   deployment
         rules          /temp       operations
             │
             └───────────┬───────────┘
                         ▼
                validation-security
```

---

## 📌 Reference Design Principles

Each reference document should:

* Focus on one SQL capability or operational domain.
* Contain reusable patterns.
* Prefer deterministic examples.
* Clearly distinguish valid and invalid patterns.
* Document database-specific differences where applicable.
* Include security considerations where relevant.
* Avoid embedding credentials or secrets.
* Remain concise enough for effective RAG retrieval.
* Use consistent terminology and formatting.

---

## ➕ Adding a New Reference

When adding a new reference document:

1. Use a descriptive filename.
2. Keep the topic focused.
3. Add practical SQL examples.
4. Document edge cases.
5. Include security considerations where applicable.
6. Update this README.
7. Ensure the content is suitable for retrieval by an AI agent.

Example:

```text
references/
├── ...
├── window-functions.md
├── stored-procedures.md
└── transaction-management.md
```

Then update the reference mapping in this README.

---

## 🚀 Role in the AI Agent

The `references` folder acts as the agent's **SQL knowledge layer**.

```text
┌─────────────────────────────────────────┐
│              AI SQL AGENT               │
├─────────────────────────────────────────┤
│ Reasoning                               │
│ Planning                                │
│ Tool Selection                           │
└──────────────────┬──────────────────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │  Reference Layer    │
        │    /references      │
        └──────────┬──────────┘
                   │
       ┌───────────┼────────────┐
       ▼           ▼            ▼
     SQL          Rules       Security
   Patterns     & Guidance    Policies
       │           │            │
       └───────────┼────────────┘
                   ▼
             SQL Generation
                   │
                   ▼
          Validation & Execution
```

**Principle: Retrieve the right reference → generate accurately → validate securely → execute responsibly.**
