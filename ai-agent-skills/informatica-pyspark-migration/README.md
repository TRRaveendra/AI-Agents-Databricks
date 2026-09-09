# Informatica → Databricks PySpark Migration Skills

A knowledge base of **enterprise-grade migration patterns** for converting **Informatica PowerCenter** and **IDMC / IICS** mappings, sessions, and workflows into production-ready **PySpark**, **Spark SQL**, **Delta Lake**, and **Databricks Jobs / Lakeflow** artifacts.

Modeled after the [ADF → Lakeflow skills](https://github.com/TRRaveendra/AI-Agents-Databricks/tree/main/ai-agent-skills/adf-to-lakeflow-skills) pattern and aligned with the MSSQL → PySpark migration skill.

## Recommended usage flow for agents

1. Load `SKILL.md`
2. Inventory transformations (from XML or user description)
3. Consult the relevant reference files
4. Emit the `run_` function + optional Job/Lakeflow definition
5. Apply the validation checklist

## Relationship to other skills

- Complements `mssql-to-pyspark-migration` for T-SQL / stored-procedure logic that often co-exists with Informatica.
- Complements Databricks agent / Lakeflow skills for final orchestration and pipeline deployment.
---

## Why this exists

Informatica and Databricks do not map one-to-one. An Expression Transformation is not a simple `withColumn`, a Lookup is not automatically a join, a Sequence Generator has different semantics, and an Update Strategy + multi-target mapping has no direct equivalent without Delta `MERGE`. Every migration team rediscovers these gaps the hard way.

This library encodes the mappings once — including the parts that *don’t* translate cleanly — so engineers, architects, and AI coding agents all work from the same conversion rules.

---

## What’s inside

```
informatica-to-pyspark-migration/
│
├── README.md                         ← you are here
├── SKILL.md                          ⭐ main skill (load this for AI agents)
│
└── references/
    ├── transformation-patterns.md    ← detailed before/after for each transformation
    ├── expression-function-map.md    ← Informatica expression language → Spark
    ├── orchestration.md              ← Session / Workflow → Jobs & Lakeflow
    ├── validation-checklist.md       ← testing & cutover checklist
    └── xml-parsing-notes.md          ← PowerCenter / IDMC export guidance
```

### Component mapping

| Informatica Component              | Databricks Equivalent                          | Complexity   | Reference |
|------------------------------------|------------------------------------------------|--------------|-----------|
| Source Qualifier / Source          | `spark.read` / Auto Loader / JDBC / UC table   | ⭐⭐ Medium   | transformation-patterns |
| Target                             | Delta table + `MERGE` / `saveAsTable`          | ⭐⭐ Medium   | transformation-patterns |
| Expression                         | `withColumn` + Spark SQL / function map        | ⭐⭐ Medium   | expression-function-map |
| Filter                             | `.filter()` / `WHERE`                          | ⭐ Low       | transformation-patterns |
| Joiner                             | DataFrame `join` (+ broadcast)                 | ⭐⭐ Medium   | transformation-patterns |
| Lookup (connected)                 | Broadcast / left join + coalesce               | ⭐⭐⭐ High    | transformation-patterns |
| Lookup (unconnected)               | Join rewrite or controlled UDF                 | ⭐⭐⭐⭐ Very High | transformation-patterns |
| Router                             | Multiple filtered DataFrames                   | ⭐⭐ Medium   | transformation-patterns |
| Aggregator                         | `groupBy().agg()`                              | ⭐⭐ Medium   | transformation-patterns |
| Sorter / Rank                      | `orderBy` / Window functions                   | ⭐⭐ Medium   | transformation-patterns |
| Sequence Generator                 | IDENTITY / `monotonically_increasing_id` / window | ⭐⭐⭐ High | transformation-patterns |
| Update Strategy                    | Delta `MERGE INTO`                             | ⭐⭐⭐ High    | transformation-patterns |
| Mapplet                            | Reusable Python function                       | ⭐⭐ Medium   | transformation-patterns |
| Session                            | Databricks Job task                            | ⭐⭐ Medium   | orchestration |
| Workflow / Worklet                 | Multi-task Job or Lakeflow pipeline            | ⭐⭐⭐ High    | orchestration |
| Parameters / Variables             | Job parameters + `params` dict                 | ⭐ Low       | SKILL.md + orchestration |

---

## Quick start

**1. Load the skill**  
For AI agents (Claude, Cursor, Grok, etc.) point the agent at `SKILL.md`. The description and front-matter make it discoverable.

**2. Inventory your Informatica assets**  
Export PowerCenter mappings/sessions/workflows as XML (or IDMC taskflows). List every transformation type in scope, then open the matching reference file.

**3. Follow the conversion contract**  
Every mapping becomes a pure, testable function:

```python
def run_<mapping_name>(spark, params: dict, table_map: dict, dry_run: bool = True):
    """Return dict of named result DataFrames and/or write plan."""
```

- Physical table/path names come **only** from `table_map`
- All writes are guarded by `if not dry_run:`
- Prefer Spark SQL for set-based logic; DataFrame API for composition
- High-risk constructs are flagged with Semantic Notes

**4. Migrate in dependency order**

| Phase | Focus                        | Components                                      |
|-------|------------------------------|-------------------------------------------------|
| 1 — Foundation | Connectivity & storage | Sources/Targets → Unity Catalog + external locations · Parameters → job params / secrets |
| 2 — Core Logic | Transformations       | Expression, Filter, Joiner, Lookup, Aggregator, Router → PySpark/SQL |
| 3 — Control Flow | Orchestration        | Session properties → Job clusters & retries · Workflow dependencies → multi-task Jobs |
| 4 — Advanced & Cutover | SCD, CDC, validation | Update Strategy → MERGE · file sources → Auto Loader · differential testing |

---

## Core principles (from SKILL.md)

1. Prefer **Spark SQL** for declarative set-based logic; use **DataFrame API** when it improves clarity or dynamic construction.
2. Always produce a pure function with the signature shown above.
3. Never hard-code catalog/schema/table or file paths — use `table_map`.
4. Guard every write with `dry_run`.
5. Treat source Informatica XML / metadata as untrusted.
6. Flag high-risk constructs (unconnected lookups, Sequence Generator, multi-target Update Strategy, stored-procedure transforms, custom Java/Python) and propose redesigns.
7. Prefer Delta Lake + Unity Catalog targets. Use Auto Loader for file-based sources.
8. Preserve business logic exactly; optimize only after functional equivalence is proven.

---

## Validation levels

| Level                    | What it means                                                                 |
|--------------------------|-------------------------------------------------------------------------------|
| **Static validated**     | XML/metadata parse + construct inventory + schema-compliant generation + policy checks |
| **Synthetic tests passed** | Restricted notebook runs successfully with `dry_run=True`                   |
| **Differentially verified** | Side-by-side Informatica vs Databricks results match (counts, keys, nulls, aggregates) |

See `references/validation-checklist.md` for the full cutover checklist.

---

## Using this with an AI agent

Each file is written to be consumed programmatically. Intended loop:

```python
def generate_migration_plan(informatica_xml_or_description):
    """
    Parse Informatica mapping/workflow and generate Databricks equivalent.
    """
    components = extract_components(informatica_xml_or_description)

    for component in components:
        skill = load_skill(component.type)          # SKILL.md + references/
        pyspark_code = skill.transform(component)   # run_ function
        job_yaml = skill.orchestrate(component)     # optional Job / Lakeflow
        validate(pyspark_code)

    return pyspark_module, job_definition
```

---

## Relationship to other skills

- **mssql-to-pyspark-migration** — for T-SQL / stored-procedure logic that often co-exists with Informatica mappings.
- **Databricks agent / Lakeflow skills** — for final job deployment, Unity Catalog setup, and declarative pipelines.
- **ADF → Lakeflow skills** — sibling knowledge base for Azure Data Factory migrations; same structural philosophy.

---

## Success criteria

- [ ] All in-scope mappings converted to pure `run_` functions
- [ ] Sessions and workflows mapped to Databricks Jobs or Lakeflow pipelines
- [ ] Sources and targets registered in Unity Catalog
- [ ] High-risk constructs documented with Semantic Notes
- [ ] Static + synthetic validation passed
- [ ] Differential validation completed for critical mappings
- [ ] Parallel-run period signed off
- [ ] Documentation and runbooks updated

---

## License & contribution

Intended for internal use and AI-agent consumption. Adapt freely for your migration program. Contributions that improve transformation coverage, expression maps, or validation patterns are welcome.
