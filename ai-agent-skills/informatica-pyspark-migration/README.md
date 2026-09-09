# informatica-to-pyspark-migration

A Claude Skill that converts Informatica PowerCenter and IDMC/CDI assets into
production-grade PySpark on Databricks — Delta Lake, Unity Catalog, Lakeflow
Jobs and Declarative Pipelines.

Built to the same shape as the ADF and MSSQL migration skills: one `SKILL.md`
holding the workflow, output contract and risk register, with progressive
reference files loaded only when a given mapping needs them.

## Layout

```
informatica-pyspark-migration/
├── SKILL.md                          # workflow, component map, output contract, risks
├── README.md
└── references/
    ├── transformation-patterns.md    # before/after for every transformation type
    ├── expression-function-map.md    # expression language → Spark, with the gotchas
    ├── orchestration.md              # Session/Workflow/Worklet → Lakeflow
    ├── validation-checklist.md       # four-gate reconciliation and cutover
    └── xml-parsing-notes.md          # PowerCenter XML + IDMC export parsing
```

## Installation

**Claude.ai / Claude Desktop** — upload the packaged `.skill` file and click
**Save skill**.

**Claude Code / filesystem** — copy the folder into your skills directory:

```bash
cp -r informatica-to-pyspark-migration ~/.claude/skills/
```

**Databricks / repo use** — commit the folder alongside your migration code so
the reference files travel with the converted mappings. Reviewers use
`transformation-patterns.md` as the spec they diff generated code against.

## What it does

| Informatica | Databricks |
|---|---|
| Source Qualifier / Sources | `spark.read`, Auto Loader, JDBC, Lakehouse Federation |
| Target | Delta + Unity Catalog + `MERGE` |
| Expression | `withColumns` / Spark SQL, with a full function map |
| Filter / Router | `.filter()` / named branch DataFrames |
| Joiner | DataFrame join + broadcast |
| Lookup (connected & unconnected) | join rewrite, with multiple-match policy preserved |
| Aggregator / Rank / Sorter | `groupBy`, window functions, `orderBy` |
| Sequence Generator | Delta IDENTITY / window offset / `monotonically_increasing_id` |
| Update Strategy | Delta `MERGE` |
| Mapplet | reusable Python function |
| Session | Lakeflow Job task (cluster, retries, pre/post SQL) |
| Workflow / Worklet | multi-task Job or Declarative Pipeline |
| Parameters / variables | job parameters + `params` / `table_map` dicts |

## Output contract

Every converted mapping becomes one pure function:

```python
def run_<mapping_name>(spark, params: dict, table_map: dict, dry_run: bool = True):
```

Physical names come only from `table_map`; business values only from `params`;
every write is guarded by `dry_run`. That guard is what makes the differential
validation in `validation-checklist.md` possible before cutover.

## Design principles

- **Semantic fidelity over clean-looking code.** PowerCenter's quiet behaviours
  (DECODE matching NULL to NULL, `||` treating NULL as empty string, Aggregator
  returning the last row's value for ungrouped ports, master/detail outer joins
  being the reverse of what people assume) are documented and preserved.
- **High-risk constructs are flagged, never silently approximated.** Unconnected
  lookups, Sequence Generators, multi-target Update Strategies, persistent
  mapping variables, dynamic lookup caches, and Stored Procedure/Java/SQL
  transformations all emit an explicit `# RISK:` comment and a call-out.
- **The dataflow is the spec, not the canvas.** Chains of Expression/Filter/Sorter
  collapse into readable Spark rather than a one-to-one transliteration.
- **Validation is part of the deliverable.** Conversion without a reconciliation
  plan is a liability, so the skill produces the harness alongside the code.

## Typical prompts that trigger it

- "Convert this PowerCenter mapping XML to PySpark."
- "We're moving off Informatica to Databricks — where do I start with this workflow?"
- "What does this Update Strategy plus Sequence Generator become in Delta?"
- "Build an inventory and complexity assessment from this folder export."
- "Write the reconciliation harness so I can prove the new job matches."

## Scope and limits

The skill converts logic; it does not convert what it cannot see. Stored
Procedure, Java, Custom and External Procedure transformations need their source
obtained separately. Data volumes, skew, and whether a mapping is still in use
are not in the export — the skill asks rather than assumes. Cluster sizing starts
from data volume, not from transcribed DTM buffer settings.
