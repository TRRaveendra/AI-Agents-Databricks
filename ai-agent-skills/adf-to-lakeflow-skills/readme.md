# ADF → Databricks Lakeflow Migration Skills

A knowledge base of **enterprise-grade migration patterns** for converting Azure Data Factory (ADF) pipelines to **Databricks Lakeflow Jobs** and **Spark Declarative Pipelines (DLT)**.

Nine component-specific skill files, a master index, and a runnable Databricks user guide — roughly 260 KB of production-oriented documentation with real PySpark, SQL, and YAML, not placeholders.

---

## Why this exists

ADF and Lakeflow do not map one-to-one. A Copy Activity is not a `spark.read`, a Linked Service is not a connection string, and a ForEach loop has no direct equivalent in a Lakeflow Job graph. Every migration team rediscovers these gaps the hard way.

This library encodes the mappings once — including the parts that *don't* translate cleanly — so engineers, architects, and AI coding agents all work from the same conversion rules.

---

## What's inside

```
adf-to-lakeflow-skills/
│
├── README.md                                  ← you are here
├── migration_index.md                         ⭐ START HERE
├── USER_GUIDE_How_to_Use_Migration_Skills.py   Databricks notebook (import this)
│
├── adf_pipeline_to_lakeflow_job.md            Pipeline orchestration
├── adf_activity_to_task.md                    Activity → Task
├── adf_copy_activity_to_spark.md              Data movement
├── adf_lookup_to_spark_sql.md                 Watermarks & metadata
├── adf_foreach_to_task_loop.md                Iteration & loops
├── adf_if_condition_to_task_dependency.md     Conditional logic
├── adf_linked_service_to_unity_catalog.md     Connections & secrets
├── adf_dataset_to_delta_table.md              Schema & formats
└── adf_trigger_to_job_schedule.md             Scheduling
```

### Component mapping

| ADF Component | Lakeflow Equivalent | Complexity | Skill File |
|---|---|---|---|
| Pipeline | Lakeflow Job | ⭐⭐ Medium | [`adf_pipeline_to_lakeflow_job.md`](adf_pipeline_to_lakeflow_job.md) |
| Activity | Task | ⭐⭐ Medium | [`adf_activity_to_task.md`](adf_activity_to_task.md) |
| Copy Activity | Spark Read/Write | ⭐⭐⭐ High | [`adf_copy_activity_to_spark.md`](adf_copy_activity_to_spark.md) |
| Lookup Activity | Spark SQL | ⭐⭐ Medium | [`adf_lookup_to_spark_sql.md`](adf_lookup_to_spark_sql.md) |
| ForEach Activity | Task Loop / Parameterized Job | ⭐⭐⭐ High | [`adf_foreach_to_task_loop.md`](adf_foreach_to_task_loop.md) |
| If Condition | Conditional Task Dependency | ⭐⭐ Medium | [`adf_if_condition_to_task_dependency.md`](adf_if_condition_to_task_dependency.md) |
| Linked Service | Unity Catalog / Secrets | ⭐⭐⭐⭐ Very High | [`adf_linked_service_to_unity_catalog.md`](adf_linked_service_to_unity_catalog.md) |
| Dataset | Delta Table / Path | ⭐⭐ Medium | [`adf_dataset_to_delta_table.md`](adf_dataset_to_delta_table.md) |
| Trigger | Job Schedule | ⭐ Low | [`adf_trigger_to_job_schedule.md`](adf_trigger_to_job_schedule.md) |

---

## Quick start

**1. Read the index.** [`migration_index.md`](migration_index.md) has the architecture, the dependency graph, and the recommended phase order.

**2. Import the user guide.** Upload `USER_GUIDE_How_to_Use_Migration_Skills.py` to your Databricks workspace — it imports as a notebook (it uses `# MAGIC %md` cells). It covers quick start, Genie Code Agent prompt templates, best practices, troubleshooting, and cheat sheets.

**3. Inventory your ADF pipelines.** List every activity type in scope, then open the matching skill file for each.

**4. Migrate in dependency order** — foundation first, control flow last. See below.

---

## Recommended migration order

| Phase | Focus | Components |
|---|---|---|
| **1 — Foundation** | Connectivity & storage | Linked Services → Unity Catalog · Datasets → Delta tables · Triggers → Job schedules |
| **2 — Core Logic** | Data movement | Copy Activities → Spark read/write · Lookups → Spark SQL · Activities → Tasks |
| **3 — Control Flow** | Orchestration | If Conditions → task dependencies · ForEach → task loops · Pipeline → full job |
| **4 — Validation** | Cutover | End-to-end testing · performance tuning · production deployment |

The indicative timeline in the index is 8 weeks; treat it as a shape, not a commitment — scale it to your pipeline count and connector variety. Phase 1 is where most schedule risk lives, because Unity Catalog external locations and secret scopes usually touch teams outside the data platform group.

---

## Every skill file follows the same 10 sections

1. **Overview** — what the ADF component actually does
2. **Lakeflow Equivalent** — concept mapping, including where it breaks down
3. **Migration Strategy** — step-by-step approach
4. **Code Conversion** — real before/after (ADF JSON → YAML / PySpark / SQL)
5. **Design Patterns** — what to do
6. **Anti-Patterns** — what to stop doing (usually: porting ADF habits verbatim)
7. **Performance Considerations** — optimization techniques
8. **Governance & Security** — Unity Catalog, secrets, ACLs
9. **Automation Hints** — structured guidance for AI agents
10. **Validation Checklist** — how you know it worked

Predictable structure is deliberate: an agent can load any file and know exactly where the transformation rules live.

---

## Using this with an AI agent

Each skill file is written to be consumed programmatically. The intended loop:

```python
def generate_migration_plan(adf_pipeline_json):
    components = extract_components(adf_pipeline_json)
    for component in components:
        skill = load_skill(component.type)      # load the matching .md
        lakeflow_code = skill.transform(component)
        validate(lakeflow_code)
    return lakeflow_job_yaml
```

Section 9 of each file (**Automation Hints**) carries the parse → map → generate rules. The user guide notebook has ready-made prompt templates for Databricks Genie Code Agent, plus worked conversation examples for single-activity, ForEach, and full-pipeline migrations.

A practical note: give the agent the skill file *and* the source ADF JSON in the same context. Handing it only the skill file produces generic Spark code; handing it only the JSON produces code that ignores your governance model.

---

## Security & governance mapping

| ADF Feature | Lakeflow Equivalent | Implementation |
|---|---|---|
| Managed Identity | Service Principal | Unity Catalog external locations |
| Key Vault | Databricks Secrets | Secret scopes |
| Data Flow encryption | Delta encryption | Transparent encryption at rest |
| Network isolation | Private Link | VNet injection |
| RBAC | Unity Catalog ACLs | Fine-grained permissions |

---

## Who this is for

| Role | Primary use |
|---|---|
| Data Engineers | Hands-on pipeline, copy, and transformation migration |
| Solution Architects | Strategy, design patterns, architecture planning |
| DevOps Engineers | Job scheduling, cluster config, CI/CD |
| AI/ML Engineers | Building automated migration tooling |
| Project Managers | Phase planning, effort estimation, tracking |

---

## Migration success criteria

- [ ] All pipelines migrated to Lakeflow Jobs
- [ ] All linked services mapped to Unity Catalog
- [ ] All datasets converted to Delta tables
- [ ] Performance benchmarks met or exceeded
- [ ] Data quality validation passed
- [ ] Security and compliance maintained
- [ ] Documentation complete
- [ ] Team training completed

---

## Reference

* [Databricks Lakeflow / Workflows](https://docs.databricks.com/workflows/)
* [Spark Declarative Pipelines (DLT)](https://docs.databricks.com/delta-live-tables/)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)
* [Delta Lake Best Practices](https://docs.databricks.com/delta/)

---

**Version:** 1.0
**Last Updated:** 2026-04-22
**Author:** [@TRRaveendra](https://github.com/TRRaveendra)
