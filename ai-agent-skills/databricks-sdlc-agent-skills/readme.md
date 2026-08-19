# Databricks Multi-Agent SDLC Platform — per-agent skills

This folder contains 15 skills, one per agent in the pipeline from your
diagram (Jira Requirement → ... → Production Monitoring). Each folder is a
standalone Claude skill (`SKILL.md` + `references/`) that tells Claude how to
act as that specific agent: what it reads, what it must produce, and how it
calls the Databricks backend to do the actual work.

![Alt text](https://github.com/TRRaveendra/AI-Agents-Databricks/blob/main/images/sdlc_ai_agents_skills.png?raw=true)

```
agent-skills/
├── 01-jira-requirement-agent/
│   ├── SKILL.md
│   └── references/
│       ├── output-contract.md      # JSON schema this agent must return
│       └── coding-backend.md       # how to call the LLM/Genie backend
├── 02-requirement-validation-agent/
├── 03-business-understanding-agent/
├── 04-metadata-discovery-agent/
├── 05-mapping-document-agent/        (approval gate)
├── 06-data-model-agent/              (approval gate)
├── 07-pyspark-development-agent/
├── 08-sql-agent/
├── 09-data-quality-agent/
├── 10-code-review-agent/             (approval gate)
├── 11-unit-test-agent/
├── 12-testing-agent/
├── 13-documentation-agent/
├── 14-deployment-agent/              (approval gate)
└── 15-production-monitoring-agent/
```

## Important correction to the "Genie coding agent via API" plan

I checked this against current Databricks docs before building the skills,
because it changes the architecture:

- **Genie Code** — the AI coding/data assistant that writes and runs code in
  notebooks, the SQL editor, jobs, and the file editor — is **UI-only today.
  There is no public API to call it headlessly.** You cannot point an
  orchestrator at "Genie Code" the way the diagram's automated pipeline
  implies.
- **Genie Agents** (formerly "Genie Spaces") **does** have a real,
  documented, callable API — the **Genie Agents Conversation API**
  (`POST /api/2.0/genie/spaces/{space_id}/start-conversation`, plus a
  Databricks SDK wrapper `w.genie.start_conversation_and_wait(...)`). But it
  answers **natural-language questions about data** (stateful, SQL-generating
  Q&A) — it is not a general code-generation agent for writing PySpark ETL,
  unit tests, DDL, deployment bundles, etc.

So every skill here is built around this split:

| Need | Backend to call |
|---|---|
| Write/review PySpark, SQL, DDL, tests, docs, bundles | **Foundation Model API** (Model Serving pay-per-token endpoint, e.g. `databricks-claude-sonnet-4-5`) — this is the real agent-callable "coding agent" surface |
| Ask an ad-hoc natural-language question about data (profiling, metrics, "why did X spike") | **Genie Agents Conversation API**, used as a *tool* inside agents 3, 4, 8, and 15 |

`references/coding-backend.md` in every skill folder has the working Python
for both, and repeats this distinction so no agent gets built around an API
that doesn't exist. If Databricks ships a headless Genie Code API later,
swap the endpoint in that one reference file rather than rewriting the
agents.

## How these skills relate to the `databricks-sdlc-agents` skill

You already have a broader skill installed (`databricks-sdlc-agents`) that
generates this entire platform as one runnable project (LangGraph
supervisor, all 15 agent nodes, RAG knowledge engine, Databricks App,
Asset Bundle deployment). These 15 per-agent skills are the **decomposed,
individually-invokable version** of the same architecture — useful when you
want to:

- Work on/regenerate one agent at a time without touching the others,
- Hand a single agent's skill to a teammate or a narrower Claude session,
- Or drop these into `agents/prompts.py` and `agents/agent_definitions.py`
  of the project the other skill scaffolds — each `SKILL.md` maps 1:1 to a
  node in that project's `sdlc_supervisor.py` graph.

If you ask me to generate the full running project, I'll use
`databricks-sdlc-agents` and wire these prompts into it. If you ask me to
work on/run one stage, I'll use that agent's skill directly.

## Pipeline order & approval gates

```
1 Jira Requirement → 2 Requirement Validation [GATE] → 3 Business Understanding
→ 4 Metadata Discovery → 5 Mapping Document [GATE] → 6 Data Model [GATE]
→ 7 PySpark Dev ⟍
                  ⟩→ 9 Data Quality → 10 Code Review [GATE]
→ 8 SQL Agent    ⟋
→ 11 Unit Test ⟍
                ⟩→ 13 Documentation → 14 Deployment [GATE] → 15 Production Monitoring
→ 12 Testing   ⟋
```

Every agent writes its artifact to
`/Volumes/{catalog}/state/artifacts/{ticket}/{NN}_{agent}/` and a row to
`state.artifacts` + `state.agent_runs`, and returns the JSON contract in its
`references/output-contract.md`. Downstream agents read the artifact
**path + summary**, not the full content, to control context size.

## Using these skills

Each skill's frontmatter `description` is written to trigger when you say
things like "run the PySpark development agent for ticket JIRA-123" or "act
as the code review agent on this notebook." You can also just say "run the
full pipeline for ticket X" and I'll walk through agents 1→15 in order,
respecting the gates (I'll stop and show you the artifact for your sign-off
at each `[GATE]` step rather than auto-continuing).

Want me to also generate the runnable orchestrator project (`setup.py`,
`agents/agent_definitions.py`, `orchestration/run_pipeline.py`, the
Databricks App) that actually wires these 15 prompts into a LangGraph
StateGraph on your workspace? That's what `databricks-sdlc-agents` produces
— say the word and I'll scaffold it against a catalog/schema you name.
