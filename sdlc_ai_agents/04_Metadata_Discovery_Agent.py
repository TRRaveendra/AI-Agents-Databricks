# Databricks notebook source
# DBTITLE 1,Agent 4: Metadata Discovery Agent
# MAGIC %md
# MAGIC # Agent 4: Metadata Discovery Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 4 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="light")
# MAGIC Tool: **Genie Agents Conversation API** (`ask_genie`) for ad-hoc data questions.
# MAGIC Skill: `04-metadata-discovery-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "04_metadata-discovery-agent"
SKILL_FOLDER = "04-metadata-discovery-agent"
STAGE = "04_metadata-discovery-agent"

skill = load_skill(SKILL_FOLDER)
print(f"✓ Loaded skill '{skill.name}' from {skill.source_path}")

# COMMAND ----------

# DBTITLE 1,Widget Declarations
# Widget declarations for orchestrator calls via dbutils.notebook.run()
dbutils.widgets.text("ticket_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("input_artifact", "")
dbutils.widgets.text("catalog", "main")

# COMMAND ----------

# DBTITLE 1,Main Logic
def run(ticket_id: str, input_artifact: str = "") -> AgentOutput:
    """
    Execute Agent 4 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:
        # -- agent-specific setup --
        def discover_uc_metadata(catalog: str) -> dict:
            """Real Unity Catalog discovery via system.information_schema (not a guess)."""
            tables = [r.asDict() for r in spark.sql(f"""
                SELECT table_catalog, table_schema, table_name, table_type, table_owner
                FROM system.information_schema.tables
                WHERE table_catalog = '{catalog}'
                ORDER BY table_schema, table_name
            """).collect()]
            columns = [r.asDict() for r in spark.sql(f"""
                SELECT table_catalog, table_schema, table_name, column_name, data_type, comment
                FROM system.information_schema.columns
                WHERE table_catalog = '{catalog}'
                ORDER BY table_schema, table_name, ordinal_position
            """).collect()]
            return {"tables": tables, "columns": columns}

        uc_metadata = discover_uc_metadata(CATALOG)
        print(f"✓ Discovered {len(uc_metadata['tables'])} tables, {len(uc_metadata['columns'])} columns in {CATALOG}")

        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "03_business-understanding-agent"
        upstream = read_upstream_json(ticket_id, "03_business-understanding-agent", "business_understanding.json")

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Given the business context and the real Unity Catalog tables/columns discovered below, identify which tables are relevant sources/targets and flag likely data quality concerns. Use ask_genie() for any ad-hoc profiling question (e.g. null rates, row counts) if a Genie space is configured.
"""
        user_prompt = f"""
Business context:
{json.dumps(upstream, indent=2)[:1200]}

Discovered tables in catalog '{CATALOG}' ({len(uc_metadata['tables'])} total, showing first 20):
{json.dumps(uc_metadata['tables'][:20], indent=2)}

Return JSON with: relevant_tables (list of {{catalog, schema, table_name, relevance_score, reason}}),
column_mappings (list of {{source_table, source_column, target_table, target_column}}),
recommendations (list).
"""

        model_response = call_foundation_model(system_prompt, user_prompt, tier="light")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        result_json = extract_json(model_response["content"])
        artifact = {
            "ticket_id": params["ticket_id"],
            "discovered_tables": uc_metadata["tables"],
            "discovered_columns": uc_metadata["columns"],
            "relevant_tables": result_json.get("relevant_tables", []),
            "column_mappings": result_json.get("column_mappings", []),
            "recommendations": result_json.get("recommendations", []),
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "metadata_discovery.json", artifact)
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Metadata Discovery Agent completed for {ticket_id}. Output at {artifact_path}.",
            clarification_questions=clarifications,
            citations=[artifact_path] + ([input_artifact] if input_artifact else []),
            tool_calls=tool_calls,
            tokens={"input": model_response["input_tokens"], "output": model_response["output_tokens"]},
            duration_ms=duration_ms,
        )
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, output.status, model_response["endpoint"],
                       model_response["input_tokens"], model_response["output_tokens"], duration_ms, tool_calls)
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"Metadata Discovery Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 4 ready — call run(ticket_id) directly, or run this notebook as a job "
      "(reads widgets ticket_id / input_artifact and exits via dbutils.notebook.exit).")

# COMMAND ----------

# DBTITLE 1,Job Entry Point
# When launched via dbutils.notebook.run() from the orchestrator, widgets are populated —
# execute and exit with the JSON contract. When run interactively cell-by-cell, this is a no-op
# (call run('YOUR-TICKET-ID') directly from a new cell instead).
if __name__ == "__main__":
    try:
        _is_job = dbutils.widgets.get("ticket_id") not in (None, "")
    except Exception:
        _is_job = False
    if _is_job:
        _p = get_params()
        _result = run(_p["ticket_id"], _p.get("input_artifact", ""))
        finish(_result)