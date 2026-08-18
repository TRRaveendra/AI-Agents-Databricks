# Databricks notebook source
# DBTITLE 1,Agent 5: Mapping Document Agent
# MAGIC %md
# MAGIC # Agent 5: Mapping Document Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 5 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="medium")
# MAGIC Skill: `05-mapping-document-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **Yes — human approval required**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "05_mapping-document-agent"
SKILL_FOLDER = "05-mapping-document-agent"
STAGE = "05_mapping-document-agent"

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
    Execute Agent 5 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:

        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "04_metadata-discovery-agent"
        upstream = read_upstream_json(ticket_id, "04_metadata-discovery-agent", "metadata_discovery.json")

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Create detailed source-to-target field mappings with transformation logic, business rules, and data quality checks per field. This is an APPROVAL GATE — be explicit and complete since a human signs off on this before Data Model generation.
"""
        user_prompt = f"""
Metadata discovery package:
{json.dumps(upstream, indent=2)[:3000]}

Return JSON with: field_mappings (list of {{source_table, source_column, source_type, target_table,
target_column, target_type, transformation_logic, business_rule, data_quality_check}}),
transformation_rules (list of {{rule_id, rule_name, description, logic}}),
data_quality_rules (list of {{rule, condition}}).
"""

        model_response = call_foundation_model(system_prompt, user_prompt, tier="medium")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        result_json = extract_json(model_response["content"])
        artifact = {
            "ticket_id": params["ticket_id"],
            "field_mappings": result_json.get("field_mappings", []),
            "transformation_rules": result_json.get("transformation_rules", []),
            "data_quality_rules": result_json.get("data_quality_rules", []),
            "source_tables": sorted({m.get("source_table","") for m in result_json.get("field_mappings", [])}),
            "target_tables": sorted({m.get("target_table","") for m in result_json.get("field_mappings", [])}),
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "mapping_document.json", artifact)
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Mapping Document Agent completed for {ticket_id}. Output at {artifact_path}.",
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
                            summary=f"Mapping Document Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 5 ready — call run(ticket_id) directly, or run this notebook as a job "
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

# COMMAND ----------

# DBTITLE 1,Test Agent 5 - KAN-6
# Interactive test for KAN-6
result = run('KAN-6', '/Volumes/main/state/artifacts/KAN-6/04_metadata-discovery-agent/metadata_discovery.json')
print(json.dumps(result.__dict__, indent=2, default=str))

# COMMAND ----------

# DBTITLE 1,Test Job Entry Point Logic
# Simulate what happens when orchestrator calls this notebook
# Set the widgets like the orchestrator does
dbutils.widgets.text("ticket_id", "KAN-6")
dbutils.widgets.text("input_artifact", "/Volumes/main/state/artifacts/KAN-6/04_metadata-discovery-agent/metadata_discovery.json")
dbutils.widgets.text("catalog", "main")

# Now test the job entry point logic
print("Testing job entry point logic...")
try:
    _is_job = dbutils.widgets.get("ticket_id") not in (None, "")
    print(f"_is_job = {_is_job}")
    print(f"ticket_id widget = '{dbutils.widgets.get('ticket_id')}'")
except Exception as e:
    print(f"Error checking widgets: {e}")
    _is_job = False

if _is_job:
    print("Would execute: _p = get_params() and _result = run(...)")
    _p = get_params()
    print(f"Params: {_p}")
else:
    print("Job entry point would NOT execute (widgets empty)")