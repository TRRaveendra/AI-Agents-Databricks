# Databricks notebook source
# DBTITLE 1,Agent 7: PySpark Development Agent
# MAGIC %md
# MAGIC # Agent 7: PySpark Development Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 7 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="strong")
# MAGIC Skill: `07-pyspark-development-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "07_pyspark-development-agent"
SKILL_FOLDER = "07-pyspark-development-agent"
STAGE = "07_pyspark-development-agent"

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

# DBTITLE 1,Main Logic (Enhanced Quality)
def run(ticket_id: str, input_artifact: str = "") -> AgentOutput:
    """
    Execute Agent 7 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:

        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "06_data-model-agent"
        upstream = read_upstream_json(ticket_id, "06_data-model-agent", "data_model.json")

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Generate production-ready PySpark notebooks (# Databricks notebook source format) implementing the bronze/silver/gold transformations implied by the data model: Auto Loader / batch ingestion, transformations, and Delta writes with the given DDL. Return each notebook's source as a separate code block.
"""
        user_prompt = f"""
Data model / DDL:
{json.dumps(upstream, indent=2)[:3000]}

Generate 2 PySpark notebooks: (1) bronze/silver ingestion + cleansing, (2) silver/gold
transformation + write. Each as a fenced ```python block, valid `# Databricks notebook source`
files. Also return JSON status via a final fenced ```json block with:
{{"notebooks": [{{"name": str, "purpose": str}}], "status": "success"}}
"""

        model_response = call_foundation_model(system_prompt, user_prompt, tier="strong")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        # result_json is produced inside write_artifact below (code-block extraction)
        code_blocks = extract_code_blocks(model_response["content"], lang="python")
        json_blocks = extract_code_blocks(model_response["content"], lang="json")
        try:
            result_json = json.loads(json_blocks[-1]) if json_blocks else {}
        except Exception:
            result_json = {}

        notebooks_meta = result_json.get("notebooks", [])
        saved = []
        for i, code in enumerate(code_blocks[:4]):
            name = notebooks_meta[i]["name"] if i < len(notebooks_meta) else f"{params['ticket_id']}_pyspark_{i+1}"
            
            # QUALITY ENHANCEMENT: Refactor code into functions with docstrings, error handling, logging
            enhanced_code = enhance_pyspark_code(code, name)
            
            path = write_text_artifact(params["ticket_id"], STAGE, f"{name}.py", enhanced_code)
            saved.append({"name": name, "path": path})

        artifact = {
            "ticket_id": params["ticket_id"],
            "notebooks": saved,
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "pyspark_code_package.json", artifact)
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"PySpark Development Agent completed for {ticket_id}. Output at {artifact_path}.",
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
                            summary=f"PySpark Development Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 7 ready — call run(ticket_id) directly, or run this notebook as a job "
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

# DBTITLE 1,Test Run Agent 7
# Test Agent 7 with KAN-6 ticket
result = run('KAN-6', '/Volumes/main/state/artifacts/KAN-6/06_data-model-agent/data_model.json')
print(f"\nStatus: {result.status}")
print(f"Summary: {result.summary}")
print(f"Artifact Path: {result.artifact_path}")
print(f"Duration: {result.duration_ms}ms")
if result.clarification_questions:
    print(f"Clarifications needed: {result.clarification_questions}")