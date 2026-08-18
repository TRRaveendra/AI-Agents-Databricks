# Databricks notebook source
# DBTITLE 1,Agent 8: SQL Agent
# MAGIC %md
# MAGIC # Agent 8: SQL Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 8 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="strong")
# MAGIC Tool: **Genie Agents Conversation API** (`ask_genie`) for ad-hoc data questions.
# MAGIC Skill: `08-sql-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "08_sql-agent"
SKILL_FOLDER = "08-sql-agent"
STAGE = "08_sql-agent"

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
    Execute Agent 8 for a single ticket. Callable directly for interactive testing,
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
Generate SQL transformations: analytical views with CTEs, window functions, aggregations, with CLUSTER BY / optimization hints and commented business logic. Use ask_genie() only if you need to sanity-check a real current metric value; it is not the SQL-generation backend.
"""
        user_prompt = f"""
Data model / DDL:
{json.dumps(upstream, indent=2)[:3000]}

Generate 2 SQL scripts as fenced ```sql blocks: (1) a transformation view with CTEs, (2) an
analytics aggregation with window functions. Add a trailing fenced ```json block:
{{"scripts": [{{"name": str, "sql_type": str}}], "status": "success"}}
"""

        model_response = call_foundation_model(system_prompt, user_prompt, tier="strong")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        # result_json is produced inside write_artifact below (code-block extraction)
        print(f"\n📝 Extracting code blocks from model response...")
        sql_blocks = extract_code_blocks(model_response["content"], lang="sql")
        json_blocks = extract_code_blocks(model_response["content"], lang="json")
        print(f"   Found {len(sql_blocks)} SQL blocks, {len(json_blocks)} JSON blocks")
        try:
            result_json = json.loads(json_blocks[-1]) if json_blocks else {}
        except Exception:
            result_json = {}

        meta = result_json.get("scripts", [])
        saved = []
        print(f"\n⚙️  Processing {min(len(sql_blocks), 4)} SQL scripts...")
        for i, sql in enumerate(sql_blocks[:4]):
            name = meta[i]["name"] if i < len(meta) else f"{params['ticket_id']}_sql_{i+1}"
            
            # QUALITY ENHANCEMENT: Add header comments, inline documentation, optimization hints
            try:
                enhanced_sql = enhance_sql_code(sql, name)
            except Exception as enhance_error:
                print(f"⚠️  Warning: enhance_sql_code failed for {name}: {enhance_error}")
                print(f"   Falling back to original SQL")
                enhanced_sql = sql
            
            path = write_text_artifact(params["ticket_id"], STAGE, f"{name}.sql", enhanced_sql)
            saved.append({"name": name, "path": path})
            print(f"   ✓ {name}: {len(enhanced_sql)} chars, saved to {path.split('/')[-1]}")

        print(f"\n📦 Creating artifact package with {len(saved)} scripts...")
        artifact = {
            "ticket_id": params["ticket_id"],
            "scripts": saved,
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "sql_code_package.json", artifact)
        print(f"   ✓ Artifact written to: {artifact_path}")
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        print(f"\n📊 Creating AgentOutput (duration: {duration_ms}ms)...")
        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"SQL Agent completed for {ticket_id}. Output at {artifact_path}.",
            clarification_questions=clarifications,
            citations=[artifact_path] + ([input_artifact] if input_artifact else []),
            tool_calls=tool_calls,
            tokens={"input": model_response["input_tokens"], "output": model_response["output_tokens"]},
            duration_ms=duration_ms,
        )
        print(f"   ✓ AgentOutput created successfully")
        print(f"\n📋 Logging to agent_runs table...")
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, output.status, model_response["endpoint"],
                       model_response["input_tokens"], model_response["output_tokens"], duration_ms, tool_calls)
        print(f"   ✓ Logged to agent_runs")
        print(f"\n✔️  Agent 8 run() completed successfully")
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"SQL Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 8 ready — call run(ticket_id) directly, or run this notebook as a job "
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
        print(f"\n{'='*80}")
        print(f"AGENT 8 JOB ENTRY POINT")
        print(f"{'='*80}")
        _p = get_params()
        print(f"Params: ticket_id={_p['ticket_id']}, input_artifact={_p.get('input_artifact', '')[:60]}...")
        print(f"\nCalling run()...")
        _result = run(_p["ticket_id"], _p.get("input_artifact", ""))
        print(f"\n📝 run() returned: status={_result.status}, artifact_path={_result.artifact_path}")
        print(f"\nCalling finish()...")
        finish(_result)
        print(f"   ✓ finish() completed - notebook should exit now")

# COMMAND ----------

# DBTITLE 1,Test Run - KAN-6
# Standalone test for Agent 8
result = run('KAN-6', '/Volumes/main/state/artifacts/KAN-6/06_data-model-agent/data_model.json')
print(json.dumps(result.to_dict(), indent=2))