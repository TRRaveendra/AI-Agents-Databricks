# Databricks notebook source
# DBTITLE 1,Agent 9: Data Quality Agent
# MAGIC %md
# MAGIC # Agent 9: Data Quality Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 9 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="strong")
# MAGIC Skill: `09-data-quality-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "09_data-quality-agent"
SKILL_FOLDER = "09-data-quality-agent"
STAGE = "09_data-quality-agent"

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
    Execute Agent 9 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    print(f"\n{'='*80}")
    print(f"AGENT 9 (Data Quality) - Starting execution for {ticket_id}")
    print(f"{'='*80}")
    
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:
        print(f"\n[1/5] Reading upstream artifact...")
        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "06_data-model-agent"
        
        try:
            upstream = read_upstream_json(ticket_id, "06_data-model-agent", "data_model.json")
            print(f"  ✓ Successfully read upstream data model ({len(upstream.get('tables', []))} tables)")
        except Exception as read_error:
            print(f"  ⚠️  Warning: Failed to read upstream artifact: {read_error}")
            upstream = {"tables": []}  # Fallback to empty

        print(f"\n[2/5] Preparing prompts...")
        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Generate comprehensive data quality checks: null validation, type/format/range checks, uniqueness, referential integrity. Prefer expressing these as Delta CHECK constraint expressions where possible so they're directly enforceable.
"""
        user_prompt = f"""
Data model tables/columns:
{json.dumps(upstream.get("tables", [])[:3], indent=2)[:2500]}

Return JSON with: checks (list of {{name, rule, severity ("error"|"warning"), column,
constraint_expression}}).
"""
        print(f"  ✓ Prompts prepared")

        print(f"\n[3/5] Calling foundation model...")
        model_response = call_foundation_model(system_prompt, user_prompt, tier="strong")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        print(f"  ✓ Model response received ({model_response['input_tokens']} in, {model_response['output_tokens']} out)")
        
        print(f"\n[4/5] Extracting JSON from response...")
        result_json = extract_json(model_response["content"])
        print(f"  ✓ JSON extracted ({len(result_json.get('checks', []))} checks generated)")
        print(f"\n[5/5] Writing artifact...")
        artifact = {
            "ticket_id": params["ticket_id"],
            "checks": result_json.get("checks", []),
            "total_checks": len(result_json.get("checks", [])),
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "dq_test_suite.json", artifact)
        print(f"  ✓ Artifact written to: {artifact_path}")
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        print(f"\n[6/6] Creating AgentOutput and logging...")
        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Data Quality Agent completed for {ticket_id}. Output at {artifact_path}.",
            clarification_questions=clarifications,
            citations=[artifact_path] + ([input_artifact] if input_artifact else []),
            tool_calls=tool_calls,
            tokens={"input": model_response["input_tokens"], "output": model_response["output_tokens"]},
            duration_ms=duration_ms,
        )
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, output.status, model_response["endpoint"],
                       model_response["input_tokens"], model_response["output_tokens"], duration_ms, tool_calls)
        print(f"  ✓ Agent execution logged")
        print(f"\n{'='*80}")
        print(f"✅ AGENT 9 COMPLETED: {output.status} ({duration_ms}ms)")
        print(f"{'='*80}")
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"Data Quality Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 9 ready — call run(ticket_id) directly, or run this notebook as a job "
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

# DBTITLE 1,Test Agent 9
# Test Agent 9 standalone execution
result = run('KAN-6', '/Volumes/main/state/artifacts/KAN-6/06_data-model-agent/data_model.json')
print(f"\n{'='*80}")
print(f"RESULT: {result.status}")
print(f"Artifact: {result.artifact_path}")
print(f"Duration: {result.duration_ms}ms")
print(f"{'='*80}")