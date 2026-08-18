# Databricks notebook source
# DBTITLE 1,Agent 3: Business Understanding Agent
# MAGIC %md
# MAGIC # Agent 3: Business Understanding Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 3 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="medium")
# MAGIC Tool: **Genie Agents Conversation API** (`ask_genie`) for ad-hoc data questions.
# MAGIC Skill: `03-business-understanding-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "03_business-understanding-agent"
SKILL_FOLDER = "03-business-understanding-agent"
STAGE = "03_business-understanding-agent"

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
    Execute Agent 3 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:

        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "02_requirement-validation-agent"
        upstream = read_upstream_json(ticket_id, "02_requirement-validation-agent", "validated_requirement.json")

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Analyze business context: objectives, current vs future state, business rules, KPIs, data flow, risks, and assumptions. If GENIE_SPACE_ID is configured and a KPI needs a real current value, call ask_genie() to look it up rather than guessing.
"""
        user_prompt = f"""
Validated requirement:
{json.dumps(upstream, indent=2)[:2500]}

Return JSON with: business_objective, current_state, future_state, business_rules (list),
kpis (list of {{name, definition, target}}), data_flow, risks (list), assumptions (list).
"""

        model_response = call_foundation_model(system_prompt, user_prompt, tier="medium")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        result_json = extract_json(model_response["content"])
        artifact = {
            "ticket_id": params["ticket_id"],
            "business_objective": result_json.get("business_objective", ""),
            "current_state": result_json.get("current_state", ""),
            "future_state": result_json.get("future_state", ""),
            "business_rules": result_json.get("business_rules", []),
            "kpis": result_json.get("kpis", []),
            "data_flow": result_json.get("data_flow", ""),
            "risks": result_json.get("risks", []),
            "assumptions": result_json.get("assumptions", []),
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "business_understanding.json", artifact)
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Business Understanding Agent completed for {ticket_id}. Output at {artifact_path}.",
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
                            summary=f"Business Understanding Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 3 ready — call run(ticket_id) directly, or run this notebook as a job "
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