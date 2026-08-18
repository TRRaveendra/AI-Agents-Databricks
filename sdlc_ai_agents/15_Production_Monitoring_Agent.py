# Databricks notebook source
# DBTITLE 1,Agent 15: Production Monitoring Agent
# MAGIC %md
# MAGIC # Agent 15: Production Monitoring Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 15 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="medium")
# MAGIC Tool: **Genie Agents Conversation API** (`ask_genie`) for ad-hoc data questions.
# MAGIC Skill: `15-production-monitoring-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "15_production-monitoring-agent"
SKILL_FOLDER = "15-production-monitoring-agent"
STAGE = "15_production-monitoring-agent"

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
    Execute Agent 15 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    print(f"\n{'='*80}")
    print(f"AGENT 15 (Production Monitoring) - Starting execution for {ticket_id}")
    print(f"{'='*80}")
    
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:
        print(f"\n[1/5] Loading deployment manifest...")
        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "14_deployment-agent"
        try:
            upstream = read_upstream_json(ticket_id, "14_deployment-agent", "deployment_manifest.json")
            print(f"  ✓ Deployment manifest loaded")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load deployment manifest: {e}")
            upstream = {"bundle_path": "", "environments": []}

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Design alerts (job failure, DQ violation, latency/SLA breach) and Lakeview dashboard specs (metrics + refresh schedule) for the deployed pipeline. Use ask_genie() if you need to check a real current baseline metric to set a sane threshold.
"""
        user_prompt = f"""
Deployment manifest: {json.dumps(upstream, indent=2)[:1500]}

Return JSON with: alerts (list of {{name, alert_type, condition, threshold, notification_channels}}),
dashboards (list of {{name, metrics, refresh_schedule}}), log_retention_days (int),
anomaly_detection_enabled (bool).
"""
        print(f"  ✓ Prompts prepared")

        print(f"\n[2/5] Calling foundation model...")
        model_response = call_foundation_model(system_prompt, user_prompt, tier="medium")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        print(f"  ✓ Model response received ({model_response['input_tokens']} in, {model_response['output_tokens']} out)")
        
        print(f"\n[3/5] Extracting monitoring configuration...")
        result_json = extract_json(model_response["content"])
        print(f"  ✓ Configuration extracted ({len(result_json.get('alerts', []))} alerts, {len(result_json.get('dashboards', []))} dashboards)")
        print(f"\n[4/5] Writing artifact...")
        artifact = {
            "ticket_id": params["ticket_id"],
            "alerts": result_json.get("alerts", []),
            "dashboards": result_json.get("dashboards", []),
            "log_retention_days": result_json.get("log_retention_days", 90),
            "anomaly_detection_enabled": result_json.get("anomaly_detection_enabled", True),
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "monitoring_configuration.json", artifact)
        print(f"  ✓ Artifact written to: {artifact_path}")
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        print(f"\n[5/5] Creating AgentOutput and logging...")
        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Production Monitoring Agent completed for {ticket_id}. Output at {artifact_path}.",
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
        print(f"✅ AGENT 15 COMPLETED: {output.status} ({duration_ms}ms)")
        print(f"{'='*80}")
        print(f"\n{'='*80}")
        print(f"✅✅✅ SDLC PIPELINE COMPLETE - ALL 15 AGENTS EXECUTED ✅✅✅")
        print(f"{'='*80}")
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"Production Monitoring Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 15 ready — call run(ticket_id) directly, or run this notebook as a job "
      "(reads widgets ticket_id / input_artifact and exits via dbutils.notebook.exit).")

# COMMAND ----------

# DBTITLE 1,Job Entry Point (Fixed with __main__ guard)
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