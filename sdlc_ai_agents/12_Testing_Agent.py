# Databricks notebook source
# DBTITLE 1,Agent 12: Testing Agent
# MAGIC %md
# MAGIC # Agent 12: Testing Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 12 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="light")
# MAGIC Skill: `12-testing-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
import json
import time
import uuid
from datetime import datetime
from typing import Dict, List

AGENT_KEY = "12_testing-agent"
SKILL_FOLDER = "12-testing-agent"
STAGE = "12_testing-agent"

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
    Execute Agent 12 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    print(f"\n{'='*80}")
    print(f"AGENT 12 (Testing) - Starting execution for {ticket_id}")
    print(f"{'='*80}")
    
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:
        print(f"\n[1/6] Running pytest...")
        import subprocess
        test_dir = f"{ARTIFACTS_VOLUME}/{params['ticket_id']}/11_unit-test-agent"
        print(f"  Test directory: {test_dir}")
        try:
            proc = subprocess.run(
                ["python", "-m", "pytest", test_dir, "--json-report", "--json-report-file=/tmp/report.json", "-q"],
                capture_output=True, text=True,
            )
            print(f"  ✓ pytest execution completed (exit code: {proc.returncode})")
            if proc.stdout:
                print(f"  pytest output (last 500 chars): {proc.stdout[-500:]}")
        except Exception as pytest_error:
            print(f"  ⚠️  Warning: pytest execution failed: {pytest_error}")
            proc = type('obj', (object,), {'stdout': '', 'stderr': str(pytest_error), 'returncode': 1})()
        
        print(f"\n[2/6] Parsing pytest results...")
        try:
            with open("/tmp/report.json") as f:
                pytest_report = json.load(f)
            tests_run = pytest_report.get("summary", {})
            print(f"  ✓ Pytest report parsed: {tests_run.get('total', 0)} tests, {tests_run.get('passed', 0)} passed, {tests_run.get('failed', 0)} failed")
        except Exception as e:
            print(f"  ⚠️  pytest-json-report not available or no tests collected ({e}); reporting raw pytest output only.")
            tests_run = {"total": 0, "passed": 0, "failed": 0}

        print(f"\n[3/6] Loading unit test suite metadata...")
        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "11_unit-test-agent"
        try:
            upstream = read_upstream_json(ticket_id, "11_unit-test-agent", "unit_test_suite.json")
            print(f"  ✓ Unit test suite loaded")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load unit test suite: {e}")
            upstream = {"test_cases": []}

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Summarize the real pytest execution results below into a readable test report with a pass/fail recommendation for whether the code is ready for documentation + deployment.
"""
        user_prompt = f"""
pytest summary: {json.dumps(tests_run)}
pytest stdout (tail): {proc.stdout[-1500:]}
pytest stderr (tail): {proc.stderr[-500:]}

Return JSON with: passed (int), failed (int), total (int), pass_rate (float),
status ("success"|"failed"), narrative_summary (string).
"""
        print(f"  ✓ Prompts prepared")

        print(f"\n[4/6] Calling foundation model to summarize results...")
        model_response = call_foundation_model(system_prompt, user_prompt, tier="light")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        print(f"  ✓ Model response received")
        
        print(f"\n[5/6] Extracting summary...")
        result_json = extract_json(model_response["content"])
        # Defensive: ensure result_json is always a dict to prevent AttributeError on .get() calls
        if not isinstance(result_json, dict):
            result_json = {}
        print(f"  ✓ Summary extracted")
        print(f"\n[6/6] Writing artifact...")
        artifact = {
            "ticket_id": params["ticket_id"],
            "pytest_summary": tests_run,
            "passed": result_json.get("passed", tests_run.get("passed", 0)),
            "failed": result_json.get("failed", tests_run.get("failed", 0)),
            "total": result_json.get("total", tests_run.get("total", 0)),
            "pass_rate": result_json.get("pass_rate", 0.0),
            "test_status": result_json.get("status", "failed" if tests_run.get("failed", 0) else "success"),
            "narrative_summary": result_json.get("narrative_summary", ""),
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "test_report.json", artifact)
        print(f"  ✓ Artifact written to: {artifact_path}")
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        print(f"\n[7/7] Creating AgentOutput and logging...")
        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Testing Agent completed for {ticket_id}. Output at {artifact_path}.",
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
        print(f"✅ AGENT 12 COMPLETED: {output.status} ({duration_ms}ms)")
        print(f"{'='*80}")
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"Testing Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 12 ready — call run(ticket_id) directly, or run this notebook as a job "
      "(reads widgets ticket_id / input_artifact and exits via dbutils.notebook.exit).")

# COMMAND ----------

# DBTITLE 1,Job Entry Point
# When launched via dbutils.notebook.run() from the orchestrator, widgets are populated —
# execute and exit with the JSON contract. When run interactively cell-by-cell, this is a no-op
# (call run('YOUR-TICKET-ID') directly from a new cell instead).
try:
    _is_job = dbutils.widgets.get("ticket_id") not in (None, "")
except Exception:
    _is_job = False
    
if _is_job:
    _p = get_params()
    _result = run(_p["ticket_id"], _p.get("input_artifact", ""))
    finish(_result)