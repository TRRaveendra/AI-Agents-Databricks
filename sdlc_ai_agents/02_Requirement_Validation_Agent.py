# Databricks notebook source
# DBTITLE 1,Agent 2: Requirement Validation Agent
# MAGIC %md
# MAGIC # Agent 2: Requirement Validation Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 2 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="light")
# MAGIC Skill: `02-requirement-validation-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **Yes — human approval required**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "02_requirement-validation-agent"
SKILL_FOLDER = "02-requirement-validation-agent"
STAGE = "02_requirement-validation-agent"

skill = load_skill(SKILL_FOLDER)
print(f"✓ Loaded skill '{skill.name}' from {skill.source_path}")

# COMMAND ----------

# DBTITLE 1,Widget Declarations
# Declare widgets for dbutils.notebook.run() parameter passing
dbutils.widgets.text("ticket_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("input_artifact", "")
dbutils.widgets.text("catalog", "main")

# COMMAND ----------

# DBTITLE 1,Main Logic
def run(ticket_id: str, input_artifact: str = "") -> AgentOutput:
    """
    Execute Agent 2 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:

        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "01_jira-requirement-agent"
        upstream = read_upstream_json(ticket_id, "01_jira-requirement-agent", "requirement.json")

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Validate the requirement package for completeness. Check for: missing acceptance criteria, ambiguous scope, undefined data sources/targets, missing non-functional requirements (SLAs, volumes, security). This is an APPROVAL GATE — a human reviews your output before the pipeline continues, so be thorough and explicit about gaps.
"""
        user_prompt = f"""
Requirement package to validate:
{json.dumps(upstream, indent=2)[:3000]}

Return JSON with: is_complete (bool), completeness_score (0-1), issues (list of
{{severity, category, description}}), clarification_questions (list), status
("complete"|"needs_clarification").
"""

        model_response = call_foundation_model(system_prompt, user_prompt, tier="light")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        result_json = extract_json(model_response["content"])
        artifact = {
            "ticket_id": params["ticket_id"],
            "source_requirement": upstream,
            "is_complete": result_json.get("is_complete", True),
            "completeness_score": result_json.get("completeness_score", 0.8),
            "issues": result_json.get("issues", []),
            "clarification_questions": result_json.get("clarification_questions", []),
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "validated_requirement.json", artifact)
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Requirement Validation Agent completed for {ticket_id}. Output at {artifact_path}.",
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
                            summary=f"Requirement Validation Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 2 ready — call run(ticket_id) directly, or run this notebook as a job "
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

# COMMAND ----------

# DBTITLE 1,Test Agent 2 with KAN-6
# Test Agent 2 run() function directly with KAN-6
input_artifact = "/Volumes/main/state/artifacts/KAN-6/01_jira-requirement-agent/requirement.json"
print(f"Testing Agent 2 with KAN-6...")
print(f"Input artifact: {input_artifact}\n")

result = run('KAN-6', input_artifact)
print("\n" + "="*80)
print("AGENT 2 EXECUTION RESULT")
print("="*80)
print(f"Status: {result.status}")
print(f"Summary: {result.summary}")
print(f"Artifact: {result.artifact_path}")
if result.error:
    print(f"Error: {result.error}")
if result.clarification_questions:
    print(f"\nClarification Questions ({len(result.clarification_questions)}):")
    for i, q in enumerate(result.clarification_questions, 1):
        print(f"  {i}. {q}")

# COMMAND ----------

# DBTITLE 1,Test Widget Parameter Flow
# Simulate what happens when orchestrator calls via dbutils.notebook.run()
# Create widgets with the same parameters the orchestrator would pass
dbutils.widgets.text("ticket_id", "")
dbutils.widgets.text("input_artifact", "")
dbutils.widgets.text("catalog", "main")

# Set values as orchestrator would
dbutils.widgets.text("ticket_id", "KAN-6")
dbutils.widgets.text("input_artifact", "/Volumes/main/state/artifacts/KAN-6/01_jira-requirement-agent/requirement.json")

print("Widget values set:")
print(f"  ticket_id: {dbutils.widgets.get('ticket_id')}")
print(f"  input_artifact: {dbutils.widgets.get('input_artifact')}")
print(f"  catalog: {dbutils.widgets.get('catalog')}")

# Test get_params() function
params = get_params()
print(f"\nget_params() returned: {params}")

# COMMAND ----------

# DBTITLE 1,Simulate Full Job Execution
# Simulate the exact flow when dbutils.notebook.run() calls this notebook
print("Simulating job execution path...\n")

# The __main__ block in cell 5 does this:
_p = get_params()
print(f"Parameters: {_p}\n")

_result = run(_p["ticket_id"], _p.get("input_artifact", ""))
print(f"\nRun result status: {_result.status}")
print(f"Run result summary: {_result.summary}")

# Test finish() which calls dbutils.notebook.exit()
print("\nTesting finish() output...")
finish_output = json.dumps(_result.to_dict())
print(f"Would call dbutils.notebook.exit() with: {finish_output[:200]}...")