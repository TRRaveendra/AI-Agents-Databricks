# Databricks notebook source
# DBTITLE 1,Agent 13: Documentation Agent
# MAGIC %md
# MAGIC # Agent 13: Documentation Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 13 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="medium")
# MAGIC Skill: `13-documentation-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "13_documentation-agent"
SKILL_FOLDER = "13-documentation-agent"
STAGE = "13_documentation-agent"

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

# DBTITLE 1,Main Logic (Fixed)
def run(ticket_id: str, input_artifact: str = "") -> AgentOutput:
    """
    Execute Agent 13 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    print(f"\n{'='*80}")
    print(f"AGENT 13 (Documentation) - Starting execution for {ticket_id}")
    print(f"{'='*80}")
    
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:
        print(f"\n[1/6] Loading upstream artifacts...")
        try:
            business_doc = read_upstream_json(params["ticket_id"], "03_business-understanding-agent", "business_understanding.json")
            print(f"  ✓ Business understanding loaded")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load business understanding: {e}")
            business_doc = {}
            
        try:
            data_model = read_upstream_json(params["ticket_id"], "06_data-model-agent", "data_model.json")
            print(f"  ✓ Data model loaded ({len(data_model.get('tables', []))} tables)")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load data model: {e}")
            data_model = {"tables": []}

        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "12_testing-agent"
        try:
            upstream = read_upstream_json(ticket_id, "12_testing-agent", "test_report.json")
            print(f"  ✓ Test report loaded")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load test report: {e}")
            upstream = {}

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Generate README, architecture doc (with a Mermaid diagram), and an operational runbook covering this pipeline end to end, grounded in the real business context, data model, and test results below.
"""
        user_prompt = f"""
Business context: {json.dumps(business_doc, indent=2)[:800]}
Data model tables: {json.dumps([t.get('table_name') for t in data_model.get('tables', [])], indent=2)}
Test results: {json.dumps(upstream, indent=2)[:600]}

Return three fenced blocks in order: a ```markdown README, a ```markdown ARCHITECTURE doc
(include a ```mermaid flow diagram inside it), and a ```markdown RUNBOOK.
"""
        print(f"  ✓ Prompts prepared")

        print(f"\n[2/5] Calling foundation model...")
        model_response = call_foundation_model(system_prompt, user_prompt, tier="medium")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        print(f"  ✓ Model response received ({model_response['input_tokens']} in, {model_response['output_tokens']} out)")
        
        print(f"\n[3/5] Extracting markdown blocks...")
        md_blocks = extract_code_blocks(model_response["content"], lang="markdown")
        print(f"  ✓ Extracted {len(md_blocks)} markdown blocks")
        print(f"\n[4/5] Saving documentation files...")
        names = ["README.md", "ARCHITECTURE.md", "RUNBOOK.md"]
        saved = []
        for name, content in zip(names, md_blocks[:3]):
            path = write_text_artifact(params["ticket_id"], STAGE, name, content)
            saved.append({"name": name, "path": path})
        print(f"  ✓ Saved {len(saved)} documentation files")

        print(f"\n[5/5] Writing artifact...")
        artifact = {
            "ticket_id": params["ticket_id"],
            "documents": saved,
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "documentation_package.json", artifact)
        print(f"  ✓ Artifact written to: {artifact_path}")
        duration_ms = int((time.time() - start) * 1000)

        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="complete",
            artifact_path=artifact_path,
            summary=f"Documentation Agent completed for {ticket_id}. Output at {artifact_path}.",
            clarification_questions=[],
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
        print(f"✅ AGENT 13 COMPLETED: {output.status} ({duration_ms}ms)")
        print(f"{'='*80}")
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"Documentation Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 13 ready — call run(ticket_id) directly, or run this notebook as a job "
      "(reads widgets ticket_id / input_artifact and exits via dbutils.notebook.exit).")

# COMMAND ----------

# DBTITLE 1,Job Entry Point (Enhanced Logging)
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

# DBTITLE 1,DIAGNOSTIC TEST
# DIAGNOSTIC: Test Agent 13 execution directly
import traceback

print("="*80)
print("DIAGNOSTIC: Testing Agent 13 run() function")
print("="*80)

try:
    print("\nCalling run() with KAN-6...")
    test_result = run(
        ticket_id="KAN-6",
        input_artifact="/Volumes/main/state/artifacts/KAN-6/12_testing-agent/test_report.json"
    )
    
    print(f"\n✅ SUCCESS!")
    print(f"Status: {test_result.status}")
    print(f"Artifact: {test_result.artifact_path}")
    print(f"Summary: {test_result.summary}")
    
except Exception as e:
    print(f"\n❌ FAILED: {type(e).__name__}")
    print(f"Error: {str(e)}")
    print(f"\nFull traceback:")
    traceback.print_exc()
    
print("\n" + "="*80)