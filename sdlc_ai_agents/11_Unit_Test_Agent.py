# Databricks notebook source
# DBTITLE 1,Agent 11: Unit Test Agent
# MAGIC %md
# MAGIC # Agent 11: Unit Test Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 11 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="strong")
# MAGIC Skill: `11-unit-test-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **No (auto-continue)**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "11_unit-test-agent"
SKILL_FOLDER = "11-unit-test-agent"
STAGE = "11_unit-test-agent"

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
    Execute Agent 11 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    print(f"\n{'='*80}")
    print(f"AGENT 11 (Unit Test) - Starting execution for {ticket_id}")
    print(f"{'='*80}")
    
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:
        print(f"\n[1/7] Loading PySpark code package...")
        try:
            pyspark_pkg = read_upstream_json(params["ticket_id"], "07_pyspark-development-agent", "pyspark_code_package.json")
            print(f"  ✓ PySpark package loaded ({len(pyspark_pkg.get('notebooks', []))} notebooks)")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load PySpark package: {e}")
            pyspark_pkg = {"notebooks": []}
            
        print(f"\n[2/7] Extracting code samples...")
        code_samples = []
        for nb in pyspark_pkg.get("notebooks", [])[:2]:
            try:
                if os.path.exists(nb["path"]):
                    with open(nb["path"]) as f:
                        code_samples.append({"name": nb["name"], "content": f.read()[:1500]})
            except Exception as file_error:
                print(f"  ⚠️  Warning: Could not read {nb.get('name', 'notebook')}: {file_error}")
        print(f"  ✓ Extracted {len(code_samples)} code samples")

        print(f"\n[3/7] Loading code review report...")
        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "10_code-review-agent"
        try:
            upstream = read_upstream_json(ticket_id, "10_code-review-agent", "code_review_report.json")
            print(f"  ✓ Code review report loaded ({len(upstream.get('findings', []))} findings)")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load code review report: {e}")
            upstream = {"findings": []}

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Generate pytest unit tests with mocked Spark DataFrames/fixtures covering happy path, edge cases (nulls, empty, duplicates), and error handling for the reviewed code. Target 80%+ coverage of the transformation logic.
"""
        user_prompt = f"""
Reviewed code:
{json.dumps(code_samples, indent=2)[:3000]}

Code review findings to account for:
{json.dumps(upstream.get('findings', [])[:5], indent=2)[:1000]}

Generate 3-5 pytest test functions as fenced ```python blocks using pytest fixtures and mocks
for Spark DataFrame operations.
"""
        print(f"  ✓ Prompts prepared")

        print(f"\n[4/7] Calling foundation model...")
        model_response = call_foundation_model(system_prompt, user_prompt, tier="strong")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        print(f"  ✓ Model response received ({model_response['input_tokens']} in, {model_response['output_tokens']} out)")
        
        print(f"\n[5/7] Extracting test code blocks...")
        code_blocks = extract_code_blocks(model_response["content"], lang="python")
        print(f"  ✓ Extracted {len(code_blocks)} code blocks")
        print(f"\n[6/7] Enhancing and saving test files...")
        saved = []
        for i, code in enumerate(code_blocks[:5]):
            test_name = f"test_{params['ticket_id']}_{i+1}"
            
            # QUALITY ENHANCEMENT: Add docstrings, fixtures, setup/teardown methods
            try:
                enhanced_code = enhance_unit_tests(code, test_name)
                print(f"  ✓ Enhanced test {i+1}")
            except Exception as enhance_error:
                print(f"  ⚠️  Warning: enhance_unit_tests failed for test {i+1}: {enhance_error}")
                enhanced_code = code  # Fallback to original
            
            path = write_text_artifact(params["ticket_id"], STAGE, f"{test_name}.py", enhanced_code)
            saved.append({"name": f"test_{i+1}", "path": path})
        print(f"  ✓ Saved {len(saved)} test files")

        print(f"\n[7/7] Writing artifact...")
        artifact = {
            "ticket_id": params["ticket_id"],
            "test_cases": saved,
            "total_tests": len(saved),
            "coverage_target": 0.80,
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "unit_test_suite.json", artifact)
        print(f"  ✓ Artifact written to: {artifact_path}")
        duration_ms = int((time.time() - start) * 1000)
        
        # Agent 11 extracts code blocks, not JSON, so status is always "complete"
        status = "complete"
        clarifications = []

        print(f"\n[8/8] Creating AgentOutput and logging...")
        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Unit Test Agent completed for {ticket_id}. Output at {artifact_path}.",
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
        print(f"✅ AGENT 11 COMPLETED: {output.status} ({duration_ms}ms)")
        print(f"{'='*80}")
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"Unit Test Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 11 ready — call run(ticket_id) directly, or run this notebook as a job "
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

# DBTITLE 1,Test Execution
# Test Agent 11 execution
print("Testing Agent 11 standalone execution:")
print(f"AgentOutput available: {'AgentOutput' in dir()}")
print(f"run function available: {'run' in dir()}")

if 'run' in dir():
    result = run(
        ticket_id="KAN-6",
        input_artifact="/Volumes/main/state/artifacts/KAN-6/10_code-review-agent/code_review_report.json"
    )
    print(f"\nTest completed: {result.status}")
    print(f"Duration: {result.duration_ms}ms")
    print(f"Artifact: {result.artifact_path}")
    print(f"Total test files: {len(result.citations) - 1}")  # -1 for input_artifact
else:
    print("ERROR: run() function not defined!")