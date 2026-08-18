# Databricks notebook source
# DBTITLE 1,Agent 10: Code Review Agent
# MAGIC %md
# MAGIC # Agent 10: Code Review Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 10 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="strong")
# MAGIC Skill: `10-code-review-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **Yes — human approval required**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "10_code-review-agent"
SKILL_FOLDER = "10-code-review-agent"
STAGE = "10_code-review-agent"

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
    Execute Agent 10 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    print(f"\n{'='*80}")
    print(f"AGENT 10 (Code Review) - Starting execution for {ticket_id}")
    print(f"{'='*80}")
    
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:
        print(f"\n[1/6] Loading upstream artifacts...")
        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "07_pyspark-development-agent"
        
        try:
            upstream = read_upstream_json(ticket_id, "07_pyspark-development-agent", "pyspark_code_package.json")
            print(f"  ✓ PySpark package loaded")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load PySpark package: {e}")
            upstream = {"notebooks": []}
            
        try:
            sql_pkg = read_upstream_json(params["ticket_id"], "08_sql-agent", "sql_code_package.json")
            print(f"  ✓ SQL package loaded")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load SQL package: {e}")
            sql_pkg = {"queries": []}
            
        try:
            dq_pkg = read_upstream_json(params["ticket_id"], "09_data-quality-agent", "dq_test_suite.json")
            print(f"  ✓ DQ suite loaded ({dq_pkg.get('total_checks', 0)} checks)")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load DQ suite: {e}")
            dq_pkg = {"checks": [], "total_checks": 0}
        
        print(f"\n[2/6] Extracting code samples...")
        code_samples = []
        for nb in upstream.get("notebooks", [])[:2]:
            try:
                if os.path.exists(nb["path"]):
                    with open(nb["path"]) as f:
                        code_samples.append({"name": nb["name"], "content": f.read()[:1500]})
            except Exception as file_error:
                print(f"  ⚠️  Warning: Could not read {nb.get('name', 'notebook')}: {file_error}")
        print(f"  ✓ Extracted {len(code_samples)} code samples")

        print(f"\n[3/6] Preparing prompts...")
        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Review the generated PySpark + SQL code for performance (inefficient joins, cartesian products), security (injection, hardcoded secrets), maintainability, and DQ coverage against the DQ suite. This is an APPROVAL GATE — findings must be specific enough for a human reviewer to act on.
"""
        user_prompt = f"""
Code to review:
{json.dumps(code_samples, indent=2)[:3000]}

DQ suite it should satisfy ({dq_pkg.get('total_checks', 0)} checks):
{json.dumps(dq_pkg.get('checks', [])[:10], indent=2)[:1200]}

Return JSON with: findings (list of {{severity ("critical"|"major"|"minor"|"info"), category
("performance"|"security"|"maintainability"|"style"), description, file, line, suggestion}}),
status ("approved"|"changes_requested"|"rejected"), summary.
"""
        print(f"  ✓ Prompts prepared")

        print(f"\n[4/6] Calling foundation model...")
        model_response = call_foundation_model(system_prompt, user_prompt, tier="strong")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        print(f"  ✓ Model response received ({model_response['input_tokens']} in, {model_response['output_tokens']} out)")
        
        print(f"\n[5/6] Extracting and analyzing results...")
        result_json = extract_json(model_response["content"])
        findings = result_json.get("findings", [])
        critical_count = sum(1 for f in findings if f.get("severity") == "critical")
        print(f"  ✓ Found {len(findings)} findings ({critical_count} critical)")
        
        print(f"\n[6/6] Writing artifact...")
        artifact = {
            "ticket_id": params["ticket_id"],
            "findings": findings,
            "total_findings": len(findings),
            "critical_count": critical_count,
            "review_status": result_json.get("status", "approved"),
            "generated_date": datetime.now().isoformat(),
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "code_review_report.json", artifact)
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
            summary=f"Code Review Agent completed for {ticket_id}. Output at {artifact_path}.",
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
        print(f"✅ AGENT 10 COMPLETED: {output.status} ({duration_ms}ms)")
        print(f"{'='*80}")
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"Code Review Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 10 ready — call run(ticket_id) directly, or run this notebook as a job "
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
# Test Agent 10 execution
print("Testing namespace after cell execution:")
print(f"AgentOutput available: {'AgentOutput' in dir()}")
print(f"run function available: {'run' in dir()}")

if 'run' in dir():
    result = run(
        ticket_id="KAN-6",
        input_artifact="/Volumes/main/state/artifacts/KAN-6/09_data-quality-agent/dq_test_suite.json"
    )
    print(f"\nTest completed: {result.status}")
    print(f"Duration: {result.duration_ms}ms")
    print(f"Artifact: {result.artifact_path}")
else:
    print("ERROR: run() function not defined!")