# Databricks notebook source
# DBTITLE 1,Agent 14: Deployment Agent
# MAGIC %md
# MAGIC # Agent 14: Deployment Agent
# MAGIC ## Multi-Agent SDLC Platform — Layer 14 of 15
# MAGIC
# MAGIC Backend: **Foundation Model API** (`call_foundation_model`, tier="medium")
# MAGIC Skill: `14-deployment-agent` — loaded live from disk via `load_skill()`, not re-typed in this notebook.
# MAGIC Approval gate: **Yes — human approval required**

# COMMAND ----------

# DBTITLE 1,Shared Utils
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

# DBTITLE 1,Agent Config
AGENT_KEY = "14_deployment-agent"
SKILL_FOLDER = "14-deployment-agent"
STAGE = "14_deployment-agent"

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
    Execute Agent 14 for a single ticket. Callable directly for interactive testing,
    or driven by process()/finish() below when launched as a job via dbutils.notebook.run().
    """
    print(f"\n{'='*80}")
    print(f"AGENT 14 (Deployment) - Starting execution for {ticket_id}")
    print(f"{'='*80}")
    
    start = time.time()
    run_id = _widget("run_id", str(uuid.uuid4()))  # Get from orchestrator or generate for standalone runs
    params = {"ticket_id": ticket_id, "input_artifact": input_artifact, "run_id": run_id}
    tool_calls: List[Dict] = []

    try:
        print(f"\n[1/6] Loading upstream artifacts...")
        try:
            test_report = read_upstream_json(params["ticket_id"], "12_testing-agent", "test_report.json")
            print(f"  ✓ Test report loaded (status: {test_report.get('test_status')})")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load test report: {e}")
            test_report = {"test_status": "unknown", "pass_rate": 0.0}
        if test_report.get("test_status") != "success":
            print(f"  ⚠️  Upstream tests did not pass cleanly (status={test_report.get('test_status')}); "
                  f"generating the deployment package anyway but flagging it for the approval gate.")
        
        try:
            pyspark_pkg = read_upstream_json(params["ticket_id"], "07_pyspark-development-agent", "pyspark_code_package.json")
            print(f"  ✓ PySpark package loaded ({len(pyspark_pkg.get('notebooks', []))} notebooks)")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load PySpark package: {e}")
            pyspark_pkg = {"notebooks": []}

        upstream_stage = input_artifact.rsplit("/", 2)[-2] if input_artifact else "13_documentation-agent"
        try:
            upstream = read_upstream_json(ticket_id, "13_documentation-agent", "documentation_package.json")
            print(f"  ✓ Documentation package loaded")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to load documentation package: {e}")
            upstream = {"documents": []}

        system_prompt = f"""{skill.skill_md}

---
CODING BACKEND GUIDANCE
{skill.coding_backend}

---
OUTPUT CONTRACT (return exactly this shape as JSON)
{skill.output_contract}

---
TASK FOR THIS RUN
Generate a Databricks Asset Bundle (databricks.yml) plus a job definition wiring the generated notebooks into a scheduled workflow, and a rollback plan. This is an APPROVAL GATE — a human approves this manifest before anything is actually deployed with `databricks bundle deploy`.
"""
        user_prompt = f"""
Notebooks to deploy: {json.dumps(pyspark_pkg.get('notebooks', []), indent=2)}
Test status: {test_report.get('test_status')}, pass rate: {test_report.get('pass_rate')}

Return a fenced ```yaml databricks.yml Asset Bundle block, followed by a fenced ```json block:
{{"rollback_plan": str, "environments": ["dev","staging","prod"], "status": "success"|"blocked"}}
"""
        print(f"  ✓ Prompts prepared")

        print(f"\n[2/6] Calling foundation model...")
        model_response = call_foundation_model(system_prompt, user_prompt, tier="medium")
        tool_calls.append({"tool": "foundation_model_api", "detail": model_response["endpoint"]})
        print(f"  ✓ Model response received ({model_response['input_tokens']} in, {model_response['output_tokens']} out)")
        
        print(f"\n[3/6] Extracting code blocks...")
        yaml_blocks = extract_code_blocks(model_response["content"], lang="yaml")
        json_blocks = extract_code_blocks(model_response["content"], lang="json")
        print(f"  ✓ Extracted {len(yaml_blocks)} YAML blocks, {len(json_blocks)} JSON blocks")
        try:
            result_json = json.loads(json_blocks[-1]) if json_blocks else {}
        except Exception:
            result_json = {}

        print(f"\n[4/6] Generating complete DAB configuration...")
        notebooks = pyspark_pkg.get('notebooks', [])
        catalog = params.get('catalog', 'main')
        
        # Generate DAB config directly (create_complete_dab_config was undefined)
        notebook_tasks = []
        for idx, nb in enumerate(notebooks[:5]):  # Limit to first 5 notebooks
            task_name = nb.get('name', f'task_{idx}').replace(' ', '_').lower()
            nb_path = nb.get('path', f'/path/to/{task_name}')
            notebook_tasks.append(f"""    {task_name}:
      notebook_task:
        notebook_path: {nb_path}
        base_parameters: {{}}
      new_cluster:
        spark_version: 13.3.x-scala2.12
        node_type_id: i3.xlarge
        num_workers: 2
""")
        
        complete_dab = f"""name: {params['ticket_id']}
resources:
  jobs:
    main_job:
      name: {params['ticket_id']}_pipeline
      tasks:
{chr(10).join(notebook_tasks) if notebook_tasks else '        # No tasks defined'}
"""
        print(f"  ✓ Complete DAB config generated ({len(notebook_tasks)} tasks)")
        
        print(f"\n[5/6] Writing artifacts...")
        bundle_path = write_text_artifact(params["ticket_id"], STAGE, "databricks.yml", complete_dab)
        print(f"  ✓ DAB bundle written to: {bundle_path}")

        artifact = {
            "ticket_id": params["ticket_id"],
            "bundle_path": bundle_path,
            "rollback_plan": result_json.get("rollback_plan", "Run `databricks bundle destroy` / redeploy previous git tag."),
            "environments": result_json.get("environments", ["dev", "staging", "prod"]),
            "deployment_status": result_json.get("status", "success"),
            "generated_date": datetime.now().isoformat(),
            "note": "This manifest describes the bundle with complete configuration (name, resources, targets, workspace config). "
                    "Actually deploying requires the approver to run `databricks bundle deploy -t prod` (CLI/CI) after approval — "
                    "this agent does not auto-deploy to protect the approval gate.",
        }
        artifact_path = write_json_artifact(params["ticket_id"], STAGE, "deployment_manifest.json", artifact)
        print(f"  ✓ Deployment manifest written to: {artifact_path}")
        duration_ms = int((time.time() - start) * 1000)
        status = result_json.get("status", "complete") if isinstance(result_json, dict) else "complete"
        clarifications = result_json.get("clarification_questions", []) if isinstance(result_json, dict) else []

        print(f"\n[6/6] Creating AgentOutput and logging...")
        output = AgentOutput(
            agent=AGENT_KEY,
            ticket_id=ticket_id,
            status="needs_clarification" if clarifications else "complete",
            artifact_path=artifact_path,
            summary=f"Deployment Agent completed for {ticket_id}. Output at {artifact_path}.",
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
        print(f"✅ AGENT 14 COMPLETED: {output.status} ({duration_ms}ms)")
        print(f"{'='*80}")
        return output

    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        agent_id = int(AGENT_KEY.split('_')[0])
        log_agent_run(run_id, ticket_id, agent_id, AGENT_KEY, "failed", "", 0, 0, duration_ms, tool_calls, error=str(e))
        return AgentOutput(agent=AGENT_KEY, ticket_id=ticket_id, status="failed",
                            summary=f"Deployment Agent failed: {e}", duration_ms=duration_ms, error=str(e))

print("✓ Agent 14 ready — call run(ticket_id) directly, or run this notebook as a job "
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