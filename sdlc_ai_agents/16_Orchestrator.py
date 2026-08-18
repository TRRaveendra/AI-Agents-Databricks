# Databricks notebook source
# DBTITLE 1,Multi-Agent SDLC Platform — Orchestrator
# MAGIC %md
# MAGIC # Databricks Multi-Agent SDLC Platform — Orchestrator
# MAGIC ## From Jira Requirement to Production-Ready Solution (15 agents)
# MAGIC
# MAGIC Drives `01_Jira_Requirement_Agent.py` … `15_Production_Monitoring_Agent.py` via
# MAGIC `dbutils.notebook.run()`. This version fixes the two structural bugs in the original:
# MAGIC
# MAGIC 1. **Every agent notebook now actually returns data.** Each one reads `ticket_id` /
# MAGIC    `input_artifact` from widgets and calls `dbutils.notebook.exit(json.dumps(...))` with the
# MAGIC    standard output contract (see `00_Shared_Utils.py`). Previously the agents only defined
# MAGIC    functions and printed to stdout — `dbutils.notebook.run()` got back an empty string.
# MAGIC 2. **Approval gates actually pause and can be resumed.** A row in
# MAGIC    `{catalog}.state.pipeline_runs` records where the run paused; `approve_and_resume()`
# MAGIC    continues the same `run_id` from the next agent. Gates: Agents **2, 5, 6, 10, 14** (per
# MAGIC    `databricks-sdlc-agent-skills/README.md`).
# MAGIC
# MAGIC ### Deploying this for real
# MAGIC Put this notebook + `00_Shared_Utils.py` + `01..15_*.py` + the
# MAGIC `databricks-sdlc-agent-skills/` folder in the **same Databricks Repo**, then run
# MAGIC `ensure_state_objects()` once. Trigger `run_pipeline(...)` interactively for testing, or wrap
# MAGIC it as a **Databricks Job** (Notebook task, serverless or job-cluster compute) parameterized by
# MAGIC `ticket_id` for production use.

# COMMAND ----------

# DBTITLE 1,Shared Utils + One-Time Setup
# MAGIC %run ./00_Shared_Utils

# COMMAND ----------

ensure_state_objects()

# COMMAND ----------

# DBTITLE 1,Pipeline Configuration
from enum import Enum

# Notebook paths relative to this orchestrator's own folder (works from a Repo checkout or a
# Workspace folder import — dbutils.notebook.run resolves relative paths against the caller).
from databricks.sdk import WorkspaceClient

# Get current user email dynamically
w = WorkspaceClient()
current_user = w.current_user.me()
user_email = current_user.user_name

# Use absolute paths to avoid working directory issues
BASE_PATH = f"/Users/{user_email}/sdlc_ai_agents"

AGENT_NOTEBOOKS = {
    1: f"{BASE_PATH}/01_Jira_Requirement_Agent",
    2: f"{BASE_PATH}/02_Requirement_Validation_Agent",
    3: f"{BASE_PATH}/03_Business_Understanding_Agent",
    4: f"{BASE_PATH}/04_Metadata_Discovery_Agent",
    5: f"{BASE_PATH}/05_Mapping_Document_Agent",
    6: f"{BASE_PATH}/06_Data_Model_Agent",
    7: f"{BASE_PATH}/07_PySpark_Development_Agent",
    8: f"{BASE_PATH}/08_SQL_Agent",
    9: f"{BASE_PATH}/09_Data_Quality_Agent",
    10: f"{BASE_PATH}/10_Code_Review_Agent",
    11: f"{BASE_PATH}/11_Unit_Test_Agent",
    12: f"{BASE_PATH}/12_Testing_Agent",
    13: f"{BASE_PATH}/13_Documentation_Agent",
    14: f"{BASE_PATH}/14_Deployment_Agent",
    15: f"{BASE_PATH}/15_Production_Monitoring_Agent",
}

AGENT_STAGE_DIR = {  # must match STAGE in each agent notebook — used to resolve artifact paths
    1: "01_jira-requirement-agent", 2: "02_requirement-validation-agent",
    3: "03_business-understanding-agent", 4: "04_metadata-discovery-agent",
    5: "05_mapping-document-agent", 6: "06_data-model-agent",
    7: "07_pyspark-development-agent", 8: "08_sql-agent",
    9: "09_data-quality-agent", 10: "10_code-review-agent",
    11: "11_unit-test-agent", 12: "12_testing-agent",
    13: "13_documentation-agent", 14: "14_deployment-agent",
    15: "15_production-monitoring-agent",
}

# Per databricks-sdlc-agent-skills/README.md pipeline diagram
APPROVAL_GATES = {2, 5, 6, 10, 14}

NOTEBOOK_TIMEOUT_SECONDS = 900  # 15 min per agent; generation steps can be slow

print("=" * 80)
print("ORCHESTRATOR CONFIGURED")
print(f"Agents: {len(AGENT_NOTEBOOKS)}  |  Approval gates: {sorted(APPROVAL_GATES)}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Pipeline State
class RunStatus(str, Enum):
    RUNNING = "running"
    AWAITING_APPROVAL = "awaiting_approval"
    FAILED = "failed"
    COMPLETED = "completed"

def _upsert_pipeline_run(run_id: str, ticket_id: str, status: str, current_agent: int, error_message: Optional[str] = None):
    # Extract value if status is an Enum, otherwise use as-is
    status_value = status.value if hasattr(status, 'value') else status
    spark.sql(f"DELETE FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs WHERE run_id = '{run_id}'")
    
    # Table schema: run_id, ticket_id, status, current_agent_id, created_at, updated_at, completed_at, error_message, metadata
    # Properly escape error message to prevent SQL injection and syntax errors
    if error_message:
        # Escape single quotes by doubling them (SQL standard)
        escaped_error = error_message.replace("'", "''")
        error_msg_sql = f"'{escaped_error}'"
    else:
        error_msg_sql = "NULL"
    
    spark.sql(f"""
        INSERT INTO {CATALOG}.{STATE_SCHEMA}.pipeline_runs
        VALUES ('{run_id}', '{ticket_id}', '{status_value}', {current_agent},
                current_timestamp(), current_timestamp(), NULL, {error_msg_sql}, NULL)
    """)

def get_pipeline_run(run_id: str) -> Optional[Dict]:
    rows = spark.sql(
        f"SELECT * FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs WHERE run_id = '{run_id}'"
    ).collect()
    return rows[0].asDict() if rows else None

def latest_run_for_ticket(ticket_id: str) -> Optional[Dict]:
    rows = spark.sql(f"""
        SELECT * FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs
        WHERE ticket_id = '{ticket_id}'
        ORDER BY updated_at DESC LIMIT 1
    """).collect()
    return rows[0].asDict() if rows else None

# COMMAND ----------

# DBTITLE 1,Run a Single Agent
def run_agent(agent_id: int, ticket_id: str, run_id: str, input_artifact: str = "") -> Dict:
    """
    Run one agent notebook via dbutils.notebook.run() and parse the standard output contract
    it returns via dbutils.notebook.exit(). Raises on unparseable results instead of silently
    treating them as empty, so contract breaks are caught immediately.
    
    WORKAROUND: Agents 14 & 15 use inline execution to bypass dbutils.notebook.run() issues.
    """
    notebook_path = AGENT_NOTEBOOKS[agent_id]
    print(f"\n{'='*80}\nRunning Agent {agent_id}: {notebook_path}\n{'='*80}")
    print(f"Arguments: ticket_id={ticket_id}, run_id={run_id}, input_artifact={input_artifact[:80] if input_artifact else 'None'}...")

    try:
        raw_result = dbutils.notebook.run(
            notebook_path,
            timeout_seconds=NOTEBOOK_TIMEOUT_SECONDS,
            arguments={"ticket_id": ticket_id, "run_id": run_id, "input_artifact": input_artifact, "catalog": CATALOG},
        )
    except Exception as notebook_error:
        # Capture the full error details for debugging
        error_msg = str(notebook_error)
        print(f"\n❌ dbutils.notebook.run() failed for Agent {agent_id}")
        print(f"Error type: {type(notebook_error).__name__}")
        print(f"Error message: {error_msg[:500]}")
        
        # Check if this is a WorkflowException and try to extract more details
        if "WorkflowException" in str(type(notebook_error)):
            print(f"\n⚠️  This is a WorkflowException - the notebook execution failed internally.")
            print(f"   Common causes:")
            print(f"   1. NameError or import failure in the notebook")
            print(f"   2. %run command failing to load dependencies")
            print(f"   3. Widget values not being read correctly")
            print(f"   4. Error in the agent's run() or finish() functions")
            print(f"\n   For Agent {agent_id}, check:")
            print(f"   - Cell 2: %run ./00_Shared_Utils executes successfully")
            print(f"   - Cell 3: load_skill() completes without errors")
            print(f"   - Cell 6: get_params() and run() execute without errors")
        
        raise  # Re-raise to be caught by the pipeline error handler

    if not raw_result:
        raise RuntimeError(
            f"Agent {agent_id} ({notebook_path}) returned no result. It should call "
            f"dbutils.notebook.exit(json.dumps(...)) via finish(AgentOutput(...)) — check that "
            f"the notebook's __main__ job-entry-point cell ran (widgets must be non-empty)."
        )

    try:
        result = json.loads(raw_result)
    except json.JSONDecodeError as json_error:
        print(f"\n❌ Failed to parse result from Agent {agent_id}")
        print(f"Raw result (first 500 chars): {raw_result[:500]}")
        raise RuntimeError(f"Agent {agent_id} returned invalid JSON: {json_error}") from json_error
    
    status_icon = "✅" if result.get("status") == "complete" else ("⚠️" if result.get("status") == "needs_clarification" else "❌")
    print(f"{status_icon} Agent {agent_id} -> {result.get('status')} :: {result.get('summary','')}")
    return result

# COMMAND ----------

# DBTITLE 1,Run the Full Pipeline (with pause/resume at approval gates)
def run_pipeline(ticket_id: str, start_agent: int = 1, end_agent: int = 15,
                  run_id: Optional[str] = None) -> Dict:
    """
    Run agents start_agent..end_agent in order, stopping automatically at an approval-gate
    agent's output (status still 'complete' from the agent's own perspective — the GATE is
    enforced by the orchestrator refusing to advance past it, not by the agent itself).

    Resumable: pass the same run_id (as returned in a previous call) to continue after approval.
    """
    run_id = run_id or str(uuid.uuid4())
    existing = get_pipeline_run(run_id)
    input_artifact = ""

    if existing and existing["status"] == RunStatus.AWAITING_APPROVAL.value:
        start_agent = existing["current_agent_id"] + 1
        prior_stage = AGENT_STAGE_DIR[existing["current_agent_id"]]
        # best-effort: locate the artifact path the paused agent wrote
        rows = spark.sql(f"""
            SELECT file_path FROM {CATALOG}.{STATE_SCHEMA}.artifacts
            WHERE run_id = '{run_id}' AND agent_id = {existing['current_agent_id']}
            ORDER BY created_at DESC LIMIT 1
        """).collect()
        input_artifact = rows[0]["file_path"] if rows else ""
        print(f"Resuming run {run_id} for {ticket_id} at Agent {start_agent}")

    print(f"\n{'='*80}\nPIPELINE RUN {run_id}  |  ticket={ticket_id}  |  agents {start_agent}-{end_agent}\n{'='*80}")
    _upsert_pipeline_run(run_id, ticket_id, RunStatus.RUNNING, start_agent)

    for agent_id in range(start_agent, end_agent + 1):
        try:
            result = run_agent(agent_id, ticket_id, run_id, input_artifact)
        except Exception as e:
            print(f"❌ Pipeline failed at Agent {agent_id}: {e}")
            _upsert_pipeline_run(run_id, ticket_id, RunStatus.FAILED, agent_id, str(e))
            return {"run_id": run_id, "ticket_id": ticket_id, "status": RunStatus.FAILED, "failed_at": agent_id, "error": str(e)}

        if result.get("status") == "failed":
            _upsert_pipeline_run(run_id, ticket_id, RunStatus.FAILED, agent_id, result.get("error"))
            return {"run_id": run_id, "ticket_id": ticket_id, "status": RunStatus.FAILED, "failed_at": agent_id, "error": result.get("error")}

        input_artifact = result.get("artifact_path", input_artifact)

        if agent_id in APPROVAL_GATES:
            _upsert_pipeline_run(run_id, ticket_id, RunStatus.AWAITING_APPROVAL, agent_id)
            print(f"\n⏸️  PAUSED FOR APPROVAL at Agent {agent_id} ({AGENT_STAGE_DIR[agent_id]}).")
            print(f"    Review: {input_artifact}")
            print(f"    Resume with: approve_and_resume('{run_id}')")
            return {"run_id": run_id, "ticket_id": ticket_id, "status": RunStatus.AWAITING_APPROVAL, "paused_at": agent_id, "artifact_to_review": input_artifact}

    _upsert_pipeline_run(run_id, ticket_id, RunStatus.COMPLETED, end_agent)
    print(f"\n✅ PIPELINE RUN {run_id} COMPLETED")
    return {"run_id": run_id, "ticket_id": ticket_id, "status": RunStatus.COMPLETED}

def approve_and_resume(run_id: str) -> Dict:
    """Human calls this after reviewing the artifact at an approval gate to continue the run."""
    existing = get_pipeline_run(run_id)
    if not existing:
        raise ValueError(f"No pipeline run found with run_id={run_id}")
    # Compare string values since database returns strings, not Enum instances
    if existing["status"] != RunStatus.AWAITING_APPROVAL.value:
        raise ValueError(f"Run {run_id} is not awaiting approval (status={existing['status']})")
    return run_pipeline(existing["ticket_id"], run_id=run_id)

print("✓ Orchestrator ready: run_pipeline(ticket_id), approve_and_resume(run_id)")

# COMMAND ----------

# DBTITLE 1,Example Usage
# MAGIC %md
# MAGIC ```python
# MAGIC # Kick off a run — stops automatically at the first approval gate (Agent 2)
# MAGIC result = run_pipeline("PROJ-123")
# MAGIC display(result)
# MAGIC
# MAGIC # ... a human reviews the artifact at result["artifact_to_review"] ...
# MAGIC
# MAGIC # Resume from exactly where it paused
# MAGIC result = approve_and_resume(result["run_id"])
# MAGIC display(result)
# MAGIC # repeat through gates at Agents 2, 5, 6, 10, 14 until status == "completed"
# MAGIC
# MAGIC # Inspect full audit trail
# MAGIC display(spark.sql(f"SELECT * FROM {CATALOG}.{STATE_SCHEMA}.agent_runs WHERE ticket_id = 'PROJ-123' ORDER BY created_at"))
# MAGIC display(spark.sql(f"SELECT * FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs WHERE ticket_id = 'PROJ-123'"))
# MAGIC ```
# MAGIC
# MAGIC ### As a Databricks Job
# MAGIC Two tasks:
# MAGIC 1. **kickoff** — Notebook task running this orchestrator with a `ticket_id` job parameter,
# MAGIC    calling `run_pipeline(dbutils.widgets.get("ticket_id"))`.
# MAGIC 2. **resume** — a separate, manually-triggered Job (or a Databricks App button) calling
# MAGIC    `approve_and_resume(run_id)`, so approval is a real human action outside the job graph
# MAGIC    rather than a `sleep()` loop burning cluster time.

# COMMAND ----------

# DBTITLE 1,End-to-End Pipeline Test
# COMPREHENSIVE END-TO-END PIPELINE TEST
# Tests the complete pipeline from Agent 1 through Agent 15 with auto-approval

import time
from datetime import datetime

print("="*80)
print("END-TO-END PIPELINE TEST: KAN-6")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Approval gates: {sorted(APPROVAL_GATES)}")
print("="*80)

# Step 1: Clean up previous runs
print("\n[1/5] Cleaning up previous KAN-6 runs...")
try:
    counts = spark.sql(f"""
        SELECT 
            COUNT(DISTINCT pr.run_id) as pipeline_runs,
            COUNT(DISTINCT ar.run_id) as agent_runs,
            COUNT(DISTINCT a.run_id) as artifacts
        FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs pr
        LEFT JOIN {CATALOG}.{STATE_SCHEMA}.agent_runs ar ON pr.run_id = ar.run_id
        LEFT JOIN {CATALOG}.{STATE_SCHEMA}.artifacts a ON pr.run_id = a.run_id
        WHERE pr.ticket_id = 'KAN-6'
    """).collect()[0]
    
    print(f"  Found: {counts['pipeline_runs']} pipeline runs, {counts['agent_runs']} agent runs, {counts['artifacts']} artifacts")
    
    # Delete in correct order to respect foreign keys
    spark.sql(f"DELETE FROM {CATALOG}.{STATE_SCHEMA}.approval_queue WHERE run_id IN (SELECT run_id FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs WHERE ticket_id = 'KAN-6')")
    spark.sql(f"DELETE FROM {CATALOG}.{STATE_SCHEMA}.artifacts WHERE run_id IN (SELECT run_id FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs WHERE ticket_id = 'KAN-6')")
    spark.sql(f"DELETE FROM {CATALOG}.{STATE_SCHEMA}.agent_runs WHERE run_id IN (SELECT run_id FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs WHERE ticket_id = 'KAN-6')")
    spark.sql(f"DELETE FROM {CATALOG}.{STATE_SCHEMA}.pipeline_runs WHERE ticket_id = 'KAN-6'")
    print("  ✓ Cleanup complete")
except Exception as e:
    print(f"  ⚠️  Cleanup warning: {str(e)[:100]}")

# Step 2: Start pipeline
print("\n[2/5] Starting pipeline execution...")
start_time = time.time()
result = run_pipeline('KAN-6')
current_run_id = result.get('run_id')
print(f"  Run ID: {current_run_id}")
print(f"  Initial status: {result.get('status')}")

# Step 3: Auto-approve through gates
print("\n[3/5] Processing pipeline (with auto-approval)...\n")
approval_count = 0
max_iterations = 25
iteration = 0

while iteration < max_iterations:
    iteration += 1
    status = result.get('status')
    
    if status == 'awaiting_approval':
        approval_count += 1
        paused_at = result.get('paused_at')
        stage_name = AGENT_STAGE_DIR.get(paused_at, 'unknown')
        artifact = result.get('artifact_to_review', 'N/A')
        
        print(f"  ✓ Gate #{approval_count}: Agent {paused_at} ({stage_name}) completed")
        if artifact and artifact != 'N/A':
            artifact_display = artifact if len(artifact) <= 80 else f"...{artifact[-77:]}"
            print(f"    Artifact: {artifact_display}")
        print(f"    Approving and resuming...\n")
        
        result = approve_and_resume(current_run_id)
        
    elif status == 'completed':
        print(f"\n  ✅ Pipeline completed successfully!\n")
        break
        
    elif status == 'failed':
        failed_at = result.get('failed_at')
        stage_name = AGENT_STAGE_DIR.get(failed_at, 'unknown')
        error = result.get('error', 'Unknown error')
        print(f"\n  ❌ Pipeline failed at Agent {failed_at} ({stage_name})")
        print(f"  Error: {error[:250]}..." if len(error) > 250 else f"  Error: {error}\n")
        break
        
    elif status == 'running':
        # Still running, wait a moment
        time.sleep(0.5)
        
    else:
        print(f"\n  ⚠️  Unexpected status: {status}\n")
        break

if iteration >= max_iterations:
    print(f"\n  ⚠️  Reached max iterations ({max_iterations})\n")

execution_time = time.time() - start_time

# Step 4: Query results
print("[4/5] Querying execution results...")

agent_runs = spark.sql(f"""
    SELECT agent_id, agent_name, status, duration_seconds, error_message
    FROM {CATALOG}.{STATE_SCHEMA}.agent_runs
    WHERE run_id = '{current_run_id}'
    ORDER BY agent_id
""").collect()

artifacts = spark.sql(f"""
    SELECT agent_id, artifact_type, artifact_name, file_path
    FROM {CATALOG}.{STATE_SCHEMA}.artifacts
    WHERE run_id = '{current_run_id}'
    ORDER BY agent_id
""").collect()

# Step 5: Display summary
print(f"\n[5/5] Summary")
print("="*80)
print("EXECUTION METRICS")
print("="*80)
print(f"Total Time: {execution_time:.1f}s ({execution_time/60:.1f}m)")
print(f"Agents Executed: {len(agent_runs)}")
print(f"Approval Gates: {approval_count}")
print(f"Artifacts Generated: {len(artifacts)}")

success_count = sum(1 for r in agent_runs if r['status'] == 'complete')
failed_count = sum(1 for r in agent_runs if r['status'] == 'failed')
total_agent_time = sum(r['duration_seconds'] or 0 for r in agent_runs)

print(f"\n✅ Successful: {success_count}/{len(agent_runs)}")
print(f"❌ Failed: {failed_count}/{len(agent_runs)}")
print(f"⏱️  Agent Time: {total_agent_time:.1f}s")

print(f"\n{'='*80}")
print("AGENT DETAILS")
print("="*80)
for run in agent_runs:
    status_icon = "✅" if run['status'] == 'complete' else ("⚠️" if run['status'] in ['needs_clarification', 'skipped'] else "❌")
    duration = f"{run['duration_seconds']:.1f}s" if run['duration_seconds'] else "N/A"
    stage_name = AGENT_STAGE_DIR.get(run['agent_id'], 'unknown')
    print(f"{status_icon} Agent {run['agent_id']:2d}: {stage_name:40s} | {run['status']:15s} | {duration:>8s}")
    if run['error_message']:
        error_preview = run['error_message'][:100] + "..." if len(run['error_message']) > 100 else run['error_message']
        print(f"         Error: {error_preview}")

if artifacts:
    print(f"\n{'='*80}")
    print("ARTIFACTS")
    print("="*80)
    for artifact in artifacts:
        stage_name = AGENT_STAGE_DIR.get(artifact['agent_id'], 'unknown')
        print(f"  Agent {artifact['agent_id']:2d} ({stage_name}):")
        print(f"    Type: {artifact['artifact_type']}")
        print(f"    Name: {artifact['artifact_name']}")
        print(f"    Path: {artifact['file_path']}")

print(f"\n{'='*80}")
print("FINAL STATUS")
print("="*80)
if result.get('status') == 'completed':
    print("✅ PIPELINE TEST PASSED")
    print(f"   • All {len(agent_runs)} agents executed successfully")
    print(f"   • Passed {approval_count} approval gates")
    print(f"   • Generated {len(artifacts)} artifacts")
    print(f"   • Total execution time: {execution_time:.1f}s")
elif result.get('status') == 'failed':
    failed_at = result.get('failed_at')
    stage_name = AGENT_STAGE_DIR.get(failed_at, 'unknown')
    print(f"❌ PIPELINE TEST FAILED")
    print(f"   • Failed at: Agent {failed_at} ({stage_name})")
    print(f"   • Completed: {success_count}/{len(agent_runs)} agents")
else:
    print(f"⚠️  PIPELINE STATUS: {result.get('status')}")

print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)