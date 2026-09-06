# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 ADF → Databricks Lakeflow Migration Skills - User Guide
# MAGIC
# MAGIC ## Complete Guide to Using the Migration Knowledge Base
# MAGIC
# MAGIC **Version:** 1.0  
# MAGIC **Last Updated:** 2026-04-22  
# MAGIC **Location:** `/Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📋 Table of Contents
# MAGIC
# MAGIC 1. [Overview](#overview)
# MAGIC 2. [Skills Library Structure](#structure)
# MAGIC 3. [Quick Start Guide](#quickstart)
# MAGIC    * 3.5 [Using Skills with Genie Code Agent](#genie) ⭐ **NEW**
# MAGIC 4. [Detailed Usage Workflows](#workflows)
# MAGIC 5. [Migration Best Practices](#bestpractices)
# MAGIC 6. [Common Scenarios](#scenarios)
# MAGIC 7. [Troubleshooting](#troubleshooting)
# MAGIC 8. [Reference & Cheat Sheets](#reference)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Overview {#overview}
# MAGIC
# MAGIC ### 🎯 Purpose
# MAGIC
# MAGIC This skills library provides **enterprise-grade, production-ready documentation** for migrating Azure Data Factory (ADF) pipelines to **Databricks Lakeflow** and **Spark Declarative Pipelines (DLT)**.
# MAGIC
# MAGIC ### ✨ What's Included
# MAGIC
# MAGIC * **10 Component-Specific Skills** - Detailed migration guides for each ADF component
# MAGIC * **Master Index** - Complete architecture overview and migration strategy
# MAGIC * **Production Code** - Real PySpark, SQL, and YAML examples (not placeholders)
# MAGIC * **AI Agent Ready** - Structured for automated migration tools
# MAGIC * **224KB** of comprehensive documentation
# MAGIC
# MAGIC ### 👥 Who Should Use This
# MAGIC
# MAGIC | Role | Primary Use Case |
# MAGIC |------|------------------|
# MAGIC | **Data Engineers** | Hands-on migration of pipelines, copy activities, transformations |
# MAGIC | **Solution Architects** | Migration strategy, design patterns, architecture planning |
# MAGIC | **DevOps Engineers** | Job scheduling, cluster configuration, CI/CD setup |
# MAGIC | **AI/ML Engineers** | Automated migration tool development, code generation |
# MAGIC | **Data Analysts** | Understanding data flow changes, query migration |
# MAGIC | **Project Managers** | Migration planning, effort estimation, tracking |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Skills Library Structure {#structure}
# MAGIC
# MAGIC ### 📁 Directory Layout
# MAGIC
# MAGIC ```
# MAGIC /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills/
# MAGIC │
# MAGIC ├── 📄 migration_index.md                    ⭐ START HERE
# MAGIC │
# MAGIC ├── 📄 adf_pipeline_to_lakeflow_job.md       Pipeline orchestration
# MAGIC ├── 📄 adf_activity_to_task.md               Activity transformation
# MAGIC ├── 📄 adf_copy_activity_to_spark.md         Data movement
# MAGIC ├── 📄 adf_lookup_to_spark_sql.md            Watermarks & metadata
# MAGIC ├── 📄 adf_foreach_to_task_loop.md           Iteration & loops
# MAGIC ├── 📄 adf_if_condition_to_task_dependency.md Conditional logic
# MAGIC ├── 📄 adf_linked_service_to_unity_catalog.md Connections
# MAGIC ├── 📄 adf_dataset_to_delta_table.md         Schema & formats
# MAGIC └── 📄 adf_trigger_to_job_schedule.md        Scheduling
# MAGIC ```
# MAGIC
# MAGIC ### 📚 Each Skill File Contains 10 Sections
# MAGIC
# MAGIC 1. **Overview** - What the ADF component is
# MAGIC 2. **Lakeflow Equivalent** - Concept mapping
# MAGIC 3. **Migration Strategy** - Step-by-step approach
# MAGIC 4. **Code Conversion** - Real examples (JSON → YAML/PySpark)
# MAGIC 5. **Design Patterns** - Best practices
# MAGIC 6. **Anti-Patterns** - What to avoid
# MAGIC 7. **Performance** - Optimization techniques
# MAGIC 8. **Governance** - Security & compliance
# MAGIC 9. **Automation Hints** - AI agent guidance
# MAGIC 10. **Validation** - Testing checklists

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quick Start Guide {#quickstart}
# MAGIC
# MAGIC ### 🚀 5-Minute Primer
# MAGIC
# MAGIC #### Step 1: Start with Master Index

# COMMAND ----------

# Read the master index
with open('/Workspace/Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills/migration_index.md', 'r') as f:
    index_preview = f.read()[:1500]

print("📖 MIGRATION INDEX PREVIEW")
print("=" * 80)
print(index_preview)
print("\n... (open full file for complete content)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Identify Your ADF Components

# COMMAND ----------

# Helper: Analyze ADF pipeline JSON
import json

def analyze_adf_pipeline(adf_json_path=None, adf_json_dict=None):
    """
    Analyze ADF pipeline and suggest skill files
    
    Args:
        adf_json_path: Path to ADF JSON export file
        adf_json_dict: Or pass JSON as dictionary
    
    Returns:
        Dictionary with component analysis and skill recommendations
    """
    
    # Load JSON
    if adf_json_path:
        with open(adf_json_path, 'r') as f:
            pipeline = json.load(f)
    elif adf_json_dict:
        pipeline = adf_json_dict
    else:
        print("❌ Provide either adf_json_path or adf_json_dict")
        return None
    
    # Skill mapping
    skill_mapping = {
        'Copy': 'adf_copy_activity_to_spark.md',
        'Lookup': 'adf_lookup_to_spark_sql.md',
        'ForEach': 'adf_foreach_to_task_loop.md',
        'IfCondition': 'adf_if_condition_to_task_dependency.md',
        'DatabricksNotebook': 'adf_activity_to_task.md',
        'ExecutePipeline': 'adf_pipeline_to_lakeflow_job.md',
        'SqlServerStoredProcedure': 'adf_activity_to_task.md',
        'WebActivity': 'adf_activity_to_task.md',
        'GetMetadata': 'adf_lookup_to_spark_sql.md'
    }
    
    # Analyze
    activities = pipeline.get('properties', {}).get('activities', [])
    
    analysis = {
        'pipeline_name': pipeline.get('name', 'Unknown'),
        'total_activities': len(activities),
        'components': []
    }
    
    print(f"📊 PIPELINE ANALYSIS: {analysis['pipeline_name']}")
    print("=" * 80)
    print(f"Total Activities: {len(activities)}\n")
    
    for activity in activities:
        activity_type = activity.get('type', 'Unknown')
        activity_name = activity.get('name', 'Unnamed')
        skill_file = skill_mapping.get(activity_type, 'adf_activity_to_task.md')
        
        component = {
            'name': activity_name,
            'type': activity_type,
            'skill_file': skill_file
        }
        
        analysis['components'].append(component)
        
        print(f"✓ {activity_name}")
        print(f"  Type: {activity_type}")
        print(f"  → Use skill: {skill_file}\n")
    
    return analysis

# Example usage (uncomment and provide your ADF JSON path):
# analysis = analyze_adf_pipeline('/path/to/your/adf_pipeline.json')

print("💡 TIP: Export your ADF pipeline as JSON and run analyze_adf_pipeline()")
print("         to get personalized skill file recommendations")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Follow the Migration Order
# MAGIC
# MAGIC **From `migration_index.md`:**
# MAGIC
# MAGIC **Phase 1: Foundation (Week 1-2)**
# MAGIC 1. Linked Services → Unity Catalog
# MAGIC 2. Datasets → Delta tables
# MAGIC 3. Triggers → Job schedules
# MAGIC
# MAGIC **Phase 2: Core Logic (Week 3-5)**
# MAGIC 4. Copy Activities → Spark read/write
# MAGIC 5. Lookup Activities → SQL queries
# MAGIC 6. Activities → Tasks
# MAGIC
# MAGIC **Phase 3: Control Flow (Week 6-7)**
# MAGIC 7. If Conditions → Conditional tasks
# MAGIC 8. ForEach → Loops
# MAGIC 9. Pipeline → Complete job
# MAGIC
# MAGIC **Phase 4: Validation (Week 8)**
# MAGIC 10. Testing & optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detailed Usage Workflows {#workflows}
# MAGIC
# MAGIC ### 🔄 Workflow 1: Manual Migration

# COMMAND ----------

# DBTITLE 1,Using Skills with Genie Code Agent
# MAGIC %md
# MAGIC ## 3.5 🤖 Using Skills with Genie Code Agent {#genie}
# MAGIC
# MAGIC ### Why Use Genie Code Agent?
# MAGIC
# MAGIC Genie Code is your AI-powered migration assistant that can:
# MAGIC * **Auto-generate** Databricks code from ADF JSON
# MAGIC * **Apply best practices** from the skills library automatically
# MAGIC * **Create complete notebooks** with migration code
# MAGIC * **Build job YAML** configurations
# MAGIC * **Validate and test** migrations iteratively
# MAGIC
# MAGIC ### How Genie Code Accesses Skills
# MAGIC
# MAGIC When you reference the skills folder in your prompts, Genie Code will:
# MAGIC 1. **Read the relevant skill files** based on your request
# MAGIC 2. **Apply the patterns** from sections 3-4 (Migration Strategy + Code Conversion)
# MAGIC 3. **Follow best practices** from sections 5-7 (Design Patterns, Anti-Patterns, Performance)
# MAGIC 4. **Generate production-ready code** that matches the skill guidance
# MAGIC
# MAGIC **You don't need to copy-paste from skills** - just reference the folder path!

# COMMAND ----------

# DBTITLE 1,Genie Code Prompt Templates
# MAGIC %md
# MAGIC ### 📝 Prompt Templates for Common Tasks
# MAGIC
# MAGIC #### Template 1: Migrate Single Copy Activity
# MAGIC
# MAGIC ```
# MAGIC Using the skills in /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills,
# MAGIC migrate this ADF Copy Activity to Databricks:
# MAGIC
# MAGIC [Paste your ADF Copy Activity JSON here]
# MAGIC
# MAGIC Generate:
# MAGIC 1. Databricks notebook with PySpark code
# MAGIC 2. Use Delta Lake format
# MAGIC 3. Include error handling
# MAGIC 4. Follow the incremental load pattern from the skills
# MAGIC ```
# MAGIC
# MAGIC #### Template 2: Migrate Complete Pipeline
# MAGIC
# MAGIC ```
# MAGIC I have an ADF pipeline that needs migration to Databricks Lakeflow.
# MAGIC Use the skills at /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills
# MAGIC
# MAGIC Pipeline components:
# MAGIC - 2 Lookup activities (watermark tables)
# MAGIC - 5 Copy activities (SQL to Delta)
# MAGIC - 1 ForEach loop (parallel table processing)
# MAGIC - Schedule trigger (daily at 2 AM UTC)
# MAGIC
# MAGIC [Paste your complete pipeline JSON]
# MAGIC
# MAGIC Create:
# MAGIC 1. Individual notebooks for each component
# MAGIC 2. Job YAML with task dependencies
# MAGIC 3. Schedule configuration
# MAGIC ```
# MAGIC
# MAGIC #### Template 3: Migrate with Unity Catalog Setup
# MAGIC
# MAGIC ```
# MAGIC Using /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills,
# MAGIC help me migrate ADF linked services to Unity Catalog.
# MAGIC
# MAGIC Current ADF linked services:
# MAGIC - Azure SQL Database (source)
# MAGIC - Azure Blob Storage (staging)
# MAGIC - Azure Data Lake Gen2 (destination)
# MAGIC
# MAGIC [Paste linked service JSON]
# MAGIC
# MAGIC Generate:
# MAGIC 1. Unity Catalog setup commands
# MAGIC 2. External location configurations
# MAGIC 3. Secret scope setup for credentials
# MAGIC 4. Connection validation queries
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Step-by-Step Genie Workflow
# MAGIC %md
# MAGIC ### 🔄 Step-by-Step Workflow with Genie Code
# MAGIC
# MAGIC #### Phase 1: Discovery & Analysis
# MAGIC
# MAGIC **Your Prompt:**
# MAGIC ```
# MAGIC Analyze this ADF pipeline and recommend which skills I need:
# MAGIC /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills
# MAGIC
# MAGIC [Paste ADF pipeline JSON]
# MAGIC ```
# MAGIC
# MAGIC **Genie Will:**
# MAGIC * Identify all activity types
# MAGIC * List required skill files
# MAGIC * Suggest migration order
# MAGIC * Estimate complexity
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Phase 2: Component Migration
# MAGIC
# MAGIC **Your Prompt:**
# MAGIC ```
# MAGIC Migrate these ADF Lookup activities to Databricks using skills at:
# MAGIC /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills
# MAGIC
# MAGIC [Paste Lookup activities JSON]
# MAGIC
# MAGIC Implement the watermark pattern from adf_lookup_to_spark_sql.md
# MAGIC ```
# MAGIC
# MAGIC **Genie Will:**
# MAGIC * Create notebook with SQL queries
# MAGIC * Implement watermark logic
# MAGIC * Add task value passing
# MAGIC * Include validation checks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Phase 3: Integration
# MAGIC
# MAGIC **Your Prompt:**
# MAGIC ```
# MAGIC Create a Lakeflow job YAML that orchestrates these notebooks:
# MAGIC - lookup_watermark.py (task key: get_watermark)
# MAGIC - copy_incremental_data.py (depends on get_watermark)
# MAGIC - validate_load.py (depends on copy_incremental_data)
# MAGIC
# MAGIC Use job patterns from:
# MAGIC /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills/adf_pipeline_to_lakeflow_job.md
# MAGIC
# MAGIC Schedule: Daily at 2 AM UTC
# MAGIC ```
# MAGIC
# MAGIC **Genie Will:**
# MAGIC * Generate complete job YAML
# MAGIC * Configure task dependencies
# MAGIC * Set up schedule
# MAGIC * Add parameter passing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Phase 4: Testing & Validation
# MAGIC
# MAGIC **Your Prompt:**
# MAGIC ```
# MAGIC Create test cases for this migrated pipeline using validation checklists
# MAGIC from /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills
# MAGIC
# MAGIC Generate:
# MAGIC 1. Unit tests for each notebook
# MAGIC 2. Integration tests for the complete job
# MAGIC 3. Data quality checks
# MAGIC 4. Performance benchmarks
# MAGIC ```
# MAGIC
# MAGIC **Genie Will:**
# MAGIC * Create test notebook
# MAGIC * Add validation queries
# MAGIC * Compare ADF vs Databricks results
# MAGIC * Generate test reports

# COMMAND ----------

# DBTITLE 1,Real-World Conversation Examples
# MAGIC %md
# MAGIC ### 💬 Real-World Conversation Examples
# MAGIC
# MAGIC #### Example 1: Iterative Copy Activity Migration
# MAGIC
# MAGIC **You:** 
# MAGIC ```
# MAGIC I need to migrate this ADF Copy Activity. Use the skills at:
# MAGIC /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills
# MAGIC
# MAGIC { "name": "CopyFromSQL", "type": "Copy", 
# MAGIC   "source": { "type": "SqlSource", "sqlReaderQuery": "SELECT * FROM orders WHERE date > '@{pipeline().parameters.startDate}'" },
# MAGIC   "sink": { "type": "ParquetSink" } }
# MAGIC ```
# MAGIC
# MAGIC **Genie:** Creates notebook with JDBC read, Delta write, parameter handling.
# MAGIC
# MAGIC **You:** 
# MAGIC ```
# MAGIC Great! Now add incremental load logic using watermarks.
# MAGIC Use the pattern from adf_lookup_to_spark_sql.md
# MAGIC ```
# MAGIC
# MAGIC **Genie:** Updates notebook with watermark query, adds merge logic.
# MAGIC
# MAGIC **You:** 
# MAGIC ```
# MAGIC Optimize this for a 10TB table with daily 100GB increments.
# MAGIC Apply performance best practices from the skills.
# MAGIC ```
# MAGIC
# MAGIC **Genie:** Adds partitioning, OPTIMIZE commands, caching strategies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Example 2: Complex ForEach Migration
# MAGIC
# MAGIC **You:**
# MAGIC ```
# MAGIC Migrate this parallel ForEach that processes 50 tables.
# MAGIC Reference: /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills/adf_foreach_to_task_loop.md
# MAGIC
# MAGIC [Paste ForEach JSON with 50-table iteration]
# MAGIC ```
# MAGIC
# MAGIC **Genie:** Shows 3 options:
# MAGIC 1. ThreadPoolExecutor approach
# MAGIC 2. Dynamic job runs
# MAGIC 3. Spark native parallelization
# MAGIC
# MAGIC **You:** 
# MAGIC ```
# MAGIC Use option 2 (dynamic job runs). Generate the orchestrator notebook
# MAGIC and the worker notebook template.
# MAGIC ```
# MAGIC
# MAGIC **Genie:** Creates both notebooks with proper error handling.
# MAGIC
# MAGIC **You:**
# MAGIC ```
# MAGIC Add retry logic and failure notifications to Slack.
# MAGIC ```
# MAGIC
# MAGIC **Genie:** Updates with try-catch blocks and webhook calls.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Example 3: Complete Pipeline with Governance
# MAGIC
# MAGIC **You:**
# MAGIC ```
# MAGIC Full migration with Unity Catalog governance:
# MAGIC
# MAGIC ADF Pipeline: daily_sales_etl
# MAGIC Skills: /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills
# MAGIC
# MAGIC Requirements:
# MAGIC - Migrate all linked services to Unity Catalog
# MAGIC - Use Delta sharing for external consumers
# MAGIC - Implement column masking for PII
# MAGIC - Add audit logging
# MAGIC - Set up ABAC policies
# MAGIC
# MAGIC [Paste complete pipeline JSON]
# MAGIC ```
# MAGIC
# MAGIC **Genie:** 
# MAGIC * Creates Unity Catalog schema
# MAGIC * Sets up external locations
# MAGIC * Generates migration notebooks
# MAGIC * Adds column masks and row filters
# MAGIC * Implements audit logging
# MAGIC * Creates job YAML with governance settings

# COMMAND ----------

# DBTITLE 1,Advanced Genie Techniques
# MAGIC %md
# MAGIC ### 🎯 Advanced Techniques for Complex Migrations
# MAGIC
# MAGIC #### Technique 1: Batch Migration
# MAGIC
# MAGIC ```
# MAGIC I have 50 ADF pipelines to migrate. Use skills at:
# MAGIC /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills
# MAGIC
# MAGIC Create a migration framework that:
# MAGIC 1. Reads ADF JSON exports from /path/to/exports/
# MAGIC 2. Generates Databricks assets for each pipeline
# MAGIC 3. Creates a migration summary report
# MAGIC 4. Validates each migration
# MAGIC
# MAGIC Use the automation hints from Section 9 of each skill file.
# MAGIC ```
# MAGIC
# MAGIC #### Technique 2: Custom Pattern Development
# MAGIC
# MAGIC ```
# MAGIC Our ADF pipelines use a custom pattern not covered in the skills.
# MAGIC Based on /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills patterns,
# MAGIC help me develop a Databricks equivalent for:
# MAGIC
# MAGIC [Describe your custom ADF pattern]
# MAGIC
# MAGIC Reference similar patterns from:
# MAGIC - adf_foreach_to_task_loop.md (for iteration concepts)
# MAGIC - adf_if_condition_to_task_dependency.md (for control flow)
# MAGIC ```
# MAGIC
# MAGIC #### Technique 3: Performance Comparison
# MAGIC
# MAGIC ```
# MAGIC Generate performance comparison tests between ADF and Databricks.
# MAGIC Use metrics from Section 7 of:
# MAGIC /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills/adf_copy_activity_to_spark.md
# MAGIC
# MAGIC Test scenarios:
# MAGIC 1. Full table copy (1TB)
# MAGIC 2. Incremental load (10GB delta)
# MAGIC 3. Wide table (1000 columns)
# MAGIC 4. High-frequency micro-batches
# MAGIC
# MAGIC Generate PySpark notebooks that measure:
# MAGIC - Execution time
# MAGIC - Data throughput
# MAGIC - Cost per GB processed
# MAGIC ```
# MAGIC
# MAGIC #### Technique 4: Rollback Planning
# MAGIC
# MAGIC ```
# MAGIC Create a rollback strategy using:
# MAGIC /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills
# MAGIC
# MAGIC Generate:
# MAGIC 1. Data validation queries (compare ADF output vs Databricks output)
# MAGIC 2. Reconciliation reports
# MAGIC 3. Delta time travel commands for rollback
# MAGIC 4. Parallel run monitoring (ADF + Databricks side-by-side)
# MAGIC 5. Cutover checklist
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Best Practices for Genie Prompts
# MAGIC %md
# MAGIC ### ✅ Best Practices for Effective Genie Prompts
# MAGIC
# MAGIC #### DO's:
# MAGIC
# MAGIC 1. **Always reference the skills folder path**
# MAGIC    ```
# MAGIC    ✅ "Using /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills..."
# MAGIC    ❌ "Migrate this ADF pipeline..." (Genie won't know about skills)
# MAGIC    ```
# MAGIC
# MAGIC 2. **Be specific about which skill file to use**
# MAGIC    ```
# MAGIC    ✅ "Apply the watermark pattern from adf_lookup_to_spark_sql.md"
# MAGIC    ✅ "Use Section 5 design patterns from adf_copy_activity_to_spark.md"
# MAGIC    ```
# MAGIC
# MAGIC 3. **Provide complete JSON context**
# MAGIC    ```
# MAGIC    ✅ Include full ADF JSON (even if long)
# MAGIC    ❌ "I have a copy activity..." (Genie needs actual structure)
# MAGIC    ```
# MAGIC
# MAGIC 4. **Request specific outputs**
# MAGIC    ```
# MAGIC    ✅ "Generate: 1) notebook, 2) job YAML, 3) test cases"
# MAGIC    ❌ "Help me migrate" (too vague)
# MAGIC    ```
# MAGIC
# MAGIC 5. **Iterate and refine**
# MAGIC    ```
# MAGIC    ✅ "Good start! Now add error handling from Section 4"
# MAGIC    ✅ "Optimize this further using Section 7 performance tips"
# MAGIC    ```
# MAGIC
# MAGIC #### DON'Ts:
# MAGIC
# MAGIC 1. **Don't assume Genie knows your ADF structure**
# MAGIC    * Always paste the actual JSON
# MAGIC
# MAGIC 2. **Don't skip the skills path**
# MAGIC    * Genie won't apply best practices without explicit reference
# MAGIC
# MAGIC 3. **Don't ask for everything at once**
# MAGIC    * Break complex migrations into phases
# MAGIC
# MAGIC 4. **Don't ignore validation**
# MAGIC    * Always request test cases and validation queries
# MAGIC
# MAGIC 5. **Don't forget about governance**
# MAGIC    * Mention Unity Catalog, secrets, permissions requirements

# COMMAND ----------

# DBTITLE 1,Helper: Generate Genie Prompt
# Helper function to generate optimized Genie Code prompts

def generate_genie_prompt(migration_type: str, adf_json: dict = None, 
                          custom_requirements: list = None) -> str:
    """
    Generate an optimized prompt for Genie Code agent
    
    Args:
        migration_type: Type of migration ('copy', 'lookup', 'foreach', 'pipeline', 'linked_service')
        adf_json: ADF component JSON (optional - will prompt to paste)
        custom_requirements: Additional requirements list
    
    Returns:
        Complete prompt string ready to use with Genie Code
    """
    
    skills_path = '/Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills'
    
    skill_mapping = {
        'copy': 'adf_copy_activity_to_spark.md',
        'lookup': 'adf_lookup_to_spark_sql.md',
        'foreach': 'adf_foreach_to_task_loop.md',
        'pipeline': 'adf_pipeline_to_lakeflow_job.md',
        'linked_service': 'adf_linked_service_to_unity_catalog.md',
        'condition': 'adf_if_condition_to_task_dependency.md',
        'dataset': 'adf_dataset_to_delta_table.md',
        'trigger': 'adf_trigger_to_job_schedule.md'
    }
    
    skill_file = skill_mapping.get(migration_type, 'migration_index.md')
    
    prompt_parts = [
        f"Using the migration skills at: {skills_path}",
        f"Specifically reference: {skill_file}",
        "",
        f"Migrate this ADF {migration_type.upper()} component to Databricks.",
        ""
    ]
    
    if adf_json:
        import json
        prompt_parts.append("ADF Component JSON:")
        prompt_parts.append("```json")
        prompt_parts.append(json.dumps(adf_json, indent=2))
        prompt_parts.append("```")
    else:
        prompt_parts.append("[Paste your ADF JSON here]")
    
    prompt_parts.append("")
    prompt_parts.append("Generate:")
    
    if migration_type == 'copy':
        prompt_parts.extend([
            "1. Databricks notebook with PySpark/SQL code",
            "2. Use Delta Lake format with schema evolution",
            "3. Implement incremental load pattern (if applicable)",
            "4. Add error handling and logging",
            "5. Include data validation checks"
        ])
    elif migration_type == 'lookup':
        prompt_parts.extend([
            "1. SQL query notebook",
            "2. Watermark/metadata query logic",
            "3. Task value passing to downstream tasks",
            "4. Validation queries"
        ])
    elif migration_type == 'foreach':
        prompt_parts.extend([
            "1. Parallelization approach (choose best option)",
            "2. Main orchestrator notebook",
            "3. Worker/template notebook",
            "4. Error handling and retry logic"
        ])
    elif migration_type == 'pipeline':
        prompt_parts.extend([
            "1. Individual notebooks for each activity",
            "2. Complete job YAML configuration",
            "3. Task dependencies and parameters",
            "4. Schedule configuration",
            "5. Cluster settings"
        ])
    elif migration_type == 'linked_service':
        prompt_parts.extend([
            "1. Unity Catalog setup commands",
            "2. External location configurations",
            "3. Secret scope and credential setup",
            "4. Connection validation tests"
        ])
    
    if custom_requirements:
        prompt_parts.append("")
        prompt_parts.append("Additional Requirements:")
        for req in custom_requirements:
            prompt_parts.append(f"- {req}")
    
    prompt_parts.append("")
    prompt_parts.append("Follow best practices from the skill file:")
    prompt_parts.append("- Apply design patterns (Section 5)")
    prompt_parts.append("- Avoid anti-patterns (Section 6)")
    prompt_parts.append("- Include performance optimizations (Section 7)")
    prompt_parts.append("- Add governance/security (Section 8)")
    
    return "\n".join(prompt_parts)

# Example usage
print("🤖 GENIE PROMPT GENERATOR")
print("=" * 80)
print()

# Generate prompt for Copy Activity migration
example_prompt = generate_genie_prompt(
    migration_type='copy',
    custom_requirements=[
        'Source: Azure SQL Database with 100M rows',
        'Destination: Unity Catalog Delta table',
        'Use Auto Loader for incremental loads',
        'Implement medallion architecture (bronze -> silver)'
    ]
)

print(example_prompt)
print()
print("💡 Copy the prompt above and paste it into Genie Code chat!")

# COMMAND ----------

# Create migration tracker
from pyspark.sql.types import *
from pyspark.sql import Row

# Define your migration tasks
migration_tasks = [
    Row(phase="Foundation", task="Create Unity Catalog", skill="adf_linked_service_to_unity_catalog.md", status="pending"),
    Row(phase="Foundation", task="Setup secrets", skill="adf_linked_service_to_unity_catalog.md", status="pending"),
    Row(phase="Foundation", task="Create Delta tables", skill="adf_dataset_to_delta_table.md", status="pending"),
    Row(phase="Migration", task="Migrate copy activities", skill="adf_copy_activity_to_spark.md", status="pending"),
    Row(phase="Migration", task="Migrate lookups", skill="adf_lookup_to_spark_sql.md", status="pending"),
    Row(phase="Integration", task="Create job YAML", skill="adf_pipeline_to_lakeflow_job.md", status="pending"),
    Row(phase="Integration", task="Setup schedule", skill="adf_trigger_to_job_schedule.md", status="pending"),
    Row(phase="Validation", task="Run tests", skill="All Section 10", status="pending")
]

# Create DataFrame
tracker_df = spark.createDataFrame(migration_tasks)

print("📋 MIGRATION TRACKER")
print("=" * 80)
display(tracker_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🤖 Workflow 2: AI-Assisted Migration
# MAGIC
# MAGIC **See Section 3.5 for comprehensive Genie Code guidance!**
# MAGIC
# MAGIC **Quick Example:**
# MAGIC
# MAGIC ```
# MAGIC Using the skills in /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills,
# MAGIC migrate this ADF Copy Activity to Databricks:
# MAGIC  
# MAGIC {paste your ADF JSON here}
# MAGIC  
# MAGIC Generate the equivalent Databricks notebook and job YAML.
# MAGIC ```
# MAGIC
# MAGIC **Genie Code will:**
# MAGIC * Read the relevant skill files automatically
# MAGIC * Apply migration patterns from the skills
# MAGIC * Generate production-ready code
# MAGIC * Follow best practices and avoid anti-patterns
# MAGIC
# MAGIC **For detailed prompt templates, workflows, and examples, see Section 3.5 above.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Migration Best Practices {#bestpractices}
# MAGIC
# MAGIC ### ✅ DO's
# MAGIC
# MAGIC 1. **Always start with migration_index.md**
# MAGIC 2. **Follow the recommended migration order**
# MAGIC 3. **Use Unity Catalog from day 1**
# MAGIC 4. **Leverage Delta Lake features (MERGE, time travel)**
# MAGIC 5. **Test incrementally - one component at a time**
# MAGIC 6. **Review Section 7 (Performance) in each skill**
# MAGIC 7. **Use validation checklists (Section 10)**
# MAGIC
# MAGIC ### ❌ DON'Ts
# MAGIC
# MAGIC 1. **Don't migrate everything at once**
# MAGIC 2. **Don't hardcode credentials**
# MAGIC 3. **Don't skip anti-pattern sections (Section 6)**
# MAGIC 4. **Don't ignore schema evolution**
# MAGIC 5. **Don't use Parquet when Delta is available**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Common Scenarios {#scenarios}
# MAGIC
# MAGIC ### Scenario 1: Daily Incremental ETL
# MAGIC
# MAGIC **Skills needed:**
# MAGIC - `adf_trigger_to_job_schedule.md` - Scheduling
# MAGIC - `adf_lookup_to_spark_sql.md` - Watermark pattern
# MAGIC - `adf_copy_activity_to_spark.md` - Incremental load
# MAGIC
# MAGIC ### Scenario 2: Multi-Table Parallel Processing
# MAGIC
# MAGIC **Skills needed:**
# MAGIC - `adf_lookup_to_spark_sql.md` - Get table list
# MAGIC - `adf_foreach_to_task_loop.md` - Parallel patterns
# MAGIC - `adf_copy_activity_to_spark.md` - Data movement
# MAGIC
# MAGIC ### Scenario 3: Event-Driven File Processing
# MAGIC
# MAGIC **Skills needed:**
# MAGIC - `adf_trigger_to_job_schedule.md` - Event alternatives
# MAGIC - `adf_copy_activity_to_spark.md` - Auto Loader pattern
# MAGIC
# MAGIC ### Scenario 4: Complex Data Validation
# MAGIC
# MAGIC **Skills needed:**
# MAGIC - `adf_lookup_to_spark_sql.md` - Validation queries
# MAGIC - `adf_if_condition_to_task_dependency.md` - Conditional logic
# MAGIC - `adf_copy_activity_to_spark.md` - Data movement

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Troubleshooting {#troubleshooting}
# MAGIC
# MAGIC ### Issue 1: Can't Find Right Skill File

# COMMAND ----------

def find_skill_for_component(adf_component_type: str) -> str:
    """
    Find the appropriate skill file for an ADF component
    
    Args:
        adf_component_type: ADF activity type (e.g., 'Copy', 'Lookup', 'ForEach')
    
    Returns:
        Skill file name
    """
    mapping = {
        'Copy': 'adf_copy_activity_to_spark.md',
        'Lookup': 'adf_lookup_to_spark_sql.md',
        'ForEach': 'adf_foreach_to_task_loop.md',
        'IfCondition': 'adf_if_condition_to_task_dependency.md',
        'DatabricksNotebook': 'adf_activity_to_task.md',
        'ExecutePipeline': 'adf_pipeline_to_lakeflow_job.md',
        'SqlServerStoredProcedure': 'adf_activity_to_task.md',
        'WebActivity': 'adf_activity_to_task.md',
        'GetMetadata': 'adf_lookup_to_spark_sql.md',
        'Until': 'adf_foreach_to_task_loop.md',
        'Wait': 'adf_activity_to_task.md'
    }
    
    skill = mapping.get(adf_component_type, 'migration_index.md')
    print(f"Component: {adf_component_type}")
    print(f"→ Use skill: {skill}")
    return skill

# Example
find_skill_for_component('Copy')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 2: Performance Slower Than ADF
# MAGIC
# MAGIC **Checklist:**
# MAGIC - [ ] Using Delta Lake optimizations? (`OPTIMIZE`, `ZORDER BY`)
# MAGIC - [ ] Cluster sized correctly? (ADF DIU ≈ Databricks cluster sizing)
# MAGIC - [ ] Using Photon for SQL workloads?
# MAGIC - [ ] Leveraging Spark parallelism? (repartition, partitioning)
# MAGIC - [ ] Reviewed Section 7 of `adf_copy_activity_to_spark.md`?
# MAGIC
# MAGIC ### Issue 3: Credentials Not Working
# MAGIC
# MAGIC **Solution:**
# MAGIC - Follow `adf_linked_service_to_unity_catalog.md` Section 3
# MAGIC - Create secret scope
# MAGIC - Add secrets
# MAGIC - Verify permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Reference & Cheat Sheets {#reference}
# MAGIC
# MAGIC ### 📊 Component Mapping Quick Reference

# COMMAND ----------

# Display component mapping table
from pyspark.sql import Row

mappings = [
    Row(adf="Pipeline", databricks="Lakeflow Job", skill="adf_pipeline_to_lakeflow_job.md"),
    Row(adf="Copy Activity", databricks="Spark read/write", skill="adf_copy_activity_to_spark.md"),
    Row(adf="Lookup", databricks="SQL task", skill="adf_lookup_to_spark_sql.md"),
    Row(adf="ForEach", databricks="ThreadPool/Spark parallel", skill="adf_foreach_to_task_loop.md"),
    Row(adf="If Condition", databricks="condition_task", skill="adf_if_condition_to_task_dependency.md"),
    Row(adf="Linked Service", databricks="Unity Catalog + Secrets", skill="adf_linked_service_to_unity_catalog.md"),
    Row(adf="Dataset", databricks="Delta table", skill="adf_dataset_to_delta_table.md"),
    Row(adf="Trigger", databricks="Job schedule", skill="adf_trigger_to_job_schedule.md")
]

mapping_df = spark.createDataFrame(mappings)
display(mapping_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📋 Code Snippets Library

# COMMAND ----------

print("🔧 ESSENTIAL CODE SNIPPETS")
print("=" * 80)

snippets = {
    "JDBC Read": """
jdbc_url = dbutils.secrets.get("secrets", "url")
df = spark.read.format("jdbc").option("url", jdbc_url).load()
    """,
    
    "Delta Write": """
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("catalog.schema.table")
    """,
    
    "Delta Merge": """
from delta.tables import DeltaTable
DeltaTable.forName(spark, "target").alias("t").merge(source.alias("s"), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    """,
    
    "Auto Loader": """
spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").load(path).writeStream.format("delta").trigger(availableNow=True).toTable(table)
    """,
    
    "Task Values": """
dbutils.jobs.taskValues.set(key="result", value=value)
# Use in job YAML: "{{tasks.task_key.values.result}}"
    """
}

for name, code in snippets.items():
    print(f"\n### {name}")
    print(code)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔗 Quick Access to Skills

# COMMAND ----------

import os

# List all skill files
skills_dir = '/Workspace/Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills'
skill_files = sorted([f for f in os.listdir(skills_dir) if f.endswith('.md')])

print("📚 AVAILABLE SKILL FILES")
print("=" * 80)

for idx, file in enumerate(skill_files, 1):
    file_path = os.path.join(skills_dir, file)
    size_kb = os.path.getsize(file_path) / 1024
    
    # Get brief description
    descriptions = {
        'migration_index.md': 'START HERE - Architecture & overview',
        'adf_pipeline_to_lakeflow_job.md': 'Pipeline orchestration (JSON → YAML)',
        'adf_activity_to_task.md': 'Activity transformation patterns',
        'adf_copy_activity_to_spark.md': 'Data movement & Auto Loader',
        'adf_lookup_to_spark_sql.md': 'Watermarks, config, metadata',
        'adf_foreach_to_task_loop.md': 'Iteration & parallel processing',
        'adf_if_condition_to_task_dependency.md': 'Conditional logic',
        'adf_linked_service_to_unity_catalog.md': 'Connections & credentials',
        'adf_dataset_to_delta_table.md': 'Schema & data formats',
        'adf_trigger_to_job_schedule.md': 'Scheduling & cron'
    }
    
    desc = descriptions.get(file, '')
    star = '⭐ ' if 'index' in file else '   '
    
    print(f"{star}{idx:2d}. {file:<45s} ({size_kb:>6.1f} KB)")
    if desc:
        print(f"     {desc}")

print(f"\n📍 Location: {skills_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🎉 You're Ready to Migrate!
# MAGIC
# MAGIC ### Quick Recap
# MAGIC
# MAGIC ✅ **10 comprehensive skill files** with production code  
# MAGIC ✅ **Structured 10-section format** for easy navigation  
# MAGIC ✅ **Complete workflows** from foundation to production  
# MAGIC ✅ **Code examples** ready to adapt  
# MAGIC ✅ **Best practices** and anti-patterns  
# MAGIC ✅ **Validation checklists** for quality  
# MAGIC
# MAGIC ### 🚀 Next Steps
# MAGIC
# MAGIC 1. **Read migration_index.md** - Overall strategy
# MAGIC 2. **Analyze your ADF pipelines** - Use `analyze_adf_pipeline()` above
# MAGIC 3. **Follow recommended order** - Foundation → Logic → Integration
# MAGIC 4. **Reference skill files** - Use this notebook as your guide
# MAGIC 5. **Validate thoroughly** - Section 10 in each skill
# MAGIC
# MAGIC **Happy Migrating! 🎯**

# COMMAND ----------

# DBTITLE 1,Genie Code Quick Reference Card
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Genie Code Quick Reference Card
# MAGIC
# MAGIC ### Essential Prompt Structure
# MAGIC
# MAGIC ```
# MAGIC Using the skills in /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills,
# MAGIC [specifically reference: <skill_file_name>]
# MAGIC
# MAGIC [Your migration request]
# MAGIC
# MAGIC [Paste ADF JSON]
# MAGIC
# MAGIC Generate:
# MAGIC 1. [Specific output 1]
# MAGIC 2. [Specific output 2]
# MAGIC 3. [Specific output 3]
# MAGIC ```
# MAGIC
# MAGIC ### Key Phrases to Use
# MAGIC
# MAGIC | Phase | Prompt Phrase |
# MAGIC |-------|---------------|
# MAGIC | **Analysis** | "Analyze this ADF pipeline and recommend which skills I need" |
# MAGIC | **Migration** | "Migrate this ADF [component] using the pattern from [skill_file]" |
# MAGIC | **Optimization** | "Apply performance best practices from Section 7" |
# MAGIC | **Validation** | "Create test cases using validation checklists from the skills" |
# MAGIC | **Integration** | "Create a Lakeflow job YAML that orchestrates these notebooks" |
# MAGIC
# MAGIC ### Skill File Quick Lookup
# MAGIC
# MAGIC | ADF Component | Skill File to Reference |
# MAGIC |---------------|------------------------|
# MAGIC | Copy Activity | `adf_copy_activity_to_spark.md` |
# MAGIC | Lookup | `adf_lookup_to_spark_sql.md` |
# MAGIC | ForEach | `adf_foreach_to_task_loop.md` |
# MAGIC | If Condition | `adf_if_condition_to_task_dependency.md` |
# MAGIC | Pipeline | `adf_pipeline_to_lakeflow_job.md` |
# MAGIC | Linked Service | `adf_linked_service_to_unity_catalog.md` |
# MAGIC | Dataset | `adf_dataset_to_delta_table.md` |
# MAGIC | Trigger | `adf_trigger_to_job_schedule.md` |
# MAGIC
# MAGIC ### Pro Tips
# MAGIC
# MAGIC 1. **Be Specific:** Reference exact section numbers ("Use Section 5 design patterns")
# MAGIC 2. **Iterate:** Start simple, then refine ("Now add error handling", "Now optimize")
# MAGIC 3. **Validate:** Always ask for test cases and validation queries
# MAGIC 4. **Context:** Provide full ADF JSON, not just descriptions
# MAGIC 5. **Best Practices:** Mention Unity Catalog, Delta Lake, and governance requirements
# MAGIC
# MAGIC ### Example Power Prompt
# MAGIC
# MAGIC ```
# MAGIC Using /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills,
# MAGIC migrate this complete ADF pipeline to Databricks Lakeflow:
# MAGIC
# MAGIC [Paste complete pipeline JSON]
# MAGIC
# MAGIC Requirements:
# MAGIC - Unity Catalog integration (adf_linked_service_to_unity_catalog.md)
# MAGIC - Delta Lake with medallion architecture (adf_copy_activity_to_spark.md Section 5)
# MAGIC - Incremental loads with watermarks (adf_lookup_to_spark_sql.md)
# MAGIC - Parallel processing for 50 tables (adf_foreach_to_task_loop.md Section 5)
# MAGIC - Daily schedule at 2 AM UTC (adf_trigger_to_job_schedule.md)
# MAGIC
# MAGIC Generate:
# MAGIC 1. Unity Catalog setup
# MAGIC 2. Individual notebooks for each component
# MAGIC 3. Complete job YAML with dependencies
# MAGIC 4. Test suite with validation queries
# MAGIC 5. Performance optimization plan
# MAGIC
# MAGIC Follow ALL best practices from Section 5-8 of each relevant skill.
# MAGIC ```
# MAGIC
# MAGIC **Copy this template and customize for your migration!** 🚀

# COMMAND ----------

# MAGIC %md
# MAGIC Using /Users/pysparktraining55@gmail.com/adf-to-lakeflow-skills,
# MAGIC specifically reference: adf_copy_activity_to_spark.md
# MAGIC
# MAGIC Migrate this ADF Copy Activity:
# MAGIC [Paste your JSON]
# MAGIC
# MAGIC Generate:
# MAGIC 1. PySpark notebook with Delta Lake
# MAGIC 2. Incremental load pattern
# MAGIC 3. Error handling and validation