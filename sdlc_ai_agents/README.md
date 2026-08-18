# SDLC AI Agents

**Open-source Agentic AI framework for automating and enhancing the Software Development Life Cycle (SDLC), with first-class Databricks and data engineering integrations.**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Enabled-red.svg)](https://www.databricks.com/)

> **License:** Apache License 2.0
> **SPDX Identifier:** Apache-2.0

---

## Overview

![Alt Text](https://github.com/TRRaveendra/AI-Agents-Databricks/blob/main/images/sdlc_archietcture_agents.jpeg?raw=true)

**SDLC AI Agents** is an open-source Agentic AI framework designed to automate and enhance different stages of the Software Development Life Cycle.

The framework provides specialized AI agents for requirements analysis, business understanding, metadata discovery, data modeling, PySpark and SQL development, data quality, code review, testing, documentation, deployment, and production monitoring.

The project is designed primarily for **Databricks and modern data engineering environments**, while keeping the agent architecture extensible for additional platforms and integrations.
# Databricks Multi-Agent SDLC Platform Orchestrator
## From Jira Requirement to Production-Ready Solution

**Overview**: This orchestrator manages the complete 15-agent pipeline for automated SDLC implementation using Databricks Assistant API (Genie Code) and specialized skills.

## Platform Architecture

```
╭───────────────────────────────────────────────────╮
│   DATABRICKS MULTI-AGENT SDLC PLATFORM   │
│   Jira to Production - Fully Automated    │
╰───────────────────────────────────────────────────╯
```

### 15-Agent Pipeline

① **Jira Requirement Agent** → Extract & parse Jira tickets  
② **Requirement Validation Agent** → Validate completeness & identify gaps  
③ **Business Understanding Agent** → Articulate business objectives  
④ **Metadata Discovery Agent** → Discover Unity Catalog metadata  
⑤ **Mapping Document Agent** → Create source-to-target mappings  
⑥ **Data Model Agent** → Design data models & DDL  
⑦ **PySpark Development Agent** → Generate production PySpark code  
⑧ **SQL Agent** → Generate SQL transformations  
⑨ **Data Quality Agent** → Implement DQ checks  
⑩ **Code Review Agent** → Review & optimize code  
⑪ **Unit Test Agent** → Generate unit tests  
⑫ **Testing Agent** → Run integration tests  
⑬ **Documentation Agent** → Generate documentation  
⑭ **Deployment Agent** → Deploy to production  
⑮ **Production Monitoring Agent** → Monitor & alert  

### Key Features

✅ **Fully Automated** - End-to-end pipeline from Jira to production  
✅ **Genie Code Integration** - Leverages Databricks Assistant API  
✅ **Skill-Based** - Uses 15 specialized SDLC agent skills  
✅ **Unity Catalog Native** - State management in Delta tables  
✅ **Approval Gates** - Human-in-the-loop at critical stages  
✅ **Vector Search** - Knowledge retrieval for context  
✅ **Production-Ready** - Runs as Databricks Jobs  
 
# Databricks Multi-Agent SDLC Platform
## From Jira Requirement to Production-Ready Solution

### Overview

This platform automates the entire Software Development Life Cycle (SDLC) using 15 specialized AI agents that work together to transform Jira requirements into production-ready data solutions. The system handles everything from requirement analysis to deployment and monitoring, with built-in approval gates for quality control.

**Key Capabilities:**
* Automated requirement extraction from Jira tickets
* Business understanding and metadata discovery
* Data modeling and mapping document generation
* PySpark and SQL code generation
* Data quality validation
* Code review and unit testing
* Documentation generation
* Deployment automation
* Production monitoring

---

## Architecture

### Agent Pipeline Flow

The platform consists of 15 agents orchestrated through `16_Orchestrator.py`:

```
01_Jira_Requirement_Agent
    ↓
02_Requirement_Validation_Agent [APPROVAL GATE]
    ↓
03_Business_Understanding_Agent
    ↓
04_Metadata_Discovery_Agent
    ↓
05_Mapping_Document_Agent [APPROVAL GATE]
    ↓
06_Data_Model_Agent [APPROVAL GATE]
    ↓
07_PySpark_Development_Agent
    ↓
08_SQL_Agent
    ↓
09_Data_Quality_Agent
    ↓
10_Code_Review_Agent [APPROVAL GATE]
    ↓
11_Unit_Test_Agent
    ↓
12_Testing_Agent
    ↓
13_Documentation_Agent
    ↓
14_Deployment_Agent [APPROVAL GATE]
    ↓
15_Production_Monitoring_Agent
```

**Approval Gates:** Human review points at agents 2, 5, 6, 10, and 14 ensure quality control at critical stages.

---

## Prerequisites

### 1. Databricks Environment

* **Workspace Access:** Databricks workspace with notebook execution permissions
* **Compute:** Serverless compute or a dedicated cluster
* **Unity Catalog:** Access to create catalogs, schemas, tables, and volumes
* **DBR Version:** DBR 13.0+ (for Foundation Model API support)

### 2. Unity Catalog Setup

The platform requires the following Unity Catalog objects:

* **Catalog:** `main` (or customize in `00_Shared_Utils`)
* **Schema:** `state` (for tracking artifacts, agent runs, and pipeline runs)
* **Tables:**
  * `state.artifacts` - Stores agent outputs
  * `state.agent_runs` - Tracks individual agent executions
  * `state.pipeline_runs` - Tracks end-to-end pipeline runs
* **Volumes:**
  * `state.artifacts` - Stores generated files (code, docs, models)
  * `state.skills` - Stores agent skill documents

### 3. Required Secrets

#### Jira API Access

Run the prerequisite notebook to set up Jira credentials:

```
/Workspace/Users/motivation4shots@gmail.com/sdlc_ai_agents/jira-setup-secrets
```

This creates:
* **Secret Scope:** `jira-scope`
* **Secrets:**
  * `jira-email` - Your Jira account email
  * `jira-api-token` - Jira API token ([Generate here](https://id.atlassian.com/manage-profile/security/api-tokens))

#### Foundation Model API

The platform uses Databricks Foundation Model API for AI capabilities:
* No additional setup required - uses workspace authentication
* Ensure your workspace has access to Foundation Model endpoints

### 4. Python Dependencies

Required libraries (typically pre-installed on DBR 13+):
* `databricks-sdk`
* `mlflow`
* `pyspark`

---

## Setup Instructions

### Step 1: Clone/Import the Notebooks

1. Navigate to your Databricks workspace
2. Import all notebooks from the repository into:
   ```
   /Workspace/Users/<your-email>/sdlc_ai_agents/
   ```

**Required Notebooks:**
* `00_Shared_Utils` - Common utilities and configurations
* `01_Jira_Requirement_Agent` through `15_Production_Monitoring_Agent`
* `16_Orchestrator` - Main orchestration notebook
* `17_Comprehensive_Validation_Tests` - Validation suite
* `jira-setup-secrets` - Credential setup

### Step 2: Configure Jira Access

1. Open and run the `jira-setup-secrets` notebook
2. Update with your Jira credentials:
   * Email: Your Jira account email
   * API Token: [Generate from Jira](https://id.atlassian.com/manage-profile/security/api-tokens)

### Step 3: Initialize State Objects

1. Open `16_Orchestrator` notebook
2. Run the first 3 cells:
   ```python
   %run ./00_Shared_Utils
   ensure_state_objects()
   ```

This creates all required Unity Catalog tables and volumes.

### Step 4: Verify Configuration

1. Check that the catalog and schema exist:
   ```sql
   SHOW TABLES IN main.state;
   ```

2. Verify volumes:
   ```python
   dbutils.fs.ls("/Volumes/main/state/artifacts/")
   ```

### Step 5: Update Configuration (Optional)

In `00_Shared_Utils`, customize:
* `CATALOG` - Default Unity Catalog name
* `STATE_SCHEMA` - Schema for state tracking
* `FOUNDATION_MODEL_ENDPOINT` - Model endpoint name
* `JIRA_BASE_URL` - Your Jira instance URL

---

## Running the Platform

### Method 1: Interactive Execution

From the `16_Orchestrator` notebook:

```python
# Run the complete pipeline for a Jira ticket
run_id = run_pipeline(ticket_id="PROJ-123")

# Check status
status = get_pipeline_run(run_id)
print(status)

# Approve and resume at approval gates
approve_and_resume(run_id)
```

### Method 2: Scheduled Job

1. Create a Databricks Job
2. Configure as a Notebook task:
   * **Notebook:** `/Workspace/Users/<your-email>/sdlc_ai_agents/16_Orchestrator`
   * **Compute:** Serverless or dedicated cluster
3. Add parameter:
   * Key: `ticket_id`
   * Value: Jira ticket ID (e.g., `PROJ-123`)
4. Set schedule or trigger

### Method 3: API/SDK Trigger

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

run = w.jobs.run_now(
    job_id=<your-job-id>,
    notebook_params={"ticket_id": "PROJ-123"}
)
```

---

## Agent Descriptions

| Agent | Name | Purpose | Approval Gate |
|-------|------|---------|---------------|
| 00 | Shared Utils | Common utilities and state management | - |
| 01 | Jira Requirement | Extracts requirements from Jira tickets | - |
| 02 | Requirement Validation | Validates completeness and clarity | ✓ |
| 03 | Business Understanding | Analyzes business context and objectives | - |
| 04 | Metadata Discovery | Discovers source/target table schemas | - |
| 05 | Mapping Document | Creates source-to-target mapping | ✓ |
| 06 | Data Model | Designs target data model and DDL | ✓ |
| 07 | PySpark Development | Generates PySpark transformation code | - |
| 08 | SQL | Generates SQL queries and views | - |
| 09 | Data Quality | Implements quality checks and validations | - |
| 10 | Code Review | Reviews generated code for best practices | ✓ |
| 11 | Unit Test | Creates unit tests for transformations | - |
| 12 | Testing | Executes tests and validates output | - |
| 13 | Documentation | Generates technical documentation | - |
| 14 | Deployment | Prepares deployment artifacts | ✓ |
| 15 | Production Monitoring | Sets up monitoring and alerting | - |
| 16 | Orchestrator | Coordinates all agents and manages flow | - |

---

## Output Artifacts

All agent outputs are stored in:
* **Tables:** `main.state.artifacts` (metadata)
* **Volume:** `/Volumes/main/state/artifacts/<run_id>/<agent>/`

**Artifact Types:**
* Requirements documents (JSON, Markdown)
* Data models and DDL scripts (SQL)
* Transformation code (PySpark, SQL)
* Test suites (Python)
* Documentation (Markdown)
* Deployment manifests (YAML, JSON)
* Monitoring configurations (JSON)

---

## Approval Workflow

When a pipeline reaches an approval gate:

1. **Pipeline pauses** and status changes to `awaiting_approval`
2. **Review artifacts** from the previous agent
3. **Approve or reject:**
   ```python
   # Get the run_id from the pipeline
   run = latest_run_for_ticket("PROJ-123")
   
   # Review artifacts
   artifacts = get_artifacts_for_run(run['run_id'])
   
   # Approve and continue
   approve_and_resume(run['run_id'])
   ```

4. **Pipeline resumes** from the next agent

---

## Monitoring and Troubleshooting

### Check Pipeline Status

```python
# Get latest run for a ticket
run = latest_run_for_ticket("PROJ-123")
print(f"Status: {run['status']}")
print(f"Current Agent: {run['current_agent_id']}")

# View all runs
spark.sql("SELECT * FROM main.state.pipeline_runs ORDER BY created_at DESC").show()
```

### View Agent Execution History

```python
spark.sql("""
  SELECT agent_id, status, error_message, created_at
  FROM main.state.agent_runs
  WHERE run_id = '<run_id>'
  ORDER BY agent_id
""").show()
```

### Common Issues

**Issue:** Agent fails with authentication error
* **Solution:** Verify Jira secrets are set correctly in `jira-scope`

**Issue:** "Catalog/schema not found"
* **Solution:** Run `ensure_state_objects()` from the Orchestrator

**Issue:** Pipeline stuck at approval gate
* **Solution:** Check pipeline status and call `approve_and_resume(run_id)`

**Issue:** Timeout errors
* **Solution:** Increase `NOTEBOOK_TIMEOUT_SECONDS` in Orchestrator configuration

---

## Advanced Configuration

### Custom Skills

Add domain-specific skills in `/Volumes/main/state/skills/`:
* Create `SKILL.md` with agent instructions
* Add reference documents in `references/` subdirectory
* Agents automatically load relevant skills

### Custom Agents

To add a new agent:
1. Create notebook following the pattern: `XX_Agent_Name`
2. Implement standard output contract (see `00_Shared_Utils`)
3. Add to `AGENT_NOTEBOOKS` dict in Orchestrator
4. Update pipeline flow and approval gates as needed

### Environment Variables

Customize in `00_Shared_Utils`:
```python
CATALOG = "main"  # Unity Catalog name
STATE_SCHEMA = "state"  # State tracking schema
FOUNDATION_MODEL_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"  # AI model
JIRA_BASE_URL = "https://your-domain.atlassian.net"  # Jira URL
```

---

## Best Practices

1. **Start Small:** Test with a simple Jira ticket before production use
2. **Review Approvals:** Always review artifacts at approval gates
3. **Monitor Costs:** Foundation Model API calls incur costs - track usage
4. **Version Control:** Store notebooks in Git for change tracking
5. **Error Handling:** Check error messages in `agent_runs` table
6. **Iterative Improvement:** Use validation tests to refine agent prompts

---

## Support and Resources

### Documentation
* [Databricks Foundation Model API](https://docs.databricks.com/en/machine-learning/foundation-models/index.html)
* [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
* [Jira REST API](https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/)

### Validation

Run the comprehensive validation suite:
```
/Workspace/Users/motivation4shots@gmail.com/sdlc_ai_agents/17_Comprehensive_Validation_Tests
```

---

## License and Attribution

This platform is built on Databricks infrastructure and uses:
* Databricks Foundation Model API
* Unity Catalog
* MLflow for observability
* PySpark for data processing

---

**Questions?** Review the agent notebooks for detailed implementation examples.
### SDLC Agent Flow

```text
Requirement
     │
     ▼
┌─────────────────────────────┐
│ Jira Requirement Agent      │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│ Requirement Validation      │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│ Business Understanding      │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│ Metadata Discovery           │
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│ Mapping & Data Modeling     │
└──────────────┬──────────────┘
               ▼
      ┌────────┴────────┐
      ▼                 ▼
┌──────────────┐  ┌──────────────┐
│ PySpark Agent│  │ SQL Agent    │
└──────┬───────┘  └──────┬───────┘
       └────────┬─────────┘
                ▼
       ┌──────────────────┐
       │ Data Quality     │
       └────────┬─────────┘
                ▼
       ┌──────────────────┐
       │ Code Review      │
       └────────┬─────────┘
                ▼
       ┌──────────────────┐
       │ Unit Testing     │
       └────────┬─────────┘
                ▼
       ┌──────────────────┐
       │ Integration Test │
       └────────┬─────────┘
                ▼
       ┌──────────────────┐
       │ Documentation    │
       └────────┬─────────┘
                ▼
       ┌──────────────────┐
       │ Deployment       │
       └────────┬─────────┘
                ▼
       ┌──────────────────┐
       │ Monitoring       │
       └──────────────────┘
```

---

## Features

### 🤖 Agentic AI Automation

* **Requirement Analysis** — Understand and structure software requirements
* **Requirement Validation** — Identify incomplete, ambiguous, or missing requirements
* **Business Understanding** — Extract business rules and transformation logic
* **Metadata Discovery** — Discover source and target metadata
* **Mapping Generation** — Generate source-to-target mapping specifications
* **Data Modeling** — Design target data models
* **PySpark Development** — Generate PySpark data engineering pipelines
* **SQL Development** — Generate SQL transformations and queries
* **Data Quality** — Generate automated data quality rules
* **Code Review** — Analyze code quality and identify potential issues
* **Unit Testing** — Generate unit test cases
* **Integration Testing** — Support integration and pipeline testing
* **Documentation** — Generate technical and functional documentation
* **Deployment** — Support CI/CD and deployment workflows
* **Production Monitoring** — Monitor pipelines and identify anomalies
* **Orchestration** — Coordinate multiple specialized agents

---

## Agent Architecture

The framework follows a modular multi-agent architecture.

```text
                         ┌─────────────────────┐
                         │   SDLC Orchestrator  │
                         └──────────┬──────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
       Requirement Agents    Development Agents    Quality Agents
              │                     │                     │
        ┌─────┼─────┐         ┌─────┼─────┐         ┌─────┼─────┐
        │     │     │         │     │     │         │     │     │
      Jira  Valid. Business   PySpark SQL  Model     DQ Review Test
              │                     │                     │
              └─────────────────────┼─────────────────────┘
                                    │
                                    ▼
                         Databricks / Data Platform
                                    │
                                    ▼
                         Deployment & Monitoring
```

---


## License

Copyright 2026 TR Raveendra

This project is licensed under the **Apache License, Version 2.0**.

You may use, reproduce, modify, distribute, and create derivative works from this project subject to the terms and conditions of the Apache License 2.0.

A copy of the license should be included in the repository as:

```text
LICENSE
```

The SPDX license identifier for this project is:

```text
Apache-2.0
```

See the full license text at:

https://www.apache.org/licenses/LICENSE-2.0

---

## Third-Party Software

This project may use third-party libraries, frameworks, APIs, models, and services that are distributed under their own licenses and terms.

Third-party software remains subject to its respective license.

Before redistributing this project or a derivative distribution, review the applicable third-party license and attribution requirements.

Where applicable, third-party attribution information should be maintained in:

```text
NOTICE
THIRD_PARTY_LICENSES.md
```

---

## Trademark Notice

**Databricks**, **Apache Spark**, **Jira**, and other referenced product and company names are trademarks of their respective owners.

This project is an independent open-source project and is not affiliated with, sponsored by, or endorsed by Databricks, the Apache Software Foundation, Atlassian, or any other third-party organization unless explicitly stated.

---

## Disclaimer

This project is provided on an **"AS IS"** basis, without warranties or conditions of any kind, either express or implied, to the maximum extent permitted by applicable law.

AI-generated code, SQL, configurations, documentation, and deployment instructions should be reviewed, tested, and validated by qualified engineers before being used in production environments.

The project maintainers are not responsible for production failures, data loss, security incidents, infrastructure costs, or other consequences resulting from the use of the software.

---

## Contact

For questions, discussions, feature requests, or contributions, please use the project's GitHub Issues and Discussions.

---

**SDLC AI Agents**
*Open-source Agentic AI for Software Development Lifecycle Automation*

**Apache-2.0 • Open Source • Databricks • Agentic AI • Data Engineering**

---

*Last updated: August 2026*
