# SDLC AI Agents

**Open-source Agentic AI framework for automating and enhancing the Software Development Life Cycle (SDLC), with first-class Databricks and data engineering integrations.**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Enabled-red.svg)](https://www.databricks.com/)

> **License:** Apache License 2.0
> **SPDX Identifier:** Apache-2.0

---

## Overview

**SDLC AI Agents** is an open-source Agentic AI framework designed to automate and enhance different stages of the Software Development Life Cycle.

The framework provides specialized AI agents for requirements analysis, business understanding, metadata discovery, data modeling, PySpark and SQL development, data quality, code review, testing, documentation, deployment, and production monitoring.

The project is designed primarily for **Databricks and modern data engineering environments**, while keeping the agent architecture extensible for additional platforms and integrations.

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

## Project Structure

```text
sdlc_ai_agents/
│
├── README.md
├── LICENSE
├── NOTICE
├── SETUP.md
├── CONTRIBUTING.md
├── SECURITY.md
│
├── 00_Shared_Utils
│   └── Shared utilities and state management
│
├── 01_Jira_Requirement_Agent
│   └── Fetch and analyze requirements from Jira
│
├── 02_Requirement_Validation_Agent
│   └── Validate requirement completeness and quality
│
├── 03_Business_Understanding_Agent
│   └── Analyze business logic and business rules
│
├── 04_Metadata_Discovery_Agent
│   └── Discover source and target metadata
│
├── 05_Mapping_Document_Agent
│   └── Generate source-to-target mappings
│
├── 06_Data_Model_Agent
│   └── Design target data models
│
├── 07_PySpark_Development_Agent
│   └── Generate PySpark ETL/data engineering code
│
├── 08_SQL_Agent
│   └── Generate SQL transformations
│
├── 09_Data_Quality_Agent
│   └── Generate data quality rules and checks
│
├── 10_Code_Review_Agent
│   └── Automated code review and quality analysis
│
├── 11_Unit_Test_Agent
│   └── Generate unit test cases
│
├── 12_Testing_Agent
│   └── Execute and validate integration tests
│
├── 13_Documentation_Agent
│   └── Generate technical documentation
│
├── 14_Deployment_Agent
│   └── Support deployment and CI/CD workflows
│
├── 15_Production_Monitoring_Agent
│   └── Monitor production pipelines
│
├── 16_Orchestrator
│   └── Main multi-agent orchestration engine
│
└── jira-setup-secrets
    └── Jira integration configuration
```

---

## Getting Started

### Prerequisites

* Databricks workspace with appropriate compute access
* Python 3.9+
* Databricks SDK
* MLflow
* Appropriate permissions for the Databricks resources used by the agents
* Jira access if using the Jira integration

### Required Libraries

Install the required Python packages in your Databricks environment:

```python
%pip install databricks-sdk mlflow
```

Additional dependencies may be required depending on the agents and integrations you enable.

---

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>

cd sdlc-ai-agents
```

### 2. Configure Databricks

Configure your Databricks authentication using an appropriate supported authentication mechanism.

Do **not** hard-code credentials or access tokens in notebooks or source code.

### 3. Configure Secrets

Use your organization's approved secret-management mechanism for sensitive configuration such as:

```text
DATABRICKS_HOST
DATABRICKS_TOKEN
JIRA_URL
JIRA_USER
JIRA_TOKEN
MODEL_ENDPOINT
```

A `.env.example` file can be provided as a template.

### 4. Import into Databricks

Import or clone the project into your Databricks workspace and configure the required workspace resources.

### 5. Configure Agents

Review the configuration for each agent before execution.

---

## Usage

Each agent is designed as a modular component that can be executed independently or through the central orchestrator.

### Running an Individual Agent

1. Open the required agent.
2. Configure the required parameters.
3. Configure authentication and integrations.
4. Execute the agent.
5. Review the generated output.
6. Pass the output to the next stage when required.

### Example

```python
# Example conceptual usage

agent = PySparkDevelopmentAgent()

result = agent.run(
    source_metadata=source_metadata,
    target_metadata=target_metadata,
    mapping_specification=mapping_specification,
    business_rules=business_rules
)

print(result)
```

> **Note:** The exact implementation and import paths may change as the framework evolves.

---

## Multi-Agent Orchestration

The orchestrator coordinates multiple agents to implement an end-to-end SDLC workflow.

```text
Jira
 │
 ▼
Requirement Agent
 │
 ▼
Requirement Validation
 │
 ▼
Business Understanding
 │
 ▼
Metadata Discovery
 │
 ▼
Mapping
 │
 ▼
Data Model
 │
 ├───────────────┐
 ▼               ▼
PySpark          SQL
 │               │
 └───────┬───────┘
         ▼
   Data Quality
         │
         ▼
    Code Review
         │
         ▼
    Unit Testing
         │
         ▼
 Integration Test
         │
         ▼
 Documentation
         │
         ▼
 Deployment
         │
         ▼
 Production Monitoring
```

---

## Configuration

Agents should be configurable rather than tightly coupled to a single model, workspace, or environment.

Example configuration:

```yaml
agents:

  requirement_validation:
    enabled: true

  business_understanding:
    enabled: true

  metadata_discovery:
    enabled: true

  mapping:
    enabled: true

  data_model:
    enabled: true

  pyspark_development:
    enabled: true

  sql_development:
    enabled: true

  data_quality:
    enabled: true

  code_review:
    enabled: true

  unit_testing:
    enabled: true

  documentation:
    enabled: true

  deployment:
    enabled: true

  monitoring:
    enabled: true
```

---

## Security

Security is a core consideration when deploying AI agents in enterprise environments.

### Never commit secrets

Do not commit:

```text
Passwords
API keys
Access tokens
Databricks PATs
OAuth secrets
Jira credentials
Cloud credentials
Private keys
Connection strings containing credentials
```

Use appropriate enterprise secret-management and authentication mechanisms instead.

### Recommended Practices

* Use OAuth or service principals where appropriate
* Use Databricks Secrets or approved enterprise secret management
* Apply least-privilege access
* Restrict agent permissions
* Audit agent activity
* Protect generated source code and metadata
* Validate generated code before production deployment

For security vulnerabilities, please follow the project's security reporting process described in `SECURITY.md`.

---

## Best Practices

* Keep agents modular and independently testable
* Keep shared utilities in `00_Shared_Utils`
* Document public functions and classes
* Use meaningful agent and function names
* Follow PEP 8 guidelines
* Add tests for new functionality
* Avoid hard-coded credentials
* Use configuration rather than hard-coded environment-specific values
* Validate AI-generated code before production deployment
* Version-control notebooks and source code
* Keep model configuration separate from business logic
* Maintain clear agent input/output contracts

---

## Documentation

### Project Documentation

* **[AGENT_SUMMARY.md](AGENT_SUMMARY.md)** — Quick overview of all agents
* **[AGENTS_REFERENCE.md](AGENTS_REFERENCE.md)** — Detailed reference for the agents
* **[SETUP.md](SETUP.md)** — Setup and configuration instructions
* **[CONTRIBUTING.md](CONTRIBUTING.md)** — Contribution guidelines
* **[SECURITY.md](SECURITY.md)** — Security reporting and practices

### External Resources

* [Databricks Documentation](https://docs.databricks.com/)
* [MLflow Documentation](https://mlflow.org/docs/latest/)
* [Databricks SDK Documentation](https://docs.databricks.com/dev-tools/sdk-python.html)
* [Databricks Foundation Model Documentation](https://docs.databricks.com/en/machine-learning/foundation-models/index.html)

---

## Contributing

Contributions are welcome.

Before submitting a contribution:

1. Create a feature branch.
2. Implement the change.
3. Add or update tests where appropriate.
4. Update documentation where required.
5. Verify that no secrets or sensitive information are included.
6. Submit a pull request with a clear description.
7. Explain the motivation and impact of the change.

Please review `CONTRIBUTING.md` for the complete contribution process.

---

## Roadmap

Potential future capabilities include:

```text
Multi-model support
        │
        ├── Databricks Models
        ├── OpenAI
        ├── Anthropic
        └── Other LLM providers
                │
                ▼
        Agent Skill Framework
                │
                ▼
        Tool / MCP Integrations
                │
                ├── Jira
                ├── GitHub
                ├── GitLab
                ├── Azure DevOps
                └── Databricks
                │
                ▼
        Enterprise Agent Governance
                │
                ├── RBAC
                ├── Audit
                ├── Observability
                └── Cost Management
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
