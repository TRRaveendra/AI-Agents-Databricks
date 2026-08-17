# SDLC AI Agents

AI-powered agents for automating and enhancing the Software Development Life Cycle (SDLC) in Databricks.

## Overview

This project provides intelligent agents that assist with various stages of the software development lifecycle, from planning and development to testing, deployment, and monitoring.

## Features

* **Automated Code Generation**: Generate boilerplate code, utilities, and data processing pipelines
* **Code Review & Quality Analysis**: Intelligent code review and quality metrics
* **Testing Automation**: Automated test generation and execution
* **Documentation Generation**: Auto-generate and maintain project documentation
* **CI/CD Integration**: Streamline deployment workflows
* **Monitoring & Alerts**: Intelligent monitoring and anomaly detection

## Project Structure

```
sdlc_ai_agents/
├── README.md                              # This file
├── SETUP.md                               # Setup instructions
├── 00_Shared_Utils                        # Shared utilities & state management
├── 01_Jira_Requirement_Agent              # Fetch requirements from Jira
├── 02_Requirement_Validation_Agent        # Validate requirements completeness
├── 03_Business_Understanding_Agent        # Analyze business logic & rules
├── 04_Metadata_Discovery_Agent            # Discover source/target metadata
├── 05_Mapping_Document_Agent              # Generate mapping specifications
├── 06_Data_Model_Agent                    # Design target data models
├── 07_PySpark_Development_Agent           # Generate PySpark ETL code
├── 08_SQL_Agent                           # Generate SQL transformations
├── 09_Data_Quality_Agent                  # Generate DQ checks
├── 10_Code_Review_Agent                   # Automated code review
├── 11_Unit_Test_Agent                     # Generate unit tests
├── 12_Testing_Agent                       # Execute integration tests
├── 13_Documentation_Agent                 # Generate documentation
├── 14_Deployment_Agent                    # Deploy to production
├── 15_Production_Monitoring_Agent         # Monitor production pipelines
├── 16_Orchestrator                        # Main orchestration engine
└── jira-setup-secrets                     # Jira integration setup
```

## Getting Started

### Prerequisites

* Databricks workspace with compute access
* Python 3.9+
* Required libraries (install via %pip):
  ```python
  %pip install databricks-sdk mlflow
  ```

### Setup

1. Clone or import this project into your Databricks workspace
2. Review the `00_Shared_Utils` notebook for common utilities
3. Configure your environment variables and secrets as needed

## Usage

### Running Agents

Each agent is implemented as a separate notebook. To use an agent:

1. Open the corresponding notebook
2. Configure parameters in the widget cells
3. Run all cells or specific agent functions

### Example

```python
# Import shared utilities
from 00_Shared_Utils import *

# Run agent functions
result = agent_function(parameters)
```

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Create a new branch for your feature
2. Write clear, documented code
3. Add tests where applicable
4. Submit a pull request with a clear description

## Best Practices

* Keep shared utilities in `00_Shared_Utils`
* Document all functions with docstrings
* Use notebook parameters for configuration
* Follow PEP 8 style guidelines
* Version control all notebooks

## Troubleshooting

### Common Issues

* **Import errors**: Ensure all required libraries are installed
* **Authentication errors**: Check your Databricks credentials and secrets
* **Performance issues**: Consider using optimized compute or caching strategies

## Documentation

### Project Documentation
* **[AGENT_SUMMARY.md](AGENT_SUMMARY.md)**: Quick one-page summary of all agents (START HERE)
* **[AGENTS_REFERENCE.md](AGENTS_REFERENCE.md)**: Complete reference guide for all 16 agents (50+ pages)
  - Detailed agent descriptions and purposes
  - Input/output specifications
  - Model tier configurations
  - Error handling patterns
  - Code examples and best practices
* **[SETUP.md](SETUP.md)**: Detailed setup and configuration instructions
* **README.md** (this file): Quick start and overview

### External Resources
* [Databricks Documentation](https://docs.databricks.com)
* [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
* [Databricks SDK](https://docs.databricks.com/dev-tools/sdk-python.html)
* [Foundation Model API](https://docs.databricks.com/en/machine-learning/foundation-models/index.html)

## License

This project is for internal use within your organization.

## Contact

For questions or support, contact the project maintainers.

---

*Last updated: August 2026*
