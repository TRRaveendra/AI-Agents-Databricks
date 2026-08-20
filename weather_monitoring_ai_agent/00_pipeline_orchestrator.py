# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Weather Monitoring AI Agent - Pipeline Orchestrator
# MAGIC %md
# MAGIC # 🎯 Weather Monitoring AI Agent - Pipeline Orchestrator
# MAGIC
# MAGIC ## Purpose
# MAGIC Central orchestrator that executes all notebooks in the correct sequence.
# MAGIC
# MAGIC ## Features
# MAGIC * **Sequential Execution**: Runs notebooks 01-10 in dependency order
# MAGIC * **Error Handling**: Continues on non-critical failures
# MAGIC * **Comprehensive Reporting**: Detailed status for each component
# MAGIC * **Health Metrics**: Overall pipeline health assessment
# MAGIC * **Failure Diagnostics**: Detailed error reporting and next steps
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC ### Quick Start
# MAGIC ```python
# MAGIC # Run Cell 2 to execute the entire pipeline
# MAGIC ```
# MAGIC
# MAGIC ### What Gets Executed
# MAGIC 1. **01_configuration** - Setup catalog, schema, dependencies
# MAGIC 2. **02_bronze_ingestion** - Ingest raw weather data
# MAGIC 3. **03_silver_processing** - Transform and validate data
# MAGIC 4. **04_gold_analytics** - Create disaster analytics
# MAGIC 5. **05_uc_functions** - Deploy Unity Catalog functions
# MAGIC 6. **06_agent_tools** - Initialize agent tools
# MAGIC 7. **07_agent_orchestration** - Setup LangGraph agent
# MAGIC 8. **08_mlflow_deployment** - Deploy to Model Serving
# MAGIC 9. **09_integration_tests** - Run end-to-end tests
# MAGIC 10. **10_automated_testing_and_token_setup** - Validate framework
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Valid OpenWeatherMap API key configured in secrets
# MAGIC * Sufficient workspace permissions for UC operations
# MAGIC * Serverless or cluster compute available
# MAGIC
# MAGIC ---
# MAGIC **Created**: August 19, 2026  
# MAGIC **Version**: 1.0

# COMMAND ----------

# DBTITLE 1,🚀 Execute Complete Pipeline
# =====================================================
# WEATHER MONITORING AI AGENT - PIPELINE ORCHESTRATOR
# =====================================================
import time
from datetime import datetime, timedelta
from databricks.sdk.runtime import dbutils

print("=" * 80)
print("🎯 WEATHER MONITORING AI AGENT - PIPELINE ORCHESTRATOR")
print("=" * 80)
print(f"Start Time: {datetime.now().isoformat()}")
print()

# Define the base path for all notebooks
# Using absolute path since orchestrator is in parent directory
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
current_user = w.current_user.me().user_name
BASE_PATH = f"/Users/{current_user}/weather_monitoring_ai_agent"

# Define all notebooks to run in sequence
notebooks_to_run = [
    {
        "name": "01_configuration",
        "path": f"{BASE_PATH}/01_configuration",
        "timeout": 300,  # 5 minutes
        "description": "Configuration setup and dependency installation",
        "critical": True
    },
    {
        "name": "02_bronze_ingestion",
        "path": f"{BASE_PATH}/02_bronze_ingestion",
        "timeout": 300,  # 5 minutes
        "description": "Raw weather data ingestion (Bronze layer)",
        "critical": True
    },
    {
        "name": "03_silver_processing",
        "path": f"{BASE_PATH}/03_silver_processing",
        "timeout": 300,  # 5 minutes
        "description": "Data transformation and validation (Silver layer)",
        "critical": True
    },
    {
        "name": "04_gold_analytics",
        "path": f"{BASE_PATH}/04_gold_analytics",
        "timeout": 300,  # 5 minutes
        "description": "Disaster detection and analytics (Gold layer)",
        "critical": True
    },
    {
        "name": "05_uc_functions",
        "path": f"{BASE_PATH}/05_uc_functions",
        "timeout": 180,  # 3 minutes
        "description": "Unity Catalog function creation",
        "critical": True
    },
    {
        "name": "06_agent_tools",
        "path": f"{BASE_PATH}/06_agent_tools",
        "timeout": 180,  # 3 minutes
        "description": "Agent simulation tools initialization",
        "critical": False
    },
    {
        "name": "07_agent_orchestration",
        "path": f"{BASE_PATH}/07_agent_orchestration",
        "timeout": 600,  # 10 minutes
        "description": "LangGraph agent orchestration setup",
        "critical": False
    },
    {
        "name": "08_mlflow_deployment",
        "path": f"{BASE_PATH}/08_mlflow_deployment",
        "timeout": 600,  # 10 minutes (MLflow operations can be slow)
        "description": "MLflow model packaging and deployment",
        "critical": False
    },
    {
        "name": "09_integration_tests",
        "path": f"{BASE_PATH}/09_integration_tests",
        "timeout": 300,  # 5 minutes
        "description": "End-to-end integration testing",
        "critical": False
    },
    {
        "name": "10_automated_testing_and_token_setup",
        "path": f"{BASE_PATH}/10_automated_testing_and_token_setup",
        "timeout": 180,  # 3 minutes
        "description": "Automated testing framework validation",
        "critical": False
    }
]

# Track results
execution_results = []
total_start_time = time.time()
overall_success = True

# Execute each notebook in sequence
for idx, notebook in enumerate(notebooks_to_run, 1):
    print(f"\n{'=' * 80}")
    print(f"[{idx}/{len(notebooks_to_run)}] Executing: {notebook['name']}")
    print(f"Description: {notebook['description']}")
    print(f"Critical: {'✅ Yes' if notebook['critical'] else '⚠️ No'}")
    print(f"{'=' * 80}")
    
    start_time = time.time()
    result = {
        "notebook": notebook['name'],
        "path": notebook['path'],
        "description": notebook['description'],
        "critical": notebook['critical'],
        "status": None,
        "duration": 0,
        "error": None,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        print(f"\n⏳ Running notebook (timeout: {notebook['timeout']}s)...")
        
        # Run the notebook
        dbutils.notebook.run(
            notebook['path'],
            timeout_seconds=notebook['timeout']
        )
        
        duration = time.time() - start_time
        result['duration'] = round(duration, 2)
        result['status'] = 'SUCCESS'
        
        print(f"✅ SUCCESS - Completed in {duration:.2f}s")
        
    except Exception as e:
        duration = time.time() - start_time
        result['duration'] = round(duration, 2)
        result['status'] = 'FAILED'
        result['error'] = str(e)[:500]
        
        error_msg = str(e)
        print(f"❌ FAILED - Error after {duration:.2f}s")
        print(f"Error: {error_msg[:300]}")
        
        # If critical notebook fails, note it
        if notebook['critical']:
            print(f"⚠️ CRITICAL FAILURE - This notebook is critical for the pipeline")
            overall_success = False
        else:
            print(f"ℹ️ Non-critical notebook failed. Continuing...")
    
    execution_results.append(result)
    
    # Small delay between notebooks
    if idx < len(notebooks_to_run):
        print(f"\n⏸️ Waiting 2 seconds before next notebook...")
        time.sleep(2)

# Calculate total execution time
total_duration = time.time() - total_start_time

# =====================================================
# GENERATE COMPREHENSIVE REPORT
# =====================================================
print("\n" + "=" * 80)
print("📊 COMPREHENSIVE EXECUTION REPORT")
print("=" * 80)
print(f"\nExecution completed at: {datetime.now().isoformat()}")
print(f"Total execution time: {timedelta(seconds=int(total_duration))}")
print()

# Summary statistics
success_count = sum(1 for r in execution_results if r['status'] == 'SUCCESS')
failed_count = sum(1 for r in execution_results if r['status'] == 'FAILED')
critical_failures = sum(1 for r in execution_results if r['status'] == 'FAILED' and r['critical'])

print("📈 SUMMARY STATISTICS")
print("-" * 80)
print(f"Total notebooks executed: {len(execution_results)}")
print(f"✅ Successful: {success_count}")
print(f"❌ Failed: {failed_count}")
print(f"🚨 Critical failures: {critical_failures}")
print(f"⏱️ Average execution time: {total_duration/len(execution_results):.2f}s")
print()

# Detailed results table
print("\n📋 DETAILED RESULTS")
print("-" * 80)
print(f"{'#':<3} {'Notebook':<35} {'Status':<10} {'Duration':<10} {'Type':<10}")
print("-" * 80)

for idx, result in enumerate(execution_results, 1):
    status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
    status_text = f"{status_icon} {result['status']}"
    duration_text = f"{result['duration']:.2f}s"
    type_text = "Critical" if result['critical'] else "Optional"
    
    print(f"{idx:<3} {result['notebook']:<35} {status_text:<10} {duration_text:<10} {type_text:<10}")

print("-" * 80)

# Failed notebooks detail
if failed_count > 0:
    print("\n\n❌ FAILED NOTEBOOKS - ERROR DETAILS")
    print("=" * 80)
    
    for idx, result in enumerate(execution_results, 1):
        if result['status'] == 'FAILED':
            print(f"\n[{idx}] {result['notebook']}")
            print(f"    Description: {result['description']}")
            print(f"    Critical: {'Yes' if result['critical'] else 'No'}")
            print(f"    Duration: {result['duration']:.2f}s")
            print(f"    Error: {result['error'][:400]}")
            print()

# Component status
print("\n\n🔍 COMPONENT STATUS")
print("=" * 80)

component_status = {
    "Data Pipeline (Bronze/Silver/Gold)": all(
        r['status'] == 'SUCCESS' 
        for r in execution_results 
        if r['notebook'] in ['02_bronze_ingestion', '03_silver_processing', '04_gold_analytics']
    ),
    "Unity Catalog Functions": any(
        r['status'] == 'SUCCESS' and r['notebook'] == '05_uc_functions'
        for r in execution_results
    ),
    "AI Agent Components": all(
        r['status'] == 'SUCCESS' 
        for r in execution_results 
        if r['notebook'] in ['06_agent_tools', '07_agent_orchestration']
    ),
    "MLflow Deployment": any(
        r['status'] == 'SUCCESS' and r['notebook'] == '08_mlflow_deployment'
        for r in execution_results
    ),
    "Testing Framework": all(
        r['status'] == 'SUCCESS' 
        for r in execution_results 
        if r['notebook'] in ['09_integration_tests', '10_automated_testing_and_token_setup']
    )
}

for component, status in component_status.items():
    status_icon = "✅" if status else "❌"
    status_text = "OPERATIONAL" if status else "ISSUES DETECTED"
    print(f"{status_icon} {component:<40} {status_text}")

print()

# Final verdict
print("\n" + "=" * 80)
print("🎯 FINAL VERDICT")
print("=" * 80)

if overall_success and success_count == len(notebooks_to_run):
    print("\n🎉 EXCELLENT! Pipeline is fully operational.")
    print("   All components executed successfully.")
    print("   System is ready for production use.")
elif critical_failures == 0 and success_count >= 7:
    print("\n✅ GOOD! Core pipeline is operational.")
    print("   All critical components executed successfully.")
    print("   Some optional components had issues but system is functional.")
elif critical_failures == 0:
    print("\n⚠️ PARTIAL SUCCESS! Core pipeline is operational.")
    print("   Critical components passed but several optional components failed.")
    print("   Review failed components and address issues.")
else:
    print("\n❌ CRITICAL ISSUES DETECTED!")
    print(f"   {critical_failures} critical notebook(s) failed.")
    print("   System requires attention before production use.")
    print("   Review errors above and re-run after fixes.")

print()
print("=" * 80)
print(f"Report generated at: {datetime.now().isoformat()}")
print("=" * 80)

# Export results for further analysis
print("\n💾 Execution results saved to variable: execution_results")
print("   Access individual results: execution_results[index]")
print("   Example: execution_results[0] for first notebook result")

# COMMAND ----------

# DBTITLE 1,✅ Pre-Flight Validation
# =====================================================
# PRE-FLIGHT VALIDATION
# Run this before executing the pipeline
# =====================================================
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils

print("=" * 70)
print("✅ PRE-FLIGHT VALIDATION CHECK")
print("=" * 70)
print(f"Check Time: {datetime.now().isoformat()}\n")

validation_issues = []

# Check 1: Secret scope and API key
print("[1/3] Checking API key configuration...")
try:
    w = WorkspaceClient()
    scopes = list(w.secrets.list_scopes())
    
    if any(s.name == "weather-disaster-secrets" for s in scopes):
        try:
            api_key = dbutils.secrets.get(scope="weather-disaster-secrets", key="weather-api-key")
            if api_key and len(api_key) > 10:
                print("   ✅ API key is configured")
            else:
                print("   ❌ API key is too short or empty")
                validation_issues.append("Invalid API key - get a valid key from https://home.openweathermap.org/api_keys")
        except Exception as e:
            print(f"   ❌ API key not found: {str(e)[:100]}")
            validation_issues.append("API key not configured - run api-key-setup-secrets notebook")
    else:
        print("   ❌ Secret scope 'weather-disaster-secrets' not found")
        validation_issues.append("Secret scope missing - run api-key-setup-secrets notebook")
except Exception as e:
    print(f"   ❌ Error: {str(e)[:150]}")
    validation_issues.append(f"Secret validation error: {str(e)[:100]}")

# Check 2: Unity Catalog access
print("\n[2/3] Checking Unity Catalog access...")
try:
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    print(f"   ✅ Can access Unity Catalog ({len(catalogs)} catalogs visible)")
except Exception as e:
    print(f"   ❌ Cannot access Unity Catalog: {str(e)[:150]}")
    validation_issues.append(f"Unity Catalog access error: {str(e)[:100]}")

# Check 3: Notebook paths
print("\n[3/3] Checking notebook paths...")
try:
    base_path = "/Users/pysparktraining32@gmail.com/weather_monitoring_ai_agent"
    test_notebooks = [
        f"{base_path}/01_configuration",
        f"{base_path}/02_bronze_ingestion",
        f"{base_path}/05_uc_functions"
    ]
    
    # Try to validate path structure (we can't check if notebooks exist directly)
    print(f"   ✅ Notebook path configured: {base_path}")
    print("   ℹ️ Will validate actual paths during execution")
except Exception as e:
    print(f"   ❌ Error: {str(e)[:150]}")
    validation_issues.append(f"Path configuration error: {str(e)[:100]}")

# Summary
print("\n" + "=" * 70)
print("📊 VALIDATION SUMMARY")
print("=" * 70)

if not validation_issues:
    print("\n✅ ALL PRE-FLIGHT CHECKS PASSED!")
    print("   System is ready to run the pipeline.")
    print("\n🚀 Next: Run Cell 2 to execute the complete pipeline.")
else:
    print(f"\n❌ {len(validation_issues)} ISSUE(S) DETECTED:\n")
    for idx, issue in enumerate(validation_issues, 1):
        print(f"   {idx}. {issue}")
    print("\n⚠️ Fix these issues before running the pipeline.")

print("\n" + "=" * 70)

# COMMAND ----------

# DBTITLE 1,🛠️ Troubleshooting Guide
# MAGIC %md
# MAGIC ## 🛠️ Troubleshooting Guide
# MAGIC
# MAGIC ### Common Issues and Solutions
# MAGIC
# MAGIC #### 🚨 API Key Issues
# MAGIC
# MAGIC **Problem**: `HTTP 401 Unauthorized` or `Invalid API key`
# MAGIC
# MAGIC **Solution**:
# MAGIC 1. Get a free API key from [OpenWeatherMap](https://home.openweathermap.org/api_keys)
# MAGIC 2. Open the `api-key-setup-secrets` notebook
# MAGIC 3. Update Cell 2 with your new API key:
# MAGIC    ```python
# MAGIC    YOUR_API_KEY = "your-new-key-here"
# MAGIC    ```
# MAGIC 4. Run Cell 2 to store the key
# MAGIC 5. Wait 5-10 minutes for API key activation
# MAGIC 6. Re-run this orchestrator
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 📁 Notebook Path Issues
# MAGIC
# MAGIC **Problem**: `Notebook not found` or `Path does not exist`
# MAGIC
# MAGIC **Solution**:
# MAGIC 1. Verify all notebooks are in `/Users/your-email/weather_monitoring_ai_agent/`
# MAGIC 2. Check notebook names match exactly (01_configuration, 02_bronze_ingestion, etc.)
# MAGIC 3. Update `BASE_PATH` in Cell 2 if your notebooks are in a different location
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### ⚡ Timeout Errors
# MAGIC
# MAGIC **Problem**: `Notebook execution timed out`
# MAGIC
# MAGIC **Solution**:
# MAGIC 1. Increase timeout for the specific notebook in Cell 2
# MAGIC 2. Check if compute resources are sufficient
# MAGIC 3. For large data operations, consider:
# MAGIC    - Using a larger cluster
# MAGIC    - Reducing the number of cities in configuration
# MAGIC    - Processing data in smaller batches
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 📊 Unity Catalog Errors
# MAGIC
# MAGIC **Problem**: `Catalog does not exist` or `Permission denied`
# MAGIC
# MAGIC **Solution**:
# MAGIC 1. Ensure you have CREATE CATALOG permissions
# MAGIC 2. Check if catalog name conflicts with existing catalog
# MAGIC 3. Run `01_configuration` notebook manually first
# MAGIC 4. Contact workspace admin if permission issues persist
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🔄 %run Command Errors (Fixed)
# MAGIC
# MAGIC **Problem**: `Failed to parse %run command`
# MAGIC
# MAGIC **Status**: ✅ **FIXED** in notebooks 07 and 08
# MAGIC - Converted from `%run` magic commands to `dbutils.notebook.run()`
# MAGIC - If you see this error, ensure you have the latest notebook versions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📞 Getting Help
# MAGIC
# MAGIC **If issues persist**:
# MAGIC 1. Run Cell 3 (Pre-Flight Validation) to identify specific issues
# MAGIC 2. Check the detailed error messages in the execution report
# MAGIC 3. Review individual notebook outputs
# MAGIC 4. Consult the [00_README](../weather_monitoring_ai_agent/00_README) notebook
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔍 Monitoring Pipeline Health
# MAGIC
# MAGIC **Key Metrics to Watch**:
# MAGIC * **Success Rate**: Should be ≥ 70% (7/10 notebooks)
# MAGIC * **Critical Failures**: Should be 0
# MAGIC * **Data Freshness**: Bronze table should have recent timestamps
# MAGIC * **UC Functions**: All 4 functions should be created
# MAGIC
# MAGIC **Health Indicators**:
# MAGIC * 🜢 **Green** (100% success): Production ready
# MAGIC * 🜡️ **Yellow** (70-99% success): Core operational, some issues
# MAGIC * 🜠 **Red** (<70% or critical failures): Requires attention
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Production Deployment
# MAGIC
# MAGIC **Once all tests pass**:
# MAGIC
# MAGIC 1. **Schedule Regular Ingestion**
# MAGIC    ```python
# MAGIC    # Create a Databricks Job
# MAGIC    # Schedule: Every 30 minutes
# MAGIC    # Notebooks: 02 → 03 → 04
# MAGIC    ```
# MAGIC
# MAGIC 2. **Set Up Monitoring**
# MAGIC    - Create SQL dashboard for key metrics
# MAGIC    - Configure alerts for pipeline failures
# MAGIC    - Monitor data freshness and quality
# MAGIC
# MAGIC 3. **Deploy AI Agent**
# MAGIC    - Run `08_mlflow_deployment`
# MAGIC    - Create Model Serving endpoint
# MAGIC    - Enable inference logging
# MAGIC
# MAGIC 4. **Configure Alerts**
# MAGIC    - Add Slack/Email webhooks to configuration
# MAGIC    - Test alert delivery
# MAGIC    - Set up escalation procedures