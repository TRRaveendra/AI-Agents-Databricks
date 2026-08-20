# Databricks notebook source
# DBTITLE 1,Automated Testing & Token Setup
# MAGIC %md
# MAGIC # 🧪 Automated Testing & Token Setup
# MAGIC
# MAGIC ## Purpose
# MAGIC Comprehensive testing suite that:
# MAGIC 1. Sets up Databricks API tokens using secrets
# MAGIC 2. Tests all notebooks in sequence
# MAGIC 3. Fixes common errors automatically
# MAGIC 4. Provides detailed test reports
# MAGIC
# MAGIC ## Test Sequence
# MAGIC 1. **Token Setup**: Create/verify Databricks secrets
# MAGIC 2. **Configuration Test**: Verify config loads properly
# MAGIC 3. **Bronze Test**: Test data ingestion
# MAGIC 4. **Silver Test**: Test data transformation
# MAGIC 5. **Gold Test**: Test analytics layer
# MAGIC 6. **UC Functions Test**: Verify all functions work
# MAGIC 7. **Agent Tools Test**: Test Python tools
# MAGIC 8. **Agent Orchestration Test**: End-to-end agent flow
# MAGIC 9. **MLflow Deployment Test**: Verify deployment readiness
# MAGIC 10. **Integration Test**: Full pipeline test

# COMMAND ----------

# DBTITLE 1,1. Setup Databricks Secrets for API Tokens
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import getpass

def setup_secrets():
    """Set up Databricks secrets for API tokens."""
    w = WorkspaceClient()
    
    scope_name = "weather-disaster-secrets"
    
    # 1. Check if scope exists
    print("🔐 Setting up Databricks Secrets...")
    try:
        scopes = w.secrets.list_scopes()
        scope_exists = any(s.name == scope_name for s in scopes)
        
        if not scope_exists:
            print(f"   Creating secret scope: {scope_name}")
            w.secrets.create_scope(scope=scope_name)
        else:
            print(f"   ✅ Secret scope '{scope_name}' already exists")
    except Exception as e:
        print(f"   ℹ️ Cannot list/create scopes (requires admin): {e}")
        print("   You can create the scope manually via Databricks CLI:")
        print(f"     databricks secrets create-scope {scope_name}")
    
    # 2. Set up Weather API key
    print("\n🌤️ Weather API Key Setup")
    print("   Get your free API key from: https://openweathermap.org/api")
    
    api_key_name = "weather-api-key"
    
    try:
        # Check if key already exists
        existing_keys = w.secrets.list_secrets(scope=scope_name)
        key_exists = any(k.key == api_key_name for k in existing_keys)
        
        if key_exists:
            print(f"   ✅ API key '{api_key_name}' already configured")
            overwrite = input("   Do you want to update it? (y/n): ").strip().lower()
            if overwrite != 'y':
                print("   Keeping existing key")
                return True
        
        # Prompt for API key
        api_key = getpass.getpass("   Enter your OpenWeatherMap API key: ").strip()
        
        if not api_key:
            print("   ⚠️ No API key provided, skipping")
            return False
        
        # Store the secret using CLI (SDK doesn't support put_secret for string values)
        print("   Storing secret...")
        import subprocess
        result = subprocess.run(
            ["databricks", "secrets", "put-secret", scope_name, api_key_name, "--string-value", api_key],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"   ✅ API key stored successfully in {scope_name}/{api_key_name}")
            return True
        else:
            print(f"   ❌ Failed to store secret: {result.stderr}")
            print("\n   Alternative: Use Databricks CLI manually:")
            print(f"     echo '<your-api-key>' | databricks secrets put-secret {scope_name} {api_key_name}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error setting up secrets: {e}")
        print("\n   Manual Setup Instructions:")
        print(f"   1. Create scope: databricks secrets create-scope {scope_name}")
        print(f"   2. Add key: echo '<your-api-key>' | databricks secrets put-secret {scope_name} {api_key_name}")
        return False

# Run setup
setup_complete = setup_secrets()

if setup_complete:
    print("\n✅ All secrets configured successfully")
else:
    print("\n⚠️ Some secrets may need manual configuration")
    print("   Notebooks will continue with mock data if API key is not available")

# COMMAND ----------

# DBTITLE 1,2. Verify Secrets Configuration
from databricks.sdk.runtime import dbutils

def verify_secrets():
    """Verify that secrets are properly configured."""
    print("🔍 Verifying Secrets Configuration...")
    
    scope_name = "weather-disaster-secrets"
    api_key_name = "weather-api-key"
    
    try:
        # Try to read the secret
        api_key = dbutils.secrets.get(scope=scope_name, key=api_key_name)
        
        if api_key and len(api_key) > 0:
            print(f"   ✅ Secret {scope_name}/{api_key_name} is accessible")
            print(f"   ✅ API key length: {len(api_key)} characters (redacted)")
            return True
        else:
            print(f"   ❌ Secret exists but is empty")
            return False
            
    except Exception as e:
        print(f"   ❌ Cannot access secret: {e}")
        print(f"   Make sure '{scope_name}' scope exists with '{api_key_name}' key")
        return False

secrets_ok = verify_secrets()

if secrets_ok:
    print("\n✅ Secrets verification passed")
else:
    print("\n⚠️ Secrets verification failed - some tests may be skipped")

# COMMAND ----------

# DBTITLE 1,3. Automated Notebook Testing Framework
import time
from typing import Dict, List, Tuple
from datetime import datetime

class NotebookTester:
    """Automated testing framework for all notebooks."""
    
    def __init__(self):
        self.test_results = []
        self.base_path = "/Users/pysparktraining31@gmail.com/weather_monitoring_ai_agent"
        
        # Define test sequence
        self.notebooks_to_test = [
            {"name": "01_configuration", "required": True, "timeout": 120},
            {"name": "02_bronze_ingestion", "required": True, "timeout": 180},
            {"name": "03_silver_processing", "required": True, "timeout": 180},
            {"name": "04_gold_analytics", "required": True, "timeout": 180},
            {"name": "05_uc_functions", "required": True, "timeout": 120},
            {"name": "06_agent_tools", "required": False, "timeout": 120},
            {"name": "07_agent_orchestration", "required": False, "timeout": 240},
            {"name": "08_mlflow_deployment", "required": False, "timeout": 180},
            {"name": "09_integration_tests", "required": False, "timeout": 300}
        ]
    
    def test_notebook(self, notebook_info: Dict) -> Tuple[bool, str, float]:
        """Test a single notebook by running it."""
        notebook_name = notebook_info["name"]
        notebook_path = f"{self.base_path}/{notebook_name}"
        
        print(f"\n{'='*60}")
        print(f"📓 Testing: {notebook_name}")
        print(f"{'='*60}")
        
        start_time = time.time()
        
        try:
            # Run the notebook
            print(f"   Running notebook...")
            result = dbutils.notebook.run(
                notebook_path,
                timeout_seconds=notebook_info["timeout"],
                arguments={}
            )
            
            elapsed = time.time() - start_time
            
            if result and "ERROR" not in result.upper():
                print(f"   ✅ PASSED ({elapsed:.1f}s)")
                return True, "Success", elapsed
            else:
                print(f"   ❌ FAILED: {result}")
                return False, result, elapsed
                
        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)
            print(f"   ❌ ERROR: {error_msg}")
            return False, error_msg, elapsed
    
    def run_all_tests(self) -> Dict:
        """Run all notebook tests in sequence."""
        print("\n" + "="*60)
        print("🧪 AUTOMATED TESTING SUITE STARTING")
        print("="*60)
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        total_start = time.time()
        passed = 0
        failed = 0
        skipped = 0
        
        for nb_info in self.notebooks_to_test:
            success, message, elapsed = self.test_notebook(nb_info)
            
            self.test_results.append({
                "notebook": nb_info["name"],
                "success": success,
                "message": message,
                "elapsed": elapsed,
                "required": nb_info["required"]
            })
            
            if success:
                passed += 1
            else:
                failed += 1
                # If a required notebook fails, stop testing
                if nb_info["required"]:
                    print(f"\n   ⚠️ Required notebook failed, stopping test suite")
                    # Mark remaining as skipped
                    idx = self.notebooks_to_test.index(nb_info)
                    for remaining in self.notebooks_to_test[idx+1:]:
                        self.test_results.append({
                            "notebook": remaining["name"],
                            "success": False,
                            "message": "Skipped due to prerequisite failure",
                            "elapsed": 0,
                            "required": remaining["required"]
                        })
                        skipped += 1
                    break
        
        total_elapsed = time.time() - total_start
        
        # Generate report
        report = {
            "summary": {
                "total_tests": len(self.notebooks_to_test),
                "passed": passed,
                "failed": failed,
                "skipped": skipped,
                "total_time": total_elapsed
            },
            "results": self.test_results
        }
        
        return report
    
    def print_report(self, report: Dict):
        """Print formatted test report."""
        print("\n" + "="*60)
        print("📊 TEST REPORT")
        print("="*60)
        
        summary = report["summary"]
        print(f"\nTotal Tests: {summary['total_tests']}")
        print(f"✅ Passed: {summary['passed']}")
        print(f"❌ Failed: {summary['failed']}")
        print(f"⏭️ Skipped: {summary['skipped']}")
        print(f"⏱️ Total Time: {summary['total_time']:.1f}s")
        
        print("\n" + "-"*60)
        print("Detailed Results:")
        print("-"*60)
        
        for result in report["results"]:
            status = "✅" if result["success"] else "❌"
            req = "[REQUIRED]" if result["required"] else "[OPTIONAL]"
            print(f"{status} {result['notebook']:25s} {req:12s} ({result['elapsed']:.1f}s)")
            if not result["success"]:
                print(f"   └─ {result['message'][:80]}..." if len(result['message']) > 80 else f"   └─ {result['message']}")
        
        print("\n" + "="*60)
        if summary['failed'] == 0:
            print("🎉 ALL TESTS PASSED!")
        else:
            print(f"⚠️ {summary['failed']} TEST(S) FAILED")
        print("="*60)

print("✅ Testing framework loaded")

# COMMAND ----------

# DBTITLE 1,4. Run Complete Test Suite
# Initialize tester
tester = NotebookTester()

# Run all tests
test_report = tester.run_all_tests()

# Print report
tester.print_report(test_report)

# Store report for analysis
import json
print("\n💾 Saving test report...")
report_json = json.dumps(test_report, indent=2)
print("\n" + report_json)

# COMMAND ----------

# DBTITLE 1,5. Common Error Fixes
def apply_common_fixes():
    """Apply common fixes for known issues."""
    print("🔧 Applying common fixes...\n")
    
    # Fix 1: Ensure catalog and schema exist
    print("1. Verifying Unity Catalog structure...")
    try:
        spark.sql("CREATE CATALOG IF NOT EXISTS weather_disaster")
        spark.sql("CREATE SCHEMA IF NOT EXISTS weather_disaster.weather_data")
        print("   ✅ Catalog and schema verified")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Fix 2: Grant permissions (if needed)
    print("\n2. Checking permissions...")
    try:
        # Try to access the catalog
        spark.sql("USE CATALOG weather_disaster")
        spark.sql("USE SCHEMA weather_data")
        print("   ✅ Permissions OK")
    except Exception as e:
        print(f"   ⚠️ Permission issue: {e}")
        print("   You may need admin to grant access")
    
    # Fix 3: Clear any invalid widgets
    print("\n3. Clearing notebook widgets...")
    try:
        dbutils.widgets.removeAll()
        print("   ✅ Widgets cleared")
    except Exception as e:
        print(f"   ℹ️ No widgets to clear")
    
    print("\n✅ Common fixes applied")

apply_common_fixes()

# COMMAND ----------

# DBTITLE 1,6. Manual Fix Helper
def get_fix_suggestions(test_report: Dict):
    """Provide fix suggestions for failed tests."""
    failed_tests = [r for r in test_report["results"] if not r["success"]]
    
    if not failed_tests:
        print("🎉 No failures detected, no fixes needed!")
        return
    
    print("\n" + "="*60)
    print("🔧 FIX SUGGESTIONS")
    print("="*60)
    
    for test in failed_tests:
        print(f"\n❌ {test['notebook']}")
        print(f"   Error: {test['message'][:200]}")
        
        # Provide context-specific suggestions
        if "01_configuration" in test['notebook']:
            print("   Suggestions:")
            print("   - Check if secrets are configured properly")
            print("   - Verify databricks.sdk is installed")
            print("   - Run: %pip install --upgrade databricks-sdk")
        
        elif "bronze" in test['notebook'].lower():
            print("   Suggestions:")
            print("   - Verify API key is in secrets")
            print("   - Check network connectivity to OpenWeatherMap")
            print("   - Verify catalog/schema exists")
        
        elif "silver" in test['notebook'].lower() or "gold" in test['notebook'].lower():
            print("   Suggestions:")
            print("   - Ensure Bronze layer has data")
            print("   - Check table permissions")
            print("   - Verify schema matches expected structure")
        
        elif "uc_functions" in test['notebook'].lower():
            print("   Suggestions:")
            print("   - Check schema reference: weather_disaster.weather_data")
            print("   - Verify tables exist before creating functions")
            print("   - Ensure proper SQL syntax for MAKE_INTERVAL")
        
        elif "agent" in test['notebook'].lower():
            print("   Suggestions:")
            print("   - Install: %pip install langgraph langchain langchain-databricks")
            print("   - Verify UC functions are created")
            print("   - Check LLM endpoint connectivity")
        
        print("")

# Show fix suggestions
get_fix_suggestions(test_report)

# COMMAND ----------

# DBTITLE 1,Next Steps
# MAGIC %md
# MAGIC ## ✅ Next Steps
# MAGIC
# MAGIC After running this notebook:
# MAGIC
# MAGIC 1. **Review test report** - Check which notebooks passed/failed
# MAGIC 2. **Apply fixes** - Use the suggestions provided
# MAGIC 3. **Re-run failed notebooks** - Test individually
# MAGIC 4. **Update configurations** - Adjust thresholds, cities, etc. in 01_configuration
# MAGIC 5. **Schedule ingestion** - Set up recurring job for data pipeline
# MAGIC 6. **Deploy agent** - Use 08_mlflow_deployment for production
# MAGIC
# MAGIC ## 🔗 API Token Usage
# MAGIC
# MAGIC Your OpenWeatherMap API token is now stored in Databricks Secrets and can be accessed via:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk.runtime import dbutils
# MAGIC api_key = dbutils.secrets.get(scope="weather-disaster-secrets", key="weather-api-key")
# MAGIC ```
# MAGIC
# MAGIC ## 📞 Support
# MAGIC
# MAGIC If issues persist:
# MAGIC - Check Databricks workspace logs
# MAGIC - Verify compute cluster state
# MAGIC - Review Unity Catalog permissions
# MAGIC - Confirm API quotas not exceeded

# COMMAND ----------

