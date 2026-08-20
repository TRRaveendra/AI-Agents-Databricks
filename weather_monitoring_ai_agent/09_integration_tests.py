# Databricks notebook source
# DBTITLE 1,Integration Tests
# MAGIC %md
# MAGIC # ✅ Integration Tests - End-to-End Validation
# MAGIC
# MAGIC ## Purpose
# MAGIC Comprehensive test suite for the entire weather disaster agent system.
# MAGIC
# MAGIC ## Tests
# MAGIC 1. Configuration loading
# MAGIC 2. Data pipeline (Bronze → Silver → Gold)
# MAGIC 3. UC Functions
# MAGIC 4. Agent orchestration
# MAGIC 5. End-to-end workflow
# MAGIC
# MAGIC ## Dependencies
# MAGIC - All components must be deployed

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run ./01_configuration

# COMMAND ----------

# DBTITLE 1,Test Suite
import time
from datetime import datetime

class IntegrationTests:
    """End-to-end integration tests."""
    
    def __init__(self, config: Config):
        self.config = config
        self.results = {}
    
    def test_configuration(self):
        """Test 1: Configuration loading."""
        try:
            assert self.config.catalog == "weather_disaster"
            assert self.config.schema == "weather_data"
            assert len(self.config.cities_to_monitor) > 0
            self.results["configuration"] = "PASS"
            print("✅ Configuration test: PASS")
        except Exception as e:
            self.results["configuration"] = f"FAIL: {e}"
            print(f"❌ Configuration test: FAIL - {e}")
    
    def test_tables_exist(self):
        """Test 2: Check all tables exist."""
        try:
            tables_to_check = [
                self.config.bronze_weather_table,
                self.config.silver_metrics_table,
                self.config.gold_events_table,
                self.config.gold_analytics_table
            ]
            
            for table in tables_to_check:
                spark.table(table)
            
            self.results["tables"] = "PASS"
            print("✅ Tables test: PASS")
        except Exception as e:
            self.results["tables"] = f"FAIL: {e}"
            print(f"❌ Tables test: FAIL - {e}")
    
    def test_uc_functions(self):
        """Test 3: UC Functions."""
        try:
            # Test assess_risk_level function
            df = spark.sql("""
                SELECT weather_disaster.weather_data.assess_risk_level(150.0, 38.0, 10.0) as risk
            """)
            risk = df.first()["risk"]
            assert risk in ["Critical", "High", "Medium", "Low"]
            
            self.results["uc_functions"] = "PASS"
            print("✅ UC Functions test: PASS")
        except Exception as e:
            self.results["uc_functions"] = f"FAIL: {e}"
            print(f"❌ UC Functions test: FAIL - {e}")
    
    def test_data_pipeline(self):
        """Test 4: Data pipeline."""
        try:
            # Check Bronze has data
            bronze_count = spark.table(self.config.bronze_weather_table).count()
            assert bronze_count > 0, "Bronze table is empty"
            
            # Check Silver has data
            silver_count = spark.table(self.config.silver_metrics_table).count()
            assert silver_count > 0, "Silver table is empty"
            
            self.results["data_pipeline"] = "PASS"
            print(f"✅ Data pipeline test: PASS ({bronze_count} bronze, {silver_count} silver)")
        except Exception as e:
            self.results["data_pipeline"] = f"FAIL: {e}"
            print(f"❌ Data pipeline test: FAIL - {e}")
    
    def run_all_tests(self):
        """Run all integration tests."""
        print("\n" + "="*50)
        print("🧪 Running Integration Tests")
        print("="*50 + "\n")
        
        start_time = time.time()
        
        self.test_configuration()
        self.test_tables_exist()
        self.test_uc_functions()
        self.test_data_pipeline()
        
        duration = time.time() - start_time
        
        print("\n" + "="*50)
        print("📊 Test Summary")
        print("="*50)
        
        passed = sum(1 for r in self.results.values() if r == "PASS")
        total = len(self.results)
        
        for test_name, result in self.results.items():
            status = "✅" if result == "PASS" else "❌"
            print(f"{status} {test_name}: {result}")
        
        print(f"\n🎯 Results: {passed}/{total} tests passed")
        print(f"⏱️  Duration: {duration:.2f}s")
        print(f"📅 Timestamp: {datetime.now().isoformat()}")
        
        return passed == total

# Run tests
tests = IntegrationTests(config)
success = tests.run_all_tests()

if success:
    print("\n✅ All integration tests passed!")
else:
    print("\n⚠️ Some tests failed - review above")

# COMMAND ----------

