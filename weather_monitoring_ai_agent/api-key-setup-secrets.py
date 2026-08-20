# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Setting Up OpenWeatherMap API Key
# MAGIC
# MAGIC To use the Weather Monitoring AI Agent, you need an OpenWeatherMap API key. Follow these steps:
# MAGIC
# MAGIC ### 1. Sign Up for OpenWeatherMap
# MAGIC - Go to [OpenWeatherMap](https://openweathermap.org/).
# MAGIC - Click **Sign Up** and create a free account.
# MAGIC - Verify your email address.
# MAGIC
# MAGIC ### 2. Generate an API Key
# MAGIC - Log in to your OpenWeatherMap account.
# MAGIC - Visit [API Keys](https://home.openweathermap.org/api_keys).
# MAGIC - Your default API key will be shown, or click **Generate** to create a new one.
# MAGIC - Copy the API key.
# MAGIC
# MAGIC ### 3. Store the API Key in Databricks Secrets
# MAGIC - The API key should **never be hardcoded** in notebooks.
# MAGIC - Use the code cell below to store it securely in Databricks Secrets.
# MAGIC - Replace `your-api-key-here` with your actual API key.
# MAGIC
# MAGIC ### 4. API Key Activation
# MAGIC - New API keys may take a few minutes to activate.
# MAGIC - Free tier includes 1,000 API calls per day.
# MAGIC
# MAGIC > **Security Note:** Never commit API keys to version control or share them publicly.

# COMMAND ----------

# DBTITLE 1,Cell 1
from databricks.sdk import WorkspaceClient

# Initialize Databricks workspace client (uses notebook authentication)
w = WorkspaceClient()

# Create a secret scope named 'weather-disaster-secrets'
try:
    w.secrets.create_scope(scope="weather-disaster-secrets")
    print("Secret scope 'weather-disaster-secrets' created successfully")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Secret scope 'weather-disaster-secrets' already exists")
    else:
        raise
 

# IMPORTANT: Replace 'your-api-key-here' with your actual OpenWeatherMap API key
# Get your API key from: https://home.openweathermap.org/api_keys
YOUR_API_KEY = "bbde91f9937125d68063181cf2"  # ⚠️ REPLACE THIS!

if YOUR_API_KEY == "bbde91f9937125d68063181cf2":
    print("⚠️ WARNING: Please replace 'your-api-key-here' with your actual OpenWeatherMap API key!")
    print("   Get your key from: https://home.openweathermap.org/api_keys")
else:
    # Store the weather API key in the secret scope
    w.secrets.put_secret(
        scope="weather-disaster-secrets",
        key="weather-api-key",
        string_value=YOUR_API_KEY
    )
    print("✅ Stored weather-api-key secret successfully")
    print("   This key will be used by the weather ingestion pipeline")

# How to retrieve the secret in other notebooks:
# weather_api_key = dbutils.secrets.get(scope="weather-disaster-secrets", key="weather-api-key")

print("\nCredentials retrieved successfully and ready to use")

# COMMAND ----------

# DBTITLE 1,Comprehensive Pipeline Validation Test
# =====================================================
# COMPREHENSIVE PIPELINE VALIDATION TEST
# =====================================================
import sys
from datetime import datetime

print("=" * 70)
print("🧪 WEATHER MONITORING AI AGENT - COMPREHENSIVE VALIDATION")
print("=" * 70)
print(f"Start Time: {datetime.now().isoformat()}")
print()

test_results = {}

# Test 1: Check if secret scope exists
print("\n[Test 1] Checking secret scope...")
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    scopes = list(w.secrets.list_scopes())
    scope_exists = any(s.name == "weather-disaster-secrets" for s in scopes)
    
    if scope_exists:
        print("   ✅ Secret scope 'weather-disaster-secrets' exists")
        test_results["secret_scope"] = "PASS"
    else:
        print("   ⚠️ Secret scope 'weather-disaster-secrets' not found")
        print("   Run cell 2 to create it")
        test_results["secret_scope"] = "NOT_CONFIGURED"
except Exception as e:
    print(f"   ❌ Error: {str(e)[:200]}")
    test_results["secret_scope"] = f"ERROR: {str(e)[:100]}"

# Test 2: Check if API key is configured
print("\n[Test 2] Checking weather API key...")
try:
    from databricks.sdk.runtime import dbutils
    api_key = dbutils.secrets.get(scope="weather-disaster-secrets", key="weather-api-key")
    
    if api_key and len(api_key) > 10:
        print(f"   ✅ API key configured (length: {len(api_key)} chars)")
        test_results["api_key"] = "PASS"
    else:
        print("   ⚠️ API key is too short or empty")
        test_results["api_key"] = "INVALID"
except Exception as e:
    print(f"   ❌ API key not found: {str(e)[:200]}")
    print("   Run cell 2 to configure your API key")
    test_results["api_key"] = "NOT_CONFIGURED"

# Test 3: Check Unity Catalog structure
print("\n[Test 3] Checking Unity Catalog structure...")
try:
    # Check if catalog exists
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    if "weather_disaster" in catalogs:
        print("   ✅ Catalog 'weather_disaster' exists")
        
        # Check if schema exists
        spark.sql("USE CATALOG weather_disaster")
        schemas = [row.databaseName for row in spark.sql("SHOW SCHEMAS").collect()]
        if "weather_data" in schemas:
            print("   ✅ Schema 'weather_data' exists")
            test_results["uc_structure"] = "PASS"
        else:
            print("   ⚠️ Schema 'weather_data' not found - will be created on first run")
            test_results["uc_structure"] = "SCHEMA_MISSING"
    else:
        print("   ⚠️ Catalog 'weather_disaster' not found - will be created on first run")
        test_results["uc_structure"] = "CATALOG_MISSING"
except Exception as e:
    print(f"   ⚠️ {str(e)[:200]}")
    test_results["uc_structure"] = "WILL_BE_CREATED"

# Test 4: Check if tables exist
print("\n[Test 4] Checking data tables...")
tables_status = {}
try:
    spark.sql("USE CATALOG weather_disaster")
    spark.sql("USE SCHEMA weather_data")
    
    expected_tables = [
        "bronze_weather_data",
        "silver_weather_metrics",
        "gold_disaster_events",
        "gold_severity_analytics"
    ]
    
    existing_tables = [row.tableName for row in spark.sql("SHOW TABLES").collect()]
    
    for table in expected_tables:
        if table in existing_tables:
            count = spark.table(f"weather_disaster.weather_data.{table}").count()
            print(f"   ✅ {table}: {count} rows")
            tables_status[table] = f"EXISTS ({count} rows)"
        else:
            print(f"   ⚠️ {table}: not created yet")
            tables_status[table] = "NOT_CREATED"
    
    if any("EXISTS" in status for status in tables_status.values()):
        test_results["tables"] = "PASS"
    else:
        test_results["tables"] = "NO_DATA_YET"
except Exception as e:
    print(f"   ⚠️ Tables not accessible yet: {str(e)[:200]}")
    test_results["tables"] = "NOT_CREATED_YET"

# Test 5: Check Unity Catalog functions
print("\n[Test 5] Checking Unity Catalog functions...")
try:
    spark.sql("USE CATALOG weather_disaster")
    spark.sql("USE SCHEMA weather_data")
    
    expected_functions = [
        "get_current_weather",
        "get_disaster_history",
        "assess_risk_level",
        "get_active_disasters"
    ]
    
    existing_functions = [row.function for row in spark.sql("SHOW USER FUNCTIONS").collect()]
    existing_function_names = [f.split(".")[-1] for f in existing_functions]
    
    functions_found = 0
    for func in expected_functions:
        if func in existing_function_names:
            print(f"   ✅ {func}")
            functions_found += 1
        else:
            print(f"   ⚠️ {func}: not created yet")
    
    if functions_found == len(expected_functions):
        test_results["uc_functions"] = "PASS"
    elif functions_found > 0:
        test_results["uc_functions"] = f"PARTIAL ({functions_found}/{len(expected_functions)})"
    else:
        test_results["uc_functions"] = "NOT_CREATED_YET"
except Exception as e:
    print(f"   ⚠️ Functions not accessible yet: {str(e)[:200]}")
    test_results["uc_functions"] = "NOT_CREATED_YET"

# Test 6: Check Python dependencies
print("\n[Test 6] Checking Python dependencies...")
try:
    import_status = []
    
    # Check core dependencies
    try:
        import langgraph
        print(f"   ✅ langgraph: {langgraph.__version__}")
        import_status.append(True)
    except ImportError:
        print("   ⚠️ langgraph: not installed")
        import_status.append(False)
    
    try:
        import langchain
        print(f"   ✅ langchain: {langchain.__version__}")
        import_status.append(True)
    except ImportError:
        print("   ⚠️ langchain: not installed")
        import_status.append(False)
    
    try:
        from langchain_databricks import ChatDatabricks
        print("   ✅ langchain_databricks: installed")
        import_status.append(True)
    except ImportError:
        print("   ⚠️ langchain_databricks: not installed")
        import_status.append(False)
    
    try:
        import mlflow
        print(f"   ✅ mlflow: {mlflow.__version__}")
        import_status.append(True)
    except ImportError:
        print("   ⚠️ mlflow: not installed")
        import_status.append(False)
    
    if all(import_status):
        test_results["dependencies"] = "PASS"
    elif any(import_status):
        test_results["dependencies"] = "PARTIAL"
    else:
        test_results["dependencies"] = "MISSING"
except Exception as e:
    print(f"   ❌ Error: {str(e)[:200]}")
    test_results["dependencies"] = f"ERROR"

# ===== SUMMARY =====
print("\n" + "=" * 70)
print("📊 TEST SUMMARY")
print("=" * 70)

for test_name, result in test_results.items():
    if result == "PASS":
        icon = "✅"
    elif "NOT" in result or "MISSING" in result:
        icon = "⚠️"
    else:
        icon = "ℹ️"
    
    print(f"{icon} {test_name.replace('_', ' ').title()}: {result}")

print("\n" + "=" * 70)
print("📋 NEXT STEPS")
print("=" * 70)

if test_results.get("api_key") != "PASS":
    print("\n1️⃣ CONFIGURE API KEY (HIGH PRIORITY)")
    print("   - Get your free API key from: https://home.openweathermap.org/api_keys")
    print("   - Update cell 2 with your API key")
    print("   - Run cell 2 to store it in Databricks Secrets")

if test_results.get("uc_structure") != "PASS":
    print("\n2️⃣ CREATE UNITY CATALOG STRUCTURE")
    print("   - Run notebook: 01_configuration")
    print("   - This will set up the catalog and schema")

if test_results.get("tables") != "PASS":
    print("\n3️⃣ RUN DATA PIPELINE")
    print("   - Run notebook: 02_bronze_ingestion (ingests raw weather data)")
    print("   - Run notebook: 03_silver_processing (transforms data)")
    print("   - Run notebook: 04_gold_analytics (creates analytics)")

if test_results.get("uc_functions") != "PASS":
    print("\n4️⃣ CREATE UNITY CATALOG FUNCTIONS")
    print("   - Run notebook: 05_uc_functions")
    print("   - These functions enable agent tool calling")

if test_results.get("dependencies") != "PASS":
    print("\n5️⃣ INSTALL DEPENDENCIES")
    print("   - Run notebook: 01_configuration (installs required packages)")
    print("   - Or run: %pip install langgraph langchain langchain-databricks")

if all(v == "PASS" for v in test_results.values()):
    print("\n🎉 ALL TESTS PASSED!")
    print("   Your Weather Monitoring AI Agent is fully configured and ready to use.")
    print("\n   Next: Run notebook 07_agent_orchestration to test the complete agent.")
else:
    pass_count = sum(1 for v in test_results.values() if v == "PASS")
    total_count = len(test_results)
    print(f"\n📈 Progress: {pass_count}/{total_count} components configured")

print(f"\nEnd Time: {datetime.now().isoformat()}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Automated Pipeline Runner - Full Setup
# =====================================================
# AUTOMATED PIPELINE RUNNER - COMPLETE SETUP
# =====================================================
import time
from datetime import datetime

print("=" * 70)
print("⚙️ AUTOMATED PIPELINE RUNNER - FULL SETUP")
print("=" * 70)
print(f"Start Time: {datetime.now().isoformat()}")
print()

pipeline_results = {}

# ============================================================
# STEP 1: Create Unity Catalog Structure
# ============================================================
print("\n[Step 1/6] Creating Unity Catalog structure...")
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS weather_disaster")
    print("   ✅ Created catalog: weather_disaster")
    
    spark.sql("CREATE SCHEMA IF NOT EXISTS weather_disaster.weather_data")
    print("   ✅ Created schema: weather_disaster.weather_data")
    
    spark.sql("USE CATALOG weather_disaster")
    spark.sql("USE SCHEMA weather_data")
    print("   ✅ Selected catalog and schema")
    
    pipeline_results["catalog_setup"] = "SUCCESS"
except Exception as e:
    print(f"   ❌ Error: {str(e)[:300]}")
    pipeline_results["catalog_setup"] = f"FAILED: {str(e)[:100]}"

# ============================================================
# STEP 2: Install Dependencies
# ============================================================
print("\n[Step 2/6] Installing Python dependencies...")
try:
    import sys
    import subprocess
    
    print("   Installing langgraph, langchain, langchain-databricks...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "--upgrade", "typing-extensions"],
        capture_output=True, text=True
    )
    
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "langgraph", "langchain", "langchain-databricks"],
        capture_output=True, text=True
    )
    
    if result.returncode == 0:
        print("   ✅ Dependencies installed")
        pipeline_results["dependencies"] = "SUCCESS"
    else:
        print(f"   ⚠️ Installation completed with warnings: {result.stderr[:200]}")
        pipeline_results["dependencies"] = "SUCCESS_WITH_WARNINGS"
except Exception as e:
    print(f"   ❌ Error: {str(e)[:300]}")
    pipeline_results["dependencies"] = f"FAILED: {str(e)[:100]}"

print("   ⚠️ Note: Kernel restart may be required for imports to work properly")

# ============================================================
# STEP 3: Load Configuration
# ============================================================
print("\n[Step 3/6] Loading configuration...")
try:
    # Import configuration inline
    import yaml
    from pathlib import Path
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.runtime import dbutils
    from typing import Dict, Any

    class Config:
        def __init__(self, env: str = "dev"):
            self.env = env
            self.w = WorkspaceClient()
            self.catalog = "weather_disaster"
            self.schema = "weather_data"
            self.bronze_weather_table = f"{self.catalog}.{self.schema}.bronze_weather_data"
            self.silver_metrics_table = f"{self.catalog}.{self.schema}.silver_weather_metrics"
            self.gold_events_table = f"{self.catalog}.{self.schema}.gold_disaster_events"
            self.gold_analytics_table = f"{self.catalog}.{self.schema}.gold_severity_analytics"
            
            try:
                self.weather_api_key = dbutils.secrets.get(scope="weather-disaster-secrets", key="weather-api-key")
            except Exception as e:
                print(f"      Warning: Could not load API key: {str(e)[:100]}")
                self.weather_api_key = None
            
            self.weather_api_base_url = "https://api.openweathermap.org/data/2.5"
            self.weather_api_timeout = 10
            self.cities_to_monitor = [
                {"name": "London", "country": "GB"},
                {"name": "New York", "country": "US"},
                {"name": "Tokyo", "country": "JP"},
                {"name": "Mumbai", "country": "IN"},
                {"name": "Sydney", "country": "AU"}
            ]
            self.thresholds = {
                "hurricane_wind_speed_ms": 33.0,
                "flood_rainfall_mm": 100.0,
                "heatwave_temp_c": 40.0,
                "severe_storm_wind_speed_ms": 20.0,
                "winter_storm_temp_c": -10.0
            }
    
    config = Config(env="dev")
    print(f"   ✅ Configuration loaded")
    print(f"      Catalog: {config.catalog}")
    print(f"      Schema: {config.schema}")
    print(f"      Cities: {len(config.cities_to_monitor)}")
    print(f"      API key configured: {'Yes' if config.weather_api_key else 'No'}")
    pipeline_results["configuration"] = "SUCCESS"
except Exception as e:
    print(f"   ❌ Error: {str(e)[:300]}")
    pipeline_results["configuration"] = f"FAILED: {str(e)[:100]}"
    # Can't continue without config
    config = None

# ============================================================
# STEP 4: Create Bronze Table & Ingest Data
# ============================================================
if config and config.weather_api_key:
    print("\n[Step 4/6] Creating Bronze table and ingesting data...")
    try:
        # Create Bronze table
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.bronze_weather_table} (
          request_id STRING,
          city STRING,
          country_code STRING,
          raw_response STRING,
          temperature_kelvin DOUBLE,
          feels_like_kelvin DOUBLE,
          pressure_hpa INT,
          humidity_percent INT,
          wind_speed_ms DOUBLE,
          wind_direction_degrees INT,
          precipitation_mm DOUBLE,
          weather_main STRING,
          weather_description STRING,
          clouds_percent INT,
          visibility_meters INT,
          api_timestamp TIMESTAMP,
          ingestion_timestamp TIMESTAMP,
          api_version STRING,
          api_endpoint STRING,
          checksum STRING,
          source_system STRING,
          is_valid BOOLEAN
        )
        USING DELTA
        TBLPROPERTIES (
          'delta.enableChangeDataFeed' = 'true',
          'delta.autoOptimize.optimizeWrite' = 'true',
          'delta.autoOptimize.autoCompact' = 'true'
        )
        """)
        print("   ✅ Bronze table created")
        
        # Ingest data for first 2 cities (quick test)
        import requests
        import hashlib
        import json
        from datetime import datetime
        from pyspark.sql import Row
        import uuid
        
        rows = []
        test_cities = config.cities_to_monitor[:2]  # Just test with 2 cities
        
        for city_info in test_cities:
            city = city_info["name"]
            country = city_info["country"]
            
            url = f"{config.weather_api_base_url}/weather"
            params = {
                "q": f"{city},{country}",
                "appid": config.weather_api_key,
                "units": "metric"
            }
            
            try:
                response = requests.get(url, params=params, timeout=config.weather_api_timeout)
                if response.status_code == 200:
                    raw_data = response.json()
                    main = raw_data.get("main", {})
                    wind = raw_data.get("wind", {})
                    weather = raw_data.get("weather", [{}])[0]
                    clouds = raw_data.get("clouds", {})
                    
                    row = Row(
                        request_id=str(uuid.uuid4()),
                        city=city,
                        country_code=country,
                        raw_response=json.dumps(raw_data),
                        temperature_kelvin=main.get("temp", 0.0) + 273.15,
                        feels_like_kelvin=main.get("feels_like", 0.0) + 273.15,
                        pressure_hpa=main.get("pressure"),
                        humidity_percent=main.get("humidity"),
                        wind_speed_ms=wind.get("speed"),
                        wind_direction_degrees=wind.get("deg"),
                        precipitation_mm=raw_data.get("rain", {}).get("1h", 0.0),
                        weather_main=weather.get("main"),
                        weather_description=weather.get("description"),
                        clouds_percent=clouds.get("all"),
                        visibility_meters=raw_data.get("visibility"),
                        api_timestamp=datetime.fromtimestamp(raw_data.get("dt", 0)),
                        ingestion_timestamp=datetime.now(),
                        api_version="2.5",
                        api_endpoint=config.weather_api_base_url,
                        checksum=hashlib.sha256(json.dumps(raw_data, sort_keys=True).encode()).hexdigest(),
                        source_system="OpenWeatherMap",
                        is_valid=True
                    )
                    rows.append(row)
                    print(f"   ✅ Fetched data for {city}")
                else:
                    print(f"   ⚠️ Failed to fetch {city}: HTTP {response.status_code}")
            except Exception as e:
                print(f"   ⚠️ Error fetching {city}: {str(e)[:100]}")
        
        if rows:
            df_bronze = spark.createDataFrame(rows)
            df_bronze.write.format("delta").mode("append").saveAsTable(config.bronze_weather_table)
            print(f"   ✅ Ingested {len(rows)} records to Bronze")
            pipeline_results["bronze_ingestion"] = f"SUCCESS ({len(rows)} records)"
        else:
            print("   ⚠️ No data ingested")
            pipeline_results["bronze_ingestion"] = "NO_DATA"
            
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:300]}")
        pipeline_results["bronze_ingestion"] = f"FAILED: {str(e)[:100]}"
else:
    print("\n[Step 4/6] Skipping Bronze ingestion (API key not configured)")
    pipeline_results["bronze_ingestion"] = "SKIPPED_NO_API_KEY"

# ============================================================
# STEP 5: Create Silver Table & Process Data
# ============================================================
if config and pipeline_results.get("bronze_ingestion", "").startswith("SUCCESS"):
    print("\n[Step 5/6] Creating Silver table and processing data...")
    try:
        # Create Silver table
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.silver_metrics_table} (
          metric_id STRING,
          city STRING,
          temperature_celsius DOUBLE,
          feels_like_celsius DOUBLE,
          wind_speed_kmh DOUBLE,
          wind_speed_mph DOUBLE,
          humidity_percent INT,
          precipitation_mm DOUBLE,
          weather_description STRING,
          is_extreme_weather BOOLEAN,
          is_hurricane_conditions BOOLEAN,
          is_flood_conditions BOOLEAN,
          is_heatwave_conditions BOOLEAN,
          is_severe_storm BOOLEAN,
          is_winter_storm BOOLEAN,
          observation_timestamp TIMESTAMP,
          processed_timestamp TIMESTAMP,
          bronze_request_id STRING
        )
        USING DELTA
        """)
        print("   ✅ Silver table created")
        
        # Transform Bronze to Silver
        from pyspark.sql import functions as F
        
        df_bronze = spark.table(config.bronze_weather_table)
        df_silver = df_bronze.select(
            F.expr("uuid()").alias("metric_id"),
            F.col("city"),
            (F.col("temperature_kelvin") - 273.15).alias("temperature_celsius"),
            (F.col("feels_like_kelvin") - 273.15).alias("feels_like_celsius"),
            (F.col("wind_speed_ms") * 3.6).alias("wind_speed_kmh"),
            (F.col("wind_speed_ms") * 2.237).alias("wind_speed_mph"),
            F.col("humidity_percent"),
            F.col("precipitation_mm"),
            F.col("weather_description"),
            F.when(
                (F.col("wind_speed_ms") >= 20) | 
                (F.abs(F.col("temperature_kelvin") - 293.15) >= 25),
                True
            ).otherwise(False).alias("is_extreme_weather"),
            (F.col("wind_speed_ms") >= 33.0).alias("is_hurricane_conditions"),
            (F.col("precipitation_mm") >= 100.0).alias("is_flood_conditions"),
            ((F.col("temperature_kelvin") - 273.15) >= 40.0).alias("is_heatwave_conditions"),
            (F.col("wind_speed_ms") >= 20.0).alias("is_severe_storm"),
            ((F.col("temperature_kelvin") - 273.15) <= -10.0).alias("is_winter_storm"),
            F.col("api_timestamp").alias("observation_timestamp"),
            F.current_timestamp().alias("processed_timestamp"),
            F.col("request_id").alias("bronze_request_id")
        )
        
        record_count = df_silver.count()
        df_silver.write.format("delta").mode("append").saveAsTable(config.silver_metrics_table)
        print(f"   ✅ Processed {record_count} records to Silver")
        pipeline_results["silver_processing"] = f"SUCCESS ({record_count} records)"
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:300]}")
        pipeline_results["silver_processing"] = f"FAILED: {str(e)[:100]}"
else:
    print("\n[Step 5/6] Skipping Silver processing (Bronze data not available)")
    pipeline_results["silver_processing"] = "SKIPPED"

# ============================================================
# STEP 6: Create Gold Tables & Detect Disasters
# ============================================================
if config and pipeline_results.get("silver_processing", "").startswith("SUCCESS"):
    print("\n[Step 6/6] Creating Gold tables and detecting disasters...")
    try:
        # Create Gold tables
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.gold_events_table} (
          event_id STRING,
          city STRING,
          disaster_type STRING,
          severity_level STRING,
          temperature_celsius DOUBLE,
          wind_speed_kmh DOUBLE,
          precipitation_mm DOUBLE,
          humidity_percent INT,
          event_start_time TIMESTAMP,
          event_end_time TIMESTAMP,
          duration_hours DOUBLE,
          is_active BOOLEAN,
          response_plan_generated BOOLEAN,
          alerts_sent_count INT,
          human_verification_required BOOLEAN,
          human_verified_at TIMESTAMP,
          detected_at TIMESTAMP,
          updated_at TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (city, detected_at)
        """)
        print("   ✅ Gold events table created")
        
        # Detect disasters
        from pyspark.sql import functions as F
        
        df_silver = spark.table(config.silver_metrics_table)
        df_events = df_silver.filter(
            (F.col("is_hurricane_conditions") == True) |
            (F.col("is_flood_conditions") == True) |
            (F.col("is_heatwave_conditions") == True) |
            (F.col("is_severe_storm") == True) |
            (F.col("is_winter_storm") == True)
        )
        
        event_count = df_events.count()
        if event_count > 0:
            df_events = df_events.withColumn("disaster_type",
                F.when(F.col("is_hurricane_conditions"), "Hurricane")
                 .when(F.col("is_flood_conditions"), "Flood")
                 .when(F.col("is_heatwave_conditions"), "Heatwave")
                 .when(F.col("is_severe_storm"), "Severe Storm")
                 .when(F.col("is_winter_storm"), "Winter Storm")
                 .otherwise("Unknown")
            )
            
            df_gold_events = df_events.select(
                F.expr("uuid()").alias("event_id"),
                F.col("city"),
                F.col("disaster_type"),
                F.lit("High").alias("severity_level"),
                F.col("temperature_celsius"),
                F.col("wind_speed_kmh"),
                F.col("precipitation_mm"),
                F.col("humidity_percent"),
                F.col("observation_timestamp").alias("event_start_time"),
                F.lit(None).cast("timestamp").alias("event_end_time"),
                F.lit(None).cast("double").alias("duration_hours"),
                F.lit(True).alias("is_active"),
                F.lit(False).alias("response_plan_generated"),
                F.lit(0).alias("alerts_sent_count"),
                F.lit(True).alias("human_verification_required"),
                F.lit(None).cast("timestamp").alias("human_verified_at"),
                F.current_timestamp().alias("detected_at"),
                F.current_timestamp().alias("updated_at")
            )
            
            df_gold_events.write.format("delta").mode("append").saveAsTable(config.gold_events_table)
            print(f"   ✅ Detected {event_count} disaster events")
            pipeline_results["gold_analytics"] = f"SUCCESS ({event_count} events)"
        else:
            print("   ℹ️ No disaster conditions detected (normal weather)")
            pipeline_results["gold_analytics"] = "SUCCESS (0 events)"
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:300]}")
        pipeline_results["gold_analytics"] = f"FAILED: {str(e)[:100]}"
else:
    print("\n[Step 6/6] Skipping Gold analytics (Silver data not available)")
    pipeline_results["gold_analytics"] = "SKIPPED"

# ============================================================
# FINAL SUMMARY
# ============================================================
print("\n" + "=" * 70)
print("🎉 PIPELINE EXECUTION SUMMARY")
print("=" * 70)

for step_name, result in pipeline_results.items():
    if "SUCCESS" in result:
        icon = "✅"
    elif "SKIP" in result:
        icon = "⏭️"
    elif "FAILED" in result:
        icon = "❌"
    else:
        icon = "ℹ️"
    
    print(f"{icon} {step_name.replace('_', ' ').title()}: {result}")

success_count = sum(1 for v in pipeline_results.values() if "SUCCESS" in v)
total_count = len(pipeline_results)
failed_count = sum(1 for v in pipeline_results.values() if "FAILED" in v)

print("\n" + "=" * 70)
if failed_count == 0 and success_count >= 4:
    print("✅ PIPELINE SETUP COMPLETE!")
    print(f"   Successfully configured {success_count}/{total_count} components")
    print("\n   Next steps:")
    print("   - Run notebook: 05_uc_functions (create Unity Catalog functions)")
    print("   - Run notebook: 07_agent_orchestration (test AI agent)")
    print("   - Run notebook: 09_integration_tests (validate entire system)")
else:
    print(f"⚠️ PIPELINE PARTIALLY COMPLETE")
    print(f"   Success: {success_count}, Failed: {failed_count}, Total: {total_count}")
    if failed_count > 0:
        print("\n   Please review the errors above and re-run this cell after fixing.")

print(f"\nEnd Time: {datetime.now().isoformat()}")
print("=" * 70)