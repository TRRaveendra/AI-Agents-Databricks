# Databricks notebook source
# DBTITLE 1,Unity Catalog Functions
# MAGIC %md
# MAGIC # 🛡️ Unity Catalog Functions - Governed Data Access
# MAGIC
# MAGIC ## Purpose
# MAGIC Governed, auditable SQL functions for AI agent tool calling.
# MAGIC
# MAGIC ## Functions
# MAGIC 1. `get_current_weather(city_name)` - Current weather conditions
# MAGIC 2. `get_disaster_history(city_name, days_back)` - Historical disaster events
# MAGIC 3. `assess_risk_level(wind, temp, precip)` - Risk assessment
# MAGIC 4. `get_active_disasters()` - List of active disasters
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Silver and Gold tables must exist
# MAGIC - Configuration loaded

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run ./01_configuration

# COMMAND ----------

# DBTITLE 1,Create UC Functions
# Function 1: Get current weather for a city
spark.sql("""
CREATE OR REPLACE FUNCTION weather_disaster.weather_data.get_current_weather(city_name STRING)
RETURNS TABLE(
  city STRING,
  temperature_celsius DOUBLE,
  feels_like_celsius DOUBLE,
  wind_speed_kmh DOUBLE,
  humidity_percent INT,
  precipitation_mm DOUBLE,
  weather_description STRING,
  is_extreme_weather BOOLEAN,
  observation_timestamp TIMESTAMP
)
COMMENT 'Returns the most recent weather metrics for a given city'
RETURN (
  SELECT 
    city,
    temperature_celsius,
    feels_like_celsius,
    wind_speed_kmh,
    humidity_percent,
    precipitation_mm,
    weather_description,
    is_extreme_weather,
    observation_timestamp
  FROM weather_disaster.weather_data.silver_weather_metrics
  WHERE city = city_name
  ORDER BY observation_timestamp DESC
  LIMIT 1
)
""")

# Function 2: Get disaster history for a city
spark.sql("""
CREATE OR REPLACE FUNCTION weather_disaster.weather_data.get_disaster_history(
  city_name STRING,
  days_back INT
)
RETURNS TABLE(
  event_id STRING,
  disaster_type STRING,
  severity_level STRING,
  event_start_time TIMESTAMP,
  duration_hours DOUBLE,
  temperature_celsius DOUBLE,
  wind_speed_kmh DOUBLE
)
COMMENT 'Returns disaster event history for a city over the past N days'
RETURN (
  SELECT 
    event_id,
    disaster_type,
    severity_level,
    event_start_time,
    duration_hours,
    temperature_celsius,
    wind_speed_kmh
  FROM weather_disaster.weather_data.gold_disaster_events
  WHERE city = city_name
    AND detected_at >= CURRENT_TIMESTAMP() - MAKE_INTERVAL(0, 0, 0, days_back, 0, 0, 0)
  ORDER BY detected_at DESC
)
""")

# Function 3: Assess risk level based on weather parameters
spark.sql("""
CREATE OR REPLACE FUNCTION weather_disaster.weather_data.assess_risk_level(
  wind_speed_kmh DOUBLE,
  temperature_celsius DOUBLE,
  precipitation_mm DOUBLE
)
RETURNS STRING
COMMENT 'Assesses disaster risk level based on weather parameters'
RETURN (
  CASE
    WHEN wind_speed_kmh >= 144 OR ABS(temperature_celsius - 20) >= 40 THEN 'Critical'
    WHEN wind_speed_kmh >= 108 OR ABS(temperature_celsius - 20) >= 35 THEN 'High'
    WHEN wind_speed_kmh >= 72 OR ABS(temperature_celsius - 20) >= 30 THEN 'Medium'
    ELSE 'Low'
  END
)
""")

# Function 4: Get cities with active disasters
spark.sql("""
CREATE OR REPLACE FUNCTION weather_disaster.weather_data.get_active_disasters()
RETURNS TABLE(
  city STRING,
  disaster_type STRING,
  severity_level STRING,
  event_start_time TIMESTAMP,
  temperature_celsius DOUBLE,
  wind_speed_kmh DOUBLE
)
COMMENT 'Returns all cities currently experiencing disaster conditions'
RETURN (
  SELECT 
    city,
    disaster_type,
    severity_level,
    event_start_time,
    temperature_celsius,
    wind_speed_kmh
  FROM weather_disaster.weather_data.gold_disaster_events
  WHERE is_active = TRUE
  ORDER BY severity_level, detected_at DESC
)
""")

print("✅ All 4 Unity Catalog functions created successfully")

# COMMAND ----------

# DBTITLE 1,Test UC Functions
# Test Function 1: Get current weather
print("Test 1: Get current weather for London")
try:
    df_weather = spark.sql("""
      SELECT * FROM weather_disaster.weather_data.get_current_weather('London')
    """)
    if df_weather.count() > 0:
        display(df_weather)
    else:
        print("  No weather data available yet - run Bronze/Silver ingestion first")
except Exception as e:
    print(f"  Error: {e}")

# Test Function 3: Assess risk level
print("\nTest 3: Assess risk level")
df_risk = spark.sql("""
  SELECT 
    150.0 as wind_speed_kmh,
    38.0 as temperature_celsius,
    10.0 as precipitation_mm,
    weather_disaster.weather_data.assess_risk_level(150.0, 38.0, 10.0) as risk_level
""")
display(df_risk)

print("\n✅ UC Functions are ready for agent tool calling")

# COMMAND ----------

