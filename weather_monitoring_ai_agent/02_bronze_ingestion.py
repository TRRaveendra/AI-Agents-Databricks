# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Layer - Raw Data Ingestion
# MAGIC %md
# MAGIC # 🪙 Bronze Layer - Raw Weather Data Ingestion
# MAGIC
# MAGIC ## Purpose
# MAGIC Raw, unprocessed weather API data ingestion following medallion architecture.
# MAGIC
# MAGIC ## Features
# MAGIC - Append-only Delta tables
# MAGIC - Checksum-based deduplication
# MAGIC - Full lineage (ingestion timestamp, source, API version)
# MAGIC - No transformations - exactly as received
# MAGIC
# MAGIC ## Dependencies
# MAGIC - `01_configuration` must be run first

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run ./01_configuration

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS weather_disaster.weather_data.bronze_weather_data (
# MAGIC   -- Primary identifiers
# MAGIC   request_id STRING COMMENT 'Unique ID for this API request',
# MAGIC   city STRING COMMENT 'City name queried',
# MAGIC   country_code STRING COMMENT 'ISO country code',
# MAGIC   
# MAGIC   -- Raw API response (as JSON string)
# MAGIC   raw_response STRING COMMENT 'Full JSON response from API',
# MAGIC   
# MAGIC   -- Raw measurements (as received from API)
# MAGIC   temperature_kelvin DOUBLE COMMENT 'Temperature in Kelvin',
# MAGIC   feels_like_kelvin DOUBLE COMMENT 'Feels like temperature in Kelvin',
# MAGIC   pressure_hpa INT COMMENT 'Atmospheric pressure in hPa',
# MAGIC   humidity_percent INT COMMENT 'Humidity percentage',
# MAGIC   wind_speed_ms DOUBLE COMMENT 'Wind speed in meters/second',
# MAGIC   wind_direction_degrees INT COMMENT 'Wind direction in degrees',
# MAGIC   precipitation_mm DOUBLE COMMENT 'Precipitation in mm',
# MAGIC   weather_main STRING COMMENT 'Main weather condition',
# MAGIC   weather_description STRING COMMENT 'Detailed weather description',
# MAGIC   clouds_percent INT COMMENT 'Cloudiness percentage',
# MAGIC   visibility_meters INT COMMENT 'Visibility in meters',
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   api_timestamp TIMESTAMP COMMENT 'Timestamp from API',
# MAGIC   ingestion_timestamp TIMESTAMP COMMENT 'When ingested to Bronze',
# MAGIC   api_version STRING COMMENT 'API version used',
# MAGIC   api_endpoint STRING COMMENT 'API endpoint called',
# MAGIC   checksum STRING COMMENT 'SHA256 checksum for deduplication',
# MAGIC   
# MAGIC   -- Audit
# MAGIC   source_system STRING COMMENT 'Source system (OpenWeatherMap)',
# MAGIC   is_valid BOOLEAN COMMENT 'Whether API response was valid'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Bronze layer: Raw weather API data'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Bronze Ingestion Class
import requests
import hashlib
import json
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import Row
import uuid

# City coordinates for Open-Meteo API (latitude, longitude)
CITY_COORDINATES = {
    "Tokyo": (35.6762, 139.6503),
    "New York": (40.7128, -74.0060),
    "London": (51.5074, -0.1278),
    "Mumbai": (19.0760, 72.8777),
    "Sydney": (-33.8688, 151.2093),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago": (41.8781, -87.6298),
    "Toronto": (43.6532, -79.3832),
    "Mexico City": (19.4326, -99.1332),
    "Sao Paulo": (-23.5505, -46.6333),
    "Paris": (48.8566, 2.3522),
    "Moscow": (55.7558, 37.6173),
    "Beijing": (39.9042, 116.4074),
    "Shanghai": (31.2304, 121.4737),
    "Cairo": (30.0444, 31.2357),
    "Lagos": (6.5244, 3.3792),
    "Jakarta": (-6.2088, 106.8456),
    "Bangkok": (13.7563, 100.5018),
    "Singapore": (1.3521, 103.8198),
    "Dubai": (25.2048, 55.2708)
}

class WeatherBronzeIngestion:
    """Handles raw weather data ingestion to Bronze layer using Open-Meteo API."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def _calculate_checksum(self, data: Dict) -> str:
        """Calculate SHA256 checksum for deduplication."""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def fetch_weather_data(self, city: str, country: str) -> Optional[Dict]:
        """Fetch weather data from Open-Meteo API (free, no API key required)."""
        
        # Get coordinates for the city
        coords = CITY_COORDINATES.get(city)
        if not coords:
            print(f"⚠️ Coordinates not found for {city}, skipping")
            return None
        
        lat, lon = coords
        
        # Open-Meteo API endpoint (free, no API key needed!)
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,weather_code,cloud_cover,pressure_msl,wind_speed_10m,wind_direction_10m",
            "timezone": "auto"
        }
        
        try:
            print(f"Fetching weather data for {city} ({lat}, {lon})...")
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                # Add city info to the response
                data["city"] = city
                data["country"] = country
                return data
            else:
                print(f"❌ API error {response.status_code} for {city}: {response.text}")
                return None
        except Exception as e:
            print(f"❌ Request error for {city}: {e}")
            return None
    
    def transform_to_bronze_row(self, raw_data: Dict, city: str, country: str) -> Row:
        """Transform Open-Meteo API response to Bronze row."""
        request_id = str(uuid.uuid4())
        checksum = self._calculate_checksum(raw_data)
        ingestion_ts = datetime.now()
        
        # Extract current weather from Open-Meteo response
        current = raw_data.get("current", {})
        
        # Weather code mapping (simplified)
        weather_code = current.get("weather_code", 0)
        weather_descriptions = {
            0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
            45: "Fog", 48: "Depositing rime fog",
            51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
            61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
            71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
            77: "Snow grains", 80: "Slight rain showers", 81: "Moderate rain showers",
            82: "Violent rain showers", 85: "Slight snow showers", 86: "Heavy snow showers",
            95: "Thunderstorm", 96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail"
        }
        weather_desc = weather_descriptions.get(weather_code, "Unknown")
        weather_main = "Clear" if weather_code <= 3 else ("Rain" if 51 <= weather_code <= 82 else ("Snow" if weather_code >= 85 else "Thunderstorm"))
        
        return Row(
            request_id=request_id,
            city=city,
            country_code=country,
            raw_response=json.dumps(raw_data),
            temperature_kelvin=current.get("temperature_2m", 0.0) + 273.15,  # Convert Celsius to Kelvin
            feels_like_kelvin=current.get("apparent_temperature", 0.0) + 273.15,  # Convert Celsius to Kelvin
            pressure_hpa=int(current.get("pressure_msl", 1013)),
            humidity_percent=int(current.get("relative_humidity_2m", 0)),
            wind_speed_ms=current.get("wind_speed_10m", 0.0) / 3.6,  # Convert km/h to m/s
            wind_direction_degrees=int(current.get("wind_direction_10m", 0)),
            precipitation_mm=current.get("precipitation", 0.0),
            weather_main=weather_main,
            weather_description=weather_desc,
            clouds_percent=int(current.get("cloud_cover", 0)),
            visibility_meters=10000,  # Open-Meteo doesn't provide visibility, using default
            api_timestamp=datetime.fromisoformat(current.get("time", datetime.now().isoformat()).replace('Z', '+00:00')),
            ingestion_timestamp=ingestion_ts,
            api_version="1.0",
            api_endpoint="https://api.open-meteo.com/v1/forecast",
            checksum=checksum,
            source_system="Open-Meteo",
            is_valid=True
        )
    
    def ingest_batch(self, cities: List[Dict]) -> int:
        """Ingest weather data for a batch of cities."""
        rows = []
        
        for city_info in cities:
            city = city_info["name"]
            country = city_info["country"]
            
            raw_data = self.fetch_weather_data(city, country)
            if raw_data:
                bronze_row = self.transform_to_bronze_row(raw_data, city, country)
                rows.append(bronze_row)
            else:
                print(f"⚠️ Skipping {city} due to API error")
        
        if not rows:
            print("⚠️ No data fetched")
            return 0
        
        # Create DataFrame and append to Bronze
        df_bronze = spark.createDataFrame(rows)
        
        # Deduplicate based on checksum (using DataFrame anti-join instead of RDD)
        df_bronze_existing = spark.table(self.config.bronze_weather_table)
        
        # Use left anti join to find new records (Spark Connect compatible)
        df_bronze_new = df_bronze.join(
            df_bronze_existing.select("checksum"),
            on="checksum",
            how="left_anti"
        )
        
        new_count = df_bronze_new.count()
        if new_count > 0:
            df_bronze_new.write.format("delta").mode("append").saveAsTable(self.config.bronze_weather_table)
            print(f"✅ Ingested {new_count} new records to Bronze")
        else:
            print("ℹ️ All records already exist (duplicate checksums)")
        
        return new_count

print("✅ Bronze ingestion class loaded")

# COMMAND ----------

# DBTITLE 1,Run Bronze Ingestion
# Initialize and run ingestion
bronze_ingestor = WeatherBronzeIngestion(config)

# Ingest data for configured cities
records_ingested = bronze_ingestor.ingest_batch(config.cities_to_monitor)

print(f"\n✅ Bronze ingestion complete: {records_ingested} records")

# Display sample
if records_ingested > 0:
    print("\n🔍 Sample Bronze data:")
    display(spark.table(config.bronze_weather_table).orderBy("ingestion_timestamp", ascending=False).limit(5))

# COMMAND ----------

# DBTITLE 1,Test Open-Meteo API
# Quick test of Open-Meteo API
import requests

print("🧪 Testing Open-Meteo API...\n")

# Test with Tokyo
lat, lon = 35.6762, 139.6503
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": lat,
    "longitude": lon,
    "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,weather_code,cloud_cover,pressure_msl,wind_speed_10m,wind_direction_10m",
    "timezone": "auto"
}

try:
    response = requests.get(url, params=params, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print("✅ API Request Successful!\n")
        print(f"Response keys: {list(data.keys())}")
        
        if "current" in data:
            current = data["current"]
            print(f"\nCurrent weather data:")
            for key, value in current.items():
                print(f"  {key}: {value}")
        else:
            print("⚠️ 'current' key not found in response")
            print(f"Full response: {data}")
    else:
        print(f"❌ API Error: {response.text}")
except Exception as e:
    print(f"❌ Exception: {e}")

# COMMAND ----------

