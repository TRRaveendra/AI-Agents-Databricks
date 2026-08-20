# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Configuration Management
# MAGIC %md
# MAGIC # ⚙️ Configuration Management
# MAGIC
# MAGIC ## Purpose
# MAGIC Centralized, environment-specific configuration for the Weather Monitoring AI Agent.
# MAGIC
# MAGIC ## Features
# MAGIC - Environment-aware (dev/staging/prod)
# MAGIC - Unity Catalog namespaces
# MAGIC - API credentials via Databricks Secrets
# MAGIC - Disaster thresholds
# MAGIC - Alert channel configuration
# MAGIC - LLM endpoint settings

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# Install required packages
%pip install -q --upgrade typing-extensions
%pip install -q --upgrade langgraph langchain langchain-databricks
print("✅ Dependencies installed")
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration Class
import yaml
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
from typing import Dict, Any

class Config:
    """Metadata-driven configuration for the weather disaster agent."""
    
    def __init__(self, env: str = "dev"):
        self.env = env
        self.w = WorkspaceClient()
        
        # Unity Catalog namespaces
        self.catalog = "weather_disaster"
        self.schema = "weather_data"
        
        # Table names (3-part naming)
        self.bronze_weather_table = f"{self.catalog}.{self.schema}.bronze_weather_data"
        self.silver_metrics_table = f"{self.catalog}.{self.schema}.silver_weather_metrics"
        self.gold_events_table = f"{self.catalog}.{self.schema}.gold_disaster_events"
        self.gold_analytics_table = f"{self.catalog}.{self.schema}.gold_severity_analytics"
        
        # API Configuration
        try:
            self.weather_api_key = dbutils.secrets.get(scope="weather-disaster-secrets", key="weather-api-key")
            print("✅ Secrets loaded successfully")
        except Exception as e:
            print(f"⚠️ Secrets not configured: {str(e)}")
            self.weather_api_key = None
        
        self.weather_api_base_url = "https://api.openweathermap.org/data/2.5"
        self.weather_api_timeout = 10
        
        # LLM Configuration - Databricks Foundation Model
        self.llm_provider = "databricks"
        self.llm_model = "databricks-meta-llama-3-3-70b-instruct"
        self.llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"
        self.llm_temperature = 0.1
        self.llm_max_tokens = 2000
        
        # Disaster Thresholds
        self.thresholds = {
            "hurricane_wind_speed_ms": 33.0,  # ~119 km/h
            "flood_rainfall_mm": 100.0,
            "heatwave_temp_c": 40.0,
            "severe_storm_wind_speed_ms": 20.0,  # ~72 km/h
            "winter_storm_temp_c": -10.0
        }
        
        # Severity Thresholds (multi-tier)
        self.severity_thresholds = {
            "critical": {
                "wind_speed_ms": 40.0,  # 144 km/h
                "temp_extreme_c": 40.0  # >±40°C from 20°C baseline
            },
            "high": {
                "wind_speed_ms": 30.0,  # 108 km/h
                "temp_extreme_c": 35.0
            },
            "medium": {
                "wind_speed_ms": 20.0,  # 72 km/h
                "temp_extreme_c": 30.0
            }
        }
        
        # Alert Channel Configuration
        self.alert_channels = {
            "email": {
                "enabled": False,
                "smtp_server": "smtp.example.com",
                "from_address": "alerts@weather-disaster.com",
                "to_addresses": ["ops@example.com"]
            },
            "slack": {
                "enabled": False,
                "webhook_url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
            },
            "pagerduty": {
                "enabled": False,
                "integration_key": "your_pagerduty_key"
            }
        }
        
        # Cities to Monitor
        self.cities_to_monitor = [
            {"name": "London", "country": "GB"},
            {"name": "New York", "country": "US"},
            {"name": "Tokyo", "country": "JP"},
            {"name": "Mumbai", "country": "IN"},
            {"name": "Sydney", "country": "AU"}
        ]
        
        # Operational Parameters
        self.ingestion_batch_size = 10
        self.max_retries = 3
        self.retry_delay_seconds = 5
        self.data_retention_days = 365
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary (excluding sensitive data)."""
        return {
            "env": self.env,
            "catalog": self.catalog,
            "schema": self.schema,
            "tables": {
                "bronze": self.bronze_weather_table,
                "silver": self.silver_metrics_table,
                "gold_events": self.gold_events_table,
                "gold_analytics": self.gold_analytics_table
            },
            "llm": {
                "provider": self.llm_provider,
                "model": self.llm_model,
                "endpoint": self.llm_endpoint
            },
            "thresholds": self.thresholds,
            "cities": self.cities_to_monitor
        }
    
    def __repr__(self):
        return f"Config(env={self.env}, catalog={self.catalog}, schema={self.schema})"

# Initialize configuration
config = Config(env="dev")

print("✅ Configuration loaded successfully")
print(f"   Environment: {config.env}")
print(f"   Catalog: {config.catalog}")
print(f"   Schema: {config.schema}")
print(f"   LLM: {config.llm_model}")
print(f"   Monitoring {len(config.cities_to_monitor)} cities")

# COMMAND ----------

# DBTITLE 1,Display Configuration
import json

print("📊 Configuration Summary")
print("=" * 50)
print(json.dumps(config.to_dict(), indent=2))

# COMMAND ----------

# DBTITLE 1,Export Config to Widgets
# Export key config values as notebook widgets for downstream notebooks
dbutils.widgets.text("catalog", config.catalog, "Unity Catalog Name")
dbutils.widgets.text("schema", config.schema, "Schema Name")
dbutils.widgets.dropdown("env", config.env, ["dev", "staging", "prod"], "Environment")

print("✅ Configuration widgets created")
print("   These can be accessed by downstream notebooks via dbutils.widgets.get()")

# COMMAND ----------

