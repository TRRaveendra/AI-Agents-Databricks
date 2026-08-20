# Databricks notebook source
# DBTITLE 1,MLflow Deployment
# MAGIC %md
# MAGIC # 🚀 MLflow Deployment - Model Serving
# MAGIC
# MAGIC ## Purpose
# MAGIC Package and deploy the AI agent to Databricks Model Serving.
# MAGIC
# MAGIC ## Features
# MAGIC - MLflow model packaging
# MAGIC - Unity Catalog registration
# MAGIC - Model Serving endpoint creation
# MAGIC - Inference table logging
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Agent must be tested and working

# COMMAND ----------

# DBTITLE 1,📦 Install Dependencies
# MAGIC %pip install --quiet mlflow langchain-core
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load Configuration
# Load configuration - recreate Config object
# Cannot use %run when executed via dbutils.notebook.run()
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
        
        # Table names
        self.bronze_weather_table = f"{self.catalog}.{self.schema}.bronze_weather_data"
        self.silver_metrics_table = f"{self.catalog}.{self.schema}.silver_weather_metrics"
        self.gold_events_table = f"{self.catalog}.{self.schema}.gold_disaster_events"
        self.gold_analytics_table = f"{self.catalog}.{self.schema}.gold_severity_analytics"
        
        # API Configuration
        try:
            self.weather_api_key = dbutils.secrets.get(scope="weather-disaster-secrets", key="weather-api-key")
        except:
            self.weather_api_key = None
        
        self.weather_api_base_url = "https://api.openweathermap.org/data/2.5"
        self.weather_api_timeout = 10
        
        # LLM Configuration
        self.llm_provider = "databricks"
        self.llm_model = "databricks-meta-llama-3-3-70b-instruct"
        self.llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"
        self.llm_temperature = 0.1
        self.llm_max_tokens = 2000
        
        # Disaster Thresholds
        self.thresholds = {
            "hurricane_wind_speed_ms": 33.0,
            "flood_rainfall_mm": 100.0,
            "heatwave_temp_c": 40.0,
            "severe_storm_wind_speed_ms": 20.0,
            "winter_storm_temp_c": -10.0
        }
        
        self.severity_thresholds = {
            "critical": {"wind_speed_ms": 40.0, "temp_extreme_c": 40.0},
            "high": {"wind_speed_ms": 30.0, "temp_extreme_c": 35.0},
            "medium": {"wind_speed_ms": 20.0, "temp_extreme_c": 30.0}
        }

# Initialize config
config = Config(env="dev")
print("✅ Configuration loaded")

# COMMAND ----------

# DBTITLE 1,MLflow Model Wrapper
import mlflow
from mlflow.models import infer_signature
import pandas as pd
from typing import TypedDict, Annotated, Sequence, Dict, Any, List
from langchain_core.messages import BaseMessage
import operator

# Define AgentState for the model
class AgentState(TypedDict):
    """State that flows through the graph."""
    messages: Annotated[Sequence[BaseMessage], operator.add]
    city: str
    current_weather: Dict[str, Any]
    disaster_detected: bool
    disaster_type: str
    severity_level: str
    impact_assessment: Dict[str, Any]
    response_plan: str
    alerts_sent: List[str]
    human_verification_needed: bool
    next_action: str

class WeatherDisasterAgent(mlflow.pyfunc.PythonModel):
    """MLflow wrapper for the Weather Disaster AI Agent."""
    
    def load_context(self, context):
        """Load the agent graph on model initialization."""
        # In production, reload the compiled graph here
        pass
    
    def predict(self, context, model_input):
        """Run agent for given cities."""
        cities = model_input["city"].tolist()
        results = []
        
        for city in cities:
            initial_state = AgentState(
                messages=[],
                city=city,
                current_weather={},
                disaster_detected=False,
                disaster_type="",
                severity_level="",
                impact_assessment={},
                response_plan="",
                alerts_sent=[],
                human_verification_needed=False,
                next_action=""
            )
            
            # Run agent (in production, use loaded graph)
            # final_state = self.graph.invoke(initial_state)
            
            results.append({
                "city": city,
                "disaster_detected": False,  # Placeholder
                "severity": "Low",
                "response_plan": "No action needed"
            })
        
        return pd.DataFrame(results)

print("✅ MLflow model wrapper defined")

# COMMAND ----------

# DBTITLE 1,Register Model
# Example input/output for signature
input_example = pd.DataFrame({"city": ["London"]})
output_example = pd.DataFrame({
    "city": ["London"],
    "disaster_detected": [False],
    "severity": ["Low"],
    "response_plan": ["No action needed"]
})

signature = infer_signature(input_example, output_example)

# Log model to MLflow
with mlflow.start_run(run_name="weather_disaster_agent_v1") as run:
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=WeatherDisasterAgent(),
        signature=signature,
        input_example=input_example,
        registered_model_name=f"{config.catalog}.{config.schema}.weather_disaster_agent"
    )
    
    # Log parameters
    mlflow.log_params({
        "catalog": config.catalog,
        "schema": config.schema,
        "llm_model": config.llm_model,
        "llm_temperature": config.llm_temperature,
        "llm_max_tokens": config.llm_max_tokens
    })
    
    print(f"✅ Model logged to MLflow")
    print(f"   Run ID: {run.info.run_id}")
    print(f"   Model: {config.catalog}.{config.schema}.weather_disaster_agent")

# COMMAND ----------

