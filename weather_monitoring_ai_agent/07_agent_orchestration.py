# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,AI Agent Orchestration
# MAGIC %md
# MAGIC # 🤖 AI Agent - LangGraph Orchestration
# MAGIC
# MAGIC ## Purpose
# MAGIC LangGraph state machine for automated disaster response.
# MAGIC
# MAGIC ## Features
# MAGIC - State-based orchestration
# MAGIC - Conditional routing based on severity
# MAGIC - Multi-channel alert integration
# MAGIC - Human-in-the-loop for critical events
# MAGIC - LLM-powered response generation
# MAGIC
# MAGIC ## Dependencies
# MAGIC - UC Functions must be created
# MAGIC - Agent tools loaded

# COMMAND ----------

# DBTITLE 1,📦 Install Dependencies
# MAGIC %pip install --quiet langgraph langchain-core langchain-databricks
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load Configuration & Tools
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
        
        self.alert_channels = {
            "email": {"enabled": False},
            "slack": {"enabled": False},
            "pagerduty": {"enabled": False}
        }
        
        self.cities_to_monitor = [
            {"name": "London", "country": "GB"},
            {"name": "New York", "country": "US"},
            {"name": "Tokyo", "country": "JP"},
            {"name": "Mumbai", "country": "IN"},
            {"name": "Sydney", "country": "AU"}
        ]

# Initialize config
config = Config(env="dev")
print("✅ Configuration loaded")

# COMMAND ----------

# DBTITLE 1,Agent State & LLM
from typing import TypedDict, Annotated, Sequence, Dict, Any, List
from langgraph.graph import StateGraph, END
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_databricks import ChatDatabricks
import operator

# Define Agent State
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

# Initialize LLM (Databricks Llama 3.3)
llm = ChatDatabricks(
    endpoint=config.llm_endpoint,
    temperature=config.llm_temperature,
    max_tokens=config.llm_max_tokens
)

print("✅ Agent state and LLM initialized")

# COMMAND ----------

# DBTITLE 1,Graph Nodes
def retrieve_weather_node(state: AgentState) -> AgentState:
    """Node 1: Retrieve current weather via UC Function."""
    city = state["city"]
    print(f"\n🌤️ Retrieving weather for {city}...")
    
    df_weather = spark.sql(f"""
        SELECT * FROM weather_disaster.weather_data.get_current_weather('{city}')
    """)
    
    if df_weather.count() > 0:
        weather_row = df_weather.first().asDict()
        state["current_weather"] = weather_row
        state["messages"].append(AIMessage(content=f"Weather: {weather_row['temperature_celsius']}°C"))
    else:
        state["current_weather"] = {}
        state["messages"].append(AIMessage(content="No weather data"))
    
    return state

def analyze_disaster_node(state: AgentState) -> AgentState:
    """Node 2: Analyze for disaster conditions."""
    weather = state["current_weather"]
    
    if not weather:
        state["disaster_detected"] = False
        state["next_action"] = "end"
        return state
    
    print("\n🔍 Analyzing disaster conditions...")
    
    # Call UC Function for risk assessment
    severity = spark.sql(f"""
        SELECT weather_disaster.weather_data.assess_risk_level(
            {weather.get('wind_speed_kmh', 0)},
            {weather.get('temperature_celsius', 20)},
            {weather.get('precipitation_mm', 0)}
        ) as risk_level
    """).first()["risk_level"]
    
    state["severity_level"] = severity
    
    if severity in ["Critical", "High"]:
        state["disaster_detected"] = True
        state["disaster_type"] = "Extreme Weather"
        state["next_action"] = "assess_risk"
    else:
        state["disaster_detected"] = False
        state["next_action"] = "end"
    
    return state

def generate_response_node(state: AgentState) -> AgentState:
    """Node 3: Generate emergency response plan."""
    print("\n📝 Generating response plan...")
    
    weather = state["current_weather"]
    
    prompt = f"""
Generate an emergency response plan for {state['city']}.

Severity: {state['severity_level']}
Conditions: {weather.get('temperature_celsius', 'N/A')}°C, Wind: {weather.get('wind_speed_kmh', 'N/A')} km/h

Provide:
1. Immediate Actions (0-2 hours)
2. Communication Strategy
3. Safety Protocols

Be concise and actionable.
"""
    
    response = llm.invoke([HumanMessage(content=prompt)])
    state["response_plan"] = response.content
    state["human_verification_needed"] = state["severity_level"] == "Critical"
    state["next_action"] = "end"
    
    return state

print("✅ Graph nodes defined")

# COMMAND ----------

# DBTITLE 1,Build & Compile Graph
# Build the graph
workflow = StateGraph(AgentState)

# Add nodes
workflow.add_node("retrieve_weather", retrieve_weather_node)
workflow.add_node("analyze_disaster", analyze_disaster_node)
workflow.add_node("generate_response", generate_response_node)

# Define edges
workflow.set_entry_point("retrieve_weather")
workflow.add_edge("retrieve_weather", "analyze_disaster")

def route_after_analysis(state: AgentState):
    if state["disaster_detected"]:
        return "generate_response"
    else:
        return END

workflow.add_conditional_edges(
    "analyze_disaster",
    route_after_analysis,
    {
        "generate_response": "generate_response",
        END: END
    }
)

workflow.add_edge("generate_response", END)

# Compile the graph
app = workflow.compile()

print("✅ LangGraph agent compiled")

# COMMAND ----------

# DBTITLE 1,Test Agent
# Test the agent
initial_state = AgentState(
    messages=[],
    city="London",
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

print("\n🤖 Running agent for London...")
final_state = app.invoke(initial_state)

print("\n✅ Agent execution complete")
print(f"Disaster Detected: {final_state['disaster_detected']}")
print(f"Severity: {final_state['severity_level']}")
if final_state['response_plan']:
    print(f"\nResponse Plan:\n{final_state['response_plan']}")

# COMMAND ----------

