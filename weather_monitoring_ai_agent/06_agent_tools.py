# Databricks notebook source
# DBTITLE 1,Agent Python Tools
# MAGIC %md
# MAGIC # 🛠️ Agent Tools - Simulation & Logic
# MAGIC
# MAGIC ## Purpose
# MAGIC Python tools for simulation, impact assessment, and response logic.
# MAGIC
# MAGIC ## Tools
# MAGIC - Disaster impact simulation
# MAGIC - Evacuation route generation
# MAGIC - Resource requirement calculations
# MAGIC - Threshold checking
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Configuration loaded

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run ./01_configuration

# COMMAND ----------

# DBTITLE 1,Simulation Tools Class
from typing import Dict, Any, List
import json

class DisasterSimulationTools:
    """Python tools for disaster simulation and response generation."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def simulate_disaster_impact(self, disaster_type: str, severity: str, population: int = 100000) -> Dict[str, Any]:
        """Simulate the potential impact of a disaster."""
        impact_factors = {
            "Critical": {"affected_pct": 0.8, "evacuation_needed_pct": 0.6, "infrastructure_risk": "Severe"},
            "High": {"affected_pct": 0.5, "evacuation_needed_pct": 0.3, "infrastructure_risk": "High"},
            "Medium": {"affected_pct": 0.2, "evacuation_needed_pct": 0.1, "infrastructure_risk": "Moderate"},
            "Low": {"affected_pct": 0.05, "evacuation_needed_pct": 0.0, "infrastructure_risk": "Low"}
        }
        
        factors = impact_factors.get(severity, impact_factors["Low"])
        
        return {
            "disaster_type": disaster_type,
            "severity": severity,
            "population": population,
            "estimated_affected": int(population * factors["affected_pct"]),
            "evacuation_needed": int(population * factors["evacuation_needed_pct"]),
            "infrastructure_risk": factors["infrastructure_risk"],
            "response_priority": "Immediate" if severity in ["Critical", "High"] else "Standard"
        }
    
    def generate_evacuation_routes(self, city: str, disaster_type: str) -> List[Dict[str, str]]:
        """Generate recommended evacuation routes."""
        routes = [
            {"route_id": "R1", "name": f"{city} Highway A to Safe Zone North", "capacity": "High", "estimated_time": "45 min"},
            {"route_id": "R2", "name": f"{city} Main Road to Shelter East", "capacity": "Medium", "estimated_time": "30 min"},
            {"route_id": "R3", "name": f"{city} Transit Line to Emergency Center", "capacity": "High", "estimated_time": "60 min"}
        ]
        return routes
    
    def calculate_resource_requirements(self, affected_population: int, disaster_type: str) -> Dict[str, Any]:
        """Calculate emergency resource requirements."""
        resources = {
            "medical_personnel": int(affected_population * 0.02),
            "emergency_shelters": int(affected_population / 500),
            "food_supply_days": 7,
            "water_liters": affected_population * 15,
            "blankets": int(affected_population * 0.6),
            "first_aid_kits": int(affected_population / 50)
        }
        
        if disaster_type in ["Hurricane", "Flood"]:
            resources["rescue_boats"] = int(affected_population / 1000)
            resources["waterproof_supplies"] = True
        elif disaster_type == "Heatwave":
            resources["cooling_centers"] = int(affected_population / 2000)
            resources["water_liters"] *= 2
        
        return resources
    
    def check_threshold_exceeded(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        """Check if weather metrics exceed disaster thresholds."""
        thresholds = self.config.thresholds
        
        wind_speed_ms = metrics.get("wind_speed_kmh", 0) / 3.6
        temp_c = metrics.get("temperature_celsius", 20)
        precip_mm = metrics.get("precipitation_mm", 0)
        
        exceeded = {
            "hurricane": wind_speed_ms >= thresholds["hurricane_wind_speed_ms"],
            "flood": precip_mm >= thresholds["flood_rainfall_mm"],
            "heatwave": temp_c >= thresholds["heatwave_temp_c"],
            "severe_storm": wind_speed_ms >= thresholds["severe_storm_wind_speed_ms"],
            "winter_storm": temp_c <= thresholds["winter_storm_temp_c"]
        }
        
        return {
            "any_threshold_exceeded": any(exceeded.values()),
            "exceeded_thresholds": [k for k, v in exceeded.items() if v],
            "metrics": metrics,
            "thresholds": thresholds
        }

# Initialize tools
sim_tools = DisasterSimulationTools(config)

print("✅ Disaster simulation tools initialized")

# COMMAND ----------

# DBTITLE 1,Test Tools
# Test impact simulation
impact = sim_tools.simulate_disaster_impact("Hurricane", "Critical", 100000)
print("Impact Simulation:")
print(json.dumps(impact, indent=2))

# Test resource calculation
resources = sim_tools.calculate_resource_requirements(80000, "Hurricane")
print("\nResource Requirements:")
print(json.dumps(resources, indent=2))

# COMMAND ----------

