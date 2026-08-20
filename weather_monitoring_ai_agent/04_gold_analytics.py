# Databricks notebook source
# DBTITLE 1,Gold Layer - Business Analytics
# MAGIC %md
# MAGIC # 🥇 Gold Layer - Disaster Analytics
# MAGIC
# MAGIC ## Purpose
# MAGIC Business-ready analytics and disaster event detection.
# MAGIC
# MAGIC ## Features
# MAGIC - Disaster event detection
# MAGIC - Severity assessment
# MAGIC - Daily aggregations
# MAGIC - Liquid Clustering
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Silver table must have data

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run ./01_configuration

# COMMAND ----------

# DBTITLE 1,Create Gold Tables
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS weather_disaster.weather_data.gold_disaster_events (
# MAGIC   event_id STRING,
# MAGIC   city STRING NOT NULL,
# MAGIC   disaster_type STRING,
# MAGIC   severity_level STRING,
# MAGIC   temperature_celsius DOUBLE,
# MAGIC   wind_speed_kmh DOUBLE,
# MAGIC   precipitation_mm DOUBLE,
# MAGIC   humidity_percent INT,
# MAGIC   event_start_time TIMESTAMP,
# MAGIC   event_end_time TIMESTAMP,
# MAGIC   duration_hours DOUBLE,
# MAGIC   is_active BOOLEAN,
# MAGIC   response_plan_generated BOOLEAN,
# MAGIC   alerts_sent_count INT,
# MAGIC   human_verification_required BOOLEAN,
# MAGIC   human_verified_at TIMESTAMP,
# MAGIC   detected_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (city, detected_at);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS weather_disaster.weather_data.gold_severity_analytics (
# MAGIC   city STRING,
# MAGIC   analysis_date DATE,
# MAGIC   avg_temperature_celsius DOUBLE,
# MAGIC   max_temperature_celsius DOUBLE,
# MAGIC   min_temperature_celsius DOUBLE,
# MAGIC   avg_wind_speed_kmh DOUBLE,
# MAGIC   max_wind_speed_kmh DOUBLE,
# MAGIC   total_precipitation_mm DOUBLE,
# MAGIC   total_events INT,
# MAGIC   critical_events INT,
# MAGIC   high_severity_events INT,
# MAGIC   created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Gold Processing
from pyspark.sql import functions as F
import uuid

class WeatherGoldProcessing:
    def __init__(self, config: Config):
        self.config = config
    
    def detect_disaster_events(self):
        """Detect and record disaster events."""
        df_silver = spark.table(self.config.silver_metrics_table)
        
        df_events = df_silver.filter(
            (F.col("is_hurricane_conditions") == True) |
            (F.col("is_flood_conditions") == True) |
            (F.col("is_heatwave_conditions") == True) |
            (F.col("is_severe_storm") == True) |
            (F.col("is_winter_storm") == True)
        )
        
        if df_events.count() == 0:
            print("ℹ️ No disaster conditions detected")
            return
        
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
        
        df_gold_events.write.format("delta").mode("append").saveAsTable(self.config.gold_events_table)
        print(f"✅ Detected {df_gold_events.count()} disaster events")

# Run Gold processing
gold_processor = WeatherGoldProcessing(config)
gold_processor.detect_disaster_events()

print("\n🔍 Gold Events Sample:")
display(spark.table(config.gold_events_table).orderBy("detected_at", ascending=False).limit(10))

# COMMAND ----------

