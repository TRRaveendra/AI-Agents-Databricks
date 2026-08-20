# Databricks notebook source
# DBTITLE 1,Silver Layer - Data Processing
# MAGIC %md
# MAGIC # 🥈 Silver Layer - Validated Weather Metrics
# MAGIC
# MAGIC ## Purpose
# MAGIC Transform Bronze data into validated, normalized, business-ready metrics.
# MAGIC
# MAGIC ## Features
# MAGIC - Temperature conversions (Kelvin → Celsius)
# MAGIC - Unit normalization
# MAGIC - Data quality checks
# MAGIC - Disaster condition flags
# MAGIC - MERGE pattern for CDC
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Bronze table must have data

# COMMAND ----------

# DBTITLE 1,Load Configuration
# MAGIC %run ./01_configuration

# COMMAND ----------

# DBTITLE 1,Create Silver Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS weather_disaster.weather_data.silver_weather_metrics (
# MAGIC   metric_id STRING,
# MAGIC   city STRING,
# MAGIC   temperature_celsius DOUBLE,
# MAGIC   feels_like_celsius DOUBLE,
# MAGIC   wind_speed_kmh DOUBLE,
# MAGIC   wind_speed_mph DOUBLE,
# MAGIC   humidity_percent INT,
# MAGIC   precipitation_mm DOUBLE,
# MAGIC   weather_description STRING,
# MAGIC   is_extreme_weather BOOLEAN,
# MAGIC   is_hurricane_conditions BOOLEAN,
# MAGIC   is_flood_conditions BOOLEAN,
# MAGIC   is_heatwave_conditions BOOLEAN,
# MAGIC   is_severe_storm BOOLEAN,
# MAGIC   is_winter_storm BOOLEAN,
# MAGIC   observation_timestamp TIMESTAMP,
# MAGIC   processed_timestamp TIMESTAMP,
# MAGIC   bronze_request_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Silver layer: Validated and normalized weather metrics';

# COMMAND ----------

# DBTITLE 1,Silver Processing
from pyspark.sql import functions as F
import uuid

class WeatherSilverProcessing:
    def __init__(self, config: Config):
        self.config = config
    
    def process_bronze_to_silver(self):
        """Transform Bronze to Silver."""
        df_bronze = spark.table(self.config.bronze_weather_table)
        
        # Unit conversions and transformations
        df_silver = df_bronze.select(
            F.lit(str(uuid.uuid4())).alias("metric_id"),
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
        
        # Write to Silver (MERGE for upserts)
        df_silver.createOrReplaceTempView("silver_updates")
        
        merge_sql = f"""
        MERGE INTO {self.config.silver_metrics_table} AS target
        USING silver_updates AS source
        ON target.bronze_request_id = source.bronze_request_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"✅ Silver processing complete")
        
        return df_silver.count()

# Run Silver processing
silver_processor = WeatherSilverProcessing(config)
records_processed = silver_processor.process_bronze_to_silver()

print(f"\n✅ {records_processed} records processed to Silver")
if records_processed > 0:
    display(spark.table(config.silver_metrics_table).limit(10))

# COMMAND ----------

