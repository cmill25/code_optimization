# Databricks notebook source
# MAGIC %md
# MAGIC # Setup
# MAGIC Creates schema, volume, and tables in Unity Catalog.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_FULL}")
print(f"Schema ready: {SCHEMA_FULL}")

# COMMAND ----------

# Create volume for raw notebook files
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_FULL}.raw_notebooks")
print(f"Volume ready: {VOLUME_PATH}")

# COMMAND ----------

# Notebooks inventory table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {NOTEBOOKS_TABLE} (
  notebook_id STRING,
  notebook_path STRING,
  notebook_name STRING,
  language STRING,
  code_content STRING,
  size_bytes LONG,
  extracted_at TIMESTAMP
)
USING DELTA
""")
print(f"Table ready: {NOTEBOOKS_TABLE}")

# COMMAND ----------

# Analysis results table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
  notebook_id STRING,
  notebook_path STRING,
  severity_score INT,
  estimated_impact STRING,
  issue_categories ARRAY<STRING>,
  reasoning STRING,
  optimized_code STRING,
  model_used STRING,
  analyzed_at TIMESTAMP
)
USING DELTA
""")
print(f"Table ready: {RESULTS_TABLE}")

# COMMAND ----------

# Optimization patterns table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PATTERNS_TABLE} (
  pattern_name STRING,
  occurrence_count INT,
  avg_severity DOUBLE,
  affected_notebooks ARRAY<STRING>,
  fix_guidance STRING,
  updated_at TIMESTAMP
)
USING DELTA
""")
print(f"Table ready: {PATTERNS_TABLE}")

# COMMAND ----------

print("\nSetup complete!")
