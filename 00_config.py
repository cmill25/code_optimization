# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration
# MAGIC Shared configuration for the code optimization solution.

# COMMAND ----------

# Widgets
dbutils.widgets.text("catalog", "cmill_demo", "Catalog")
dbutils.widgets.text("schema", "optimization", "Schema")
MODEL = "databricks-gpt-5-2"

# COMMAND ----------

# Configuration
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# Full paths
SCHEMA_FULL = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_notebooks"

# Tables
NOTEBOOKS_TABLE = f"{SCHEMA_FULL}.notebooks_inventory"
RESULTS_TABLE = f"{SCHEMA_FULL}.analysis_results"
PATTERNS_TABLE = f"{SCHEMA_FULL}.optimization_patterns"

# COMMAND ----------

print(f"""
Configuration:
  Catalog:  {CATALOG}
  Schema:   {SCHEMA}
  Model:    {MODEL}

Tables:
  {NOTEBOOKS_TABLE}
  {RESULTS_TABLE}
  {PATTERNS_TABLE}
""")
