# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregate Patterns
# MAGIC Summarizes common issues found across all notebooks.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern Summary

# COMMAND ----------

display(spark.sql(f"""
SELECT
    issue_category,
    COUNT(*) as occurrences,
    ROUND(AVG(severity_score), 1) as avg_severity,
    COLLECT_SET(notebook_path) as affected_notebooks
FROM {RESULTS_TABLE}
LATERAL VIEW EXPLODE(issue_categories) t AS issue_category
GROUP BY issue_category
ORDER BY occurrences DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix Guidance

# COMMAND ----------

GUIDANCE = {
    "COLLECT_ANTIPATTERN": "Avoid .collect() on large data. Use Spark transformations or .limit() first.",
    "TOPANDAS_ANTIPATTERN": "Avoid .toPandas() on large data. Use .limit() or Spark native operations.",
    "PYTHON_UDF": "Replace Python UDFs with Spark SQL functions or pandas UDFs for better performance.",
    "MISSING_BROADCAST": "Use F.broadcast() for small tables (<10MB) in joins.",
    "SELECT_STAR": "Select only needed columns instead of SELECT *.",
    "NO_PARTITION_FILTER": "Always filter on partition columns to avoid full table scans.",
    "MISSING_CACHE": "Use .cache() when a DataFrame is reused multiple times.",
    "CROSS_JOIN": "Avoid cross joins - they create cartesian products.",
    "OTHER": "Review the specific issue in the analysis results."
}

# COMMAND ----------

# Build patterns table
patterns_df = spark.sql(f"""
SELECT
    issue_category as pattern_name,
    CAST(COUNT(*) AS INT) as occurrence_count,
    CAST(ROUND(AVG(severity_score), 2) AS DOUBLE) as avg_severity,
    COLLECT_SET(notebook_path) as affected_notebooks
FROM {RESULTS_TABLE}
LATERAL VIEW EXPLODE(issue_categories) t AS issue_category
GROUP BY issue_category
""")

patterns = patterns_df.collect()

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType

rows = []
for p in patterns:
    rows.append((
        p.pattern_name,
        int(p.occurrence_count),
        float(p.avg_severity),
        list(p.affected_notebooks),
        GUIDANCE.get(p.pattern_name, GUIDANCE["OTHER"]),
        datetime.now()
    ))

schema = StructType([
    StructField("pattern_name", StringType()),
    StructField("occurrence_count", IntegerType()),
    StructField("avg_severity", DoubleType()),
    StructField("affected_notebooks", ArrayType(StringType())),
    StructField("fix_guidance", StringType()),
    StructField("updated_at", TimestampType())
])

if rows:
    df_patterns = spark.createDataFrame(rows, schema)
    df_patterns.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PATTERNS_TABLE)
    print(f"Patterns saved to {PATTERNS_TABLE}")

# COMMAND ----------

display(spark.table(PATTERNS_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Priority Matrix

# COMMAND ----------

display(spark.sql(f"""
SELECT
    pattern_name,
    occurrence_count,
    avg_severity,
    CASE
        WHEN avg_severity >= 7 THEN 'HIGH'
        WHEN avg_severity >= 4 THEN 'MEDIUM'
        ELSE 'LOW'
    END as priority,
    fix_guidance
FROM {PATTERNS_TABLE}
ORDER BY avg_severity DESC
"""))
