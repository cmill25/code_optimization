# Databricks notebook source
# MAGIC %md
# MAGIC # Explore Results
# MAGIC Interactive exploration of analysis results.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall Summary

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) as total_notebooks,
    SUM(CASE WHEN severity_score >= 7 THEN 1 ELSE 0 END) as high_severity,
    SUM(CASE WHEN severity_score BETWEEN 4 AND 6 THEN 1 ELSE 0 END) as medium_severity,
    SUM(CASE WHEN severity_score <= 3 THEN 1 ELSE 0 END) as low_severity,
    ROUND(AVG(severity_score), 1) as avg_severity
FROM {RESULTS_TABLE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Issues by Category

# COMMAND ----------

display(spark.sql(f"""
SELECT
    issue_category,
    COUNT(*) as count,
    ROUND(AVG(severity_score), 1) as avg_severity
FROM {RESULTS_TABLE}
LATERAL VIEW EXPLODE(issue_categories) t AS issue_category
GROUP BY issue_category
ORDER BY count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Priority Issues (Severity >= 7)

# COMMAND ----------

display(spark.sql(f"""
SELECT
    notebook_path,
    severity_score,
    estimated_impact,
    issue_categories,
    reasoning
FROM {RESULTS_TABLE}
WHERE severity_score >= 7
ORDER BY severity_score DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Results

# COMMAND ----------

display(spark.sql(f"""
SELECT
    notebook_path,
    severity_score,
    estimated_impact,
    issue_categories,
    reasoning,
    model_used,
    analyzed_at
FROM {RESULTS_TABLE}
ORDER BY severity_score DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code Comparison
# MAGIC Select a notebook to see original vs optimized code.

# COMMAND ----------

# Get notebook list for widget
notebooks = [row.notebook_path for row in spark.table(RESULTS_TABLE).select("notebook_path").distinct().collect()]
if notebooks:
    dbutils.widgets.dropdown("notebook", notebooks[0], notebooks, "Select Notebook")

# COMMAND ----------

selected = dbutils.widgets.get("notebook")
result = spark.table(RESULTS_TABLE).filter(f"notebook_path = '{selected}'").collect()
original = spark.table(NOTEBOOKS_TABLE).filter(f"notebook_path = '{selected}'").collect()

if result and original:
    r = result[0]
    o = original[0]

    print("=" * 60)
    print(f"NOTEBOOK: {selected}")
    print(f"Severity: {r.severity_score} | Impact: {r.estimated_impact}")
    print(f"Issues: {r.issue_categories}")
    print("=" * 60)

    print("\nREASONING:")
    print(r.reasoning)

    print("\n" + "=" * 60)
    print("ORIGINAL CODE:")
    print("=" * 60)
    print(o.code_content)

    print("\n" + "=" * 60)
    print("OPTIMIZED CODE:")
    print("=" * 60)
    print(r.optimized_code if r.optimized_code else "No optimization provided")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern Fix Guidance

# COMMAND ----------

display(spark.sql(f"""
SELECT pattern_name, occurrence_count, avg_severity, fix_guidance
FROM {PATTERNS_TABLE}
ORDER BY occurrence_count DESC
"""))
