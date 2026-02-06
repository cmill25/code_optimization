# Databricks notebook source
# MAGIC %md
# MAGIC # Analyze Notebooks
# MAGIC Uses Databricks Foundation Models to analyze code for optimization opportunities.

# COMMAND ----------

# MAGIC %pip install openai -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from openai import OpenAI
from datetime import datetime
import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup OpenAI Client for Databricks

# COMMAND ----------

# Get Databricks token and workspace URL
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

# Initialize OpenAI client pointing to Databricks
client = OpenAI(
    api_key=token,
    base_url=f"https://{host}/serving-endpoints"
)

print(f"Connected to: {host}")
print(f"Using model: {MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Functions

# COMMAND ----------

PROMPT_TEMPLATE = """You are a Databricks Spark performance expert. Analyze this {language} code for performance issues and anti-patterns.

CODE:
```
{code}
```

Return ONLY valid JSON (no markdown, no code fences):
{{
  "issues": [
    {{"category": "CATEGORY", "description": "what's wrong", "severity": 8}}
  ],
  "reasoning": "summary of all problems found and how they were fixed",
  "optimized_code": "the complete optimized code",
  "estimated_impact": "high"
}}

CRITICAL RULES FOR optimized_code:
- Return the COMPLETE notebook code with all fixes applied
- Output ONLY valid, executable PySpark code
- NO comments explaining what was changed
- NO reasoning or explanations in the code
- NO alternative options or suggestions
- NO markdown formatting
- Just clean, production-ready PySpark code

Categories: COLLECT_ANTIPATTERN, TOPANDAS_ANTIPATTERN, PYTHON_UDF, MISSING_BROADCAST, SELECT_STAR, NO_PARTITION_FILTER, MISSING_CACHE, CROSS_JOIN, OTHER

Severity: 1-10 (10=critical)
Impact: low/medium/high"""

# COMMAND ----------

def analyze_code(code: str, language: str) -> dict:
    """Call Foundation Model to analyze code."""
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a Spark optimization expert. Respond only with valid JSON. The optimized_code field must contain only clean, executable PySpark code with no comments about changes."},
                {"role": "user", "content": PROMPT_TEMPLATE.format(language=language, code=code)}
            ],
            max_tokens=8192,
            temperature=0.1
        )

        text = response.choices[0].message.content

        # Extract JSON from response
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            data = json.loads(json_match.group())

            # Extract max severity and categories
            categories = []
            max_sev = 0
            for issue in data.get("issues", []):
                categories.append(issue.get("category", "OTHER"))
                sev = issue.get("severity", 0)
                if isinstance(sev, str):
                    sev = int(sev)
                max_sev = max(max_sev, sev)

            return {
                "severity_score": max_sev,
                "estimated_impact": data.get("estimated_impact", "medium"),
                "issue_categories": categories,
                "reasoning": data.get("reasoning", ""),
                "optimized_code": data.get("optimized_code", ""),
                "success": True
            }
    except Exception as e:
        print(f"Error: {e}")

    return {
        "severity_score": 0,
        "estimated_impact": "unknown",
        "issue_categories": [],
        "reasoning": "Analysis failed",
        "optimized_code": "",
        "success": False
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Analysis

# COMMAND ----------

# Load notebooks to analyze
df_notebooks = spark.table(NOTEBOOKS_TABLE)
notebooks = df_notebooks.collect()
print(f"Analyzing {len(notebooks)} notebooks...")

# COMMAND ----------

# Analyze each notebook
results = []
for nb in notebooks:
    print(f"\nAnalyzing: {nb.notebook_name}")

    analysis = analyze_code(nb.code_content, nb.language)

    results.append({
        "notebook_id": nb.notebook_id,
        "notebook_path": nb.notebook_path,
        "severity_score": analysis["severity_score"],
        "estimated_impact": analysis["estimated_impact"],
        "issue_categories": analysis["issue_categories"],
        "reasoning": analysis["reasoning"],
        "optimized_code": analysis["optimized_code"],
        "model_used": MODEL,
        "analyzed_at": datetime.now()
    })

    status = "OK" if analysis["success"] else "FAILED"
    print(f"  [{status}] Severity: {analysis['severity_score']}, Issues: {analysis['issue_categories']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("notebook_id", StringType()),
    StructField("notebook_path", StringType()),
    StructField("severity_score", IntegerType()),
    StructField("estimated_impact", StringType()),
    StructField("issue_categories", ArrayType(StringType())),
    StructField("reasoning", StringType()),
    StructField("optimized_code", StringType()),
    StructField("model_used", StringType()),
    StructField("analyzed_at", TimestampType())
])

df_results = spark.createDataFrame(results, schema)
df_results.write.mode("overwrite").saveAsTable(RESULTS_TABLE)

print(f"\nResults saved to {RESULTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN severity_score >= 7 THEN 1 ELSE 0 END) as high_severity,
    SUM(CASE WHEN severity_score BETWEEN 4 AND 6 THEN 1 ELSE 0 END) as medium_severity,
    SUM(CASE WHEN severity_score <= 3 THEN 1 ELSE 0 END) as low_severity,
    ROUND(AVG(severity_score), 1) as avg_severity
FROM {RESULTS_TABLE}
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT notebook_path, severity_score, estimated_impact, issue_categories, reasoning
FROM {RESULTS_TABLE}
ORDER BY severity_score DESC
"""))
