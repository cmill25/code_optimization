# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Notebooks
# MAGIC Extracts notebook code from Databricks Workspace/Repos for analysis.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import datetime
import uuid
import base64
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Update these paths to point to your notebooks.

# COMMAND ----------

# Paths to scan for notebooks (add your paths here)
SCAN_PATHS = [
    "/Repos/production",           # Example: production repo
    # "/Repos/your-org/your-repo",
    # "/Workspace/Shared/ETL",
    # "/Workspace/Users/team@company.com/projects",
]

# Optional: filter by naming pattern (set to None to include all)
INCLUDE_PATTERN = None  # e.g., "_etl" to only include notebooks with "_etl" in the name
EXCLUDE_PATTERN = None  # e.g., "_test" to exclude test notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extraction Functions

# COMMAND ----------

def list_notebooks_recursive(path):
    """Recursively list all notebooks under a path."""
    notebooks = []

    try:
        items = dbutils.workspace.list(path)
        if items is None:
            return notebooks

        for item in items:
            if item.object_type == "NOTEBOOK":
                notebooks.append({
                    "path": item.path,
                    "name": item.path.split("/")[-1],
                    "language": getattr(item, "language", "PYTHON")
                })
            elif item.object_type == "DIRECTORY":
                notebooks.extend(list_notebooks_recursive(item.path))
    except Exception as e:
        print(f"Warning: Could not access {path}: {e}")

    return notebooks

# COMMAND ----------

def extract_notebook_content(path):
    """Export notebook source code."""
    try:
        content_b64 = dbutils.workspace.export(path, format="SOURCE")
        content = base64.b64decode(content_b64).decode("utf-8")
        return content, len(content), None
    except Exception as e:
        return None, 0, str(e)

# COMMAND ----------

def should_include(notebook_name):
    """Check if notebook should be included based on filters."""
    if INCLUDE_PATTERN and INCLUDE_PATTERN not in notebook_name:
        return False
    if EXCLUDE_PATTERN and EXCLUDE_PATTERN in notebook_name:
        return False
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan and Extract

# COMMAND ----------

# Discover all notebooks
print("Scanning for notebooks...")
all_notebooks = []

for scan_path in SCAN_PATHS:
    print(f"  Scanning: {scan_path}")
    found = list_notebooks_recursive(scan_path)
    print(f"    Found {len(found)} notebooks")
    all_notebooks.extend(found)

print(f"\nTotal notebooks discovered: {len(all_notebooks)}")

# COMMAND ----------

# Apply filters
filtered_notebooks = [nb for nb in all_notebooks if should_include(nb["name"])]
print(f"Notebooks after filtering: {len(filtered_notebooks)}")

# COMMAND ----------

# Extract content
print("\nExtracting notebook content...")
rows = []
errors = []

for i, nb in enumerate(filtered_notebooks):
    if (i + 1) % 100 == 0:
        print(f"  Processed {i + 1}/{len(filtered_notebooks)}")

    content, size, error = extract_notebook_content(nb["path"])

    if error:
        errors.append({"path": nb["path"], "error": error})
        continue

    if content:
        rows.append(Row(
            notebook_id=str(uuid.uuid4()),
            notebook_path=nb["path"],
            notebook_name=nb["name"],
            language=nb["language"].lower() if nb["language"] else "python",
            code_content=content,
            size_bytes=size,
            extracted_at=datetime.now()
        ))

print(f"\nSuccessfully extracted: {len(rows)}")
print(f"Errors: {len(errors)}")

# COMMAND ----------

# Show any errors
if errors:
    print("Extraction errors:")
    for e in errors[:10]:
        print(f"  {e['path']}: {e['error']}")
    if len(errors) > 10:
        print(f"  ... and {len(errors) - 10} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Inventory Table

# COMMAND ----------

if rows:
    df = spark.createDataFrame(rows)
    df.write.mode("overwrite").saveAsTable(NOTEBOOKS_TABLE)
    print(f"Wrote {len(rows)} notebooks to {NOTEBOOKS_TABLE}")
else:
    print("No notebooks extracted. Check your SCAN_PATHS configuration.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

if rows:
    display(spark.sql(f"""
    SELECT
        COUNT(*) as total_notebooks,
        SUM(size_bytes) as total_bytes,
        ROUND(SUM(size_bytes) / 1024 / 1024, 2) as total_mb,
        ROUND(AVG(size_bytes) / 1024, 2) as avg_kb
    FROM {NOTEBOOKS_TABLE}
    """))

# COMMAND ----------

if rows:
    display(spark.sql(f"""
    SELECT notebook_path, language, size_bytes
    FROM {NOTEBOOKS_TABLE}
    ORDER BY size_bytes DESC
    LIMIT 20
    """))
