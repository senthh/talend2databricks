# Usage Guide

Complete guide to using `talend2databricks` — from installation through production deployment on Databricks.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Commands](#cli-commands)
  - [analyze](#1-analyze)
  - [convert](#2-convert)
  - [deploy](#3-deploy)
- [Generated Project Structure](#generated-project-structure)
- [Validation & Testing](#validation--testing)
  - [Phase 1: Local Static Checks](#phase-1-local-static-checks)
  - [Phase 2: Local PySpark Dry-Run](#phase-2-local-pyspark-dry-run)
  - [Phase 3: Databricks Schema Validation](#phase-3-databricks-schema-validation)
  - [Phase 4: Sample Data Run](#phase-4-sample-data-run)
  - [Phase 5: Full Production Run](#phase-5-full-production-run)
- [Submitting to Databricks](#submitting-to-databricks)
  - [Option A: Databricks UI (Workspace Import)](#option-a-databricks-ui-workspace-import)
  - [Option B: Databricks CLI](#option-b-databricks-cli)
  - [Option C: Databricks REST API](#option-c-databricks-rest-api)
- [Pre-Deployment Checklist](#pre-deployment-checklist)
- [Adding New Talend Components](#adding-new-talend-components)
- [Troubleshooting](#troubleshooting)

---

## Installation

### Prerequisites

- Python 3.9+
- pip

### Install from source

```bash
git clone https://github.com/senthh/talend2databricks.git
cd talend2databricks
pip install -e .
```

### Verify installation

```bash
talend2db --version
# talend2databricks, version 0.1.0
```

Both `talend2db` and `talend2databricks` work as command aliases.

---

## Quick Start

```bash
# 1. Analyze the Talend job to check migration readiness
talend2db analyze path/to/YOUR_JOB.item

# 2. Generate the Databricks project
talend2db convert path/to/YOUR_JOB.item --output-dir ./my-migrated-job

# 3. Review the generated files
ls ./my-migrated-job/
# main.py  transformations.py  sql_statements.py  connections.py
# config/job.yml  tests/test_main.py  migration-report.json
```

---

## CLI Commands

### 1. `analyze`

Produce a migration readiness report **before** attempting conversion.

```bash
talend2db analyze path/to/job.item
```

**Output includes:**
- Job summary (node count, connection count, contexts)
- Component coverage (which Talend components are supported vs. need manual work)
- Expression translation stats (auto/partial/manual percentages)
- tMap details (input/output flows, complex expressions found)
- Security audit (credential parameters that need secret scope migration)
- Overall auto-conversion estimate

**JSON output** (for CI/automation):

```bash
talend2db analyze path/to/job.item --json-output
```

**Example output:**

```
====================================================================
  MIGRATION READINESS REPORT: LARGE_ENTERPRISE_RPT
====================================================================

📊 Job Summary
  Nodes:       41
  Connections: 36
  Contexts:    3 (Development default)

🧩 Component Coverage
  Found types:    19
  Supported:      19 (100%)

🔀 Expression Translation
  Total expressions:  86
  Auto-translated:    86 (100%)

🔐 Security
  Credential params: 14 (→ Databricks secret scope)
  Secret scope keys needed: 5

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  🎯 Overall Auto-Conversion Estimate: 100%
  ✅ HIGH readiness — mostly automated migration
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### 2. `convert`

Generate a complete Databricks-native project from a Talend `.item` file.

```bash
talend2db convert path/to/job.item --output-dir ./output
```

If `--output-dir` is omitted, it creates a folder named `<job_name>_databricks/` in the current directory.

**What it generates:**

| File | Contains |
|---|---|
| `main.py` | Spark orchestration — runs the full job in correct DAG order |
| `transformations.py` | PySpark DataFrame pipelines (tMap joins, Excel reads, lookups) |
| `sql_statements.py` | All SQL operations (DDL, DML, COPY commands) as Spark SQL |
| `connections.py` | Connection configs with `dbutils.secrets.get()` for credentials |
| `config/job.yml` | Full YAML config (connections, parameters, file paths) |
| `tests/test_main.py` | Test scaffold for the generated code |
| `migration-report.json` | Detailed conversion report with warnings |

### 3. `deploy`

Generate the project **plus** a Databricks Jobs API JSON payload.

```bash
talend2db deploy path/to/job.item \
  --workspace https://your-workspace.cloud.databricks.com \
  --job-name LARGE_ENTERPRISE_RPT
```

This does **not** call the Databricks API — it produces a `deploy.json` file you submit yourself. The output tells you the exact commands to run:

```
To deploy:
  databricks jobs create --json @output/deploy.json
  databricks workspace import-dir output/ /jobs/LARGE_ENTERPRISE_RPT
```

---

## Generated Project Structure

```
output/
├── main.py                  # Entry point — orchestrates the full job
│                            #   1. Loads config
│                            #   2. Sets up connections
│                            #   3. Runs SQL statements in order
│                            #   4. Runs PySpark transformations
│                            #   5. Writes output
│
├── transformations.py       # PySpark transformation logic
│                            #   - Source reads (Redshift/MySQL → spark.read.jdbc)
│                            #   - tMap → DataFrame joins with expressions
│                            #   - File outputs → df.write
│
├── sql_statements.py        # SQL operations as Spark SQL
│                            #   - DDL (CREATE/DROP/ALTER TABLE)
│                            #   - DML (INSERT/UPDATE/DELETE)
│                            #   - COPY commands → cloud storage reads
│                            #   - GRANT statements
│
├── connections.py           # Connection and credential management
│                            #   - dbutils.secrets.get() for passwords
│                            #   - JDBC URL builders
│                            #   - S3 → Unity Catalog path mapping
│
├── config/
│   └── job.yml              # Job configuration
│                            #   - Connection parameters
│                            #   - Context variables
│                            #   - File path mappings
│
├── tests/
│   ├── __init__.py
│   └── test_main.py         # Test scaffold
│
├── migration-report.json    # Full migration analysis
│
└── deploy.json              # (deploy command only) Databricks Jobs API payload
```

---

## Validation & Testing

Follow these phases in order — each catches a different class of errors.

### Phase 1: Local Static Checks

Verify the generated Python is syntactically valid. **No Databricks or Spark needed.**

```bash
# Check each generated file compiles
python -m py_compile output/main.py
python -m py_compile output/transformations.py
python -m py_compile output/sql_statements.py
python -m py_compile output/connections.py

# If no output → all good. Errors print the file/line.
```

You can also lint with `ruff` or `flake8`:

```bash
pip install ruff
ruff check output/
```

### Phase 2: Local PySpark Dry-Run

Verify the generated code imports and the PySpark function calls resolve. **Needs PySpark installed locally, no cluster needed.**

```bash
pip install pyspark
```

```python
# validate_local.py
from pyspark.sql import SparkSession
import importlib.util
import sys

spark = SparkSession.builder.master("local[1]").appName("validate").getOrCreate()

files = ["transformations.py", "sql_statements.py", "connections.py", "main.py"]
for fname in files:
    path = f"output/{fname}"
    spec = importlib.util.spec_from_file_location(fname.replace(".py", ""), path)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
        print(f"  ✅ {fname} — loads cleanly")
    except Exception as e:
        print(f"  ❌ {fname} — {e}")

spark.stop()
```

```bash
python validate_local.py
```

This confirms all `F.when()`, `F.length()`, `F.trim()`, etc. resolve to real PySpark functions.

### Phase 3: Databricks Schema Validation

Run the generated job on Databricks with **zero rows** to validate SQL syntax and table/column references.

**In `transformations.py`**, temporarily add `.limit(0)` after each source read:

```python
# Before
df_row2 = spark.read.jdbc(url, query, ...)

# After (validation only)
df_row2 = spark.read.jdbc(url, query, ...).limit(0)
```

**In `sql_statements.py`**, comment out destructive operations (DROP, DELETE, UPDATE) and run only the SELECT/CREATE statements.

This validates:
- Table names exist in your catalog
- Column names match
- SQL syntax is valid for Spark SQL (some Redshift-specific syntax may need adjustment)

### Phase 4: Sample Data Run

Add `LIMIT 100` to source SQL queries for a cheap end-to-end test:

```python
# In transformations.py, modify the source queries
query = """
SELECT order_stat, sales_doc_item, ...
FROM fact_le_order_shipment a
LEFT JOIN product_final_master b ON ltrim(a.prod_cd,'0') = b.prod_code
LIMIT 100
"""
```

Run the full job. Then compare:

```python
# Row counts
print(f"Output rows: {df_output.count()}")

# Sample values — spot-check a few columns
df_output.select("prod_cd", "country", "segment", "order_status").show(10, truncate=False)
```

Compare these against the original Talend job's output CSV to verify correctness.

### Phase 5: Full Production Run

Remove all `LIMIT` clauses and run the complete job. Compare the final output against the Talend-generated output:

```bash
# If you have the original Talend output CSV
diff <(sort talend_output.csv) <(sort databricks_output.csv)
```

Or in PySpark:

```python
df_talend = spark.read.csv("path/to/talend_output.csv", header=True)
df_new = spark.read.csv("path/to/databricks_output.csv", header=True)

# Row count comparison
print(f"Talend:     {df_talend.count()}")
print(f"Databricks: {df_new.count()}")

# Schema comparison
assert df_talend.columns == df_new.columns, "Column mismatch!"

# Value comparison (sample)
df_talend.exceptAll(df_new).show()  # rows in Talend but not Databricks
df_new.exceptAll(df_talend).show()  # rows in Databricks but not Talend
```

---

## Submitting to Databricks

### Option A: Databricks UI (Workspace Import)

Quickest for one-off testing.

1. Open your Databricks workspace
2. Navigate to **Workspace → Users → your user folder**
3. Click **Import** → select the `output/` folder or upload files individually
4. Open `main.py` — it will open as a Python file
5. Attach to a cluster and run

> **Tip:** Convert `main.py` to a Databricks Notebook by adding `# Databricks notebook source` as the first line and using `# COMMAND ----------` separators between logical sections.

### Option B: Databricks CLI

```bash
# Install and configure
pip install databricks-cli
databricks configure --token
# Enter: workspace URL + Personal Access Token

# Upload the project
databricks workspace import_dir output/ /Workspace/Users/you@company.com/LARGE_ENTERPRISE_RPT

# Create a job
databricks jobs create --json '{
  "name": "LARGE_ENTERPRISE_RPT",
  "tasks": [{
    "task_key": "main",
    "spark_python_task": {
      "python_file": "/Workspace/Users/you@company.com/LARGE_ENTERPRISE_RPT/main.py"
    },
    "existing_cluster_id": "<your-cluster-id>"
  }]
}'

# Run the job
databricks jobs run-now --job-id <job-id>

# Monitor
databricks runs get --run-id <run-id>
```

### Option C: Databricks REST API

Use the `deploy.json` generated by `talend2db deploy`:

```bash
# Generate the deployment package
talend2db deploy job.item \
  --workspace https://your-workspace.cloud.databricks.com \
  --job-name LARGE_ENTERPRISE_RPT

# Upload files to DBFS
for f in main.py transformations.py sql_statements.py connections.py; do
  curl -X POST "https://your-workspace.cloud.databricks.com/api/2.0/dbfs/put" \
    -H "Authorization: Bearer $TOKEN" \
    -F "path=/jobs/LARGE_ENTERPRISE_RPT/$f" \
    -F "contents=@output/$f" \
    -F "overwrite=true"
done

# Create the job
curl -X POST "https://your-workspace.cloud.databricks.com/api/2.1/jobs/create" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d @output/deploy.json

# Run it
curl -X POST "https://your-workspace.cloud.databricks.com/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"job_id": <job-id>}'
```

---

## Pre-Deployment Checklist

Before running on Databricks, complete these steps:

### 1. Create a Secret Scope

The generated code uses `dbutils.secrets.get("talend-migration", "<key>")` for credentials. Create the scope and populate it:

```python
# In a Databricks notebook:
dbutils.secrets.createScope("talend-migration")

# Or via CLI:
databricks secrets create-scope --scope talend-migration
databricks secrets put --scope talend-migration --key luciskystg_password
databricks secrets put --scope talend-migration --key luciskydata_password
databricks secrets put --scope talend-migration --key luciskyrds_password
databricks secrets put --scope talend-migration --key s3_access_key_luci_redshift
databricks secrets put --scope talend-migration --key s3_secret_key_luci_redshift
```

### 2. Configure Connections

Update `connections.py` to point to your actual data sources:

- **Redshift** → Use Databricks JDBC connector or migrate tables to Unity Catalog
- **MySQL** → Use Databricks JDBC connector or Lakehouse Federation
- **S3** → Use Unity Catalog external volumes or direct `s3://` paths with instance profiles
- **Excel files** → Upload to DBFS/Volumes, update paths in `config/job.yml`

### 3. Adjust SQL Dialect

Some Redshift-specific SQL may need tweaks for Spark SQL:

| Redshift | Spark SQL |
|---|---|
| `VARCHAR(n)` | `STRING` |
| `FLOAT8` | `DOUBLE` |
| `INT4` | `INT` |
| `COPY ... FROM 's3://...'` | `spark.read.csv("s3://...")` |
| `GRANT SELECT ON ...` | Remove (use Unity Catalog permissions) |
| `column ~ '[0-9]'` | `column RLIKE '[0-9]'` |

### 4. Review Warnings

Check `migration-report.json` for any warnings that need manual attention (e.g., `tJava` custom code, `tRunJob` child job references, `GetConnDetails` joblet).

---

## Adding New Talend Components

The component registry is pluggable. To support a new component type:

1. Open `src/talend2databricks/component_registry.py`
2. Add a new translator method:

```python
def _translate_tNewComponent(self, node, job, expr_translator):
    # Extract parameters from node
    param_value = node.get_param("SOME_PARAM")

    return CodeFragment(
        component=node.unique_name,
        component_type=node.component_name,
        category="transformation",  # or "sql", "connection", "control", "file"
        pyspark_code="# Generated PySpark code here",
        sql_code=None,
        warnings=[],
        secrets=[],
    )
```

3. Register it in `__init__` by adding `"tNewComponent"` to `self.supported_types`

---

## Troubleshooting

### `talend2db: command not found`

```bash
pip install -e /path/to/talend2databricks
```

### XML parsing errors

Ensure the `.item` file is a valid Talend Studio export. The parser expects the `talendfile:ProcessType` root element with `node`, `connection`, and `context` children.

### Expression translation warnings

If the analyzer reports partial or manual expressions, check `migration-report.json` for the specific expressions. Common issues:

- **Custom Java code in tJava** — must be manually rewritten in Python
- **Unknown Talend functions** — add mappings in `src/talend2databricks/expression_translator.py`
- **Complex ternary nesting** — review the translated `F.when().otherwise()` chains

### Spark SQL errors at runtime

Most runtime SQL errors come from Redshift → Spark SQL dialect differences. Check the [SQL dialect adjustment table](#3-adjust-sql-dialect) above.

### `dbutils.secrets` errors

Ensure you've created the secret scope and populated all required keys. Run `dbutils.secrets.list("talend-migration")` in a notebook to verify.
