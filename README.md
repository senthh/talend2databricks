# talend2databricks

A reusable CLI tool that converts Talend `.item` job files into Databricks PySpark/Spark SQL projects.

## Architecture

```
.item XML → Parser → Intermediate Representation → Code Generator → Databricks Project
```

### 5 Core Modules

| Module | Purpose |
|---|---|
| `talend_parser` | Parse `.item` XML → `TalendJob` IR (nodes, connections, contexts, tMap MapperData) |
| `component_registry` | 19 pluggable translators for Talend component types |
| `expression_translator` | Talend Java expressions → PySpark `F.*` calls |
| `databricks_generator` | Generate complete Databricks project structure |
| `cli` | Click-based CLI: `analyze`, `convert`, `deploy` |

## Install

```bash
git clone https://github.com/senthh/talend2databricks.git
cd talend2databricks
pip install -e .
```

## Quick Start

```bash
# Analyze migration readiness
talend2db analyze path/to/job.item

# Generate Databricks project
talend2db convert path/to/job.item --output-dir ./output

# Generate + Databricks Jobs API JSON
talend2db deploy path/to/job.item --workspace https://xxx.databricks.com --job-name MY_JOB
```

📖 **Full usage guide, validation workflow, and Databricks deployment instructions:** [docs/USAGE.md](docs/USAGE.md)

## Supported Talend Components

tRedshiftInput, tRedshiftRow, tRedshiftConnection, tRedshiftClose, tFileInputExcel, tFileOutputDelimited, tMysqlInput, tMysqlConnection, tMysqlClose, tMap, tSendMail, tRunJob, tJava, S3Put, tS3Connection, tDie, tPrejob, tPostjob

## Expression Translation

| Talend | PySpark |
|---|---|
| `StringHandling.LEN(x)` | `F.length(col("x"))` |
| `StringHandling.TRIM(x)` | `F.trim(col("x"))` |
| `StringHandling.UPCASE(x)` | `F.upper(col("x"))` |
| `CommonDb.replace(x, a, b)` | `F.regexp_replace(col("x"), a, b)` |
| `TalendDate.formatDate(fmt, d)` | `F.date_format(col("d"), fmt)` |
| `Relational.ISNULL(x)` | `F.isnull(col("x"))` |
| `cond ? a : b` | `F.when(cond, a).otherwise(b)` |
| `"str".equals(x)` | `x == "str"` |

## License

MIT
