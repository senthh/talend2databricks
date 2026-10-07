"""Generate Databricks-native project structure from Talend job IR.

Produces:
  main.py             – orchestration entry point
  transformations.py  – PySpark DataFrame pipelines
  sql_statements.py   – collected Spark SQL statements
  connections.py      – connection/secret scope configuration
  config/job.yml      – job configuration
  tests/test_main.py  – basic test scaffold
  migration-report.json – detailed migration report
"""
from __future__ import annotations

import json
import os
import textwrap
from collections import defaultdict
from datetime import datetime
from typing import Any

import yaml

from .component_registry import CodeFragment, ComponentRegistry
from .expression_translator import ExpressionTranslator
from .ir import TalendJob, TalendNode


class DatabricksGenerator:
    """Generate a Databricks project from a TalendJob IR."""

    def __init__(self, job: TalendJob, output_dir: str):
        self.job = job
        self.output_dir = output_dir
        self.registry = ComponentRegistry()
        self.expr_translator = ExpressionTranslator()
        self.fragments: list[CodeFragment] = []
        self.report: dict[str, Any] = {}

    def generate(self) -> dict[str, Any]:
        """Run the full generation pipeline. Returns migration report."""
        # 1. Translate all nodes
        self._translate_nodes()

        # 2. Optimize fragments
        self._optimize_fragments()

        # 3. Generate output files
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "config"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "tests"), exist_ok=True)

        self._generate_main_py()
        self._generate_transformations_py()
        self._generate_sql_statements_py()
        self._generate_connections_py()
        self._generate_config_yml()
        self._generate_test_main_py()

        # 4. Build migration report
        self.report = self._build_report()
        report_path = os.path.join(self.output_dir, "migration-report.json")
        with open(report_path, "w") as f:
            json.dump(self.report, f, indent=2, default=str)

        return self.report

    def _translate_nodes(self):
        """Translate all job nodes using component registry."""
        for node in self.job.nodes:
            frag = self.registry.translate_node(node, self.job, self.expr_translator)
            self.fragments.append(frag)

    def _optimize_fragments(self):
        """Optimize: collapse consecutive SQL ops, merge pipelines."""
        # Group consecutive SQL fragments
        optimized = []
        sql_batch = []

        for frag in self.fragments:
            if frag.category == "sql" and frag.sql_statements:
                sql_batch.append(frag)
            else:
                if sql_batch:
                    optimized.append(self._merge_sql_batch(sql_batch))
                    sql_batch = []
                optimized.append(frag)

        if sql_batch:
            optimized.append(self._merge_sql_batch(sql_batch))

        self.fragments = optimized

    def _merge_sql_batch(self, batch: list[CodeFragment]) -> CodeFragment:
        """Merge consecutive SQL fragments into a single task."""
        if len(batch) == 1:
            return batch[0]

        all_sql = []
        all_code_lines = [f"# Merged SQL batch ({len(batch)} operations)"]
        names = []
        for frag in batch:
            all_sql.extend(frag.sql_statements)
            names.append(frag.node_name)

        for i, stmt in enumerate(all_sql):
            all_code_lines.append(f"# Statement {i + 1}")
            all_code_lines.append(f'spark.sql("""{stmt}""")')
            all_code_lines.append("")

        return CodeFragment(
            category="sql",
            code="\n".join(all_code_lines),
            sql_statements=all_sql,
            imports=["from pyspark.sql import SparkSession"],
            description=f"Merged SQL: {', '.join(names)}",
            node_name=f"sql_batch_{'_'.join(names[:3])}",
            component_type="merged_sql",
        )

    def _generate_main_py(self):
        """Generate the main orchestration script."""
        # Collect all imports
        all_imports = set()
        for frag in self.fragments:
            all_imports.update(frag.imports)
        all_imports.add("from pyspark.sql import SparkSession")
        all_imports.add("from pyspark.sql import functions as F")

        # Collect secrets
        all_secrets = set()
        for frag in self.fragments:
            all_secrets.update(frag.secrets)

        # Build execution phases
        prejob_frags = [f for f in self.fragments if f.component_type in ("tPrejob", "tJava", "GetConnDetails")]
        conn_frags = [f for f in self.fragments if f.category == "connection"]
        transform_frags = [f for f in self.fragments if f.category == "transformation"]
        sql_frags = [f for f in self.fragments if f.category == "sql"]
        io_frags = [f for f in self.fragments if f.category in ("file_io", "cloud_storage")]
        notify_frags = [f for f in self.fragments if f.category == "notification"]
        control_frags = [f for f in self.fragments if f.category == "control" and
                         f.component_type not in ("tPrejob", "tPostjob", "tJava", "GetConnDetails", "tDie")]
        postjob_frags = [f for f in self.fragments if f.component_type == "tPostjob"]
        error_frags = [f for f in self.fragments if f.component_type == "tDie"]

        # Context params for config
        ctx = self.job.default_context_obj
        credential_params = set()
        if ctx:
            for p in ctx.parameters:
                pname = p.name.lower()
                if any(k in pname for k in ('password', 'secret', 'key', 'pass')):
                    credential_params.add(p.name)

        lines = [
            '"""',
            f"Databricks job: {self.job.name}",
            f"Migrated from Talend job on {datetime.now().strftime('%Y-%m-%d')}",
            f"Components: {len(self.job.nodes)} nodes, {len(self.job.connections)} connections",
            '"""',
            "",
            *sorted(all_imports),
            "from connections import get_config",
            "from transformations import run_transformations",
            "from sql_statements import run_sql_statements",
            "",
            "",
            "def main():",
            '    """Main job entry point."""',
            "    spark = SparkSession.builder.appName(",
            f'        "{self.job.name}"',
            "    ).getOrCreate()",
            "",
            "    config = get_config(spark)",
            "",
        ]

        # Pre-job
        if prejob_frags:
            lines.append("    # ── Pre-job Phase ──")
            for frag in prejob_frags:
                for line in frag.code.split("\n"):
                    lines.append(f"    {line}")
                lines.append("")

        # Main execution
        lines.append("    try:")
        lines.append("        # ── SQL Statements ──")
        lines.append("        run_sql_statements(spark, config)")
        lines.append("")
        lines.append("        # ── Transformations ──")
        lines.append("        run_transformations(spark, config)")
        lines.append("")

        # Control flow (tRunJob, etc)
        if control_frags:
            lines.append("        # ── Child Jobs / Control ──")
            for frag in control_frags:
                for line in frag.code.split("\n"):
                    lines.append(f"        {line}")
                lines.append("")

        # Notifications
        if notify_frags:
            lines.append("        # ── Notifications ──")
            for frag in notify_frags:
                for line in frag.code.split("\n"):
                    lines.append(f"        {line}")
                lines.append("")

        # Error handling
        lines.append("    except Exception as e:")
        if error_frags:
            lines.append('        print(f"Job failed: {e}")')
            lines.append("        raise")
        else:
            lines.append('        print(f"Job failed: {e}")')
            lines.append("        raise")

        # Post-job
        lines.append("    finally:")
        lines.append("        # ── Post-job Phase ──")
        lines.append('        print("Post-job cleanup")')
        lines.append("        spark.stop()")
        lines.append("")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append("    main()")
        lines.append("")

        path = os.path.join(self.output_dir, "main.py")
        with open(path, "w") as f:
            f.write("\n".join(lines))

    def _generate_transformations_py(self):
        """Generate the transformations module."""
        transform_frags = [f for f in self.fragments if f.category == "transformation"]
        io_frags = [f for f in self.fragments if f.category in ("file_io", "cloud_storage")]

        lines = [
            '"""PySpark transformations for the migrated Talend job."""',
            "",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.types import *",
            "",
            "",
            "def run_transformations(spark: SparkSession, config: dict):",
            '    """Execute all transformation steps."""',
            "",
        ]

        if not transform_frags and not io_frags:
            lines.append("    pass  # No transformations")
        else:
            for frag in transform_frags:
                for line in frag.code.split("\n"):
                    lines.append(f"    {line}")
                lines.append("")

            if io_frags:
                lines.append("    # ── File I/O ──")
                for frag in io_frags:
                    for line in frag.code.split("\n"):
                        lines.append(f"    {line}")
                    lines.append("")

        lines.append("")
        path = os.path.join(self.output_dir, "transformations.py")
        with open(path, "w") as f:
            f.write("\n".join(lines))

    def _generate_sql_statements_py(self):
        """Generate the SQL statements module."""
        sql_frags = [f for f in self.fragments if f.category == "sql"]

        lines = [
            '"""Spark SQL statements for the migrated Talend job."""',
            "",
            "from pyspark.sql import SparkSession",
            "",
            "",
            "SQL_STATEMENTS = [",
        ]

        all_stmts = []
        for frag in sql_frags:
            all_stmts.extend(frag.sql_statements)

        for stmt in all_stmts:
            escaped = stmt.replace('\\', '\\\\').replace('"""', '\\"\\"\\"')
            lines.append(f'    """{escaped}""",')

        lines.extend([
            "]",
            "",
            "",
            "def run_sql_statements(spark: SparkSession, config: dict):",
            '    """Execute all SQL statements in order."""',
            "    for i, stmt in enumerate(SQL_STATEMENTS):",
            "        # Substitute config variables",
            "        resolved = stmt",
            "        for key, val in config.items():",
            '            resolved = resolved.replace(f"${{config.{key}}}", str(val))',
            '        print(f"Executing SQL statement {i + 1}/{len(SQL_STATEMENTS)}")',
            "        spark.sql(resolved)",
            '        print(f"  ✓ Statement {i + 1} complete")',
            "",
        ])

        path = os.path.join(self.output_dir, "sql_statements.py")
        with open(path, "w") as f:
            f.write("\n".join(lines))

    def _generate_connections_py(self):
        """Generate the connections/config module."""
        ctx = self.job.default_context_obj

        # Classify params
        credential_keys = []
        config_keys = []
        if ctx:
            for p in ctx.parameters:
                pname = p.name.lower()
                if any(k in pname for k in ('password', 'secret', 'key', 'pass', 'credential')):
                    credential_keys.append(p.name)
                else:
                    config_keys.append(p.name)

        lines = [
            '"""Connection and configuration management for Databricks."""',
            "",
            "",
            "def _get_dbutils(spark):",
            '    """Get dbutils - works in notebooks, spark-submit, and local testing."""',
            "    try:",
            "        # Available as a global in Databricks notebooks",
            "        return dbutils  # noqa: F821",
            "    except NameError:",
            "        pass",
            "    try:",
            "        # Spark-submit on Databricks: resolve from the JVM gateway",
            "        from pyspark.dbutils import DBUtils",
            "        return DBUtils(spark)",
            "    except (ImportError, Exception):",
            "        pass",
            "    # Local / non-Databricks: return a stub that raises clear errors",
            "    class _Stub:",
            "        class secrets:",
            "            @staticmethod",
            "            def get(scope, key):",
            '                raise RuntimeError(',
            '                    f"dbutils.secrets.get({scope!r}, {key!r}) called outside Databricks. "',
            '                    f"Set the value via environment variable TALEND_{key.upper()} or pass it in config."',
            "                )",
            "    return _Stub()",
            "",
            "",
            "def get_config(spark=None) -> dict:",
            '    """Load job configuration with secret scope references.',
            "",
            "    Credentials are loaded from Databricks secret scopes.",
            "    Config values come from job parameters or widgets.",
            '    """',
            "    config = {}",
            "",
            "    # Resolve dbutils (works in notebooks, spark-submit, and locally)",
            "    _dbutils = _get_dbutils(spark)",
            "",
            "    # ── Secret scope credentials ──",
            "    # Replace hardcoded credentials with Databricks secrets",
        ]

        for key in credential_keys:
            lines.append(
                f'    config["{key}"] = _dbutils.secrets.get(scope="talend-migration", key="{key}")'
            )

        lines.append("")
        lines.append("    # ── Configuration parameters ──")
        if ctx:
            for p in ctx.parameters:
                if p.name not in credential_keys:
                    val = p.value
                    # Clean embedded quotes from XML entity escaping
                    val = val.strip('"').replace('"', '\\"')
                    if p.type == "id_Integer":
                        val_clean = val.strip() or "0"
                        try:
                            int(val_clean)
                            lines.append(f'    config["{p.name}"] = {val_clean}')
                        except ValueError:
                            lines.append(f'    config["{p.name}"] = "{val}"')
                    elif p.type == "id_Boolean":
                        lines.append(f'    config["{p.name}"] = {val.capitalize() if val else "False"}')
                    else:
                        lines.append(f'    config["{p.name}"] = "{val}"')

        lines.extend([
            "",
            "    return config",
            "",
        ])

        path = os.path.join(self.output_dir, "connections.py")
        with open(path, "w") as f:
            f.write("\n".join(lines))

    def _generate_config_yml(self):
        """Generate job configuration YAML."""
        ctx = self.job.default_context_obj

        config = {
            "job": {
                "name": self.job.name,
                "migrated_from": "Talend",
                "version": "1.0",
                "default_context": self.job.default_context,
            },
            "databricks": {
                "secret_scope": "talend-migration",
                "catalog": "main",
                "schema": "default",
            },
            "connections": {},
            "file_paths": {},
            "parameters": {},
        }

        # Extract connection info
        for node in self.job.get_connection_nodes():
            conn_type = node.component_name.replace("t", "").replace("Connection", "").lower()
            config["connections"][node.unique_name] = {
                "type": conn_type,
                "host": node.get_param("HOST", "").strip('"'),
                "port": node.get_param("PORT", "").strip('"'),
                "database": node.get_param("DBNAME", "").strip('"'),
                "schema": node.get_param("SCHEMA_DB", "").strip('"'),
                "note": "Migrated to Databricks Unity Catalog",
            }

        # Parameters
        if ctx:
            for p in ctx.parameters:
                pname = p.name.lower()
                if not any(k in pname for k in ('password', 'secret', 'key', 'pass')):
                    config["parameters"][p.name] = p.value

        path = os.path.join(self.output_dir, "config", "job.yml")
        with open(path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    def _generate_test_main_py(self):
        """Generate test scaffold."""
        lines = [
            '"""Tests for the migrated Databricks job."""',
            "import pytest",
            "",
            "",
            f'class Test{self.job.name.replace("-", "_").title().replace("_", "")}:',
            '    """Test suite for the migrated job."""',
            "",
            "    def test_config_loads(self):",
            '        """Verify configuration loads without errors."""',
            "        from connections import get_config",
            "        # In test mode, dbutils won't be available",
            "        # config = get_config()",
            "        assert True  # Placeholder",
            "",
            "    def test_sql_statements_parseable(self):",
            '        """Verify SQL statements are valid strings."""',
            "        from sql_statements import SQL_STATEMENTS",
            "        assert isinstance(SQL_STATEMENTS, list)",
            f"        assert len(SQL_STATEMENTS) > 0",
            "",
            "    def test_transformations_importable(self):",
            '        """Verify transformations module is importable."""',
            "        from transformations import run_transformations",
            "        assert callable(run_transformations)",
            "",
        ]

        path = os.path.join(self.output_dir, "tests", "test_main.py")
        with open(path, "w") as f:
            f.write("\n".join(lines))

        # Also write tests/__init__.py
        with open(os.path.join(self.output_dir, "tests", "__init__.py"), "w") as f:
            f.write("")

    def _build_report(self) -> dict[str, Any]:
        """Build the migration report."""
        component_counts = defaultdict(int)
        for node in self.job.nodes:
            component_counts[node.component_name] += 1

        # Coverage analysis
        supported = self.registry.supported_types
        found_types = self.job.component_types
        covered = found_types & supported
        uncovered = found_types - supported

        # Expression stats
        expr_stats = self.expr_translator.stats

        # Classify fragments
        categories = defaultdict(int)
        warnings_list = []
        secrets_needed = set()
        for frag in self.fragments:
            categories[frag.category] += 1
            warnings_list.extend(frag.warnings)
            secrets_needed.update(frag.secrets)

        # Auto-conversion estimate
        total_exprs = max(expr_stats.get("total", 1), 1)
        auto_pct = (expr_stats.get("auto", 0) / total_exprs) * 100

        total_components = len(self.job.nodes)
        covered_count = sum(1 for n in self.job.nodes if n.component_name in supported)
        component_pct = (covered_count / max(total_components, 1)) * 100

        overall_pct = (auto_pct * 0.6 + component_pct * 0.4)

        report = {
            "job_name": self.job.name,
            "migration_date": datetime.now().isoformat(),
            "source_file": "",
            "summary": {
                "total_nodes": len(self.job.nodes),
                "total_connections": len(self.job.connections),
                "total_contexts": len(self.job.contexts),
                "component_types_found": sorted(found_types),
                "component_types_covered": sorted(covered),
                "component_types_uncovered": sorted(uncovered),
                "component_coverage_pct": round(component_pct, 1),
                "expression_auto_conversion_pct": round(auto_pct, 1),
                "overall_auto_conversion_pct": round(overall_pct, 1),
            },
            "components": dict(component_counts),
            "expression_stats": expr_stats,
            "categories": dict(categories),
            "secrets_required": sorted(secrets_needed),
            "warnings": warnings_list,
            "generated_files": [
                "main.py",
                "transformations.py",
                "sql_statements.py",
                "connections.py",
                "config/job.yml",
                "tests/test_main.py",
                "migration-report.json",
            ],
            "tmap_details": self._tmap_details(),
            "context_parameters": self._context_summary(),
        }

        return report

    def _tmap_details(self) -> list[dict]:
        """Extract tMap details for the report."""
        details = []
        for node in self.job.get_nodes_by_type("tMap"):
            if node.mapper_data:
                md = node.mapper_data
                d = {
                    "node": node.unique_name,
                    "input_tables": [
                        {
                            "name": t.name,
                            "matching_mode": t.matching_mode,
                            "lookup_mode": t.lookup_mode,
                            "columns": len(t.entries),
                        }
                        for t in md.input_tables
                    ],
                    "output_tables": [
                        {
                            "name": t.name,
                            "columns": len(t.entries),
                            "has_filter": bool(t.expression_filter),
                            "filter": t.expression_filter or None,
                            "complex_expressions": [
                                {"name": e.name, "expression": e.expression}
                                for e in t.entries
                                if e.expression and not e.expression.strip().startswith(
                                    tuple(f"{t2.name}." for t2 in md.input_tables)
                                )
                            ],
                        }
                        for t in md.output_tables
                    ],
                    "var_tables": len(md.var_tables),
                }
                details.append(d)
        return details

    def _context_summary(self) -> dict:
        """Summarize context parameters."""
        ctx = self.job.default_context_obj
        if not ctx:
            return {}

        credentials = []
        configs = []
        for p in ctx.parameters:
            pname = p.name.lower()
            if any(k in pname for k in ('password', 'secret', 'key', 'pass', 'credential')):
                credentials.append(p.name)
            else:
                configs.append(p.name)

        return {
            "total": len(ctx.parameters),
            "credentials_to_migrate": credentials,
            "config_params": configs,
        }


def generate_deploy_json(job: TalendJob, workspace_url: str, job_name: str) -> dict:
    """Generate Databricks Jobs API JSON payload."""
    return {
        "name": job_name,
        "tags": {
            "migrated_from": "talend",
            "original_job": job.name,
        },
        "tasks": [
            {
                "task_key": "main_task",
                "spark_python_task": {
                    "python_file": f"dbfs:/jobs/{job_name}/main.py",
                    "parameters": [],
                },
                "libraries": [
                    {"pypi": {"package": "pyyaml"}},
                ],
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true",
                    },
                },
            }
        ],
        "schedule": {
            "quartz_cron_expression": "0 0 6 * * ?",
            "timezone_id": "UTC",
            "pause_status": "PAUSED",
        },
        "email_notifications": {
            "on_failure": [],
            "on_success": [],
        },
        "max_concurrent_runs": 1,
    }
