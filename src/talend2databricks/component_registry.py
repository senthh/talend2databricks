"""Pluggable component translators for Talend → Databricks.

Each translator converts a specific Talend component type into
PySpark/Spark SQL code fragments and metadata.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

from .expression_translator import ExpressionTranslator, TranslationResult
from .ir import MapperTable, TalendJob, TalendNode


@dataclass
class CodeFragment:
    """A generated code fragment from a component translator."""
    category: str  # "transformation", "sql", "file_io", "cloud_storage", "control", "connection", "notification"
    code: str
    imports: list[str] = field(default_factory=list)
    config_keys: list[str] = field(default_factory=list)
    secrets: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    sql_statements: list[str] = field(default_factory=list)
    description: str = ""
    node_name: str = ""
    component_type: str = ""


class ComponentTranslator:
    """Base for component translators."""

    supported_components: list[str] = []

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        raise NotImplementedError


class RedshiftInputTranslator(ComponentTranslator):
    supported_components = ["tRedshiftInput"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        query = node.get_param("QUERY", "").strip().strip('"')
        use_conn = node.get_param("USE_EXISTING_CONNECTION", "false")
        conn_ref = node.get_param("CONNECTION", "")
        table_name = node.get_param("TABLE", "").strip('"')
        schema = node.get_param("SCHEMA_DB", "").strip('"')

        # Translate to Spark SQL read
        if query:
            code = f'''# {node.display_name}: Read from Redshift via Spark SQL
{node.unique_name}_query = """{query}"""
df_{node.unique_name} = spark.sql({node.unique_name}_query)
'''
        else:
            full_table = f"{schema}.{table_name}" if schema and not schema.startswith("context.") else table_name
            code = f'''# {node.display_name}: Read table
df_{node.unique_name} = spark.table("{full_table}")
'''

        return CodeFragment(
            category="transformation",
            code=code,
            imports=["from pyspark.sql import SparkSession"],
            description=f"Read from {table_name or 'query'}",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class RedshiftRowTranslator(ComponentTranslator):
    supported_components = ["tRedshiftRow"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        query = node.get_param("QUERY", "").strip().strip('"')
        label = node.display_name

        # Parse multi-statement SQL
        statements = [s.strip() for s in query.split(';') if s.strip()]
        if not statements:
            statements = [query] if query else []

        sql_list = []
        code_lines = [f"# {label}: Execute SQL statements"]
        for i, stmt in enumerate(statements):
            # Replace context variables
            clean_stmt = _replace_context_vars_sql(stmt)
            sql_list.append(clean_stmt)
            code_lines.append(f'spark.sql("""{clean_stmt}""")')

        return CodeFragment(
            category="sql",
            code="\n".join(code_lines),
            sql_statements=sql_list,
            imports=["from pyspark.sql import SparkSession"],
            description=f"SQL: {label}",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class FileInputExcelTranslator(ComponentTranslator):
    supported_components = ["tFileInputExcel"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        filename = node.get_param("FILENAME", "").strip('"')
        sheet = node.get_param("SHEETNAME", "").strip('"')
        header_row = node.get_param("HEADER", "0")

        # Map to Databricks volume path
        volume_path = _to_volume_path(filename)

        code = f'''# {node.display_name}: Read Excel file
df_{node.unique_name} = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'{sheet or "Sheet1"}'!A1")
    .load("{volume_path}")
)
'''
        return CodeFragment(
            category="file_io",
            code=code,
            imports=[
                "from pyspark.sql import SparkSession",
            ],
            config_keys=[f"file_path_{node.unique_name}"],
            description=f"Read Excel: {filename}",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class FileOutputDelimitedTranslator(ComponentTranslator):
    supported_components = ["tFileOutputDelimited"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        filename = node.get_param("FILENAME", "").strip('"')
        delimiter = node.get_param("FIELDSEPARATOR", '","').strip('"')
        include_header = node.get_param("INCLUDEHEADER", "true")

        volume_path = _to_volume_path(filename)

        code = f'''# {node.display_name}: Write delimited file
df_output.coalesce(1).write.mode("overwrite").option(
    "header", {include_header.lower()}
).option("delimiter", "{delimiter}").csv("{volume_path}")
'''
        return CodeFragment(
            category="file_io",
            code=code,
            imports=["from pyspark.sql import SparkSession"],
            description=f"Write CSV: {filename}",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class MysqlInputTranslator(ComponentTranslator):
    supported_components = ["tMysqlInput"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        query = node.get_param("QUERY", "").strip().strip('"')
        conn_ref = node.get_param("CONNECTION", "")

        code = f'''# {node.display_name}: Read from MySQL (migrated to Spark SQL)
{node.unique_name}_query = """{_replace_context_vars_sql(query)}"""
df_{node.unique_name} = spark.sql({node.unique_name}_query)
'''
        return CodeFragment(
            category="transformation",
            code=code,
            imports=["from pyspark.sql import SparkSession"],
            description=f"Read MySQL query",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class TMapTranslator(ComponentTranslator):
    supported_components = ["tMap"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        if not node.mapper_data:
            return CodeFragment(
                category="transformation",
                code=f"# {node.display_name}: tMap (no mapper data found)",
                warnings=["No mapper data in tMap node"],
                node_name=node.unique_name,
                component_type=node.component_name,
            )

        md = node.mapper_data
        code_lines = [f"# {node.display_name}: Multi-input transformation (tMap)"]
        code_lines.append("from pyspark.sql import functions as F")
        code_lines.append("")

        warnings = []

        # Build joins from input tables
        input_names = [t.name for t in md.input_tables]
        if len(input_names) > 1:
            main_table = input_names[0]
            code_lines.append(f"# Main input: {main_table}")
            code_lines.append(f"df_joined = df_{main_table}")
            code_lines.append("")

            for lookup_table in md.input_tables[1:]:
                join_type = "left" if not lookup_table.inner_join else "inner"
                # Find join keys from entries with expressions
                join_exprs = [
                    e for e in lookup_table.entries if e.expression
                ]
                if join_exprs:
                    conditions = []
                    for je in join_exprs:
                        conditions.append(
                            f'F.col("{main_table}__{je.name}") == F.col("{lookup_table.name}__{je.name}")'
                        )
                    cond_str = " & ".join(conditions) if conditions else '"placeholder_key"'
                    code_lines.append(
                        f'df_joined = df_joined.join(df_{lookup_table.name}, {cond_str}, "{join_type}")'
                    )
                else:
                    code_lines.append(
                        f'# TODO: determine join condition for {lookup_table.name}'
                    )
                    code_lines.append(
                        f'df_joined = df_joined.crossJoin(df_{lookup_table.name})'
                    )
                    warnings.append(f"No join key found for lookup: {lookup_table.name}")
        else:
            main_table = input_names[0] if input_names else "input"
            code_lines.append(f"df_joined = df_{main_table}")

        code_lines.append("")

        # Output tables with expression mappings
        for out_table in md.output_tables:
            code_lines.append(f"# Output: {out_table.name}")

            # Expression filter
            if out_table.expression_filter and out_table.activate_expression_filter:
                filter_expr = expr_translator.translate_filter(out_table.expression_filter)
                code_lines.append(f"df_filtered = df_joined.filter({filter_expr})")
                code_lines.append("")
            else:
                code_lines.append("df_filtered = df_joined")
                code_lines.append("")

            # Column mappings
            select_exprs = []
            for entry in out_table.entries:
                result = expr_translator.translate(entry.expression, entry.name)
                select_exprs.append(f"    {result.pyspark_expr},")
                if result.warnings:
                    warnings.extend(result.warnings)

            code_lines.append(f"df_{out_table.name} = df_filtered.select(")
            code_lines.extend(select_exprs)
            code_lines.append(")")
            code_lines.append("")

        return CodeFragment(
            category="transformation",
            code="\n".join(code_lines),
            imports=[
                "from pyspark.sql import SparkSession",
                "from pyspark.sql import functions as F",
            ],
            warnings=warnings,
            description=f"tMap: {len(md.input_tables)} inputs → {len(md.output_tables)} outputs",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class SendMailTranslator(ComponentTranslator):
    supported_components = ["tSendMail"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        to = node.get_param("TO", "").strip('"')
        subject = node.get_param("SUBJECT", "").strip('"')

        code = f'''# {node.display_name}: Send notification (tSendMail → webhook)
# Original: to={to}, subject={subject}
# TODO: Replace with Databricks webhook or notification integration
import requests
webhook_url = dbutils.secrets.get(scope="notifications", key="webhook_url")
to_addr = config.get("tsendemail_to", "{_replace_context_vars_py(to)}")
subject_text = config.get("tsendemail_subject", "{_replace_context_vars_py(subject)}")
body_text = config.get("jobMailContent", "Job completed")
payload = {{
    "to": to_addr,
    "subject": subject_text,
    "body": body_text,
}}
# requests.post(webhook_url, json=payload)
print(f"NOTIFICATION: {{payload['subject']}}")
'''
        return CodeFragment(
            category="notification",
            code=code,
            secrets=["notifications/webhook_url"],
            description=f"Email notification → webhook",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class RunJobTranslator(ComponentTranslator):
    supported_components = ["tRunJob"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        process_name = node.get_param("PROCESS:PROCESS_TYPE_PROCESS", "").strip('"')
        process_version = node.get_param("PROCESS:PROCESS_TYPE_VERSION", "").strip('"')

        code = f'''# {node.display_name}: Run child job (tRunJob → Databricks task reference)
# Original Talend job: {process_name} (version: {process_version})
# In Databricks, this becomes a task dependency in the workflow
dbutils.notebook.run("{process_name}", timeout_seconds=3600, arguments={{}})
'''
        return CodeFragment(
            category="control",
            code=code,
            description=f"Run job: {process_name}",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class JavaTranslator(ComponentTranslator):
    supported_components = ["tJava"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        java_code = node.get_param("CODE", "").strip('"')

        code = f'''# {node.display_name}: Custom Java code → Python equivalent
# Original Java:
# {java_code[:200]}{"..." if len(java_code) > 200 else ""}
# TODO: Manual translation required for custom Java logic
print("{node.display_name}: Custom logic placeholder")
'''
        return CodeFragment(
            category="control",
            code=code,
            warnings=[f"tJava requires manual translation: {java_code[:100]}"],
            description="Custom Java code",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class S3PutTranslator(ComponentTranslator):
    supported_components = ["S3Put"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        bucket = node.get_param("BUCKET", "").strip('"')
        key = node.get_param("KEY", "").strip('"')
        local_file = node.get_param("LOCALFILE", "").strip('"')

        code = f'''# {node.display_name}: Upload to S3 (→ Unity Catalog Volume / cloud storage)
# Original: s3://{bucket}/{key}
source_path = "{_to_volume_path(local_file)}"
dest_path = "/Volumes/catalog/schema/volume/{key or 'output'}"
dbutils.fs.cp(source_path, dest_path)
'''
        return CodeFragment(
            category="cloud_storage",
            code=code,
            description=f"S3 upload → cloud storage",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class S3ConnectionTranslator(ComponentTranslator):
    supported_components = ["tS3Connection"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        code = f'''# {node.display_name}: S3 Connection (handled by Databricks Unity Catalog)
# AWS credentials managed via instance profile or secret scope
# No explicit connection needed in Databricks
'''
        return CodeFragment(
            category="connection",
            code=code,
            description="S3 connection → Databricks native",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class RedshiftConnectionTranslator(ComponentTranslator):
    supported_components = ["tRedshiftConnection"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        host = node.get_param("HOST", "").strip('"')
        port = node.get_param("PORT", "").strip('"')
        db = node.get_param("DBNAME", "").strip('"')
        schema = node.get_param("SCHEMA_DB", "").strip('"')

        code = f'''# {node.display_name}: Redshift Connection → Databricks catalog/schema
# Original: {host}:{port}/{db}.{schema}
# In Databricks: data is migrated to Unity Catalog
# spark.catalog.setCurrentCatalog("catalog_name")
# spark.catalog.setCurrentDatabase("schema_name")
'''
        secrets = []
        if 'context.' in host or 'context.' in node.get_param("PASS", ""):
            secrets.append("redshift/host")
            secrets.append("redshift/password")

        return CodeFragment(
            category="connection",
            code=code,
            secrets=secrets,
            description=f"Redshift connection → Databricks catalog",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class RedshiftCloseTranslator(ComponentTranslator):
    supported_components = ["tRedshiftClose"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        return CodeFragment(
            category="connection",
            code=f"# {node.display_name}: Redshift Close (no-op in Databricks)",
            description="Connection close → no-op",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class MysqlConnectionTranslator(ComponentTranslator):
    supported_components = ["tMysqlConnection"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        host = node.get_param("HOST", "").strip('"')
        db = node.get_param("DBNAME", "").strip('"')

        code = f'''# {node.display_name}: MySQL Connection → Databricks catalog/schema
# Original: {host}/{db}
# In Databricks: data is migrated to Unity Catalog
'''
        return CodeFragment(
            category="connection",
            code=code,
            secrets=["mysql/host", "mysql/password"] if 'context.' in host else [],
            description=f"MySQL connection → Databricks catalog",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class MysqlCloseTranslator(ComponentTranslator):
    supported_components = ["tMysqlClose"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        return CodeFragment(
            category="connection",
            code=f"# {node.display_name}: MySQL Close (no-op in Databricks)",
            description="Connection close → no-op",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class DieTranslator(ComponentTranslator):
    supported_components = ["tDie"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        message = node.get_param("MESSAGE", "").strip('"')
        exit_code = node.get_param("CODE", "1")

        code = f'''# {node.display_name}: Error handler (tDie → raise exception)
raise RuntimeError("{_replace_context_vars_py(message) or 'Job failed'}")
'''
        return CodeFragment(
            category="control",
            code=code,
            description=f"Error handler: exit {exit_code}",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class PrejobTranslator(ComponentTranslator):
    supported_components = ["tPrejob"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        return CodeFragment(
            category="control",
            code=f"# === PRE-JOB PHASE: {node.display_name} ===",
            description="Pre-job phase marker",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class PostjobTranslator(ComponentTranslator):
    supported_components = ["tPostjob"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        return CodeFragment(
            category="control",
            code=f"# === POST-JOB PHASE: {node.display_name} ===",
            description="Post-job phase marker",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class GenericTranslator(ComponentTranslator):
    """Fallback translator for unrecognized components."""
    supported_components = ["GetConnDetails"]

    def translate(self, node: TalendNode, job: TalendJob, expr_translator: ExpressionTranslator) -> CodeFragment:
        return CodeFragment(
            category="control",
            code=f"# {node.display_name}: {node.component_name} (custom/generic component)\n# TODO: Manual translation needed",
            warnings=[f"No specific translator for {node.component_name}"],
            description=f"Generic: {node.component_name}",
            node_name=node.unique_name,
            component_type=node.component_name,
        )


class ComponentRegistry:
    """Registry of component translators."""

    def __init__(self):
        self._translators: dict[str, ComponentTranslator] = {}
        self._register_defaults()

    def _register_defaults(self):
        """Register all built-in translators."""
        translators = [
            RedshiftInputTranslator(),
            RedshiftRowTranslator(),
            FileInputExcelTranslator(),
            FileOutputDelimitedTranslator(),
            MysqlInputTranslator(),
            TMapTranslator(),
            SendMailTranslator(),
            RunJobTranslator(),
            JavaTranslator(),
            S3PutTranslator(),
            S3ConnectionTranslator(),
            RedshiftConnectionTranslator(),
            RedshiftCloseTranslator(),
            MysqlConnectionTranslator(),
            MysqlCloseTranslator(),
            DieTranslator(),
            PrejobTranslator(),
            PostjobTranslator(),
            GenericTranslator(),
        ]
        for t in translators:
            for comp in t.supported_components:
                self._translators[comp] = t

    def register(self, component_name: str, translator: ComponentTranslator):
        self._translators[component_name] = translator

    def get_translator(self, component_name: str) -> Optional[ComponentTranslator]:
        return self._translators.get(component_name)

    def translate_node(self, node: TalendNode, job: TalendJob,
                       expr_translator: ExpressionTranslator) -> CodeFragment:
        translator = self.get_translator(node.component_name)
        if translator:
            return translator.translate(node, job, expr_translator)

        # Fallback
        return CodeFragment(
            category="control",
            code=f"# {node.unique_name}: {node.component_name} (UNSUPPORTED)\n# TODO: Manual migration required",
            warnings=[f"Unsupported component: {node.component_name}"],
            description=f"Unsupported: {node.component_name}",
            node_name=node.unique_name,
            component_type=node.component_name,
        )

    @property
    def supported_types(self) -> set[str]:
        return set(self._translators.keys())


# ── Helpers ──────────────────────────────────────────────────────────

def _replace_context_vars_sql(sql: str) -> str:
    """Replace context.var references in SQL with config lookups."""
    import re
    # "\" + context.var + \"  or  \" + context.var
    sql = re.sub(
        r'"\s*\+\s*context\.(\w+)\s*\+\s*"',
        r"' + config['\1'] + '",
        sql,
    )
    # context.var as standalone
    sql = re.sub(
        r'context\.(\w+)',
        r"${config.\1}",
        sql,
    )
    return sql


def _replace_context_vars_py(s: str) -> str:
    """Replace context.var references with Python config lookups."""
    import re
    return re.sub(r'context\.(\w+)', r"config_\1", s)


def _to_volume_path(filepath: str) -> str:
    """Convert local/S3 file paths to Databricks Volume paths."""
    if not filepath:
        return "/Volumes/catalog/schema/volume/data"
    # Strip context vars
    import re
    clean = re.sub(r'"\s*\+\s*context\.\w+\s*\+\s*"', '', filepath)
    clean = re.sub(r'context\.\w+', '', clean)
    clean = clean.strip('"+ ')
    if clean.startswith('/talend/'):
        clean = clean.replace('/talend/', '')
    if clean.startswith('s3://'):
        clean = clean.split('/', 3)[-1] if clean.count('/') > 2 else clean
    return f"/Volumes/catalog/schema/volume/{clean}" if clean else "/Volumes/catalog/schema/volume/data"
