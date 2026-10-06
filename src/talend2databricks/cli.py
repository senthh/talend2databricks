"""CLI entry point for talend2databricks."""
from __future__ import annotations

import json
import os
import sys

import click

from .talend_parser import parse_item_file
from .component_registry import ComponentRegistry
from .expression_translator import ExpressionTranslator
from .databricks_generator import DatabricksGenerator, generate_deploy_json


@click.group()
@click.version_option(version="0.1.0", prog_name="talend2databricks")
def cli():
    """Convert Talend .item XML files to Databricks PySpark/Spark SQL jobs."""
    pass


@cli.command()
@click.argument("item_file", type=click.Path(exists=True))
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
def analyze(item_file: str, json_output: bool):
    """Analyze a Talend .item file for migration readiness.

    Produces a report covering component coverage, expression complexity,
    and estimated auto-conversion percentage.
    """
    if not json_output:
        click.echo(f"🔍 Analyzing: {item_file}")
        click.echo()

    job = parse_item_file(item_file)
    registry = ComponentRegistry()
    expr_translator = ExpressionTranslator()

    # Translate all nodes to get expression stats
    fragments = []
    for node in job.nodes:
        frag = registry.translate_node(node, job, expr_translator)
        fragments.append(frag)

    # Component coverage
    supported = registry.supported_types
    found_types = job.component_types
    covered = found_types & supported
    uncovered = found_types - supported

    total_nodes = len(job.nodes)
    covered_nodes = sum(1 for n in job.nodes if n.component_name in supported)
    component_pct = (covered_nodes / max(total_nodes, 1)) * 100

    # Expression stats
    stats = expr_translator.stats
    total_exprs = max(stats.get("total", 1), 1)
    auto_pct = (stats.get("auto", 0) / total_exprs) * 100
    partial_pct = (stats.get("partial", 0) / total_exprs) * 100
    manual_pct = (stats.get("manual", 0) / total_exprs) * 100

    overall_pct = (auto_pct * 0.6 + component_pct * 0.4)

    # Warnings
    all_warnings = []
    for frag in fragments:
        all_warnings.extend(frag.warnings)

    # Secrets needed
    secrets = set()
    for frag in fragments:
        secrets.update(frag.secrets)

    # Context params
    ctx = job.default_context_obj
    cred_count = 0
    if ctx:
        for p in ctx.parameters:
            pname = p.name.lower()
            if any(k in pname for k in ('password', 'secret', 'key', 'pass')):
                cred_count += 1

    if json_output:
        report = {
            "job_name": job.name,
            "total_nodes": total_nodes,
            "total_connections": len(job.connections),
            "component_types": sorted(found_types),
            "covered_types": sorted(covered),
            "uncovered_types": sorted(uncovered),
            "component_coverage_pct": round(component_pct, 1),
            "expression_stats": stats,
            "expression_auto_pct": round(auto_pct, 1),
            "overall_auto_conversion_pct": round(overall_pct, 1),
            "warnings": all_warnings,
            "secrets_required": sorted(secrets),
            "credential_params": cred_count,
        }
        click.echo(json.dumps(report, indent=2))
        return

    # Pretty print
    click.echo("=" * 68)
    click.echo(f"  MIGRATION READINESS REPORT: {job.name}")
    click.echo("=" * 68)
    click.echo()

    click.echo("📊 Job Summary")
    click.echo(f"  Nodes:       {total_nodes}")
    click.echo(f"  Connections: {len(job.connections)}")
    click.echo(f"  Contexts:    {len(job.contexts)} ({job.default_context} default)")
    click.echo()

    click.echo("🧩 Component Coverage")
    click.echo(f"  Found types:    {len(found_types)}")
    click.echo(f"  Supported:      {len(covered)} ({component_pct:.0f}%)")
    if uncovered:
        click.echo(f"  Unsupported:    {len(uncovered)}")
        for t in sorted(uncovered):
            click.echo(f"    ⚠  {t}")
    click.echo()

    click.echo("  Component breakdown:")
    from collections import Counter
    comp_counts = Counter(n.component_name for n in job.nodes)
    for comp, count in comp_counts.most_common():
        marker = "✅" if comp in supported else "❌"
        click.echo(f"    {marker} {comp}: {count}")
    click.echo()

    click.echo("🔀 Expression Translation")
    click.echo(f"  Total expressions:  {stats['total']}")
    click.echo(f"  Auto-translated:    {stats['auto']} ({auto_pct:.0f}%)")
    click.echo(f"  Partial:            {stats['partial']} ({partial_pct:.0f}%)")
    click.echo(f"  Manual needed:      {stats['manual']} ({manual_pct:.0f}%)")
    click.echo()

    # tMap details
    for node in job.get_nodes_by_type("tMap"):
        if node.mapper_data:
            md = node.mapper_data
            click.echo(f"  📋 tMap: {node.unique_name}")
            click.echo(f"    Inputs:  {len(md.input_tables)} ({', '.join(t.name for t in md.input_tables)})")
            click.echo(f"    Outputs: {len(md.output_tables)} ({', '.join(t.name for t in md.output_tables)})")
            for ot in md.output_tables:
                click.echo(f"    Output '{ot.name}': {len(ot.entries)} columns")
                if ot.expression_filter:
                    click.echo(f"    Filter: {ot.expression_filter}")
                complex_exprs = [
                    e for e in ot.entries
                    if e.expression and any(
                        func in e.expression for func in
                        ['StringHandling', 'CommonDb', 'TalendString', 'TalendDate',
                         'Relational', '?', '.equals(']
                    )
                ]
                if complex_exprs:
                    click.echo(f"    Complex expressions: {len(complex_exprs)}")
                    for e in complex_exprs[:5]:
                        click.echo(f"      • {e.name}: {e.expression[:80]}...")
            click.echo()

    click.echo("🔐 Security")
    click.echo(f"  Credential params: {cred_count} (→ Databricks secret scope)")
    if secrets:
        click.echo(f"  Secret scope keys needed: {len(secrets)}")
    click.echo()

    if all_warnings:
        click.echo("⚠️  Warnings")
        for w in all_warnings[:10]:
            click.echo(f"  • {w}")
        if len(all_warnings) > 10:
            click.echo(f"  ... and {len(all_warnings) - 10} more")
        click.echo()

    click.echo("━" * 68)
    click.echo(f"  🎯 Overall Auto-Conversion Estimate: {overall_pct:.0f}%")
    if overall_pct >= 80:
        click.echo("  ✅ HIGH readiness — mostly automated migration")
    elif overall_pct >= 50:
        click.echo("  🟡 MEDIUM readiness — significant manual work needed")
    else:
        click.echo("  🔴 LOW readiness — substantial manual effort required")
    click.echo("━" * 68)


@cli.command()
@click.argument("item_file", type=click.Path(exists=True))
@click.option("--output-dir", "-o", default=None, help="Output directory for generated project")
def convert(item_file: str, output_dir: str | None):
    """Convert a Talend .item file to a Databricks project.

    Generates main.py, transformations.py, sql_statements.py, connections.py,
    config/job.yml, tests/test_main.py, and migration-report.json.
    """
    if output_dir is None:
        base = os.path.splitext(os.path.basename(item_file))[0]
        output_dir = os.path.join(os.getcwd(), f"{base}_databricks")

    click.echo(f"🔄 Converting: {item_file}")
    click.echo(f"📁 Output:     {output_dir}")
    click.echo()

    job = parse_item_file(item_file)

    generator = DatabricksGenerator(job, output_dir)
    report = generator.generate()
    report["source_file"] = os.path.abspath(item_file)

    # Update report file with source
    report_path = os.path.join(output_dir, "migration-report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    click.echo("✅ Generated files:")
    for fname in report.get("generated_files", []):
        fpath = os.path.join(output_dir, fname)
        size = os.path.getsize(fpath) if os.path.exists(fpath) else 0
        click.echo(f"  📄 {fname} ({size:,} bytes)")

    click.echo()
    click.echo(f"📊 Summary:")
    summary = report.get("summary", {})
    click.echo(f"  Nodes converted:    {summary.get('total_nodes', 0)}")
    click.echo(f"  Component coverage: {summary.get('component_coverage_pct', 0)}%")
    click.echo(f"  Expression auto:    {summary.get('expression_auto_conversion_pct', 0)}%")
    click.echo(f"  Overall:            {summary.get('overall_auto_conversion_pct', 0)}%")

    warnings = report.get("warnings", [])
    if warnings:
        click.echo(f"\n⚠️  {len(warnings)} warnings — see migration-report.json for details")


@cli.command()
@click.argument("item_file", type=click.Path(exists=True))
@click.option("--workspace", "-w", required=True, help="Databricks workspace URL")
@click.option("--job-name", "-n", required=True, help="Databricks job name")
@click.option("--output-dir", "-o", default=None, help="Output directory")
def deploy(item_file: str, workspace: str, job_name: str, output_dir: str | None):
    """Generate Databricks project + Jobs API deployment JSON.

    Does NOT actually call the Databricks API — produces the JSON payload
    that can be used with `databricks jobs create --json @deploy.json`.
    """
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), f"{job_name}_deploy")

    click.echo(f"🚀 Generating deployment for: {item_file}")
    click.echo(f"   Workspace: {workspace}")
    click.echo(f"   Job name:  {job_name}")
    click.echo(f"   Output:    {output_dir}")
    click.echo()

    job = parse_item_file(item_file)

    # Generate project
    generator = DatabricksGenerator(job, output_dir)
    report = generator.generate()

    # Generate deploy JSON
    deploy_payload = generate_deploy_json(job, workspace, job_name)
    deploy_payload["tasks"][0]["spark_python_task"]["python_file"] = (
        f"dbfs:/jobs/{job_name}/main.py"
    )

    deploy_path = os.path.join(output_dir, "deploy.json")
    with open(deploy_path, "w") as f:
        json.dump(deploy_payload, f, indent=2)

    click.echo("✅ Generated deployment package:")
    click.echo(f"  📄 deploy.json (Databricks Jobs API payload)")
    for fname in report.get("generated_files", []):
        click.echo(f"  📄 {fname}")

    click.echo()
    click.echo("To deploy:")
    click.echo(f"  databricks jobs create --json @{deploy_path}")
    click.echo(f"  databricks workspace import-dir {output_dir} /jobs/{job_name}")


if __name__ == "__main__":
    cli()
