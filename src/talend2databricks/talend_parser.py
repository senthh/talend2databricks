"""Parse Talend .item XML files into TalendJob IR."""
from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from html import unescape
from typing import Optional

from .ir import (
    Column,
    ContextParameter,
    ElementParameter,
    MapperData,
    MapperTable,
    MapperTableEntry,
    SchemaMetadata,
    TalendConnection,
    TalendContext,
    TalendJob,
    TalendNode,
)

# Namespace map for Talend .item XML
NS = {
    "xmi": "http://www.omg.org/XMI",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "TalendMapper": "http://www.talend.org/mapper",
    "talendfile": "platform:/resource/org.talend.model/model/TalendFile.xsd",
}


def _attr(elem: ET.Element, name: str, default: str = "") -> str:
    """Get attribute, unescaping HTML entities."""
    val = elem.get(name, default)
    return unescape(val) if val else default


def _bool_attr(elem: ET.Element, name: str) -> bool:
    return _attr(elem, name, "false").lower() == "true"


def _parse_column(elem: ET.Element) -> Column:
    return Column(
        name=_attr(elem, "name"),
        type=_attr(elem, "type"),
        nullable=_bool_attr(elem, "nullable"),
        length=int(_attr(elem, "length", "0") or "0"),
        precision=int(_attr(elem, "precision", "0") or "0"),
        default_value=_attr(elem, "defaultValue"),
        source_type=_attr(elem, "sourceType"),
        key=_bool_attr(elem, "key"),
        comment=_attr(elem, "comment"),
    )


def _parse_metadata(elem: ET.Element) -> SchemaMetadata:
    sm = SchemaMetadata(
        connector=_attr(elem, "connector"),
        name=_attr(elem, "name"),
        label=_attr(elem, "label"),
    )
    for col_elem in elem.findall("column"):
        sm.columns.append(_parse_column(col_elem))
    return sm


def _parse_mapper_entry(elem: ET.Element) -> MapperTableEntry:
    return MapperTableEntry(
        name=_attr(elem, "name"),
        expression=_attr(elem, "expression"),
        type=_attr(elem, "type"),
        nullable=_bool_attr(elem, "nullable"),
        operator=_attr(elem, "operator"),
    )


def _parse_mapper_table(elem: ET.Element, tag_name: str) -> MapperTable:
    mt = MapperTable(
        name=_attr(elem, "name"),
        matching_mode=_attr(elem, "matchingMode"),
        lookup_mode=_attr(elem, "lookupMode"),
        size_state=_attr(elem, "sizeState"),
        expression_filter=_attr(elem, "expressionFilter"),
        activate_expression_filter=_bool_attr(elem, "activateExpressionFilter"),
        inner_join=_bool_attr(elem, "innerJoin"),
        persistent=_bool_attr(elem, "persistent"),
    )
    for entry_elem in elem.findall("mapperTableEntries"):
        mt.entries.append(_parse_mapper_entry(entry_elem))
    return mt


def _parse_mapper_data(elem: ET.Element) -> MapperData:
    md = MapperData()
    for it in elem.findall("inputTables"):
        md.input_tables.append(_parse_mapper_table(it, "inputTables"))
    for ot in elem.findall("outputTables"):
        md.output_tables.append(_parse_mapper_table(ot, "outputTables"))
    for vt in elem.findall("varTables"):
        md.var_tables.append(_parse_mapper_table(vt, "varTables"))
    return md


def _parse_node(elem: ET.Element) -> TalendNode:
    node = TalendNode(
        component_name=_attr(elem, "componentName"),
        unique_name="",  # filled from UNIQUE_NAME param
        pos_x=int(_attr(elem, "posX", "0")),
        pos_y=int(_attr(elem, "posY", "0")),
    )

    # Parse elementParameters
    for ep in elem.findall("elementParameter"):
        param = ElementParameter(
            field_type=_attr(ep, "field"),
            name=_attr(ep, "name"),
            value=_attr(ep, "value"),
            show=_bool_attr(ep, "show") if ep.get("show") is not None else True,
        )
        node.parameters[param.name] = param
        if param.name == "UNIQUE_NAME":
            node.unique_name = param.value
        elif param.name == "LABEL":
            node.label = param.value

    # Parse metadata
    for md in elem.findall("metadata"):
        node.metadata.append(_parse_metadata(md))

    # Parse nodeData (tMap MapperData)
    for nd in elem.findall("nodeData"):
        xsi_type = nd.get(f"{{{NS['xsi']}}}type", "")
        if "MapperData" in xsi_type:
            node.mapper_data = _parse_mapper_data(nd)

    return node


def _parse_connection(elem: ET.Element) -> TalendConnection:
    return TalendConnection(
        connector_name=_attr(elem, "connectorName"),
        label=_attr(elem, "label"),
        source=_attr(elem, "source"),
        target=_attr(elem, "target"),
        line_style=int(_attr(elem, "lineStyle", "0")),
        metaname=_attr(elem, "metaname"),
    )


def _parse_context(elem: ET.Element) -> TalendContext:
    ctx = TalendContext(name=_attr(elem, "name"))
    for cp in elem.findall("contextParameter"):
        ctx.parameters.append(ContextParameter(
            name=_attr(cp, "name"),
            type=_attr(cp, "type"),
            value=_attr(cp, "value"),
            prompt=_attr(cp, "prompt"),
            comment=_attr(cp, "comment"),
        ))
    return ctx


def parse_item_file(filepath: str) -> TalendJob:
    """Parse a Talend .item XML file into a TalendJob IR.

    Args:
        filepath: Path to the .item file.

    Returns:
        Populated TalendJob with all nodes, connections, contexts, and indexes.
    """
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Item file not found: {filepath}")

    tree = ET.parse(filepath)
    root = tree.getroot()

    job = TalendJob(
        name=os.path.splitext(os.path.basename(filepath))[0],
        default_context=root.get("defaultContext", ""),
        job_type=root.get("jobType", "Standard"),
    )

    # Strip version suffix from name (e.g. LARGE_ENTERPRISE_RPT_3.0 -> LARGE_ENTERPRISE_RPT)
    base = job.name
    for suffix in [".0", "_0.1", "_1.0", "_2.0", "_3.0"]:
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    # Further strip trailing version patterns like _3, _2 etc
    import re
    base = re.sub(r"_\d+$", "", base)
    job.name = base

    # Parse contexts
    for ctx_elem in root.findall("context"):
        job.contexts.append(_parse_context(ctx_elem))

    # Parse nodes
    for node_elem in root.findall("node"):
        job.nodes.append(_parse_node(node_elem))

    # Parse connections
    for conn_elem in root.findall("connection"):
        job.connections.append(_parse_connection(conn_elem))

    job.build_indexes()
    return job
