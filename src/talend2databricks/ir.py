"""Intermediate Representation dataclasses for Talend job model."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Column:
    name: str
    type: str
    nullable: bool = True
    length: int = 0
    precision: int = 0
    default_value: str = ""
    source_type: str = ""
    key: bool = False
    comment: str = ""


@dataclass
class SchemaMetadata:
    connector: str
    name: str
    label: str = ""
    columns: list[Column] = field(default_factory=list)


@dataclass
class ElementParameter:
    field_type: str  # TEXT, CHECK, MEMO_SQL, etc.
    name: str
    value: str
    show: bool = True


@dataclass
class MapperTableEntry:
    name: str
    expression: str = ""
    type: str = ""
    nullable: bool = True
    operator: str = ""  # for join expressions


@dataclass
class MapperTable:
    name: str
    matching_mode: str = ""  # UNIQUE_MATCH, ALL_MATCHES
    lookup_mode: str = ""  # LOAD_ONCE, RELOAD
    size_state: str = ""
    expression_filter: str = ""
    activate_expression_filter: bool = False
    entries: list[MapperTableEntry] = field(default_factory=list)
    inner_join: bool = False
    persistent: bool = False


@dataclass
class MapperData:
    input_tables: list[MapperTable] = field(default_factory=list)
    output_tables: list[MapperTable] = field(default_factory=list)
    var_tables: list[MapperTable] = field(default_factory=list)


@dataclass
class TalendConnection:
    connector_name: str  # FLOW, SUBJOB_OK, SUBJOB_ERROR, COMPONENT_OK, etc.
    label: str
    source: str
    target: str
    line_style: int = 0
    metaname: str = ""


@dataclass
class TalendNode:
    component_name: str
    unique_name: str
    parameters: dict[str, ElementParameter] = field(default_factory=dict)
    metadata: list[SchemaMetadata] = field(default_factory=list)
    mapper_data: Optional[MapperData] = None
    pos_x: int = 0
    pos_y: int = 0
    label: str = ""

    def get_param(self, name: str, default: str = "") -> str:
        """Get parameter value by name."""
        p = self.parameters.get(name)
        return p.value if p else default

    def get_param_bool(self, name: str) -> bool:
        return self.get_param(name, "false").lower() == "true"

    @property
    def display_name(self) -> str:
        return self.label or self.unique_name


@dataclass
class ContextParameter:
    name: str
    type: str
    value: str
    prompt: str = ""
    comment: str = ""


@dataclass
class TalendContext:
    name: str
    parameters: list[ContextParameter] = field(default_factory=list)


@dataclass
class TalendJob:
    """Full intermediate representation of a parsed Talend .item file."""
    name: str = ""
    job_type: str = "Standard"
    default_context: str = ""
    nodes: list[TalendNode] = field(default_factory=list)
    connections: list[TalendConnection] = field(default_factory=list)
    contexts: list[TalendContext] = field(default_factory=list)

    # Derived indexes (populated after parse)
    _node_map: dict[str, TalendNode] = field(default_factory=dict, repr=False)
    _flow_graph: dict[str, list[str]] = field(default_factory=dict, repr=False)
    _control_graph: dict[str, list[tuple[str, str]]] = field(default_factory=dict, repr=False)

    def build_indexes(self):
        """Build lookup indexes from parsed data."""
        self._node_map = {n.unique_name: n for n in self.nodes}
        self._flow_graph = {}
        self._control_graph = {}
        for c in self.connections:
            if c.connector_name == "FLOW":
                self._flow_graph.setdefault(c.source, []).append(c.target)
            else:
                self._control_graph.setdefault(c.source, []).append(
                    (c.target, c.connector_name)
                )

    def get_node(self, unique_name: str) -> Optional[TalendNode]:
        return self._node_map.get(unique_name)

    def get_flow_targets(self, node_name: str) -> list[str]:
        return self._flow_graph.get(node_name, [])

    def get_control_targets(self, node_name: str) -> list[tuple[str, str]]:
        return self._control_graph.get(node_name, [])

    @property
    def component_types(self) -> set[str]:
        return {n.component_name for n in self.nodes}

    @property
    def default_context_obj(self) -> Optional[TalendContext]:
        for c in self.contexts:
            if c.name == self.default_context:
                return c
        return self.contexts[0] if self.contexts else None

    def get_nodes_by_type(self, component_name: str) -> list[TalendNode]:
        return [n for n in self.nodes if n.component_name == component_name]

    def get_prejob_nodes(self) -> list[TalendNode]:
        return self.get_nodes_by_type("tPrejob")

    def get_postjob_nodes(self) -> list[TalendNode]:
        return self.get_nodes_by_type("tPostjob")

    def get_connection_nodes(self) -> list[TalendNode]:
        return [n for n in self.nodes if n.component_name in (
            "tRedshiftConnection", "tMysqlConnection", "tS3Connection"
        )]

    def get_close_nodes(self) -> list[TalendNode]:
        return [n for n in self.nodes if n.component_name in (
            "tRedshiftClose", "tMysqlClose"
        )]
