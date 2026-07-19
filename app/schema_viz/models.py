"""
models.py
=========
Plain dataclasses representing a snapshot of the data model. This is the
shape that gets serialized to JSON by store.py and consumed by mermaid.py
and webapp.py. Keeping this as a stable, explicit contract is what makes
the pipeline reproducible: extract.py can change internally, but as long
as it keeps producing a SchemaSnapshot, nothing downstream breaks.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional


@dataclass
class ColumnMeta:
    name: str
    data_type: str            # raw SQL type, e.g. "varchar(50)", "datetime2"
    simple_type: str          # collapsed type used in the Mermaid diagram, e.g. "string"
    nullable: bool
    ordinal_position: int
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_geometry: bool = False
    notes: Optional[str] = None   # from extended property, if present


@dataclass
class RelationshipMeta:
    from_table: str        # "schema.table"
    to_table: str          # "schema.table"
    from_column: str
    to_column: str
    label: str = "FK"


@dataclass
class TableMeta:
    schema: str
    name: str
    columns: list[ColumnMeta]
    row_count: Optional[int] = None
    row_count_is_exact: bool = True
    geometry_type: Optional[str] = None      # e.g. "Point", "Polygon", "Polyline"
    srid: Optional[int] = None
    notes: Optional[str] = None              # table-level MS_Description
    data_quality_notes: Optional[str] = None  # table-level custom "DQ_Notes" property
    last_modified: Optional[str] = None      # from sys.dm_db_index_usage_stats if available

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def safe_id(self) -> str:
        """Mermaid-safe identifier (no dots)."""
        return f"{self.schema}_{self.name}"


@dataclass
class SchemaSnapshot:
    generated_at: str
    source_description: str
    tables: list[TableMeta] = field(default_factory=list)
    relationships: list[RelationshipMeta] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "SchemaSnapshot":
        tables = [
            TableMeta(
                **{**t, "columns": [ColumnMeta(**c) for c in t["columns"]]}
            )
            for t in d["tables"]
        ]
        rels = [RelationshipMeta(**r) for r in d.get("relationships", [])]
        return cls(
            generated_at=d["generated_at"],
            source_description=d["source_description"],
            tables=tables,
            relationships=rels,
        )

    def get_table(self, schema: str, name: str) -> Optional[TableMeta]:
        for t in self.tables:
            if t.schema.lower() == schema.lower() and t.name.lower() == name.lower():
                return t
        return None


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")
