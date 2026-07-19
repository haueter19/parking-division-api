"""
mermaid.py
==========
Turns a SchemaSnapshot into Mermaid `erDiagram` syntax. Two renderers:

  overview_diagram()  : all tables, minimal attributes (PK/FK + geometry
                         flag only) — meant for the landing page so it
                         stays readable with 10+ tables.
  table_diagram()     : one table, full column list — used on the detail
                         page.

Mermaid's erDiagram grammar is picky: identifiers can't contain dots or
spaces, and attribute comments must be double-quoted with no embedded
quotes. `_safe()` and `_comment()` handle that.
"""

from __future__ import annotations

from .models import SchemaSnapshot, TableMeta


def _comment(text: str | None, limit: int = 60) -> str:
    if not text:
        return ""
    cleaned = text.replace('"', "'").replace("\n", " ").strip()
    if len(cleaned) > limit:
        cleaned = cleaned[: limit - 1] + "\u2026"
    return f' "{cleaned}"'


def _table_block(t: TableMeta, full: bool) -> str:
    lines = [f"    {t.safe_id} {{"]
    cols = t.columns if full else [c for c in t.columns if c.is_primary_key or c.is_foreign_key or c.is_geometry]
    for c in cols:
        flags = []
        if c.is_primary_key:
            flags.append("PK")
        if c.is_foreign_key:
            flags.append("FK")
        flag_str = " ".join(flags)
        note = c.notes if full else None
        lines.append(f"        {c.simple_type} {c.name} {flag_str}{_comment(note)}".rstrip())
    if not full and len(cols) < len(t.columns):
        lines.append(f'        string ... "+{len(t.columns) - len(cols)} more columns"')
    lines.append("    }")
    return "\n".join(lines)


def overview_diagram(snapshot: SchemaSnapshot) -> str:
    lines = ["erDiagram"]
    for t in snapshot.tables:
        lines.append(_table_block(t, full=False))

    id_by_full_name = {t.full_name: t.safe_id for t in snapshot.tables}
    for r in snapshot.relationships:
        a, b = id_by_full_name.get(r.from_table), id_by_full_name.get(r.to_table)
        if a and b:
            lines.append(f'    {b} ||--o{{ {a} : "{r.label}"')
    return "\n".join(lines)


def table_diagram(snapshot: SchemaSnapshot, schema: str, table: str) -> str:
    t = snapshot.get_table(schema, table)
    if t is None:
        return "erDiagram"
    lines = ["erDiagram", _table_block(t, full=True)]

    id_by_full_name = {tt.full_name: tt.safe_id for tt in snapshot.tables}
    for r in snapshot.relationships:
        if r.from_table == t.full_name or r.to_table == t.full_name:
            other_full = r.to_table if r.from_table == t.full_name else r.from_table
            other_id = id_by_full_name.get(other_full)
            if other_id and other_id != t.safe_id:
                # Just draw the related table as a stub box so context is visible
                lines.append(f"    {other_id} {{\n        string ... \"related table\"\n    }}")
                if r.from_table == t.full_name:
                    lines.append(f'    {t.safe_id} }}o--|| {other_id} : "{r.label}"')
                else:
                    lines.append(f'    {other_id} ||--o{{ {t.safe_id} : "{r.label}"')
    return "\n".join(lines)
