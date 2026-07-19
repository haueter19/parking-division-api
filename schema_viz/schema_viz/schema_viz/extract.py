"""
extract.py
==========
Pulls live metadata from SQL Server (and, where present, an SDE-managed
geodatabase) into a SchemaSnapshot. This is the only module that talks to
the database — mermaid.py and webapp.py never touch SQL directly, so the
extraction logic can evolve independently of the rendering.

Usage
-----
    from sqlalchemy import create_engine
    engine = create_engine("mssql+pyodbc://...")   # or reuse your ConnectionManager's engine
    snapshot = build_snapshot(engine, schemas=["dw"], source_description="PUReporting.dw")

Notes on documentation-as-you-go
---------------------------------
Table/column "notes" and data-quality notes are read from SQL Server
EXTENDED PROPERTIES, not a separate wiki or spreadsheet. To document a
table:

    EXEC sys.sp_addextendedproperty
        @name = N'MS_Description',
        @value = N'Entry/exit event pairs; source of truth for occupancy rebuild.',
        @level0type = N'SCHEMA', @level0name = 'dw',
        @level1type = N'TABLE',  @level1name = 'VisitDetails';

    EXEC sys.sp_addextendedproperty
        @name = N'DQ_Notes',
        @value = N'OOS backlog causes both under- and overstated occupancy; see worklist.',
        @level0type = N'SCHEMA', @level0name = 'dw',
        @level1type = N'TABLE',  @level1name = 'VisitDetails';

Because these live in the database, they show up automatically on the next
extractor run — no separate step to keep documentation in sync.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .models import ColumnMeta, TableMeta, RelationshipMeta, SchemaSnapshot, now_iso
from .type_map import simplify

# ---------------------------------------------------------------------------
# SQL Server system-view queries
# ---------------------------------------------------------------------------

_TABLES_SQL = """
SELECT s.name AS schema_name, t.name AS table_name
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN :schemas
ORDER BY s.name, t.name
"""

_COLUMNS_SQL = """
SELECT
    c.name                                   AS column_name,
    ty.name                                  AS data_type,
    c.max_length,
    c.is_nullable,
    c.column_id                              AS ordinal_position,
    CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
    CASE WHEN fk.parent_column_id IS NOT NULL THEN 1 ELSE 0 END AS is_foreign_key
FROM sys.columns c
JOIN sys.types ty ON c.user_type_id = ty.user_type_id
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN (
    SELECT ic.object_id, ic.column_id
    FROM sys.index_columns ic
    JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    WHERE i.is_primary_key = 1
) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
LEFT JOIN sys.foreign_key_columns fk
    ON fk.parent_object_id = c.object_id AND fk.parent_column_id = c.column_id
WHERE s.name = :schema AND t.name = :table
ORDER BY c.column_id
"""

_ROWCOUNT_SQL = """
SELECT SUM(p.rows) AS row_count
FROM sys.partitions p
JOIN sys.tables t ON p.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = :schema AND t.name = :table AND p.index_id IN (0, 1)
"""

_TABLE_EXT_PROPS_SQL = """
SELECT ep.name, ep.value
FROM sys.extended_properties ep
JOIN sys.tables t ON ep.major_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE ep.minor_id = 0 AND s.name = :schema AND t.name = :table
"""

_COLUMN_EXT_PROPS_SQL = """
SELECT c.name AS column_name, ep.value
FROM sys.extended_properties ep
JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE ep.name = 'MS_Description' AND s.name = :schema AND t.name = :table
"""

_FOREIGN_KEYS_SQL = """
SELECT
    ps.name AS parent_schema, pt.name AS parent_table, pc.name AS parent_column,
    rs.name AS ref_schema,    rt.name AS ref_table,    rc.name AS ref_column,
    fk.name AS fk_name
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.tables pt ON fkc.parent_object_id = pt.object_id
JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
JOIN sys.tables rt ON fkc.referenced_object_id = rt.object_id
JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
WHERE ps.name IN :schemas
"""

# Native SQL Server 'geometry'/'geography' typed columns (not SDE feature
# classes — those are covered separately below via GDB_GeomColumns, since
# SDE stores geometry in a binary column with type metadata in its catalog).
_NATIVE_GEOMETRY_SQL = """
SELECT s.name AS schema_name, t.name AS table_name, c.name AS column_name,
       ty.name AS type_name
FROM sys.columns c
JOIN sys.types ty ON c.user_type_id = ty.user_type_id
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE ty.name IN ('geometry', 'geography') AND s.name IN :schemas
"""

# SDE catalog lookup for feature-class geometry type + SRID. Adjust the
# owner-qualified table name (SDE.GDB_GeomColumns / SDE.GDB_SpatialRefs) if
# your SDE catalog uses a different qualifier.
_SDE_GEOM_SQL = """
SELECT
    gc.TABLE_NAME AS table_name,
    gc.GEOM_COLUMN AS column_name,
    gc.GEOM_TYPE AS geom_type_code,
    sr.SRID AS srid
FROM SDE.GDB_GeomColumns gc
LEFT JOIN SDE.GDB_SpatialRefs sr ON gc.SRID = sr.SRID
"""

# SDE geometry type codes -> human-readable labels (per Esri's ST_Geometry
# / SDE catalog convention: 1=Point, 2=Line/Polyline, 3=Area/Polygon, ...)
_SDE_GEOM_TYPE_LABELS = {
    1: "Point", 2: "Polyline", 3: "Polygon",
    4: "MultiPoint", 9: "MultiPatch",
}


def _fetch_all(engine: Engine, sql: str, params: dict) -> list[dict]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        return [dict(row._mapping) for row in result]


def _get_extended_properties(engine: Engine, schema: str, table: str) -> tuple[dict, dict]:
    """Returns (table_props: {name: value}, column_props: {col_name: description})."""
    table_rows = _fetch_all(engine, _TABLE_EXT_PROPS_SQL, {"schema": schema, "table": table})
    table_props = {r["name"]: r["value"] for r in table_rows}
    col_rows = _fetch_all(engine, _COLUMN_EXT_PROPS_SQL, {"schema": schema, "table": table})
    col_props = {r["column_name"]: r["value"] for r in col_rows}
    return table_props, col_props


def _get_row_count(engine: Engine, schema: str, table: str) -> tuple[int | None, bool]:
    rows = _fetch_all(engine, _ROWCOUNT_SQL, {"schema": schema, "table": table})
    count = rows[0]["row_count"] if rows else None
    return count, True  # sys.partitions counts are exact for heap/clustered index


def _get_native_geometry_map(engine: Engine, schemas: list[str]) -> dict[tuple[str, str], str]:
    rows = _fetch_all(engine, _NATIVE_GEOMETRY_SQL, {"schemas": tuple(schemas)})
    return {(r["schema_name"], r["table_name"]): r["type_name"] for r in rows}


def _get_sde_geometry_map(engine: Engine) -> dict[str, tuple[str, int | None]]:
    """Best-effort: returns {table_name: (geom_type_label, srid)}. Swallows
    errors if SDE.GDB_GeomColumns isn't reachable from this connection."""
    try:
        rows = _fetch_all(engine, _SDE_GEOM_SQL, {})
    except Exception:
        return {}
    out = {}
    for r in rows:
        label = _SDE_GEOM_TYPE_LABELS.get(r["geom_type_code"], f"code_{r['geom_type_code']}")
        out[r["table_name"]] = (label, r["srid"])
    return out


def build_snapshot(
    engine: Engine,
    schemas: list[str],
    source_description: str,
    include_sde_geometry: bool = False,
) -> SchemaSnapshot:
    table_rows = _fetch_all(engine, _TABLES_SQL, {"schemas": tuple(schemas)})
    native_geom = _get_native_geometry_map(engine, schemas)
    sde_geom = _get_sde_geometry_map(engine) if include_sde_geometry else {}

    tables: list[TableMeta] = []
    for tr in table_rows:
        schema, table = tr["schema_name"], tr["table_name"]
        col_rows = _fetch_all(engine, _COLUMNS_SQL, {"schema": schema, "table": table})
        table_props, col_props = _get_extended_properties(engine, schema, table)
        row_count, is_exact = _get_row_count(engine, schema, table)

        geom_type, srid = None, None
        if table in sde_geom:
            geom_type, srid = sde_geom[table]

        columns = []
        for cr in col_rows:
            is_geom_col = (schema, table) in native_geom or cr["column_name"].lower() in (
                "shape", "geom", "geometry",
            )
            columns.append(ColumnMeta(
                name=cr["column_name"],
                data_type=f"{cr['data_type']}({cr['max_length']})" if cr["data_type"] in
                    ("varchar", "nvarchar", "char", "nchar") else cr["data_type"],
                simple_type="geometry" if is_geom_col else simplify(cr["data_type"]),
                nullable=bool(cr["is_nullable"]),
                ordinal_position=cr["ordinal_position"],
                is_primary_key=bool(cr["is_primary_key"]),
                is_foreign_key=bool(cr["is_foreign_key"]),
                is_geometry=is_geom_col,
                notes=col_props.get(cr["column_name"]),
            ))

        tables.append(TableMeta(
            schema=schema,
            name=table,
            columns=columns,
            row_count=row_count,
            row_count_is_exact=is_exact,
            geometry_type=geom_type,
            srid=srid,
            notes=table_props.get("MS_Description"),
            data_quality_notes=table_props.get("DQ_Notes"),
        ))

    fk_rows = _fetch_all(engine, _FOREIGN_KEYS_SQL, {"schemas": tuple(schemas)})
    relationships = [
        RelationshipMeta(
            from_table=f"{r['parent_schema']}.{r['parent_table']}",
            to_table=f"{r['ref_schema']}.{r['ref_table']}",
            from_column=r["parent_column"],
            to_column=r["ref_column"],
            label=r["fk_name"],
        )
        for r in fk_rows
    ]

    return SchemaSnapshot(
        generated_at=now_iso(),
        source_description=source_description,
        tables=tables,
        relationships=relationships,
    )
