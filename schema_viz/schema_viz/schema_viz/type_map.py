"""
type_map.py
===========
Collapses SQL Server data types into a small vocabulary Mermaid can render
cleanly. Mermaid's erDiagram attribute-type token doesn't handle things like
"varchar(50)" well (parentheses/spaces break parsing in some renderers), so
the raw type is kept in ColumnMeta.data_type for the detail view, and this
simplified token is what actually goes in the diagram.
"""

_TYPE_GROUPS = {
    "string": {"char", "varchar", "nchar", "nvarchar", "text", "ntext", "xml"},
    "int": {"int", "bigint", "smallint", "tinyint"},
    "decimal": {"decimal", "numeric", "float", "real", "money", "smallmoney"},
    "bool": {"bit"},
    "datetime": {"datetime", "datetime2", "smalldatetime", "date", "time", "datetimeoffset"},
    "binary": {"binary", "varbinary", "image", "rowversion", "timestamp"},
    "guid": {"uniqueidentifier"},
    "geometry": {"geometry", "geography", "st_geometry"},
}

_LOOKUP = {v: k for k, vs in _TYPE_GROUPS.items() for v in vs}


def simplify(sql_type: str) -> str:
    base = sql_type.split("(")[0].strip().lower()
    return _LOOKUP.get(base, "other")
