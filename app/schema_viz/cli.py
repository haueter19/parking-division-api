"""
cli.py
======
Reproducible entry point: run this any time the schema changes and the
snapshot (and therefore the diagrams) update.

    python -m schema_viz.cli --conn-string "mssql+pyodbc://..." \\
        --schemas dw --source "PUReporting.dw" --out snapshots

If you already have a SQLAlchemy engine from your own ConnectionManager,
skip this CLI and call build_snapshot()/save_snapshot() directly from a
notebook or scheduled job instead — that's often more convenient since it
reuses your existing connection/auth setup.
"""

from __future__ import annotations

import argparse

from sqlalchemy import create_engine

from .extract import build_snapshot
from .store import save_snapshot


def main():
    parser = argparse.ArgumentParser(description="Extract a schema snapshot for visualization.")
    parser.add_argument("--conn-string", required=True, help="SQLAlchemy connection string")
    parser.add_argument("--schemas", required=True, help="Comma-separated schema names, e.g. dw")
    parser.add_argument("--source", required=True, help="Human-readable label for this source")
    parser.add_argument("--out", default="snapshots", help="Output directory for snapshot JSON")
    parser.add_argument("--include-sde-geometry", action="store_true",
                         help="Also look up SDE.GDB_GeomColumns for feature-class geometry types")
    args = parser.parse_args()

    engine = create_engine(args.conn_string)
    schemas = [s.strip() for s in args.schemas.split(",")]

    snapshot = build_snapshot(
        engine,
        schemas=schemas,
        source_description=args.source,
        include_sde_geometry=args.include_sde_geometry,
    )
    path = save_snapshot(snapshot, args.out)
    print(f"Wrote snapshot: {path}  ({len(snapshot.tables)} tables, "
          f"{len(snapshot.relationships)} relationships)")


if __name__ == "__main__":
    main()
