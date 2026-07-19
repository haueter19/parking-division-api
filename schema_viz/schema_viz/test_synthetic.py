"""Builds a fake snapshot mirroring Daniel's dw.VisitDetails / dw.VisitSummary
so the pipeline (models -> mermaid -> store -> webapp) can be verified without
a live SQL Server connection."""

import sys
sys.path.insert(0, "/home/claude/schema_viz")

from schema_viz.models import ColumnMeta, TableMeta, RelationshipMeta, SchemaSnapshot, now_iso
from schema_viz.store import save_snapshot
from schema_viz.mermaid import overview_diagram, table_diagram

visit_details = TableMeta(
    schema="dw", name="VisitDetails",
    row_count=4_812_330, row_count_is_exact=True,
    notes="Entry/exit event pairs; source of truth for occupancy rebuild.",
    data_quality_notes="OOS backlog causes both under- and overstated occupancy in opposite directions; see worklist output from occupancy_pipeline.py.",
    columns=[
        ColumnMeta("TripDetailID", "varchar(32)", "string", False, 1, is_primary_key=True),
        ColumnMeta("GarageID", "int", "int", False, 2),
        ColumnMeta("EntryDate", "datetime2", "datetime", False, 3, notes="Clipped at rebuild window bounds"),
        ColumnMeta("ExitDate", "datetime2", "datetime", True, 4),
        ColumnMeta("customer_type", "varchar(20)", "string", True, 5),
        ColumnMeta("LoadDate", "datetime2", "datetime", False, 6, notes="Bounds rebuild window"),
        ColumnMeta("PermitNumber", "varchar(20)", "string", True, 7, is_foreign_key=True),
    ],
)

visit_summary = TableMeta(
    schema="dw", name="VisitSummary",
    row_count=712_004_112, row_count_is_exact=False,
    notes="Occupancy counts by minute. Fully rebuildable from dw.VisitDetails.",
    columns=[
        ColumnMeta("GarageID", "int", "int", False, 1, is_primary_key=True),
        ColumnMeta("date", "date", "datetime", False, 2, is_primary_key=True),
        ColumnMeta("minute_of_day", "int", "int", False, 3, is_primary_key=True),
        ColumnMeta("dayofweek", "tinyint", "int", False, 4),
        ColumnMeta("transient", "int", "int", False, 5),
        ColumnMeta("permit", "int", "int", False, 6),
    ],
)

meters_on_st = TableMeta(
    schema="SDE", name="PU_METERS_ON_ST",
    row_count=3_204, row_count_is_exact=True,
    notes="On-street meter space inventory.",
    geometry_type="Point", srid=2263,
    columns=[
        ColumnMeta("OBJECTID", "int", "int", False, 1, is_primary_key=True),
        ColumnMeta("cwAssetID", "varchar(20)", "string", True, 2, is_foreign_key=True),
        ColumnMeta("Hrs_Operation", "varchar(100)", "string", True, 3,
                   notes="Freeform enforced-hours string; parsed by meter_hours.py"),
        ColumnMeta("Status", "varchar(20)", "string", True, 4),
        ColumnMeta("Shape", "geometry", "geometry", True, 5, is_geometry=True),
    ],
)

snapshot = SchemaSnapshot(
    generated_at=now_iso(),
    source_description="PUReporting.dw + Traffic.SDE (synthetic test data)",
    tables=[visit_details, visit_summary, meters_on_st],
    relationships=[
        RelationshipMeta("dw.VisitDetails", "dw.VisitSummary", "GarageID", "GarageID", label="rebuilds_into"),
    ],
)

path = save_snapshot(snapshot, "/home/claude/schema_viz/snapshots")
print(f"Saved: {path}")
print("\n--- overview_diagram ---\n")
print(overview_diagram(snapshot))
print("\n--- table_diagram (dw.VisitDetails) ---\n")
print(table_diagram(snapshot, "dw", "VisitDetails"))
