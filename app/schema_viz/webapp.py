"""
webapp.py
=========
Minimal FastAPI app that reads snapshots/latest.json and serves:

  GET /                          high-level ERD (all tables, key columns only)
  GET /table/{schema}/{table}    detail view (full columns, notes, geometry,
                                  row count) with its own focused diagram

Run standalone for development:
    uvicorn schema_viz.webapp:app --reload

To fold into the existing Parking Division API, mount this as a
sub-application or copy the two route functions + `include_router` into
your main FastAPI app; the templates/ directory is self-contained.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .store import load_snapshot
from .mermaid import overview_diagram, table_diagram

BASE_DIR = Path(__file__).parent
SNAPSHOT_PATH = BASE_DIR / "snapshots" / "latest.json"

app = FastAPI(title="Parking Division Data Model Explorer")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _get_snapshot():
    if not SNAPSHOT_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"No snapshot found at {SNAPSHOT_PATH}. Run the extractor first: "
                   f"python -m schema_viz.cli --conn-string ... --schemas dw --source '...'",
        )
    return load_snapshot(SNAPSHOT_PATH)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    snapshot = _get_snapshot()
    diagram = overview_diagram(snapshot)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "diagram": diagram,
        "snapshot": snapshot,
        "tables": sorted(snapshot.tables, key=lambda t: (t.schema, t.name)),
    })


@app.get("/table/{schema}/{table}", response_class=HTMLResponse)
def table_detail(request: Request, schema: str, table: str):
    snapshot = _get_snapshot()
    t = snapshot.get_table(schema, table)
    if t is None:
        raise HTTPException(status_code=404, detail=f"Table {schema}.{table} not found in latest snapshot")
    diagram = table_diagram(snapshot, schema, table)
    return templates.TemplateResponse("table_detail.html", {
        "request": request,
        "diagram": diagram,
        "table": t,
        "snapshot": snapshot,
    })
