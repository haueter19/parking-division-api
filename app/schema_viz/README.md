# Parking Division Data Model Explorer

A reproducible, Python-driven pipeline that extracts your SQL Server / SDE
schema metadata and renders it as interactive Mermaid ER diagrams — a
high-level overview plus a full detail page per table.

## How it fits together

```
extract.py   -> pulls live metadata from SQL Server/SDE (sys.* views + extended properties)
   |
   v
models.py    -> typed dataclasses (SchemaSnapshot / TableMeta / ColumnMeta)
   |
   v
store.py     -> writes snapshots/snapshot_<timestamp>.json + snapshots/latest.json
   |
   v
mermaid.py   -> turns a snapshot into Mermaid erDiagram syntax
   |
   v
webapp.py    -> FastAPI app; reads latest.json, renders overview + drill-down pages
```

Nothing downstream of `store.py` touches the database. That's what makes
reruns cheap and safe — the webapp just reads whatever JSON is on disk.

## Reproducibility workflow

1. Document tables/columns **in the database** using extended properties
   (see the docstring at the top of `extract.py` for the exact `EXEC
   sys.sp_addextendedproperty` calls — one for general notes
   (`MS_Description`) and one for data-quality notes (`DQ_Notes`)).
2. Whenever the schema changes (new table, new column, updated notes), rerun:

   ```bash
   python -m schema_viz.cli \
       --conn-string "mssql+pyodbc://@PUReporting?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes" \
       --schemas dw \
       --source "PUReporting.dw" \
       --out snapshots \
       --include-sde-geometry
   ```

   Or, if you already have a SQLAlchemy engine from your `ConnectionManager`,
   skip the CLI and call `build_snapshot()` / `save_snapshot()` directly from
   a notebook — this is often more convenient since it reuses your existing
   connection/auth setup.

3. `snapshots/latest.json` updates automatically; the webapp picks it up on
   the next request (or app restart, if you add caching later).
4. `snapshots/snapshot_<timestamp>.json` files accumulate as a history —
   diff two of them to see exactly what changed in the schema over time.

## Running the viewer standalone

```bash
pip install -r requirements.txt
uvicorn schema_viz.webapp:app --reload
```

Then open `http://localhost:8000/`.

## Folding into the Parking Division API

Two options:

- **Mount as a sub-app**: `main_app.mount("/schema", schema_viz_app)` if you
  want it isolated.
- **Copy the two route functions** (`index`, `table_detail`) plus
  `include_router` into your main FastAPI app, and copy `templates/` next
  to your existing templates. Since everything here is plain Jinja2 + the
  Mermaid CDN script, there's no build step or JS tooling to wire in.

## Extending

- **More schemas**: pass a comma-separated list to `--schemas`, e.g.
  `dw,Traffic`.
- **SDE geometry types**: `--include-sde-geometry` looks up
  `SDE.GDB_GeomColumns`/`SDE.GDB_SpatialRefs`. If your SDE catalog is
  qualified differently, adjust `_SDE_GEOM_SQL` in `extract.py`.
- **Foreign keys across schemas** (e.g. dw -> SDE) currently only draw if
  both tables are in the same `--schemas` run; if that's common for you,
  it's worth extracting all relevant schemas in one pass.
- **Completeness metrics beyond row count** (null rates, date coverage,
  etc.) would slot in as additional fields on `TableMeta` — happy to add a
  `profile.py` module for that when you're ready; row-count-only was kept
  as the first pass so this stays fast to run against very large tables
  like `dw.VisitSummary`.

## Testing without a live DB connection

`test_synthetic.py` builds a fake snapshot mirroring `dw.VisitDetails` /
`dw.VisitSummary` / a sample SDE feature class, so you can verify the
pipeline (models -> mermaid -> store -> webapp) renders correctly before
pointing it at production.
