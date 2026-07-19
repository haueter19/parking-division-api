"""
store.py
========
Writes each extraction run to a timestamped JSON file AND overwrites
latest.json. Keeping the timestamped history means you can diff two runs
to see exactly what changed in the schema over time; latest.json is what
the webapp always reads, so a fresh `python -m schema_viz.cli` run is all
it takes to bring the visualization up to date.
"""

from __future__ import annotations

import json
from pathlib import Path

from .models import SchemaSnapshot


def save_snapshot(snapshot: SchemaSnapshot, out_dir: str | Path) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = snapshot.generated_at.replace(":", "").replace("-", "")
    dated_path = out_dir / f"snapshot_{stamp}.json"
    latest_path = out_dir / "latest.json"

    payload = json.dumps(snapshot.to_dict(), indent=2, default=str)
    dated_path.write_text(payload)
    latest_path.write_text(payload)
    return latest_path


def load_snapshot(path: str | Path) -> SchemaSnapshot:
    path = Path(path)
    data = json.loads(path.read_text())
    return SchemaSnapshot.from_dict(data)
