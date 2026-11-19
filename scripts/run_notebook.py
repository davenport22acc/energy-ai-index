"""Run a Fabric notebook (notebook-content.py) outside Fabric, papermill style.

Usage:
  python scripts/run_notebook.py fabric/01_bronze_mastr.Notebook --param LOAD_DATE=20261101 --param MAX_FILES_PER_TABLE=1

Executes code cells in order in one namespace. --param values are applied right after the
PARAMETERS cell (values are parsed as JSON when possible, else kept as string).
Markdown and metadata blocks are skipped. Set LAKE_ROOT to choose the local lakehouse folder.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

MARKER = re.compile(r"^# (CELL|PARAMETERS CELL|MARKDOWN|METADATA) \*+\s*$")


def cells(text: str) -> list[tuple[str, str]]:
    out, kind, buf = [], None, []
    for line in text.splitlines():
        m = MARKER.match(line)
        if m:
            if kind:
                out.append((kind, "\n".join(buf)))
            kind, buf = m.group(1), []
        else:
            buf.append(line)
    if kind:
        out.append((kind, "\n".join(buf)))
    return [(k, c) for k, c in out if k in ("CELL", "PARAMETERS CELL")]


def parse_value(raw: str):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("notebook", help="path to <name>.Notebook folder or notebook-content.py")
    ap.add_argument("--param", action="append", default=[], help="KEY=VALUE")
    args = ap.parse_args()

    path = Path(args.notebook)
    if path.is_dir():
        path = path / "notebook-content.py"
    overrides = dict(p.split("=", 1) for p in args.param)
    ns: dict = {"__name__": "__notebook__"}
    for i, (kind, code) in enumerate(cells(path.read_text(encoding="utf-8")), 1):
        print(f"--- cell {i} ({kind.lower()})", flush=True)
        exec(compile(code, f"{path}#cell{i}", "exec"), ns)
        if kind == "PARAMETERS CELL":
            for k, v in overrides.items():
                ns[k] = parse_value(v)
                print(f"param {k} = {ns[k]!r}")


if __name__ == "__main__":
    sys.exit(main())
