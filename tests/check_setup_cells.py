"""The shared setup block must be identical in every notebook (Fabric notebooks are self-contained)."""
import re
import sys
from pathlib import Path

blocks = {}
for nb in sorted(Path("fabric").glob("*.Notebook/notebook-content.py")):
    text = nb.read_text(encoding="utf-8")
    m = re.search(r"# --- shared setup.*?# --- end shared setup ---", text, re.S)
    blocks[nb.parent.name] = m.group(0) if m else None

missing = [k for k, v in blocks.items() if v is None]
distinct = {v for v in blocks.values() if v}
print("notebooks:", list(blocks))
if missing or len(distinct) != 1:
    print("setup block missing in", missing, "| distinct versions:", len(distinct))
    sys.exit(1)
print("OK shared setup identical")
