"""Check which events have xBD chips geographically overlapping their bbox."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tdrd.config import EVENTS
from tdrd.core.geospatial import build_xbd_chip_index, tile_overlaps_xbd

XBD_BASE = Path("data/xbd")

print(f"{'ID':<8} {'Name':<22} {'xbd_folder':<25} {'Chips':>6} {'In bbox':>8}  Result")
print("-" * 80)
for ev in EVENTS:
    folder = ev.get("xbd_folder")
    if not folder:
        print(f"{ev['id']:<8} {ev['name']:<22} {'None':<25} {'-':>6} {'-':>8}  no xBD folder")
        continue
    chips = build_xbd_chip_index(XBD_BASE, folder)
    chips_in = [c for c in chips if tile_overlaps_xbd(ev["bbox"], [c])]
    result = "GEOGRAPHIC MATCH" if chips_in else "MISMATCH — wrong location"
    print(f"{ev['id']:<8} {ev['name']:<22} {folder:<25} {len(chips):>6} {len(chips_in):>8}  {result}")
