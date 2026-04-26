"""Verify each event bbox generates enough tiles to hit AOI targets."""
import sys, math
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tdrd.config import EVENTS, EVENT_TARGETS
from tdrd.core.geospatial import generate_tiles

ASSUME_PASS_RATE = 0.30  # conservative: 30% survive STAC + OSMnx

print(f"{'ID':<8} {'Name':<22} {'Tiles':>6} {'Est AOIs':>9} {'Target':>7} {'OK?'}")
print("-" * 60)
total_ok = True
for ev in EVENTS:
    tiles = generate_tiles(ev['bbox'])
    target = EVENT_TARGETS[ev['id']]
    est = int(len(tiles) * ASSUME_PASS_RATE)
    ok = est >= target
    if not ok:
        total_ok = False
    flag = "OK" if ok else f"NEED {math.ceil(target/ASSUME_PASS_RATE)} tiles"
    print(f"{ev['id']:<8} {ev['name']:<22} {len(tiles):>6} {est:>9} {target:>7}  {flag}")

print()
print("All events OK" if total_ok else "WARNING: some events may fall short")
