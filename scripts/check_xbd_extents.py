"""Show geographic extents of each xBD event folder to find correct bbox matches."""
import sys, json, re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tdrd.core.geospatial import build_xbd_chip_index

XBD_BASE = Path("data/xbd")

CHECK = ["midwest-flooding", "hurricane-harvey", "tuscaloosa-tornado",
         "santa-rosa-wildfire", "lower-puna-volcano", "nepal-flooding"]

for folder in CHECK:
    chips = build_xbd_chip_index(XBD_BASE, folder)
    if not chips:
        print(f"{folder}: no chips")
        continue
    all_w = [c[0] for c in chips]; all_s = [c[1] for c in chips]
    all_e = [c[2] for c in chips]; all_n = [c[3] for c in chips]
    print(f"{folder}:")
    print(f"  lon [{min(all_w):.3f}, {max(all_e):.3f}]  lat [{min(all_s):.3f}, {max(all_n):.3f}]")
    print(f"  centroid ~({(min(all_w)+max(all_e))/2:.2f}, {(min(all_s)+max(all_n))/2:.2f})")
