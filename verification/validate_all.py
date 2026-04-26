"""
End-to-end integrity validator — checks all 600 AOIs across every pipeline step.

Checks performed:
  raw_scenes   : dir exists, download_complete.flag present, ≥3 dated .tif folders
  stacks       : _stack.tif exists, opens OK, CRS valid, band count ≥ 3
  meta.json    : n_epochs ≥ 3, shape matches actual TIF shape
  annotations  : dir exists, has ≥1 damage_polygons_T*.geojson, files are valid GeoJSON

Prints a structured report and exits non-zero if any CRITICAL failures exist.

Run from repo root:
    python verification/validate_all.py
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

import rasterio

BASE       = Path(__file__).resolve().parent.parent
AOI_LIST   = json.load(open(BASE / "data" / "aoi_list.json"))
RAW_DIR    = BASE / "data" / "raw_scenes"
STACKS_DIR = BASE / "data" / "stacks"
ANN_DIR    = BASE / "data" / "annotations"

EXPECTED_TOTAL = len(AOI_LIST)
MIN_EPOCHS = 3


# ── helpers ──────────────────────────────────────────────────────────────────

class Report:
    def __init__(self):
        self.failures = defaultdict(list)   # severity -> [(aoi_id, msg)]
        self.warnings = defaultdict(list)
        self.ok_counts = defaultdict(int)

    def fail(self, stage, aoi_id, msg):
        self.failures[stage].append((aoi_id, msg))

    def warn(self, stage, aoi_id, msg):
        self.warnings[stage].append((aoi_id, msg))

    def ok(self, stage):
        self.ok_counts[stage] += 1

    def print_summary(self):
        stages = ["raw_scenes", "stacks", "meta", "annotations"]
        total_failures = sum(len(v) for v in self.failures.values())
        total_warnings = sum(len(v) for v in self.warnings.values())

        print()
        print("=" * 70)
        print(f"  VALIDATION REPORT  |  {EXPECTED_TOTAL} AOIs")
        print("=" * 70)

        for stage in stages:
            n_ok   = self.ok_counts[stage]
            n_fail = len(self.failures.get(stage, []))
            n_warn = len(self.warnings.get(stage, []))
            status = "OK" if n_fail == 0 else "FAIL"
            color  = "" if n_fail == 0 else "  <--"
            print(f"  {stage:<20}  {status}  ok={n_ok:>3}  fail={n_fail:>3}  warn={n_warn:>3}{color}")

        print("-" * 70)
        print(f"  Total failures : {total_failures}")
        print(f"  Total warnings : {total_warnings}")
        print("=" * 70)

        if total_failures:
            print("\nFAILURES:")
            for stage in stages:
                for aoi_id, msg in self.failures.get(stage, []):
                    print(f"  [{stage}]  {aoi_id}  —  {msg}")

        if total_warnings:
            print("\nWARNINGS (first 20):")
            shown = 0
            for stage in stages:
                for aoi_id, msg in self.warnings.get(stage, []):
                    print(f"  [{stage}]  {aoi_id}  —  {msg}")
                    shown += 1
                    if shown >= 20:
                        remaining = total_warnings - shown
                        if remaining:
                            print(f"  ... {remaining} more warnings omitted")
                        return

        return total_failures


# ── stage validators ─────────────────────────────────────────────────────────

def check_raw_scenes(aoi, rep):
    aoi_id  = aoi["aoi_id"]
    aoi_dir = RAW_DIR / aoi_id

    if not aoi_dir.exists():
        rep.fail("raw_scenes", aoi_id, "directory missing")
        return

    flag = aoi_dir / "download_complete.flag"
    if not flag.exists():
        rep.fail("raw_scenes", aoi_id, "download_complete.flag missing")
        return

    dated_dirs = {f.stem.split("_")[0] for f in aoi_dir.glob("*.tif")}
    if len(dated_dirs) < MIN_EPOCHS:
        rep.fail("raw_scenes", aoi_id,
                 f"only {len(dated_dirs)} dated .tif groups (need ≥{MIN_EPOCHS})")
        return

    rep.ok("raw_scenes")


def check_stack(aoi, rep):
    aoi_id   = aoi["aoi_id"]
    tif_path = STACKS_DIR / f"{aoi_id}_stack.tif"

    if not tif_path.exists():
        rep.fail("stacks", aoi_id, "_stack.tif missing")
        return None, None

    try:
        with rasterio.open(tif_path) as src:
            if src.crs is None:
                rep.fail("stacks", aoi_id, "CRS is None")
                return None, None
            if src.count < MIN_EPOCHS:
                rep.fail("stacks", aoi_id,
                         f"only {src.count} bands (need ≥{MIN_EPOCHS})")
                return None, None
            actual_shape = (src.height, src.width)
            actual_bands = src.count
            actual_crs   = src.crs.to_epsg()
    except Exception as e:
        rep.fail("stacks", aoi_id, f"rasterio.open error: {e}")
        return None, None

    rep.ok("stacks")
    return actual_shape, actual_bands


def check_meta(aoi, actual_shape, actual_bands, rep):
    aoi_id    = aoi["aoi_id"]
    meta_path = STACKS_DIR / f"{aoi_id}_meta.json"

    if not meta_path.exists():
        rep.fail("meta", aoi_id, "_meta.json missing")
        return

    try:
        m = json.load(open(meta_path))
    except Exception as e:
        rep.fail("meta", aoi_id, f"JSON parse error: {e}")
        return

    n_epochs = m.get("n_epochs", 0)
    if n_epochs < MIN_EPOCHS:
        rep.fail("meta", aoi_id, f"n_epochs={n_epochs} < {MIN_EPOCHS}")
        return

    if actual_shape is not None:
        meta_shape = tuple(m.get("shape", [])[-2:])
        if meta_shape != actual_shape:
            rep.warn("meta", aoi_id,
                     f"shape mismatch: meta={meta_shape} tif={actual_shape}")

    if actual_bands is not None:
        bpe = m.get("bands_per_epoch", [])
        declared = sum(bpe)
        if declared != actual_bands:
            rep.warn("meta", aoi_id,
                     f"bands_per_epoch sum={declared} != tif bands={actual_bands}")

    rep.ok("meta")


def check_annotations(aoi, rep):
    aoi_id  = aoi["aoi_id"]
    ann_dir = ANN_DIR / aoi_id

    if not ann_dir.exists():
        rep.fail("annotations", aoi_id, "annotation directory missing")
        return

    damage_files = sorted(ann_dir.glob("damage_polygons_T*.geojson"))
    if not damage_files:
        rep.fail("annotations", aoi_id, "no damage_polygons_T*.geojson found")
        return

    for gf in damage_files:
        try:
            data = json.load(open(gf))
        except Exception as e:
            rep.fail("annotations", aoi_id, f"{gf.name}: JSON parse error: {e}")
            return
        if "features" not in data:
            rep.fail("annotations", aoi_id, f"{gf.name}: missing 'features' key")
            return

    rep.ok("annotations")


def _annotations_stage_present() -> bool:
    """Returns False if the entire annotations directory is absent (Step 3 not yet run)."""
    return ANN_DIR.exists() and any(ANN_DIR.iterdir())


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    rep = Report()
    ann_present = _annotations_stage_present()
    if not ann_present:
        print("  NOTE: data/annotations/ not found — Step 3 not yet run. Skipping annotation checks.")

    print(f"Validating {EXPECTED_TOTAL} AOIs...")

    for i, aoi in enumerate(AOI_LIST, 1):
        check_raw_scenes(aoi, rep)
        shape, bands = check_stack(aoi, rep)
        check_meta(aoi, shape, bands, rep)
        if ann_present:
            check_annotations(aoi, rep)
        if i % 100 == 0:
            print(f"  ... {i}/{EXPECTED_TOTAL}")

    n_fail = rep.print_summary()
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
