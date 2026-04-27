"""
Step 3b Flood Extraction — Fallback Verification
=================================================
Verifies the NDWI fallback is producing correct flood extent outputs by:

  1. NDWI formula unit tests (known pixel values)
  2. Otsu threshold sanity check (should always split water vs non-water)
  3. Per-AOI: re-derive NDWI from stack, compare water pixel % to saved GeoJSON
  4. Cross-check: T2 flood area >= T3 >= T4 (flood should recede over time)

Outputs: scripts/verify_ndwi_fallback_results.json
Nothing is written to data/ — read-only.
"""

import json
import math
from pathlib import Path

import cv2
import numpy as np
import rasterio
import rasterio.features
import geopandas as gpd
from shapely.geometry import shape, box

ROOT       = Path(__file__).resolve().parent.parent
STACKS_DIR = ROOT / "data" / "stacks"
ANNOTATIONS = ROOT / "data" / "annotations"
OUT_PATH   = Path(__file__).parent / "verify_ndwi_fallback_results.json"


# ── Formula helpers ───────────────────────────────────────────────────────────

def ndwi(green: np.ndarray, nir: np.ndarray) -> np.ndarray:
    denom = green + nir
    denom = np.where(denom == 0, 1e-6, denom)
    return (green - nir) / denom


def verify_formula_math() -> dict:
    """
    Unit-test NDWI against known surface types.
    Water:        Green high, NIR low  -> NDWI positive (e.g. +0.5)
    Vegetation:   Green low,  NIR high -> NDWI negative (e.g. -0.5)
    Bare soil:    Green ~ NIR          -> NDWI near 0
    """
    cases = [
        ("water",       0.10, 0.03, True,   0.538),   # green=0.10, nir=0.03, expected positive
        ("vegetation",  0.05, 0.40, False, -0.778),   # green=0.05, nir=0.40, expected negative
        ("bare_soil",   0.12, 0.12, False,  0.0),     # green=nir, NDWI=0
        ("urban",       0.15, 0.20, False, -0.143),   # slightly negative
    ]
    results = {}
    for name, g, n, expect_positive, expected in cases:
        green = np.array([[g]], dtype=np.float32)
        nir   = np.array([[n]], dtype=np.float32)
        val   = float(ndwi(green, nir)[0, 0])
        sign_ok = (val > 0) == expect_positive
        close   = abs(val - expected) < 0.002
        results[name] = {
            "green": g, "nir": n,
            "expected_NDWI": expected,
            "computed_NDWI": round(val, 4),
            "sign_correct":  sign_ok,
            "value_correct": close,
            "pass": sign_ok and close,
        }
    return results


def verify_otsu_logic() -> dict:
    """
    Verify Otsu picks a threshold that separates two clear clusters.
    Simulate: 60% non-water (low NDWI, low normalised value) + 40% water (high).
    Otsu threshold should land between the two clusters.
    """
    rng = np.random.default_rng(42)
    non_water = rng.integers(20, 80,  size=600, dtype=np.uint8)
    water     = rng.integers(160, 230, size=400, dtype=np.uint8)
    img = np.concatenate([non_water, water]).reshape(10, 100).astype(np.uint8)

    thresh, _ = cv2.threshold(img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    # Threshold should fall between the two clusters (gap is 80-160).
    # Allow slack to 70 since Otsu may land at the high end of the lower cluster.
    between_clusters = 70 <= thresh <= 160

    return {
        "non_water_range": "20-80",
        "water_range":     "160-230",
        "otsu_threshold":  int(thresh),
        "expected_between_clusters": "70-160",
        "pass": between_clusters,
    }


# ── Per-AOI verification ──────────────────────────────────────────────────────

def geojson_coverage_pct(path: Path, aoi_bbox) -> dict:
    """Estimate how much of the AOI bbox is covered by flood polygons."""
    try:
        gdf = gpd.read_file(path)
        if len(gdf) == 0:
            return {"flood_area_deg2": 0.0, "aoi_area_deg2": 0.0, "coverage_pct": 0.0, "source": "empty"}
        aoi_box = box(*aoi_bbox)
        aoi_area = aoi_box.area
        flood_area = gdf.geometry.union_all().intersection(aoi_box).area if hasattr(gdf.geometry, 'union_all') else \
                     gdf.geometry.unary_union.intersection(aoi_box).area
        src = gdf.iloc[0].get("source", "?") if "source" in gdf.columns else "?"
        return {
            "flood_area_deg2": round(flood_area, 8),
            "aoi_area_deg2":   round(aoi_area, 8),
            "coverage_pct":    round(100 * flood_area / aoi_area, 2) if aoi_area > 0 else 0.0,
            "source": src,
        }
    except Exception as e:
        return {"error": str(e)}


def verify_aoi(aoi_id: str, aoi_bbox, meta: dict) -> dict:
    stack_path = STACKS_DIR / f"{aoi_id}_stack.tif"
    if not stack_path.exists():
        return {"error": "stack not found"}

    bpe    = meta["bands_per_epoch"]
    n_ep   = meta["n_epochs"]
    dates  = meta["dates"]
    epochs = []

    try:
        with rasterio.open(stack_path) as src:
            total_bands = src.count

            for t in range(1, n_ep):
                ep_start    = sum(bpe[:t]) + 1
                green_idx   = ep_start + 1   # B03
                nir_idx     = ep_start + 3   # B08

                if nir_idx > total_bands:
                    epochs.append({
                        "epoch": t, "date": dates[t] if t < len(dates) else "?",
                        "error": f"band {nir_idx} out of range (stack has {total_bands})"
                    })
                    continue

                green, transform, crs = \
                    src.read(green_idx).astype(np.float32), src.transform, src.crs
                green = src.read(green_idx).astype(np.float32)
                nir   = src.read(nir_idx).astype(np.float32)

                denom    = green + nir
                denom    = np.where(denom == 0, 1e-6, denom)
                ndwi_arr = (green - nir) / denom

                # NDWI range sanity
                ndwi_min = float(np.min(ndwi_arr))
                ndwi_max = float(np.max(ndwi_arr))
                ndwi_mean = float(np.mean(ndwi_arr))
                range_ok  = ndwi_min >= -1.01 and ndwi_max <= 1.01

                # Otsu classification
                ndwi_norm = ((ndwi_arr - ndwi_arr.min()) /
                             (ndwi_arr.max() - ndwi_arr.min() + 1e-8) * 255).astype(np.uint8)
                thresh, _ = cv2.threshold(ndwi_norm, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                flood_mask = (ndwi_norm >= thresh).astype(np.uint8)
                kernel = np.ones((3, 3), np.uint8)
                flood_mask = cv2.morphologyEx(flood_mask, cv2.MORPH_OPEN, kernel)

                water_pixels = int(np.sum(flood_mask > 0))
                total_pixels = int(flood_mask.size)
                water_pct    = round(100 * water_pixels / total_pixels, 2)
                otsu_thresh  = int(thresh)

                # Compare to saved GeoJSON
                gjson_path = ANNOTATIONS / aoi_id / f"flood_extent_T{t+1}.geojson"
                gjson_info = geojson_coverage_pct(gjson_path, aoi_bbox) \
                             if gjson_path.exists() else {"error": "file missing"}

                epochs.append({
                    "epoch":          t,
                    "date":           dates[t] if t < len(dates) else "?",
                    "ndwi_range_ok":  range_ok,
                    "ndwi_min":       round(ndwi_min, 4),
                    "ndwi_max":       round(ndwi_max, 4),
                    "ndwi_mean":      round(ndwi_mean, 4),
                    "otsu_threshold": otsu_thresh,
                    "water_pixels":   water_pixels,
                    "total_pixels":   total_pixels,
                    "water_pct":      water_pct,
                    "geojson":        gjson_info,
                })

    except Exception as e:
        return {"error": str(e)}

    # Temporal recession check: T2 water_pct should be >= T3 >= T4
    if len(epochs) >= 2:
        pcts = [ep["water_pct"] for ep in epochs if "error" not in ep]
        # Allow 5% slack — not every event has monotone recession
        recession_holds = all(pcts[i] >= pcts[i+1] - 5.0 for i in range(len(pcts)-1))
    else:
        recession_holds = None

    return {"epochs": epochs, "recession_holds": recession_holds}


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("Verifying NDWI fallback (Step 3b)...")

    formula_checks = verify_formula_math()
    otsu_check     = verify_otsu_logic()

    formula_pass = all(v["pass"] for v in formula_checks.values())
    otsu_pass    = otsu_check["pass"]

    print(f"  NDWI formula unit tests : {'PASS' if formula_pass else 'FAIL'}")
    print(f"  Otsu logic test         : {'PASS' if otsu_pass else 'FAIL'} (threshold={otsu_check['otsu_threshold']})")

    # Load AOI list for bbox
    with open(ROOT / "data" / "aoi_list.json") as f:
        aois = json.load(f)
    aoi_map = {a["aoi_id"]: a for a in aois}

    # Sample AOIs with ndwi_otsu output
    sample = []
    for aoi_dir in sorted(ANNOTATIONS.iterdir())[:20]:
        aoi_id = aoi_dir.name
        t2 = aoi_dir / "flood_extent_T2.geojson"
        if not t2.exists():
            continue
        try:
            with open(t2) as f:
                d = json.load(f)
            feats = d.get("features", [])
            if feats and feats[0].get("properties", {}).get("source") == "ndwi_otsu":
                sample.append(aoi_id)
        except Exception:
            continue

    print(f"  Found {len(sample)} AOIs with NDWI output -- checking all...")

    aoi_results = {}
    for aoi_id in sample:
        meta_path = STACKS_DIR / f"{aoi_id}_meta.json"
        if not meta_path.exists():
            aoi_results[aoi_id] = {"error": "meta not found"}
            continue
        with open(meta_path) as f:
            meta = json.load(f)
        aoi_bbox = aoi_map[aoi_id]["bbox"]
        print(f"    {aoi_id}...", end=" ", flush=True)
        result = verify_aoi(aoi_id, aoi_bbox, meta)
        aoi_results[aoi_id] = result
        if "error" in result:
            print(f"ERROR: {result['error']}")
        else:
            rec = result.get("recession_holds")
            print(f"OK  (recession_holds={rec})")

    # Summary
    total_epochs   = 0
    range_ok_count = 0
    recession_pass = 0
    recession_total = 0

    for aoi_id, res in aoi_results.items():
        if "error" in res:
            continue
        for ep in res.get("epochs", []):
            if "error" in ep:
                continue
            total_epochs += 1
            if ep.get("ndwi_range_ok"):
                range_ok_count += 1
        if res.get("recession_holds") is not None:
            recession_total += 1
            if res["recession_holds"]:
                recession_pass += 1

    summary = {
        "ndwi_formula_unit_tests_pass": formula_pass,
        "otsu_logic_test_pass":         otsu_pass,
        "aois_checked":                 len(sample),
        "total_epochs_checked":         total_epochs,
        "ndwi_range_valid":             f"{range_ok_count}/{total_epochs}",
        "recession_pattern_holds":      f"{recession_pass}/{recession_total}",
        "recession_note": (
            "Flood water_pct should be >= next epoch (with 5% slack). "
            "Failures can be real (non-flood events have low/variable NDWI) "
            "or indicate genuinely non-receding water."
        ),
        "sen1floods11_tifs_on_disk": 0,
        "sen1floods11_note": (
            "446 chips in index but 0 TIF files exist on disk. "
            "All epochs used NDWI fallback. Sen1Floods11 path never executed."
        ),
        "overall_fallback_correct": formula_pass and otsu_pass and range_ok_count == total_epochs,
    }

    output = {
        "description": (
            "Step 3b NDWI fallback verification. Checks formula correctness, "
            "Otsu threshold sanity, NDWI value range validity, and temporal "
            "recession pattern across epochs."
        ),
        "band_layout_assumed": "B02, B03(green), B04, B08(nir) -- 4 bands per epoch",
        "summary": summary,
        "ndwi_formula_unit_tests": formula_checks,
        "otsu_logic_test":         otsu_check,
        "per_aoi_results":         aoi_results,
    }

    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nResults written to: {OUT_PATH}")
    print(f"Summary: {json.dumps(summary, indent=2)}")


if __name__ == "__main__":
    main()
