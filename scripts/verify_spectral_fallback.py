"""
Spectral Fallback Verification
================================
For AOIs that used the spectral-change fallback in Step 3a, this script
re-derives the NDVI delta independently and cross-checks it against the
saved damage_polygons_T*.geojson files.

Outputs: scripts/verify_spectral_fallback_results.json

Nothing is written to data/ — this is purely a read + compare script.
"""

import json
import math
from collections import Counter
from pathlib import Path

import numpy as np
import rasterio
import rasterio.featuresimport geopandas as gpd
from shapely.geometry import shape

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent
STACKS_DIR  = ROOT / "data" / "stacks"
ANNOTATIONS = ROOT / "data" / "annotations"
OUT_PATH    = Path(__file__).parent / "verify_spectral_fallback_results.json"

# Thresholds from step3a_damage_annotation.py — we verify against these
THRESHOLDS = {1: 0.10, 2: 0.25, 3: 0.40}   # class: lower bound (exclusive)

# ── Helpers ───────────────────────────────────────────────────────────────────

def ndvi(red: np.ndarray, nir: np.ndarray) -> np.ndarray:
    """Standard NDVI: (NIR - RED) / (NIR + RED), safe divide."""
    denom = nir + red
    denom = np.where(denom == 0, 1e-6, denom)
    return (nir - red) / denom


def classify_delta(delta: np.ndarray) -> np.ndarray:
    """Apply the same thresholds used in step3a."""
    out = np.zeros_like(delta, dtype=np.uint8)
    out[delta > THRESHOLDS[1]] = 1
    out[delta > THRESHOLDS[2]] = 2
    out[delta > THRESHOLDS[3]] = 3
    return out


def pixel_class_counts(damage_raster: np.ndarray) -> dict:
    counts = {}
    for cls in [1, 2, 3]:
        counts[cls] = int(np.sum(damage_raster == cls))
    return counts


def geojson_class_counts(path: Path) -> dict:
    """Count features per damage_class in a saved GeoJSON."""
    counts = {1: 0, 2: 0, 3: 0}
    try:
        with open(path) as f:
            d = json.load(f)
        for feat in d.get("features", []):
            cls = feat.get("properties", {}).get("damage_class")
            if cls in counts:
                counts[cls] += 1
    except Exception as e:
        return {"error": str(e)}
    return counts


def verify_formula_math() -> dict:
    """
    Unit-test the NDVI formula against known pixel values.
    For a pure-vegetation pixel: NIR=0.8, RED=0.1 → NDVI ≈ 0.778
    For bare soil: NIR=0.3, RED=0.3 → NDVI = 0.0
    For water: NIR=0.05, RED=0.07 → NDVI < 0 (−0.167)
    Returns a dict with expected vs computed values.
    """
    cases = [
        ("vegetation",   0.8,  0.1,  0.778),
        ("bare_soil",    0.3,  0.3,  0.0),
        ("water",        0.05, 0.07, -0.167),
        ("urban_bright", 0.25, 0.22,  0.064),
    ]
    results = {}
    for name, nir_v, red_v, expected in cases:
        red = np.array([[red_v]], dtype=np.float32)
        nir = np.array([[nir_v]], dtype=np.float32)
        got = float(ndvi(red, nir)[0, 0])
        ok  = abs(got - expected) < 0.002
        results[name] = {
            "NIR": nir_v, "RED": red_v,
            "expected_NDVI": expected,
            "computed_NDVI": round(got, 4),
            "pass": ok,
        }
    return results


def verify_threshold_logic() -> dict:
    """
    Check the threshold → class mapping with explicit delta values.
    """
    test_deltas = {
        "no_change (0.05)":          (0.05,  0),
        "minor (0.15)":              (0.15,  1),
        "at_boundary_1_2 (0.25)":   (0.25,  1),   # 0.25 is NOT >0.25, stays class 1
        "just_over_1_2 (0.251)":    (0.251, 2),   # 0.251 IS >0.25, becomes class 2
        "major (0.30)":              (0.30,  2),
        "at_boundary_2_3 (0.40)":   (0.40,  2),   # 0.40 is NOT >0.40, stays class 2
        "just_over_2_3 (0.401)":    (0.401, 3),   # 0.401 IS >0.40, becomes class 3
        "destroyed (0.60)":          (0.60,  3),
        "negative (-0.10)":          (-0.10, 0),   # regrowth, no damage
    }
    results = {}
    for label, (delta_val, expected_class) in test_deltas.items():
        delta  = np.array([[delta_val]], dtype=np.float32)
        cls    = int(classify_delta(delta)[0, 0])
        results[label] = {
            "delta":          delta_val,
            "expected_class": expected_class,
            "computed_class": cls,
            "pass":           cls == expected_class,
        }
    return results


def verify_aoi(aoi_id: str, meta: dict) -> dict:
    """
    Re-derive NDVI delta from the stack and compare to saved GeoJSON.
    Returns a dict with per-epoch stats.
    """
    stack_path = STACKS_DIR / f"{aoi_id}_stack.tif"
    if not stack_path.exists():
        return {"error": "stack not found"}

    bpe     = meta["bands_per_epoch"]   # list of int, one per epoch
    n_ep    = meta["n_epochs"]
    dates   = meta["dates"]

    epochs = []

    try:
        with rasterio.open(stack_path) as src:
            # Pre-event (epoch 0): bands 1-based at positions 1..bpe[0]
            # Band order per epoch: B02, B03, B04(RED), B08(NIR)
            pre_red_idx = 1 + 2   # band 3 → RED (B04)
            pre_nir_idx = 1 + 3   # band 4 → NIR (B08)

            pre_red  = src.read(pre_red_idx).astype(np.float32)
            pre_nir  = src.read(pre_nir_idx).astype(np.float32)
            pre_ndvi = ndvi(pre_red, pre_nir)

            # Basic sanity: NDVI should be in [-1, 1]
            pre_ndvi_range_ok = (
                float(np.nanmin(pre_ndvi)) >= -1.01 and
                float(np.nanmax(pre_ndvi)) <=  1.01
            )

            for t in range(1, n_ep):
                ep_start = sum(bpe[:t]) + 1   # 1-based band index for this epoch

                post_red_idx = ep_start + 2
                post_nir_idx = ep_start + 3

                # Guard against band index out of range
                if post_nir_idx > src.count:
                    epochs.append({
                        "epoch": t, "date": dates[t] if t < len(dates) else "?",
                        "error": f"band {post_nir_idx} out of range (stack has {src.count})"
                    })
                    continue

                post_red  = src.read(post_red_idx).astype(np.float32)
                post_nir  = src.read(post_nir_idx).astype(np.float32)
                post_ndvi_arr = ndvi(post_red, post_nir)

                delta = pre_ndvi - post_ndvi_arr
                damage_raster = classify_delta(delta)

                pix_counts  = pixel_class_counts(damage_raster)
                total_pixels = int(damage_raster.size)
                damaged_pixels = sum(pix_counts.values())

                # Delta stats per class
                class_delta_stats = {}
                for cls in [1, 2, 3]:
                    mask = damage_raster == cls
                    if np.any(mask):
                        vals = delta[mask]
                        class_delta_stats[str(cls)] = {
                            "min": round(float(np.min(vals)), 4),
                            "max": round(float(np.max(vals)), 4),
                            "mean": round(float(np.mean(vals)), 4),
                            # All values should satisfy the threshold contract
                            "all_above_threshold": bool(np.all(vals > THRESHOLDS[cls])),
                        }

                # Compare against saved GeoJSON
                gjson_path = ANNOTATIONS / aoi_id / f"damage_polygons_T{t+1}.geojson"
                gjson_counts = geojson_class_counts(gjson_path) if gjson_path.exists() else {"error": "file missing"}

                # Rank agreement: class with most pixels in raster should also be most in GeoJSON
                raster_rank = sorted(pix_counts, key=pix_counts.get, reverse=True)
                gjson_rank  = (
                    sorted(gjson_counts, key=gjson_counts.get, reverse=True)
                    if "error" not in gjson_counts else None
                )
                rank_agrees = (raster_rank == gjson_rank) if gjson_rank else None

                epochs.append({
                    "epoch":             t,
                    "date":              dates[t] if t < len(dates) else "?",
                    "pre_date":          dates[0],
                    "pre_ndvi_range_ok": pre_ndvi_range_ok,
                    "pre_ndvi_mean":     round(float(np.mean(pre_ndvi)), 4),
                    "post_ndvi_mean":    round(float(np.mean(post_ndvi_arr)), 4),
                    "delta_mean":        round(float(np.mean(delta)), 4),
                    "delta_min":         round(float(np.min(delta)), 4),
                    "delta_max":         round(float(np.max(delta)), 4),
                    "total_pixels":      total_pixels,
                    "damaged_pixels":    damaged_pixels,
                    "damage_pct":        round(100 * damaged_pixels / total_pixels, 2),
                    "raster_class_pixel_counts": pix_counts,
                    "class_delta_stats":         class_delta_stats,
                    "geojson_feature_counts":    gjson_counts,
                    "class_rank_agrees":         rank_agrees,
                    # Are thresholds respected in the delta stats?
                    "threshold_contracts_hold":  all(
                        s.get("all_above_threshold", False)
                        for s in class_delta_stats.values()
                    ),
                })

    except Exception as e:
        return {"error": str(e)}

    return {"epochs": epochs}


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("Verifying spectral fallback (Step 3a)...")

    # 1. Formula / math unit tests (no files needed)
    formula_checks = verify_formula_math()
    threshold_checks = verify_threshold_logic()

    formula_all_pass = all(v["pass"] for v in formula_checks.values())
    threshold_all_pass = all(v["pass"] for v in threshold_checks.values())

    print(f"  NDVI formula unit tests : {'PASS' if formula_all_pass else 'FAIL'}")
    print(f"  Threshold logic tests   : {'PASS' if threshold_all_pass else 'FAIL'}")

    # 2. Per-AOI raster vs GeoJSON cross-check
    # Scan for AOIs that have spectral-sourced output (check source field)
    aoi_results = {}
    sample_aois = []

    for aoi_dir in sorted(ANNOTATIONS.iterdir())[:20]:   # check first 20
        aoi_id = aoi_dir.name
        t2 = aoi_dir / "damage_polygons_T2.geojson"
        if not t2.exists():
            continue
        try:
            with open(t2) as f:
                d = json.load(f)
            feats = d.get("features", [])
            if feats and feats[0].get("properties", {}).get("source") == "spectral":
                sample_aois.append(aoi_id)
        except Exception:
            continue

    print(f"  Found {len(sample_aois)} AOIs with spectral fallback output -- checking all...")

    for aoi_id in sample_aois:
        meta_path = STACKS_DIR / f"{aoi_id}_meta.json"
        if not meta_path.exists():
            aoi_results[aoi_id] = {"error": "meta not found"}
            continue
        with open(meta_path) as f:
            meta = json.load(f)
        print(f"    {aoi_id}...", end=" ", flush=True)
        result = verify_aoi(aoi_id, meta)
        aoi_results[aoi_id] = result
        if "error" in result:
            print(f"ERROR: {result['error']}")
        else:
            epochs = result["epochs"]
            contracts_ok = all(ep.get("threshold_contracts_hold", False) for ep in epochs if "error" not in ep)
            print(f"OK  (contracts_hold={contracts_ok})")

    # 3. Summary
    total_epochs = 0
    contracts_pass = 0
    rank_pass = 0
    rank_total = 0
    for aoi_id, res in aoi_results.items():
        if "error" in res:
            continue
        for ep in res.get("epochs", []):
            if "error" in ep:
                continue
            total_epochs += 1
            if ep.get("threshold_contracts_hold"):
                contracts_pass += 1
            if ep.get("class_rank_agrees") is not None:
                rank_total += 1
                if ep.get("class_rank_agrees"):
                    rank_pass += 1

    summary = {
        "ndvi_formula_unit_tests_pass": formula_all_pass,
        "threshold_logic_tests_pass":   threshold_all_pass,
        "aois_checked":                 len(sample_aois),
        "total_epochs_checked":         total_epochs,
        "threshold_contracts_hold":     f"{contracts_pass}/{total_epochs}",
        "class_rank_agrees_with_geojson": f"{rank_pass}/{rank_total}",
        "overall_fallback_correct":     (
            formula_all_pass and threshold_all_pass and
            contracts_pass == total_epochs and
            rank_pass == rank_total
        ),
    }

    output = {
        "description": (
            "Step 3a spectral fallback verification. "
            "Checks NDVI formula correctness, threshold logic, "
            "and raster-vs-GeoJSON class rank agreement."
        ),
        "thresholds_used": THRESHOLDS,
        "band_layout_assumed": "B02, B03, B04(RED), B08(NIR) — 4 bands per epoch",
        "summary": summary,
        "ndvi_formula_unit_tests": formula_checks,
        "threshold_logic_unit_tests": threshold_checks,
        "per_aoi_results": aoi_results,
    }

    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nResults written to: {OUT_PATH}")
    print(f"Summary: {json.dumps(summary, indent=2)}")


if __name__ == "__main__":
    main()
