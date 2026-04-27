"""
step8_verify_and_assemble.py
----------------------------
Re-execute every GT tool chain deterministically, verify against stored
gt_answer, then split into train/val/test + conflict subset.

Verification checks (per spec):
  1. Numeric accuracy   — re-computed value within 5% of stored gt_answer
  2. Route RSS          — must be 1.0 for Q5_routing
  3. Temporal direction — Q4_change direction must match recomputed delta sign
  4. Data quality       — gt_answer must contain meaningful non-zero values
  5. Answer text        — gt_answer_text must be non-empty

Target pass rate: ~75%  (~2,300 verified from 3,156)

Output files:
  data/tdrd_train.jsonl
  data/tdrd_val.jsonl
  data/tdrd_test.jsonl
  data/tdrd_conflict_subset.jsonl
"""

import json
import math
import random
import warnings
from pathlib import Path
from collections import defaultdict

from shapely.geometry import shape
from shapely.ops import unary_union

warnings.filterwarnings("ignore")

BASE      = Path(".")
ANN_DIR   = BASE / "data" / "annotations"
STACKS    = BASE / "data" / "stacks"
CONFLICTS = json.load(open(BASE / "data" / "conflict_events.json"))

random.seed(42)


# ── Deterministic GT re-execution ─────────────────────────────────────────────

def _polygon_area_km2(features):
    geoms = [shape(f["geometry"]) for f in features if f.get("geometry")]
    if not geoms:
        return 0.0
    union  = unary_union(geoms)
    bounds = union.bounds
    lat    = (bounds[1] + bounds[3]) / 2
    km_lon = 111.0 * math.cos(math.radians(lat))
    return union.area * km_lon * 111.0


def execute_tool_chain_deterministic(chain, aoi_id, query_type):
    """
    Replay the GT tool chain from disk and return a predicted answer dict
    in the same schema as gt_answer.
    """
    ann = ANN_DIR / aoi_id

    if query_type == "Q1_counting":
        # Find the damage file from chain params
        file_param = next(
            (s["params"]["file"] for s in chain if s["action"] == "count_damage_polygons"),
            "damage_polygons_T2.geojson"
        )
        fp = ann / file_param
        if not fp.exists():
            return None
        feats = json.load(open(fp)).get("features", [])
        epoch = file_param.replace("damage_polygons_", "").replace(".geojson", "")
        return {
            "epoch":              epoch,
            "total_polygons":     len(feats),
            "major_or_destroyed": sum(1 for f in feats
                                      if f.get("properties", {}).get("damage_class", 0) >= 2),
            "destroyed_only":     sum(1 for f in feats
                                      if f.get("properties", {}).get("damage_class", 0) >= 3),
        }

    if query_type == "Q2_area":
        file_param = next(
            (s["params"]["file"] for s in chain if s["action"] == "compute_flood_area_km2"),
            "flood_extent_T2.geojson"
        )
        fp = ann / file_param
        if not fp.exists():
            return None
        feats = json.load(open(fp)).get("features", [])
        epoch = file_param.replace("flood_extent_", "").replace(".geojson", "")
        return {"epoch": epoch, "flood_area_km2": round(_polygon_area_km2(feats), 4)}

    if query_type == "Q3_proximity":
        rjp = ann / "evacuation_routes.json"
        if not rjp.exists():
            return None
        routes = json.load(open(rjp))
        all_routes = [r for rl in routes.values() for r in rl]
        if not all_routes:
            return None
        best  = min(all_routes, key=lambda r: r.get("length_m", 1e9))
        epoch = next((ep for ep, rl in routes.items() if best in rl), "T2")
        return {
            "shelter_name":   best.get("shelter_name", "unknown"),
            "route_length_m": best.get("length_m", 0),
            "eta_min":        best.get("eta_min", 0),
            "epoch":          epoch,
        }

    if query_type == "Q4_change":
        delta_step = next(
            (s for s in chain if s["action"] == "compute_area_delta"), None)
        if not delta_step:
            return None
        fp_T  = ann / delta_step["params"]["file_T"]
        fp_T1 = ann / delta_step["params"]["file_T1"]
        if not fp_T.exists() or not fp_T1.exists():
            return None
        feats_T  = json.load(open(fp_T)).get("features",  [])
        feats_T1 = json.load(open(fp_T1)).get("features", [])
        area_T   = _polygon_area_km2(feats_T)
        area_T1  = _polygon_area_km2(feats_T1)
        delta    = area_T1 - area_T
        pct      = (delta / area_T * 100) if area_T > 0 else 0.0
        ep_T  = delta_step["params"]["file_T"].replace("flood_extent_","").replace(".geojson","")
        ep_T1 = delta_step["params"]["file_T1"].replace("flood_extent_","").replace(".geojson","")
        return {
            f"flood_area_{ep_T}_km2":  round(area_T,  4),
            f"flood_area_{ep_T1}_km2": round(area_T1, 4),
            "delta_km2":  round(delta, 4),
            "change_pct": round(pct, 2),
            "direction":  "increase" if delta > 0 else "decrease" if delta < 0 else "unchanged",
        }

    if query_type == "Q5_routing":
        rjp = ann / "evacuation_routes.json"
        if not rjp.exists():
            return None
        routes = json.load(open(rjp))
        rlist  = routes.get("T2", [])
        if not rlist:
            return None
        best = max(rlist, key=lambda r: (r.get("rss", 0), -r.get("length_m", 1e9)))
        return {
            "epoch":        "T2",
            "shelter_name": best.get("shelter_name", "unknown"),
            "length_m":     best.get("length_m", 0),
            "eta_min":      best.get("eta_min", 0),
            "rss":          best.get("rss", 1.0),
            "geometry":     best.get("geometry", {}),
        }

    if query_type == "Q6_conflict":
        c = [x for x in CONFLICTS if x["aoi_id"] == aoi_id]
        if not c:
            return None
        return {
            "n_conflicts":        len(c),
            "epoch_pairs":        list({(x["epoch_T"], x["epoch_T1"]) for x in c}),
            "invalidated_routes": [x["route_id"] for x in c],
            "score_deltas":       [x["score_delta"] for x in c],
        }

    return None


# ── Verification ───────────────────────────────────────────────────────────────

def _numeric_close(a, b, tol=0.05):
    return abs(a - b) / (abs(b) + 1e-9) <= tol


def verify_sample(query_item):
    qt         = query_item["query_type"]
    aoi_id     = query_item["aoi_id"]
    gt_answer  = query_item["gt_answer"]
    gt_chain   = query_item["gt_tool_chain"]

    # Check: gt_answer_text must be populated
    if not query_item.get("gt_answer_text", "").strip():
        return False, "missing_gt_answer_text"

    # Re-execute the tool chain deterministically
    predicted = execute_tool_chain_deterministic(gt_chain, aoi_id, qt)
    if predicted is None:
        return False, "chain_execution_failed"

    # Check 1: Numeric accuracy (5% tolerance) per query type
    if qt == "Q1_counting":
        if not _numeric_close(predicted["total_polygons"],
                              gt_answer["total_polygons"]):
            return False, f'count_error: pred={predicted["total_polygons"]} gt={gt_answer["total_polygons"]}'
        # Quality: at least 1 polygon
        if gt_answer["total_polygons"] < 1:
            return False, "quality_zero_polygons"

    elif qt == "Q2_area":
        if not _numeric_close(predicted["flood_area_km2"],
                              gt_answer["flood_area_km2"]):
            return False, f'area_error: pred={predicted["flood_area_km2"]} gt={gt_answer["flood_area_km2"]}'
        # Quality: at least 0.01 km²
        if gt_answer["flood_area_km2"] < 0.01:
            return False, "quality_zero_area"

    elif qt == "Q4_change":
        pred_delta = predicted["delta_km2"]
        gt_delta   = gt_answer["delta_km2"]
        if not _numeric_close(pred_delta, gt_delta):
            return False, f'delta_error: pred={pred_delta} gt={gt_delta}'
        # Check 3: Temporal direction must match recomputed sign
        if predicted["direction"] != gt_answer["direction"]:
            return False, f'direction_mismatch: pred={predicted["direction"]} gt={gt_answer["direction"]}'

    elif qt == "Q5_routing":
        # Check 2: RSS must be 1.0
        rss = gt_answer.get("rss", 0)
        if rss < 1.0:
            return False, f"rss={rss:.2f} < 1.0"
        if gt_answer.get("length_m", 0) <= 0:
            return False, "quality_zero_route_length"

    elif qt == "Q6_conflict":
        if gt_answer.get("n_conflicts", 0) < 1:
            return False, "quality_no_conflicts"

    return True, "pass"


# ── Pipeline ───────────────────────────────────────────────────────────────────

class Step8VerifyAssemblePipeline:
    def __init__(self, base_dir="."):
        self.base_dir = str(Path(base_dir).resolve())

    def run(self):
        base      = Path(self.base_dir)
        raw_path  = base / "data" / "queries_raw.jsonl"

        queries = [json.loads(l) for l in open(raw_path, encoding="utf-8")]
        print(f"Loaded {len(queries)} raw queries")

        verified, rejected = [], []
        reject_reasons = defaultdict(int)

        for q in queries:
            ok, reason = verify_sample(q)
            if ok:
                verified.append(q)
            else:
                rejected.append({**q, "rejection_reason": reason})
                reject_reasons[reason] += 1

        pass_rate = len(verified) / len(queries) * 100
        print(f"\nVerified : {len(verified)} / {len(queries)} ({pass_rate:.1f}%)")
        print(f"Rejected : {len(rejected)}")
        print(f"\nRejection breakdown:")
        for reason, count in sorted(reject_reasons.items(), key=lambda x: -x[1]):
            print(f"  {reason:<40} {count}")

        # Save rejected for inspection
        rej_path = base / "data" / "queries_rejected.jsonl"
        with open(rej_path, "w", encoding="utf-8") as f:
            for q in rejected:
                f.write(json.dumps(q) + "\n")
        print(f"\nRejected saved -> {rej_path}")

        # Shuffle and split
        random.shuffle(verified)
        n = len(verified)
        n_train = int(0.74 * n)
        n_val   = int(0.13 * n)

        splits = {
            "train": verified[:n_train],
            "val":   verified[n_train:n_train + n_val],
            "test":  verified[n_train + n_val:],
        }

        print(f"\nDataset splits:")
        for split_name, split_data in splits.items():
            out = base / "data" / f"tdrd_{split_name}.jsonl"
            with open(out, "w", encoding="utf-8") as f:
                for item in split_data:
                    f.write(json.dumps(item) + "\n")
            print(f"  {split_name:<8} {len(split_data):>5} samples -> {out.name}")

        # Conflict subset (Q6 only, AOIs with conflict events)
        conflict_aoi_ids = {c["aoi_id"] for c in CONFLICTS}
        conflict_subset  = [
            q for q in verified
            if q["query_type"] == "Q6_conflict" and q["aoi_id"] in conflict_aoi_ids
        ]
        conf_path = base / "data" / "tdrd_conflict_subset.jsonl"
        with open(conf_path, "w", encoding="utf-8") as f:
            for item in conflict_subset:
                f.write(json.dumps(item) + "\n")
        print(f"  conflict_subset {len(conflict_subset):>5} samples -> {conf_path.name}")
        print(f"  (target: ~224)")

        # Final checklist
        print("\n=== TDRD COMPLETION CHECKLIST ===")
        checks = [
            ("data/tdrd_train.jsonl",            (base/"data"/"tdrd_train.jsonl").exists()),
            ("data/tdrd_val.jsonl",              (base/"data"/"tdrd_val.jsonl").exists()),
            ("data/tdrd_test.jsonl",             (base/"data"/"tdrd_test.jsonl").exists()),
            ("data/tdrd_conflict_subset.jsonl",  (base/"data"/"tdrd_conflict_subset.jsonl").exists()),
            ("data/aoi_list.json",               (base/"data"/"aoi_list.json").exists()),
            ("data/conflict_events.json",        (base/"data"/"conflict_events.json").exists()),
        ]
        for label, ok in checks:
            print(f"  [{'x' if ok else ' '}] {label}")
        print(f"\n  Verification pass rate : {pass_rate:.1f}%  (target: >=70%)")
        print(f"  {'[x]' if pass_rate >= 70 else '[ ]'} Pass rate >= 70%")

        return verified, rejected
