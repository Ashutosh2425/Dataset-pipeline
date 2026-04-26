"""
scripts/trim_aoi_list.py
------------------------
Run after Step 1 completes. Trims aoi_list.json to the exact per-event
counts from the build guide (total = 600). Randomly samples within each
event so spatial diversity is preserved. Re-assigns sequential aoi_ids.
"""

import json
import random
import sys
from collections import Counter
from pathlib import Path

AOI_LIST_PATH = Path("data/aoi_list.json")

# Exact targets from the build guide §1.1
TARGETS = {
    'EVT001': 68,   # Hurricane Harvey        (flood,       USA)
    'EVT002': 52,   # Kerala Floods           (flood,       India)
    'EVT003': 41,   # Camp Fire               (fire,        USA)
    'EVT004': 44,   # Cyclone Fani            (wind+flood,  India)
    'EVT005': 55,   # Midwest Floods          (flood,       USA)
    'EVT006': 28,   # Beirut Explosion        (structural,  Lebanon)
    'EVT007': 48,   # Cyclone Amphan          (wind+flood,  India/Bangladesh)
    'EVT008': 22,   # Natchez Tornadoes       (wind,        USA)
    'EVT009': 72,   # Pakistan Floods         (flood,       Pakistan)
    'EVT010': 58,   # Turkey Earthquake       (structural,  Turkey)
    'EVT011': 35,   # Hawaii Wildfires        (fire,        USA)
    'EVT012': 77,   # Libya Derna Floods      (flash flood, Libya)
}

SEED = 42


def main():
    if not AOI_LIST_PATH.exists():
        sys.exit(f"ERROR: {AOI_LIST_PATH} not found")

    with open(AOI_LIST_PATH) as f:
        aois = json.load(f)

    # Group by event
    by_event = {}
    for aoi in aois:
        eid = aoi['event_id']
        by_event.setdefault(eid, []).append(aoi)

    # Check all 12 events are present
    missing = [e for e in TARGETS if e not in by_event]
    if missing:
        sys.exit(f"Step 1 not finished — missing events: {missing}")

    rng = random.Random(SEED)
    trimmed = []
    ok = True

    print(f"\n{'Event':<8} {'Have':>6} {'Target':>8} {'Status'}")
    print("-" * 36)
    for eid, target in TARGETS.items():
        pool = [a for a in by_event[eid] if a.get('n_osm_roads', 0) >= 5 and a.get('n_sentinel_scenes', 0) >= 3]
        have = len(pool)
        if have < target:
            print(f"{eid:<8} {have:>6} {target:>8}  UNDER — only {have} available")
            ok = False
            trimmed.extend(pool)
        else:
            chosen = rng.sample(pool, target)
            trimmed.extend(chosen)
            status = "trimmed" if have > target else "exact"
            print(f"{eid:<8} {have:>6} {target:>8}  {status}")

    print("-" * 36)
    print(f"{'TOTAL':<8} {len(aois):>6} {sum(TARGETS.values()):>8}  -> {len(trimmed)}")

    if not ok:
        sys.exit("\nAborted: one or more events are under target. Re-run Step 1.")

    # Re-assign sequential aoi_ids
    for i, aoi in enumerate(trimmed):
        aoi['aoi_id'] = f"{aoi['event_id']}_{i:04d}"

    with open(AOI_LIST_PATH, 'w') as f:
        json.dump(trimmed, f, indent=2)

    print(f"\nSaved {len(trimmed)} AOIs to {AOI_LIST_PATH}")


if __name__ == '__main__':
    main()
