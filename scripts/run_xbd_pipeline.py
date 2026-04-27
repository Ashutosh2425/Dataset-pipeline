"""
Run Steps 3a -> 4 -> 5 -> 6 for all events that now have xBD coverage.

Run this after adding new xBD archives and extracting labels with:
    python scripts/extract_xbd_labels.py

Then:
    python scripts/run_xbd_pipeline.py

It will:
  - Discover which pipeline events have xBD labels in data/xbd/
  - Clear existing annotations only for those events
  - Re-run Steps 3a, 4, 5, 6 end-to-end
"""

import sys, os, json, importlib.util
from pathlib import Path
from collections import defaultdict

os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

BASE    = Path(__file__).resolve().parents[1]
XBD_DIR = BASE / "data" / "xbd"
ANN_DIR = BASE / "data" / "annotations"


def load_module(rel_path):
    path = BASE / rel_path
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def discover_xbd_events():
    """Return set of pipeline event IDs that now have xBD data."""
    if not XBD_DIR.exists():
        return set()
    available = {d.name for d in XBD_DIR.iterdir()
                 if d.is_dir() and (d / "labels").exists()
                 and any((d / "labels").glob("*_post_disaster.json"))}
    if not available:
        return set()

    keywords = {
        'EVT001': ['hurricane-harvey', 'harvey'],
        'EVT002': ['hurricane-florence', 'florence'],
        'EVT003': ['hurricane-michael', 'michael'],
        'EVT004': ['hurricane-matthew', 'matthew'],
        'EVT005': ['mexico-earthquake', 'mexico'],
        'EVT006': ['nepal-flooding', 'nepal'],
        'EVT007': ['santa-rosa-wildfire', 'santa-rosa'],
        'EVT008': ['socal-fire', 'socal'],
        'EVT009': ['palu-tsunami', 'palu'],
        'EVT010': ['lower-puna-volcano', 'lower-puna', 'puna'],
        'EVT011': ['portugal-wildfire', 'portugal'],
        'EVT012': ['sunda-tsunami', 'sunda'],
    }
    covered = set()
    for eid, kws in keywords.items():
        if any(any(kw in folder.lower() for kw in kws) for folder in available):
            covered.add(eid)
    return covered


def clear_files(aoi_list, event_ids, pattern):
    """Delete annotation files matching pattern for given events."""
    deleted = 0
    for a in aoi_list:
        if a['event_id'] not in event_ids:
            continue
        meta_path = BASE / 'data' / 'stacks' / f"{a['aoi_id']}_meta.json"
        if not meta_path.exists():
            continue
        meta = json.load(open(meta_path))
        n_ep = meta['n_epochs']
        for t in range(2, n_ep + 1):
            p = ANN_DIR / a['aoi_id'] / pattern.format(t=t)
            if p.exists():
                try:
                    p.unlink()
                    deleted += 1
                except PermissionError:
                    pass
    return deleted


def main():
    aoi_list   = json.load(open(BASE / 'data' / 'aoi_list.json'))
    xbd_events = discover_xbd_events()

    if not xbd_events:
        print("No xBD data found in data/xbd/")
        print("Run: python scripts/extract_xbd_labels.py first")
        return

    print(f"xBD coverage found for: {sorted(xbd_events)}")
    print(f"AOIs to re-process: "
          f"{sum(1 for a in aoi_list if a['event_id'] in xbd_events)}")

    # ── Step 3a ───────────────────────────────────────────────────────────────
    print("\n" + "="*55)
    print("STEP 3a — Building Damage Annotation (xBD)")
    print("="*55)
    step3a = load_module("tdrd/pipelines/step3a_damage_annotation.py")
    p3a = step3a.Step3aDamagePipeline()
    p3a.run(force_xbd=True)

    # ── Step 4 ────────────────────────────────────────────────────────────────
    print("\n" + "="*55)
    print("STEP 4 — Road Damage Scoring")
    print("="*55)
    d = clear_files(aoi_list, xbd_events, "road_damage_scores.gpkg".replace("{t}", ""))
    # clear_files won't work for gpkg (no epoch in name), do it directly
    deleted4 = 0
    for a in aoi_list:
        if a['event_id'] not in xbd_events:
            continue
        gpkg = ANN_DIR / a['aoi_id'] / 'road_damage_scores.gpkg'
        if gpkg.exists():
            try:
                gpkg.unlink()
                deleted4 += 1
            except PermissionError:
                pass
    print(f"Cleared {deleted4} GPKGs for re-scoring")

    step4 = load_module("tdrd/pipelines/step4_road_damage_scoring.py")
    p4 = step4.Step4RoadDamagePipeline(workers=4)
    p4.run(force=False)   # force=False: only re-runs the ones we just deleted

    # ── Step 5 ────────────────────────────────────────────────────────────────
    print("\n" + "="*55)
    print("STEP 5 — Evacuation Routes")
    print("="*55)
    deleted5 = 0
    for a in aoi_list:
        if a['event_id'] not in xbd_events:
            continue
        rp = ANN_DIR / a['aoi_id'] / 'evacuation_routes.json'
        if rp.exists():
            try:
                rp.unlink()
                deleted5 += 1
            except PermissionError:
                pass
    print(f"Cleared {deleted5} route files")

    step5 = load_module("tdrd/pipelines/step5b_compute_routes.py")
    p5 = step5.Step5EvacuationPipeline(workers=4)
    p5.run()

    # ── Step 6 ────────────────────────────────────────────────────────────────
    print("\n" + "="*55)
    print("STEP 6 — Conflict Detection")
    print("="*55)
    step6 = load_module("tdrd/pipelines/step6_conflict_annotation.py")
    p6 = step6.Step6ConflictPipeline(workers=4)
    p6.run()

    print("\n" + "="*55)
    print("Pipeline update complete.")
    print("="*55)


if __name__ == "__main__":
    main()
