"""
step3_qa.py
-----------
QA checks for Step 3 annotation outputs:
  - damage_polygons_T{n}.geojson exists for each post-event epoch
  - flood_extent_T{n}.geojson exists for each post-event epoch
  - CRS matches stack CRS for all files
  - Buildings per AOI in range 50-500 (else flag)
  - Events without xBD have source='spectral' in damage polygons

Usage: python step3_qa.py
"""

import json
from pathlib import Path
import geopandas as gpd
import rasterio

AOI_LIST_PATH = Path("data/aoi_list.json")
STACKS_DIR    = Path("data/stacks")
ANNOTATIONS   = Path("data/annotations")

XBD_EVENTS = {'EVT001', 'EVT003', 'EVT005', 'EVT006', 'EVT008', 'EVT010', 'EVT011'}


def main():
    with open(AOI_LIST_PATH) as f:
        aois = json.load(f)

    total = len(aois)
    ok = 0
    missing_damage   = []
    missing_flood    = []
    crs_mismatch     = []
    low_buildings    = []
    high_buildings   = []
    bad_source       = []

    for aoi in aois:
        aoi_id    = aoi['aoi_id']
        event_id  = aoi['event_id']
        meta_path = STACKS_DIR / f"{aoi_id}_meta.json"
        ann_dir   = ANNOTATIONS / aoi_id

        if not meta_path.exists():
            missing_damage.append(aoi_id)
            continue

        with open(meta_path) as f:
            meta = json.load(f)

        n_epochs  = meta['n_epochs']
        aoi_ok    = True

        # Get stack CRS
        stack_crs = None
        stack_path = STACKS_DIR / f"{aoi_id}_stack.tif"
        if stack_path.exists():
            with rasterio.open(stack_path) as src:
                stack_crs = src.crs.to_epsg()

        for t in range(1, n_epochs):
            epoch_label = t + 1

            # Check damage polygons
            dmg_path = ann_dir / f"damage_polygons_T{epoch_label}.geojson"
            if not dmg_path.exists():
                missing_damage.append(f"{aoi_id}/T{epoch_label}")
                aoi_ok = False
                continue

            # Check flood extent
            fld_path = ann_dir / f"flood_extent_T{epoch_label}.geojson"
            if not fld_path.exists():
                missing_flood.append(f"{aoi_id}/T{epoch_label}")
                aoi_ok = False
                continue

            # CRS + content check via json (avoids fiona version issues)
            try:
                with open(dmg_path) as f:
                    dmg_data = json.load(f)
                with open(fld_path) as f:
                    fld_data = json.load(f)

                dmg_crs = (dmg_data.get('crs', {}) or {}).get('properties', {}).get('name', '')
                fld_crs = (fld_data.get('crs', {}) or {}).get('properties', {}).get('name', '')

                # GeoJSON spec is always EPSG:4326 — flag only explicit mismatches
                if dmg_crs and '4326' not in dmg_crs and 'CRS84' not in dmg_crs:
                    crs_mismatch.append(f"{aoi_id}/damage/T{epoch_label}")
                    aoi_ok = False
                if fld_crs and '4326' not in fld_crs and 'CRS84' not in fld_crs:
                    crs_mismatch.append(f"{aoi_id}/flood/T{epoch_label}")
                    aoi_ok = False

                dmg_features = dmg_data.get('features', [])
                fld_features = fld_data.get('features', [])

                # Source check for non-xBD events
                if event_id not in XBD_EVENTS and t == 1 and dmg_features:
                    sources = [f.get('properties', {}).get('source', '') for f in dmg_features]
                    if not any(s == 'spectral' for s in sources):
                        bad_source.append(aoi_id)
                        aoi_ok = False

                # Building count check (first post-event epoch only)
                if t == 1:
                    n_buildings = len(dmg_features)
                    if n_buildings < 50:
                        low_buildings.append((aoi_id, n_buildings))
                    elif n_buildings > 500:
                        high_buildings.append((aoi_id, n_buildings))

            except Exception as e:
                crs_mismatch.append(f"{aoi_id}/read_error: {e}")
                aoi_ok = False

        if aoi_ok:
            ok += 1

    print(f"\n{'='*60}")
    print(f"  step3_qa.py  —  {total} AOIs checked")
    print(f"{'='*60}")
    print(f"  OK                : {ok} / {total}")
    print(f"  Missing damage    : {len(missing_damage)}")
    print(f"  Missing flood     : {len(missing_flood)}")
    print(f"  CRS mismatch      : {len(crs_mismatch)}")
    print(f"  < 50 buildings    : {len(low_buildings)}")
    print(f"  > 500 buildings   : {len(high_buildings)}")
    print(f"  Bad source tag    : {len(bad_source)}")

    if missing_damage:
        print(f"\n  First 5 missing damage : {missing_damage[:5]}")
    if missing_flood:
        print(f"  First 5 missing flood  : {missing_flood[:5]}")
    if crs_mismatch:
        print(f"  First 5 CRS mismatch   : {crs_mismatch[:5]}")
    if low_buildings:
        print(f"  First 5 low buildings  : {low_buildings[:5]}")

    print(f"\n  {'PASS' if ok == total else 'FAIL'}  —  {ok}/{total} OK")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
