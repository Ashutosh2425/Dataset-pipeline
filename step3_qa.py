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

            # CRS check
            try:
                dmg_gdf = gpd.read_file(dmg_path)
                fld_gdf = gpd.read_file(fld_path)

                if stack_crs:
                    if dmg_gdf.crs and dmg_gdf.crs.to_epsg() != 4326:
                        crs_mismatch.append(f"{aoi_id}/damage/T{epoch_label}")
                        aoi_ok = False
                    if fld_gdf.crs and fld_gdf.crs.to_epsg() != 4326:
                        crs_mismatch.append(f"{aoi_id}/flood/T{epoch_label}")
                        aoi_ok = False

                # Source check for non-xBD events
                if event_id not in XBD_EVENTS and t == 1 and len(dmg_gdf) > 0:
                    if 'source' not in dmg_gdf.columns or not (dmg_gdf['source'] == 'spectral').any():
                        bad_source.append(aoi_id)
                        aoi_ok = False

                # Building count check (only on first post-event epoch)
                if t == 1:
                    n_buildings = len(dmg_gdf)
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
