"""
Replace 15 low-building xBD AOIs with denser alternatives.

Removes AOIs where xBD transfer produced <50 building polygons,
finds replacement tiles from the same event bbox that have >=50 xBD
buildings AND pass OSMnx road density, then reindexes aoi_list.json.

Run from repo root:
    python scripts/replace_sparse_aois.py
"""

import json, os, asyncio, random, shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tdrd.config import EVENTS
from tdrd.core.geospatial import generate_tiles, build_xbd_chip_index, tile_overlaps_xbd
from tdrd.core.networks import check_road_density
from tdrd.pipelines.step3a_damage_annotation import (
    load_xbd_polygons, discover_xbd_event_map, XBD_DIR
)

AOI_LIST  = Path('data/aoi_list.json')
RAW_DIR   = Path('data/raw_scenes')
STACKS    = Path('data/stacks')
ANN_DIR   = Path('data/annotations')
MIN_ROADS = 5
MIN_BLDGS = 50
WORKERS   = 4

SPARSE_IDS = {
    'EVT001_0008', 'EVT001_0032', 'EVT001_0056',
    'EVT003_0122',
    'EVT005_0228', 'EVT005_0251', 'EVT005_0255', 'EVT005_0258', 'EVT005_0259',
    'EVT008_0348', 'EVT008_0354',
    'EVT012_0552', 'EVT012_0560', 'EVT012_0565', 'EVT012_0570',
}


def sort_and_reindex(aois):
    event_order = {e['id']: i for i, e in enumerate(EVENTS)}
    for j, a in enumerate(aois):
        a['_idx'] = j
    aois.sort(key=lambda a: (event_order.get(a['event_id'], 99), a['_idx']))
    for a in aois:
        del a['_idx']
    for i, a in enumerate(aois):
        a['aoi_id'] = f"{a['event_id']}_{i:04d}"
    return aois


def delete_aoi_data(aoi_id):
    def _force_rm(func, path, _):
        os.chmod(path, 0o777)
        func(path)
    for d in [RAW_DIR / aoi_id, ANN_DIR / aoi_id]:
        if d.exists():
            shutil.rmtree(d, onerror=_force_rm)
    for suffix in ['_stack.tif', '_meta.json']:
        p = STACKS / f'{aoi_id}{suffix}'
        if p.exists():
            p.unlink()


def count_xbd_buildings(tile_bbox, event_id, xbd_map):
    gdf = load_xbd_polygons(tile_bbox, event_id, xbd_map)
    return len(gdf) if gdf is not None else 0


def find_replacements(event_id, event, need, existing_bboxes, xbd_map):
    all_tiles = generate_tiles(event['bbox'])
    folder = event.get('xbd_folder')
    chip_bboxes = build_xbd_chip_index(XBD_DIR, folder) if folder else []

    # Must overlap xBD (we need real building labels)
    candidates = [
        t for t in all_tiles
        if tuple(t) not in existing_bboxes
        and tile_overlaps_xbd(t, chip_bboxes)
    ]
    print(f'  {len(candidates)} candidate tiles with xBD overlap')

    random.seed(99)
    random.shuffle(candidates)

    passing_roads = []
    checked = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(check_road_density, t): t for t in candidates}
        for fut in as_completed(futures):
            t = futures[fut]
            checked += 1
            try:
                n = fut.result(timeout=120)
            except Exception:
                n = 0
            if n >= MIN_ROADS:
                passing_roads.append((t, n))
            if checked % 20 == 0:
                print(f'    OSMnx: {checked}/{len(candidates)}  roads_ok={len(passing_roads)}')
            if len(passing_roads) >= need * 3:
                for f in futures: f.cancel()
                break

    print(f'  {len(passing_roads)} passed road check — now checking xBD building counts...')

    good = []
    for tile, n_roads in passing_roads:
        n_bldgs = count_xbd_buildings(list(tile), event_id, xbd_map)
        if n_bldgs >= MIN_BLDGS:
            good.append((tile, n_roads, n_bldgs))
            print(f'    GOOD: {n_bldgs} buildings')
        if len(good) >= need:
            break

    return good


def main():
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    aois = json.load(open(AOI_LIST))
    sparse = [a for a in aois if a['aoi_id'] in SPARSE_IDS]
    keep   = [a for a in aois if a['aoi_id'] not in SPARSE_IDS]

    print(f'Removing {len(sparse)} sparse AOIs:')
    for a in sparse:
        print(f'  {a["aoi_id"]} ({a["event_id"]})')
        delete_aoi_data(a['aoi_id'])

    # Count replacements needed per event
    needs = {}
    for a in sparse:
        needs[a['event_id']] = needs.get(a['event_id'], 0) + 1

    existing_bboxes = {tuple(a['bbox']) for a in keep}
    event_map = {e['id']: e for e in EVENTS}
    xbd_map   = discover_xbd_event_map()

    new_aois = []
    for eid, need in sorted(needs.items()):
        event = event_map[eid]
        print(f'\n{eid}: need {need} replacements from {event["name"]} bbox')
        good = find_replacements(eid, event, need, existing_bboxes, xbd_map)
        print(f'  Found {len(good)} (need {need})')

        for tile, n_roads, n_bldgs in good[:need]:
            existing_bboxes.add(tuple(tile))
            new_aois.append({
                'aoi_id':            None,
                'event_id':          eid,
                'event_type':        event['type'],
                'bbox':              list(tile),
                'date_range':        event['dates'],
                'n_sentinel_scenes': 3,
                'n_osm_roads':       n_roads,
                'has_xbd_overlap':   True,
            })

    all_aois = keep + new_aois
    all_aois = sort_and_reindex(all_aois)

    with open(AOI_LIST, 'w') as f:
        json.dump(all_aois, f, indent=2)

    print(f'\nFinal total: {len(all_aois)} AOIs')
    print(f'New AOIs added: {len(new_aois)}')
    new_ids = [a['aoi_id'] for a in all_aois if a in new_aois or a.get('aoi_id') is None]
    # Print newly added IDs by cross-referencing bbox
    new_bboxes = {tuple(a['bbox']) for a in new_aois}
    added_ids = [a['aoi_id'] for a in all_aois if tuple(a['bbox']) in new_bboxes]
    print('New AOI IDs:', added_ids)


if __name__ == '__main__':
    main()
