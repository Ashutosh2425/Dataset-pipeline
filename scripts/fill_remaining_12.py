"""
Fills the 12 remaining empty slots after replace_sparse_aois.py.

EVT003, EVT005, EVT008, EVT012 had no more xBD tiles available,
so we fall back to non-xBD spectral tiles from the same event bboxes
(same approach as fill_aoi_targets.py).

Run from repo root:
    python scripts/fill_remaining_12.py
"""

import json, os, asyncio, random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tdrd.config import EVENTS
from tdrd.core.geospatial import generate_tiles, build_xbd_chip_index, tile_overlaps_xbd
from tdrd.core.networks import check_road_density

AOI_LIST  = Path('data/aoi_list.json')
XBD_BASE  = Path('data/xbd')
MIN_ROADS = 5
WORKERS   = 4

NEEDS = {'EVT003': 1, 'EVT005': 5, 'EVT008': 2, 'EVT012': 4}


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


def road_filter(tiles, need):
    passing = []
    checked = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(check_road_density, t): t for t in tiles}
        for fut in as_completed(futures):
            t = futures[fut]
            checked += 1
            try:
                n = fut.result(timeout=120)
            except Exception:
                n = 0
            if n >= MIN_ROADS:
                passing.append((t, n))
            if checked % 20 == 0:
                print(f'    OSMnx: {checked}/{len(tiles)}  pass={len(passing)}')
            if len(passing) >= need:
                for f in futures: f.cancel()
                break
    return passing


def main():
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    aois = json.load(open(AOI_LIST))
    existing_bboxes = {tuple(a['bbox']) for a in aois}
    event_map = {e['id']: e for e in EVENTS}

    print(f'Current total: {len(aois)} AOIs — need {sum(NEEDS.values())} more')

    new_aois = []
    for eid, need in sorted(NEEDS.items()):
        event = event_map[eid]
        print(f'\n{eid}: need {need} non-xBD tiles from {event["name"]} bbox')

        all_tiles = generate_tiles(event['bbox'])
        folder = event.get('xbd_folder')
        if folder:
            chip_bboxes = build_xbd_chip_index(XBD_BASE, folder)
        else:
            chip_bboxes = []

        # Candidates: not in list, NOT overlapping xBD (to avoid double-counting)
        candidates = [
            t for t in all_tiles
            if tuple(t) not in existing_bboxes
            and not tile_overlaps_xbd(t, chip_bboxes)
        ]
        print(f'  {len(candidates)} candidate tiles outside xBD zone')

        random.seed(77)
        random.shuffle(candidates)
        passing = road_filter(candidates, need)
        print(f'  {len(passing)} tiles pass road check (need {need})')

        for tile, n_roads in passing[:need]:
            existing_bboxes.add(tuple(tile))
            new_aois.append({
                'aoi_id':            None,
                'event_id':          eid,
                'event_type':        event['type'],
                'bbox':              list(tile),
                'date_range':        event['dates'],
                'n_sentinel_scenes': 3,
                'n_osm_roads':       n_roads,
                'has_xbd_overlap':   False,
            })

    all_aois = aois + new_aois
    all_aois = sort_and_reindex(all_aois)

    with open(AOI_LIST, 'w') as f:
        json.dump(all_aois, f, indent=2)

    print(f'\nFinal total: {len(all_aois)} AOIs')
    xbd_true = sum(1 for a in all_aois if a.get('has_xbd_overlap'))
    print(f'xBD coverage: {xbd_true}/{len(all_aois)} = {100*xbd_true/len(all_aois):.1f}%')


if __name__ == '__main__':
    main()
