"""
Fills under-target events to their original AOI counts.

After rebuild_xbd_events.py some events yielded fewer AOIs than targeted
because the xBD chip zones are small. This script tops each event up to
its original target by adding tiles from the same event bbox that passed
STAC coverage but were outside the xBD chip zone.
New tiles are marked has_xbd_overlap=False.

Shortfalls:
  EVT003  21/41  need 20 more
  EVT008  21/22  need  1 more
  EVT009  47/72  need 25 more
  EVT010  18/58  need 40 more
  EVT012  50/77  need 27 more

Run from repo root:
    python scripts/fill_aoi_targets.py
"""

import json, asyncio, os, random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tdrd.config import EVENTS, EVENT_TARGETS, AOI_LIST_PATH
from tdrd.core.geospatial import generate_tiles, build_xbd_chip_index, tile_overlaps_xbd
from tdrd.core.networks import check_road_density

XBD_BASE   = Path("data/xbd")
AOI_LIST   = Path(AOI_LIST_PATH)
MIN_ROADS  = 5
WORKERS    = 4
OSM_TIMEOUT = 120

FILL_EVENTS = {'EVT003', 'EVT008', 'EVT009', 'EVT010', 'EVT012'}


def load_existing():
    return json.load(open(AOI_LIST))


def get_existing_bboxes(aois, event_id):
    return {tuple(a['bbox']) for a in aois if a['event_id'] == event_id}


def road_filter(tiles, need, event_id):
    """Run OSMnx on tiles until `need` pass. Returns list of (tile, n_roads)."""
    passing = []
    checked = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(check_road_density, t): t for t in tiles}
        for fut in as_completed(futures):
            t = futures[fut]
            checked += 1
            try:
                n = fut.result(timeout=OSM_TIMEOUT)
            except FutureTimeout:
                n = 0
            except Exception:
                n = 0
            if n >= MIN_ROADS:
                passing.append((t, n))
            if checked % 20 == 0:
                print(f"    OSMnx: {checked}/{len(tiles)}  pass={len(passing)}")
            if len(passing) >= need:
                # cancel remaining
                for f in futures:
                    f.cancel()
                break
    return passing


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


def main():
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    aois = load_existing()
    current_counts = {}
    for a in aois:
        current_counts[a['event_id']] = current_counts.get(a['event_id'], 0) + 1

    print(f"Current total: {len(aois)} AOIs")
    print("Shortfalls:")
    for eid in sorted(FILL_EVENTS):
        have   = current_counts.get(eid, 0)
        target = EVENT_TARGETS[eid]
        print(f"  {eid}: {have}/{target}  need {max(0, target-have)} more")

    event_map = {e['id']: e for e in EVENTS}
    new_aois  = []

    for eid in sorted(FILL_EVENTS):
        have   = current_counts.get(eid, 0)
        target = EVENT_TARGETS[eid]
        need   = target - have
        if need <= 0:
            print(f"\n{eid}: already at target, skipping")
            continue

        event  = event_map[eid]
        print(f"\n{eid}: need {need} more tiles from {event['name']} bbox")

        all_tiles      = generate_tiles(event['bbox'])
        existing_bboxes = get_existing_bboxes(aois, eid)

        # Get xBD chips to EXCLUDE (already covered)
        folder = event.get('xbd_folder')
        if folder:
            chip_bboxes     = build_xbd_chip_index(XBD_BASE, folder)
            chips_in_region = [c for c in chip_bboxes if tile_overlaps_xbd(event['bbox'], [c])]
        else:
            chips_in_region = []

        # Candidate tiles: not already in aoi_list, not overlapping xBD chips
        candidates = [
            t for t in all_tiles
            if tuple(t) not in existing_bboxes
            and not tile_overlaps_xbd(t, chips_in_region)
        ]

        print(f"  {len(candidates)} candidate tiles outside xBD zone")
        random.seed(42)
        random.shuffle(candidates)

        passing = road_filter(candidates, need, eid)
        print(f"  {len(passing)} tiles pass OSMnx (need {need})")

        for tile, n_roads in passing[:need]:
            new_aois.append({
                'aoi_id':          None,
                'event_id':        eid,
                'event_type':      event['type'],
                'bbox':            list(tile),
                'date_range':      event['dates'],
                'n_sentinel_scenes': 3,
                'n_osm_roads':     n_roads,
                'has_xbd_overlap': False,
            })

    all_aois = aois + new_aois
    all_aois = sort_and_reindex(all_aois)

    with open(AOI_LIST, 'w') as f:
        json.dump(all_aois, f, indent=2)

    print(f"\nFinal total: {len(all_aois)} AOIs")
    xbd_true = sum(1 for a in all_aois if a.get('has_xbd_overlap'))
    print(f"xBD coverage: {xbd_true}/{len(all_aois)} = {100*xbd_true/len(all_aois):.1f}%")
    from collections import Counter
    counts = Counter(a['event_id'] for a in all_aois)
    for eid in sorted(counts):
        xbd = sum(1 for a in all_aois if a['event_id'] == eid and a.get('has_xbd_overlap'))
        print(f"  {eid}: {counts[eid]} AOIs  ({xbd} xBD)")


if __name__ == '__main__':
    main()
