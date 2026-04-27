"""
Replace the 15 AOIs that slipped through with n_osm_roads < 5.
Scans each affected event's bbox for verified replacement tiles.
"""
import json, sys, time, socket
socket.setdefaulttimeout(12)
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import osmnx as ox
ox.settings.use_cache = True; ox.settings.timeout = 8; ox.settings.log_console = False

from tdrd.core.geospatial import generate_tiles
from tdrd.config import EVENTS

AOI_PATH = Path("data/aoi_list.json")
d = json.loads(AOI_PATH.read_text())

bad_ids = {a['aoi_id'] for a in d if a.get('n_osm_roads', 0) < 5}
print(f"Bad entries to replace: {len(bad_ids)}", flush=True)

bad_by_event = Counter(a['event_id'] for a in d if a['aoi_id'] in bad_ids)
print(f"By event: {dict(bad_by_event)}", flush=True)

good = [a for a in d if a['aoi_id'] not in bad_ids]
event_map = {e['id']: e for e in EVENTS}

replacements = []
for eid, need in sorted(bad_by_event.items()):
    event = event_map[eid]
    tiles = generate_tiles(event['bbox'])
    existing = {tuple(a['bbox']) for a in good if a['event_id'] == eid}
    found = 0
    print(f"\n{eid}: scanning {len(tiles)} tiles, need {need} replacements...", flush=True)
    for tile in tiles:
        if tuple(tile) in existing:
            continue
        try:
            G = ox.graph_from_bbox((tile[0], tile[1], tile[2], tile[3]), network_type='drive')
            n = len(G.edges)
        except Exception:
            n = 0
        if n >= 5:
            replacements.append({
                'aoi_id': None,
                'event_id': eid,
                'event_type': event['type'],
                'bbox': list(tile),
                'date_range': event['dates'],
                'n_sentinel_scenes': 3,
                'n_osm_roads': n,
                'has_xbd_overlap': event['xbd_overlap'],
            })
            found += 1
            print(f"  {eid}: replacement {found}/{need} roads={n}", flush=True)
            if found >= need:
                break
        time.sleep(0.3)
    if found < need:
        print(f"  WARNING: only found {found}/{need} replacements for {eid}", flush=True)

all_aois = good + replacements
for i, a in enumerate(all_aois):
    a['aoi_id'] = f"{a['event_id']}_{i:04d}"

AOI_PATH.write_text(json.dumps(all_aois, indent=2))
print(f"\nSaved {len(all_aois)} AOIs", flush=True)

c = Counter(a['event_id'] for a in all_aois)
print(dict(sorted(c.items())), flush=True)
bad_after = [a for a in all_aois if a.get('n_osm_roads', 0) < 5]
print(f"n_osm_roads < 5 after fix: {len(bad_after)}", flush=True)
