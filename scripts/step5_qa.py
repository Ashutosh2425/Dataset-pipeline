"""
step5_qa.py
-----------
QA checks for Step 5 evacuation route ground truth.
Prints per-event-type route stats and flags missing/empty outputs.
"""

import json
from pathlib import Path
from collections import defaultdict

AOI_LIST   = json.load(open('data/aoi_list.json'))
AOI_LOOKUP = {a['aoi_id']: a for a in AOI_LIST}

missing        = []
no_roads       = []   # AOIs where graph had no nodes (carry-over from Step 4)
empty_routes   = []   # AOIs with graph but 0 routes found
bad_files      = []

event_route_counts = defaultdict(list)   # event_type -> [n_routes per AOI]
event_rss          = defaultdict(list)   # event_type -> [rss per route]
event_eta          = defaultdict(list)   # event_type -> [eta_min per route]

for aoi in AOI_LIST:
    aoi_id     = aoi['aoi_id']
    event_type = aoi.get('event_type', 'unknown')
    out_path   = Path(f'data/annotations/{aoi_id}/evacuation_routes.json')

    if not out_path.exists():
        missing.append(aoi_id)
        continue

    try:
        data = json.loads(out_path.read_text())
    except Exception as e:
        bad_files.append((aoi_id, str(e)))
        continue

    if not data:
        no_roads.append(aoi_id)
        continue

    total_routes = sum(len(v) for v in data.values())
    if total_routes == 0:
        empty_routes.append(aoi_id)

    event_route_counts[event_type].append(total_routes)
    for epoch_routes in data.values():
        for r in epoch_routes:
            event_rss[event_type].append(r.get('rss', 0))
            event_eta[event_type].append(r.get('eta_min', 0))

SEP = '=' * 60
print(SEP)
print(f'  step5_qa.py  —  {len(AOI_LIST)} AOIs checked')
print(SEP)
ok = len(AOI_LIST) - len(missing) - len(no_roads) - len(bad_files)
print(f'  Missing outputs  : {len(missing)}')
print(f'  No OSM graph     : {len(no_roads)}  (carry-over from Step 4, expected)')
print(f'  Empty routes     : {len(empty_routes)}  (reachable but 0 paths found)')
print(f'  Bad JSON         : {len(bad_files)}')
print(f'  OK               : {ok}')
print()

if missing:
    print(f'  First 10 missing : {missing[:10]}')
    print()

if bad_files:
    print('  Bad files:')
    for aoi_id, err in bad_files[:5]:
        print(f'    {aoi_id}: {err}')
    print()

print('  Route stats by event type:')
print(f'    {"event_type":<15} {"routes/AOI":>10} {"mean_RSS":>10} {"mean_ETA":>10}')
print(f'    {"-"*15} {"-"*10} {"-"*10} {"-"*10}')
for et in sorted(event_route_counts):
    counts = event_route_counts[et]
    rss    = event_rss[et]
    eta    = event_eta[et]
    mean_r = sum(counts) / len(counts) if counts else 0
    mean_s = sum(rss) / len(rss) if rss else 0
    mean_e = sum(eta) / len(eta) if eta else 0
    print(f'    {et:<15} {mean_r:>10.1f} {mean_s:>10.3f} {mean_e:>10.1f} min')

print()
if missing or bad_files:
    print(f'  FAIL — {len(missing)} missing + {len(bad_files)} bad files')
else:
    print(f'  PASS — {ok} scored + {len(no_roads)} no-road AOIs = {len(AOI_LIST)}/{len(AOI_LIST)}')
print(SEP)
