import json
from collections import defaultdict
from pathlib import Path

p = Path('data/conflict_events.json')
aois = json.load(open('data/aoi_list.json'))
aoi_map = {a['aoi_id']: a for a in aois}

# 1. File exists
print('1. data/conflict_events.json exists:', 'PASS' if p.exists() else 'FAIL')
if not p.exists():
    print('   Run step6 pipeline first.')
    exit()

events = json.load(open(p))

# 2. Count in target range
n = len(events)
if n >= 400 and n <= 500:
    status = f'PASS ({n} — in 400-500 target)'
elif n >= 300:
    status = f'PASS ({n} — acceptable, target ~468)'
else:
    status = f'FAIL ({n} < 300 — lower score change threshold)'
print(f'2. Conflict count 400-500 (target ~468): {status}')

# 3. Distribution across events
by_event = defaultdict(int)
by_type  = defaultdict(int)
for c in events:
    a = aoi_map.get(c['aoi_id'], {})
    by_event[a.get('event_id', '?')] += 1
    by_type[c.get('conflict_type', '?')] += 1

n_events_with = sum(1 for v in by_event.values() if v > 0)
all_events    = sorted(set(a['event_id'] for a in aois))
dist_ok = n_events_with >= len(all_events) * 0.7
print(f'3. Distribution spans events: {"PASS" if dist_ok else "FAIL"} '
      f'({n_events_with}/{len(all_events)} events have conflicts)')
for eid in all_events:
    mark = 'ok' if by_event[eid] > 0 else '--'
    print(f'   {eid}: {by_event[eid]:3d}  [{mark}]')

# 4. Conflict types present
print(f'4. Conflict types: {dict(by_type)}')

# 5. Required fields
req = ['aoi_id', 'epoch_T', 'epoch_T1', 'route_id', 'score_T', 'score_T1', 'conflict_type']
bad = [c for c in events if any(f not in c for f in req)]
print(f'5. Required fields on all events: {"PASS" if not bad else f"FAIL ({len(bad)} bad)"}')

# 6. Sync point
print('6. SYNC POINT with Person 2: MANUAL — share conflict_events.json')

print()
print('Summary:')
print(f'  Total conflict events : {n}')
print(f'  Events covered        : {n_events_with}/{len(all_events)}')
print(f'  Conflict types        : {dict(by_type)}')
print(f'  AOIs with conflicts   : {len(set(c["aoi_id"] for c in events))}')
