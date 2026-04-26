"""Re-run step5b for FAILed AOIs with impassable_threshold=0.50."""
import json, sys
from pathlib import Path

sys.path.insert(0, '.')

# Patch threshold before importing pipeline
import tdrd.pipelines.step5b_compute_routes as s5b
s5b.IMPASSABLE_THRESHOLD = 0.50

FAILED_AOIS = ['EVT002_0114', 'EVT008_0352']
BASE        = Path('.')
CACHE_DIR   = str(BASE / 'data' / 'osm_cache')

aois = json.load(open(BASE / 'data' / 'aoi_list.json'))
aoi_map = {a['aoi_id']: a for a in aois}

for aoi_id in FAILED_AOIS:
    print(f'\nRe-running {aoi_id} (threshold=0.50)...')
    a    = aoi_map[aoi_id]
    bbox = a['bbox']

    # Remove old routes so process_aoi doesn't skip
    out = BASE / 'data' / 'annotations' / aoi_id / 'evacuation_routes.json'
    if out.exists():
        out.unlink()

    meta_p = BASE / 'data' / 'stacks' / f'{aoi_id}_meta.json'
    epochs = list(range(2, json.load(open(meta_p)).get('n_epochs', 4) + 1)) if meta_p.exists() else [2,3,4]

    result = s5b.process_aoi(aoi_id, bbox, epochs, str(BASE), CACHE_DIR)
    print(f'  -> {result}')

    # Show new RSS values
    if out.exists():
        routes = json.load(open(out))
        for epoch, rlist in routes.items():
            for r in rlist:
                print(f'  {epoch} route {rlist.index(r)+1}: rss={r["rss"]} len={r["length_m"]}m')
        low = [(ep,r) for ep,rl in routes.items() for r in rl if r.get('rss',1)<1.0]
        if low:
            print(f'  WARNING: still {len(low)} routes with RSS<1.0')
        else:
            print(f'  All routes RSS=1.0')
