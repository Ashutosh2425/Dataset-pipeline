import json, glob
from pathlib import Path
from collections import Counter, defaultdict

BASE = Path('.')
aois = json.load(open('data/aoi_list.json'))
print(f'=== AOI LIST ({len(aois)} AOIs) ===')

# Global sequential check
nums = sorted(int(a['aoi_id'].split('_')[1]) for a in aois)
gaps = [nums[i] for i in range(1, len(nums)) if nums[i] != nums[i-1]+1]
print(f'ID range: {nums[0]:04d} - {nums[-1]:04d}, gaps={gaps}')

# Step 1: raw scenes
raw_dirs = set(p.name for p in (BASE/'data'/'raw_scenes').iterdir() if p.is_dir())
missing_raw = [a['aoi_id'] for a in aois if a['aoi_id'] not in raw_dirs]
print(f'\n=== STEP 1: raw_scenes ===')
print(f'Dirs found: {len(raw_dirs)}, Missing: {len(missing_raw)}')
if missing_raw[:5]:
    print('  Missing:', missing_raw[:5])

# Step 2: stacks
stack_meta = set(p.stem.replace('_meta','') for p in (BASE/'data'/'stacks').glob('*_meta.json'))
missing_meta = [a['aoi_id'] for a in aois if a['aoi_id'] not in stack_meta]
print(f'\n=== STEP 2: stacks ===')
print(f'Meta files: {len(stack_meta)}, Missing: {len(missing_meta)}')

# Check epochs coverage
n_epochs_dist = Counter()
for a in aois:
    mp = BASE/'data'/'stacks'/f'{a["aoi_id"]}_meta.json'
    if mp.exists():
        n = json.load(open(mp)).get('n_epochs', 0)
        n_epochs_dist[n] += 1
print(f'n_epochs distribution: {dict(sorted(n_epochs_dist.items()))}')

# Step 3: annotations
ann_root = BASE/'data'/'annotations'
ann_dirs = set(p.name for p in ann_root.iterdir() if p.is_dir()) if ann_root.exists() else set()
missing_ann = [a['aoi_id'] for a in aois if a['aoi_id'] not in ann_dirs]
print(f'\n=== STEP 3: damage annotations ===')
print(f'Ann dirs: {len(ann_dirs)}, Missing: {len(missing_ann)}')

# Check damage polygon files
missing_dp = []
bad_dp = []
for a in aois:
    aid = a['aoi_id']
    mp = BASE/'data'/'stacks'/f'{aid}_meta.json'
    if not mp.exists():
        continue
    meta = json.load(open(mp))
    n_ep = meta.get('n_epochs', 4)
    for t in range(2, n_ep+1):
        fp = ann_root/aid/f'damage_polygons_T{t}.geojson'
        if not fp.exists():
            missing_dp.append(f'{aid}_T{t}')
        else:
            data = json.load(open(fp))
            if data.get('type') != 'FeatureCollection':
                bad_dp.append(f'{aid}_T{t}')
print(f'damage_polygons files missing: {len(missing_dp)}')
print(f'damage_polygons bad format: {len(bad_dp)}')

# flood extent files
missing_fl = []
for a in aois:
    aid = a['aoi_id']
    mp = BASE/'data'/'stacks'/f'{aid}_meta.json'
    if not mp.exists():
        continue
    meta = json.load(open(mp))
    n_ep = meta.get('n_epochs', 4)
    for t in range(2, n_ep+1):
        fp = ann_root/aid/f'flood_extent_T{t}.geojson'
        if not fp.exists():
            missing_fl.append(f'{aid}_T{t}')
print(f'flood_extent files missing: {len(missing_fl)}')

# Step 4: road damage scores
print(f'\n=== STEP 4: road damage scores ===')
missing_rd = []
for a in aois:
    fp = ann_root/a['aoi_id']/'road_damage_scores.gpkg'
    if not fp.exists():
        missing_rd.append(a['aoi_id'])
print(f'road_damage_scores.gpkg missing: {len(missing_rd)}')

# Step 5: evacuation routes
print(f'\n=== STEP 5: evacuation routes ===')
missing_ev = []
low_rss = []
for a in aois:
    fp = ann_root/a['aoi_id']/'evacuation_routes.json'
    if not fp.exists():
        missing_ev.append(a['aoi_id'])
    else:
        routes = json.load(open(fp))
        for epoch, rlist in routes.items():
            for r in rlist:
                if r.get('rss', 1.0) < 1.0:
                    low_rss.append((a['aoi_id'], epoch, r.get('shelter_name'), r.get('rss')))
print(f'evacuation_routes.json missing: {len(missing_ev)}')
print(f'Routes with RSS < 1.0: {len(low_rss)}')

# human review
hr = Path('data/human_review/route_review.csv')
print(f'\n=== HUMAN REVIEW ===')
print(f'CSV exists: {hr.exists()}')
if hr.exists():
    lines = hr.read_text().splitlines()
    print(f'Rows (incl header): {len(lines)}')

print('\n=== OVERALL ===')
issues = []
if missing_raw: issues.append(f'{len(missing_raw)} AOIs missing raw_scenes')
if missing_meta: issues.append(f'{len(missing_meta)} AOIs missing stack meta')
if missing_dp: issues.append(f'{len(missing_dp)} damage polygon files missing')
if missing_fl: issues.append(f'{len(missing_fl)} flood extent files missing')
if missing_rd: issues.append(f'{len(missing_rd)} AOIs missing road scores')
if missing_ev: issues.append(f'{len(missing_ev)} AOIs missing evacuation routes')
if low_rss: issues.append(f'{len(low_rss)} routes with RSS < 1.0')
if gaps: issues.append(f'ID gaps: {gaps}')
if issues:
    for i in issues:
        print(f'  WARN: {i}')
else:
    print('  All checks PASS')
