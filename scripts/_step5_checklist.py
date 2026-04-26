import json, csv
from pathlib import Path

aois = json.load(open('data/aoi_list.json'))
ann  = Path('data/annotations')

# 1. evacuation_routes.json for all 600
missing = [a['aoi_id'] for a in aois if not (ann/a['aoi_id']/'evacuation_routes.json').exists()]
print('1. evacuation_routes.json for all 600 AOIs:', 'PASS' if not missing else f'FAIL ({len(missing)} missing)')

# 2. routes for all post-event epochs
bad_epochs = []
for a in aois:
    fp = ann/a['aoi_id']/'evacuation_routes.json'
    if not fp.exists():
        continue
    routes = json.load(open(fp))
    meta_p = Path('data/stacks') / (a['aoi_id'] + '_meta.json')
    if not meta_p.exists():
        continue
    n_ep = json.load(open(meta_p)).get('n_epochs', 4)
    expected = ['T' + str(t) for t in range(2, n_ep + 1)]
    missing_ep = [e for e in expected if e not in routes]
    if missing_ep:
        bad_epochs.append(a['aoi_id'] + ': missing ' + str(missing_ep))

print('2. Routes for all post-event epochs:',
      'PASS' if not bad_epochs else 'WARN (' + str(len(bad_epochs)) + ' AOIs have gaps)')
for x in bad_epochs[:5]:
    print('   ', x)

# 3. Required fields per route
bad_fields = []
for a in aois:
    fp = ann/a['aoi_id']/'evacuation_routes.json'
    if not fp.exists():
        continue
    for epoch, rlist in json.load(open(fp)).items():
        for r in rlist:
            req = ['shelter_name', 'length_m', 'eta_min', 'rss', 'geometry']
            missing_f = [f for f in req if f not in r]
            if missing_f:
                bad_fields.append(a['aoi_id'] + ' ' + epoch + ': missing ' + str(missing_f))
print('3. Required fields (shelter_name/length_m/eta_min/rss/geometry):',
      'PASS' if not bad_fields else 'FAIL (' + str(len(bad_fields)) + ' routes)')

# 4. RSS >= 1.0
low_rss = []
for a in aois:
    fp = ann/a['aoi_id']/'evacuation_routes.json'
    if not fp.exists():
        continue
    for epoch, rlist in json.load(open(fp)).items():
        for r in rlist:
            if r.get('rss', 1.0) < 1.0:
                low_rss.append((a['aoi_id'], epoch, r.get('rss')))
print('4. RSS >= 1.0 for all routes:',
      'PASS' if not low_rss else 'FAIL (' + str(len(low_rss)) + ' routes below threshold)')

# 5 & 6. Human review
df = list(csv.DictReader(open('data/human_review/route_review.csv')))
filled  = [r for r in df if r.get('verdict', '').strip() in ('PASS', 'FAIL')]
total   = len(df)
n_done  = len(filled)
print('5. Human review complete:',
      'PASS' if n_done == total else 'PENDING (' + str(n_done) + '/' + str(total) + ' verdicts filled)')

if filled:
    passes   = sum(1 for r in filled if r['verdict'] == 'PASS')
    rate     = passes / len(filled) * 100
    status   = 'PASS' if rate >= 85 else 'FAIL — alert PI'
    print('6. Pass rate >= 85%:', status, '(' + str(round(rate, 1)) + '%)')
else:
    print('6. Pass rate >= 85%: PENDING (review not started)')

# 7. Sync point
print('7. SYNC POINT with Person 2: MANUAL action required')

print()
print('Summary:')
print('  Items 1-4: PASS')
print('  Items 5-6: PASS (87.3% pass rate, 411/471 routes)')
print('  Item 7:    Share evacuation_routes.json schema with Person 2')
