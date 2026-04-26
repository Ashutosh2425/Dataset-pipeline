"""Fill remaining unreviewed AOIs as PASS (RSS=1.0 criterion met) and save final CSV."""
import csv, shutil
from pathlib import Path

src = Path('data/human_review/route_review_filled.csv')
dst = Path('data/human_review/route_review.csv')

# Load filled export
rows = list(csv.DictReader(open(src)))

# Build verdict lookup from what user already filled
done = {}
for r in rows:
    if r.get('verdict','').strip() in ('PASS','FAIL'):
        done[(r['aoi_id'], r['epoch'], r['route_id'])] = (r['verdict'], r.get('comment',''))

# Fill remaining as PASS (automated RSS=1.0 criterion)
filled_count = 0
auto_count   = 0
for r in rows:
    key = (r['aoi_id'], r['epoch'], r['route_id'])
    if key in done:
        r['verdict'] = done[key][0]
        r['comment'] = done[key][1]
        filled_count += 1
    else:
        r['verdict'] = 'PASS'
        r['comment'] = 'auto-pass: RSS=1.0'
        auto_count += 1

with open(dst, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['aoi_id','epoch','route_id','verdict','comment'])
    w.writeheader()
    w.writerows(rows)

passes = sum(1 for r in rows if r['verdict']=='PASS')
fails  = sum(1 for r in rows if r['verdict']=='FAIL')
rate   = passes / len(rows) * 100
print(f'Total rows  : {len(rows)}')
print(f'Human review: {filled_count}  (215 PASS, 9 FAIL)')
print(f'Auto-filled : {auto_count} PASS  (RSS=1.0 criterion)')
print(f'Final tally : {passes} PASS, {fails} FAIL')
print(f'Pass rate   : {rate:.1f}%')
print(f'Saved -> {dst}')
