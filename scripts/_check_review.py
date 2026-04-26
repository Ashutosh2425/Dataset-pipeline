import csv
from pathlib import Path

rows   = list(csv.DictReader(open('data/human_review/route_review_filled.csv')))
filled = [r for r in rows if r.get('verdict','').strip() in ('PASS','FAIL')]
passes = [r for r in filled if r['verdict']=='PASS']
fails  = [r for r in filled if r['verdict']=='FAIL']

aois_reviewed  = set(r['aoi_id'] for r in filled)
all_aois       = set(r['aoi_id'] for r in rows)
unreviewed     = sorted(all_aois - aois_reviewed)

print('=== HUMAN REVIEW RESULTS ===')
print(f'Total rows      : {len(rows)}')
print(f'Filled verdicts : {len(filled)}  ({len(passes)} PASS, {len(fails)} FAIL)')
print(f'AOIs reviewed   : {len(aois_reviewed)} / 60')
print(f'AOIs unreviewed : {len(unreviewed)}')
print(f'Pass rate       : {len(passes)/len(filled)*100:.1f}%  (threshold: 85%)')

if fails:
    print(f'\nFAIL verdicts:')
    for r in fails:
        comment = r.get('comment','').strip()[:60]
        print(f'  {r["aoi_id"]:15s} {r["epoch"]} route {r["route_id"]}  {comment}')

if unreviewed:
    print(f'\nUnreviewed AOIs:')
    for a in unreviewed:
        print(f'  {a}')

print('\n=== CHECKLIST STATUS ===')
rate = len(passes)/len(filled)*100 if filled else 0
print(f'[ {"X" if len(aois_reviewed)==60 else " "} ] 60 AOIs reviewed ({len(aois_reviewed)}/60)')
print(f'[ {"X" if rate>=85 else " "} ] Pass rate >= 85% ({rate:.1f}%)')
print(f'[ {"X" if len(fails)==0 else " "} ] Zero FAILs ({"0" if not fails else str(len(fails))+" FAILs need step5b re-run"})')
