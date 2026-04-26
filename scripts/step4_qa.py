"""
step4_qa.py
-----------
QA checks for Step 4 road damage scoring.
Prints per-event-type mean scores and flags missing/malformed outputs.
"""

import json
from pathlib import Path
from collections import defaultdict

import geopandas as gpd

AOI_LIST   = json.load(open('data/aoi_list.json'))
AOI_LOOKUP = {a['aoi_id']: a for a in AOI_LIST}

missing        = []
no_roads       = []
bad_columns    = []
event_scores   = defaultdict(list)   # event_type -> [mean_score]
event_statuses = defaultdict(lambda: defaultdict(int))  # event_type -> status -> count

for aoi in AOI_LIST:
    aoi_id     = aoi['aoi_id']
    event_type = aoi.get('event_type', 'unknown')
    gpkg_path  = Path(f'data/annotations/{aoi_id}/road_damage_scores.gpkg')

    if not gpkg_path.exists():
        missing.append(aoi_id)
        continue

    try:
        import sqlite3
        con = sqlite3.connect(gpkg_path)
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        # Pick the table that has score_T columns
        tbl = None
        for t in tables:
            if t.startswith('gpkg'):
                continue
            t_cols = [r[1] for r in con.execute(f'PRAGMA table_info("{t}")').fetchall()]
            if any(c.startswith('score_T') for c in t_cols):
                tbl = t
                break
        if tbl is None:
            no_roads.append(aoi_id)
            con.close()
            continue
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{tbl}")').fetchall()]
        row_count = con.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
        score_cols_raw  = [c for c in cols if c.startswith('score_T')]
        status_cols_raw = [c for c in cols if c.startswith('status_T')]
        # Read scores directly from SQLite
        scores_data, statuses_data = {}, {}
        for sc in score_cols_raw:
            vals = [r[0] for r in con.execute(f'SELECT "{sc}" FROM "{tbl}"').fetchall() if r[0] is not None]
            scores_data[sc] = vals
        for stc in status_cols_raw:
            vals = [r[0] for r in con.execute(f'SELECT "{stc}" FROM "{tbl}"').fetchall() if r[0] is not None]
            statuses_data[stc] = vals
        con.close()
        score_cols  = score_cols_raw
        status_cols = status_cols_raw
        # Fake gdf interface for rest of loop
        class _GDF:
            def __init__(self): self.columns = cols
            def __getitem__(self, k):
                class _Series:
                    def __init__(self, v): self._v = v
                    def mean(self): return sum(self._v)/len(self._v) if self._v else 0
                    def value_counts(self):
                        from collections import Counter
                        return Counter(self._v).items()
                return _Series(scores_data.get(k, statuses_data.get(k, [])))
        gdf = _GDF()
    except Exception as e:
        bad_columns.append((aoi_id, str(e)))
        continue

    score_cols  = [c for c in gdf.columns if c.startswith('score_T')]
    status_cols = [c for c in gdf.columns if c.startswith('status_T')]

    if not score_cols or not status_cols:
        bad_columns.append((aoi_id, f'missing score/status cols — found: {list(gdf.columns)}'))
        continue

    # Aggregate scores for this AOI
    for sc in score_cols:
        event_scores[event_type].append(gdf[sc].mean())

    # Aggregate status counts
    for stc in status_cols:
        for status, cnt in gdf[stc].value_counts():
            event_statuses[event_type][status] += int(cnt)

SEP = '=' * 60
print(SEP)
print(f'  step4_qa.py  —  {len(AOI_LIST)} AOIs checked')
print(SEP)
ok_count = len(AOI_LIST) - len(missing) - len(no_roads) - len(bad_columns)
print(f'  Missing GPKGs    : {len(missing)}')
print(f'  No OSM roads     : {len(no_roads)}  (rural/unmapped — expected)')
print(f'  Bad columns/read : {len(bad_columns)}')
print(f'  OK               : {ok_count}')
print()

if missing:
    print(f'  First 10 missing : {missing[:10]}')
    print()

if bad_columns:
    print(f'  Bad files:')
    for aoi_id, err in bad_columns[:5]:
        print(f'    {aoi_id}: {err}')
    print()

print('  Mean score by event type (flood should be highest):')
for et in sorted(event_scores):
    scores = event_scores[et]
    mean   = sum(scores) / len(scores) if scores else 0
    sts    = event_statuses[et]
    total  = sum(sts.values())
    pct_imp = 100 * sts.get('impassable', 0) / total if total else 0
    pct_deg = 100 * sts.get('degraded', 0)   / total if total else 0
    print(f'    {et:<15} mean={mean:.3f}  impassable={pct_imp:.1f}%  degraded={pct_deg:.1f}%')

print()
if missing or bad_columns:
    print(f'  FAIL — {len(missing)} missing + {len(bad_columns)} bad files')
else:
    print(f'  PASS — {ok_count} scored + {len(no_roads)} no-road AOIs = {len(AOI_LIST)}/{len(AOI_LIST)}')
print(SEP)
