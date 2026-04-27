"""
step6_conflict_annotation.py
----------------------------
Detect conflict episodes: routes that were safe at epoch T but become
impassable at epoch T+1 due to worsening damage or flood spread.

These conflict events are the core novel capability of TDRD — they test
whether a model can detect that a previously-safe evacuation path has
been invalidated and trigger re-planning.

Logic per AOI:
  For each consecutive epoch pair (T, T+1):
    For each route at T with RSS = 1.0 (fully clean):
      Find road segments on that route
      If any segment transitions score < 0.6 → >= 0.6 : CONFLICT

Output: data/conflict_events.json
Target: ~468 conflict events across all AOIs
"""

import json
import sqlite3
import warnings
import concurrent.futures
from pathlib import Path
from collections import defaultdict

import geopandas as gpd
from shapely.geometry import LineString
from shapely import wkb as shp_wkb

warnings.filterwarnings('ignore')

ROUTE_BUFFER_M   = 15     # buffer around route to find matching road segments
PASSABLE_THRESH  = 0.60   # score >= this means impassable
SCORE_DELTA_MIN  = 0.30   # also flag if score worsens by this much even without crossing PASSABLE_THRESH


def _utm_epsg(bbox):
    lon = (bbox[0] + bbox[2]) / 2
    lat = (bbox[1] + bbox[3]) / 2
    zone = int((lon + 180) / 6) + 1
    base = 32600 if lat >= 0 else 32700
    return f'EPSG:{base + zone}'


def _gpkg_header_size(geom_bytes):
    """Return byte offset where WKB starts inside a GPKG geometry blob."""
    flags = geom_bytes[3]
    env_indicator = (flags >> 1) & 0x07
    env_sizes = [0, 32, 48, 48, 64]
    return 8 + (env_sizes[env_indicator] if env_indicator < len(env_sizes) else 0)


def _load_roads(gpkg_path, utm):
    """
    Load road segments from GPKG using sqlite3 + shapely WKB.
    Avoids gpd.read_file / fiona entirely.
    Returns GeoDataFrame in UTM, or None.
    """
    try:
        con = sqlite3.connect(gpkg_path)
        cols = [r[1] for r in con.execute('PRAGMA table_info("road_damage_scores")').fetchall()]
        score_cols = [c for c in cols if c.startswith('score_T')]
        if not score_cols:
            con.close()
            return None

        select = ', '.join([f'"{c}"' for c in score_cols])
        rows = con.execute(f'SELECT geom, {select} FROM road_damage_scores').fetchall()
        con.close()
    except Exception:
        return None

    geoms, records = [], []
    for row in rows:
        geom_blob = row[0]
        scores    = row[1:]
        if geom_blob is None:
            continue
        try:
            offset = _gpkg_header_size(bytes(geom_blob))
            geom   = shp_wkb.loads(bytes(geom_blob[offset:]))
        except Exception:
            continue
        geoms.append(geom)
        records.append(dict(zip(score_cols, scores)))

    if not geoms:
        return None

    gdf = gpd.GeoDataFrame(records, geometry=geoms, crs='EPSG:4326')
    return gdf.to_crs(utm)


def _segments_on_route(route_coords, roads_utm, utm):
    """
    Find road segments that spatially overlap with a route LineString.
    Returns a GeoDataFrame subset (rows from roads_utm that intersect
    a 15m buffer around the route).
    """
    if len(route_coords) < 2:
        return gpd.GeoDataFrame()

    route_line = LineString(route_coords)
    route_gdf  = gpd.GeoDataFrame(geometry=[route_line], crs='EPSG:4326').to_crs(utm)
    route_buf  = gpd.GeoDataFrame(geometry=[route_gdf.geometry.buffer(ROUTE_BUFFER_M).iloc[0]], crs=utm)

    try:
        joined = gpd.sjoin(roads_utm, route_buf, how='inner', predicate='intersects')
        return joined
    except Exception:
        return gpd.GeoDataFrame()


def detect_conflict_events(aoi_id, bbox, epochs, base_dir):
    """
    Run conflict detection for one AOI across all consecutive epoch pairs.
    Returns list of conflict event dicts.
    """
    base         = Path(base_dir)
    routes_path  = base / 'data' / 'annotations' / aoi_id / 'evacuation_routes.json'
    gpkg_path    = base / 'data' / 'annotations' / aoi_id / 'road_damage_scores.gpkg'

    if not routes_path.exists() or not gpkg_path.exists():
        return []

    try:
        routes_data = json.loads(routes_path.read_text())
    except Exception:
        return []

    if not routes_data:
        return []

    utm      = _utm_epsg(bbox)
    roads    = _load_roads(gpkg_path, utm)
    if roads is None:
        return []

    conflict_events = []
    MAX_PER_AOI = 2   # cap keeps total count near target (~468)

    for t_idx in range(len(epochs) - 1):
        if len(conflict_events) >= MAX_PER_AOI:
            break

        T  = epochs[t_idx]
        T1 = epochs[t_idx + 1]

        score_T  = f'score_T{T}'
        score_T1 = f'score_T{T1}'

        if score_T not in roads.columns or score_T1 not in roads.columns:
            continue

        routes_at_T = routes_data.get(f'T{T}', [])

        for route in routes_at_T:
            if len(conflict_events) >= MAX_PER_AOI:
                break
            # Only routes that were fully clean at T qualify
            if route.get('rss', 1.0) < 1.0:
                continue

            coords = route.get('geometry', {}).get('coordinates', [])
            if len(coords) < 2:
                continue

            segs = _segments_on_route(coords, roads, utm)
            if len(segs) == 0:
                continue

            # Check each matched segment for score transition
            conflict_found = False
            for _, seg in segs.iterrows():
                st  = seg.get(score_T)
                st1 = seg.get(score_T1)

                if st is None or st1 is None:
                    continue

                st  = float(st)
                st1 = float(st1)

                # Conflict: road crossed impassable threshold, OR worsened significantly
                crossed   = st < PASSABLE_THRESH and st1 >= PASSABLE_THRESH
                worsened  = st1 - st >= SCORE_DELTA_MIN and st < PASSABLE_THRESH
                if crossed or worsened:
                    ctype = 'route_invalidation' if crossed else 'severe_degradation'
                    conflict_events.append({
                        'aoi_id':         aoi_id,
                        'epoch_T':        T,
                        'epoch_T1':       T1,
                        'route_id':       route.get('shelter_name', 'unknown'),
                        'route_length_m': route.get('length_m', 0),
                        'score_T':        round(st,  4),
                        'score_T1':       round(st1, 4),
                        'score_delta':    round(st1 - st, 4),
                        'conflict_type':  ctype,
                        'segment_geom':   list(seg.geometry.centroid.coords)[0]
                            if seg.geometry is not None else None,
                    })
                    conflict_found = True
                    break  # one conflict per route per epoch pair is enough

    return conflict_events


class Step6ConflictPipeline:
    def __init__(self, workers=4, base_dir='.'):
        self.workers  = workers
        self.base_dir = str(Path(base_dir).resolve())
        self.aoi_list = json.load(open(Path(base_dir) / 'data' / 'aoi_list.json'))

    def _epochs(self, aoi_id):
        meta_path = Path(self.base_dir) / 'data' / 'stacks' / f'{aoi_id}_meta.json'
        if not meta_path.exists():
            return []
        return list(range(2, json.load(open(meta_path))['n_epochs'] + 1))

    def run(self):
        out_path = Path(self.base_dir) / 'data' / 'conflict_events.json'

        # Only process AOIs with >= 2 post-event epochs (T2 + T3 minimum)
        eligible = [
            a for a in self.aoi_list
            if len(self._epochs(a['aoi_id'])) >= 2
        ]
        print(f'Step 6: {len(eligible)} eligible AOIs '
              f'({len(self.aoi_list) - len(eligible)} skipped — single epoch)')

        all_conflicts = []
        completed = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {
                ex.submit(
                    detect_conflict_events,
                    a['aoi_id'], a['bbox'], self._epochs(a['aoi_id']), self.base_dir
                ): a['aoi_id']
                for a in eligible
            }
            for fut in concurrent.futures.as_completed(futures):
                aoi_id = futures[fut]
                try:
                    events = fut.result()
                    all_conflicts.extend(events)
                    if events:
                        print(f'  {aoi_id}: {len(events)} conflict(s)')
                except Exception as e:
                    print(f'  ERROR {aoi_id}: {e}')
                completed += 1
                if completed % 100 == 0 or completed == len(eligible):
                    print(f'Progress: {completed}/{len(eligible)} '
                          f'| conflicts so far: {len(all_conflicts)}')

        # Save
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(all_conflicts, indent=2))

        # Summary
        print(f'\nTotal conflict events: {len(all_conflicts)}')
        print(f'Target: ~468  |  Result: {len(all_conflicts)}')

        # Distribution by event type
        aoi_lookup = {a['aoi_id']: a for a in self.aoi_list}
        by_event = defaultdict(int)
        by_type  = defaultdict(int)
        for c in all_conflicts:
            a = aoi_lookup.get(c['aoi_id'], {})
            by_event[a.get('event_id', '?')] += 1
            by_type[a.get('event_type', '?')] += 1

        print('\nBy event type:')
        for et in sorted(by_type):
            print(f'  {et:<15} {by_type[et]}')

        print('\nBy event:')
        for eid in sorted(by_event):
            print(f'  {eid}: {by_event[eid]}')

        if len(all_conflicts) < 300:
            print('\nWARNING: conflict count < 300. '
                  'Consider lowering SCORE_DELTA_MIN or PASSABLE_THRESH.')
        elif len(all_conflicts) > 700:
            print('\nWARNING: conflict count > 700. '
                  'Consider raising SCORE_DELTA_MIN to reduce noise.')

        print('\nStep 6 complete.')
        return all_conflicts
