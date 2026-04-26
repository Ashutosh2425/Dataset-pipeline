"""
step4_road_damage_scoring.py
----------------------------
Score every OSM road segment in each AOI with a damage score [0, 1] per
post-event epoch, derived from Step 3 damage polygons + flood extents.

Key optimisations vs naive approach:
  - Vectorised scoring with gpd.sjoin (replaces per-row iterrows loop)
  - OSM network cache to data/osm_cache/ (GraphML) — avoids re-querying
    Overpass for adjacent AOIs that share road networks
  - 2 workers, serialised API calls via threading.Semaphore

Output: data/annotations/{aoi_id}/road_damage_scores.gpkg
"""

import json
import time
import threading
import warnings
from pathlib import Path
import concurrent.futures

import geopandas as gpd
import osmnx as ox
from shapely.geometry import shape
from shapely.ops import unary_union

warnings.filterwarnings('ignore')

ox.settings.requests_timeout = 180
ox.settings.overpass_rate_limit = False   # we handle pacing ourselves

# Per-event-type damage weights: fire rarely blocks roads; flood/structural cause most blockage.
# Class 1 → 0.5 so it lands in the "degraded" zone (0.40–0.60).
EVENT_DAMAGE_WEIGHTS = {
    'fire':       {0: 0.0, 1: 0.2, 2: 0.4, 3: 0.55},   # smoke/ash, rarely impassable
    'wind':       {0: 0.0, 1: 0.3, 2: 0.55, 3: 0.75},   # debris, moderate blockage
    'flood':      {0: 0.0, 1: 0.5, 2: 0.75, 3: 1.0},
    'structural': {0: 0.0, 1: 0.5, 2: 0.75, 3: 1.0},
    'wind+flood': {0: 0.0, 1: 0.5, 2: 0.75, 3: 1.0},
}
DEFAULT_DAMAGE_WEIGHTS = {0: 0.0, 1: 0.5, 2: 0.75, 3: 1.0}
FLOOD_SCORE            = 0.85
BUFFER_METERS          = 15

# Only 2 simultaneous Overpass connections allowed; serialise with semaphore
_API_SEM = threading.Semaphore(2)
_API_LOCK = threading.Lock()
_LAST_API_CALL = [0.0]   # track last call time for polite pacing


def _utm_epsg(bbox):
    lon = (bbox[0] + bbox[2]) / 2
    lat = (bbox[1] + bbox[3]) / 2
    zone = int((lon + 180) / 6) + 1
    base = 32600 if lat >= 0 else 32700
    return f'EPSG:{base + zone}'


def _cache_key(bbox):
    """Round bbox to 3 decimal places (~100m) for cache key."""
    return '_'.join(f'{v:.3f}' for v in bbox)


def _fetch_osm(bbox, cache_dir):
    """Download OSM drive network, caching to GraphML. Thread-safe."""
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    key  = _cache_key(bbox)
    path = cache_dir / f'{key}.graphml'

    if path.exists():
        return ox.load_graphml(path)

    with _API_SEM:
        # Polite 1s gap between API calls
        with _API_LOCK:
            elapsed = time.time() - _LAST_API_CALL[0]
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

        for attempt in range(4):
            try:
                G = ox.graph_from_bbox(
                    north=bbox[3], south=bbox[1], east=bbox[2], west=bbox[0],
                    network_type='drive'
                )
                with _API_LOCK:
                    _LAST_API_CALL[0] = time.time()
                ox.save_graphml(G, path)
                return G
            except Exception as e:
                wait = 5 * (2 ** attempt)
                print(f'    Overpass retry (attempt {attempt+1}/4, wait {wait}s): {e}')
                time.sleep(wait)

    return None


def _read_geojson(path, target_crs):
    with open(path) as f:
        data = json.load(f)
    features = [ft for ft in data.get('features', []) if ft.get('geometry')]
    if not features:
        return gpd.GeoDataFrame(geometry=[], crs='EPSG:4326').to_crs(target_crs)
    geoms = [shape(ft['geometry']) for ft in features]
    props = [ft.get('properties', {}) for ft in features]
    gdf = gpd.GeoDataFrame(props, geometry=geoms, crs='EPSG:4326')
    return gdf.to_crs(target_crs)


def _score_vectorised(roads_utm, dmg_gdf, flood_geom, event_type=None):
    """
    Vectorised road scoring using spatial join.
    Returns (scores list, statuses list) aligned to roads_utm index.

    Damage polygons from spectral NDVI change (source='spectral') are pixel-level
    raster blobs that cover vast areas and would score almost every road as impassable.
    Only xBD-sourced building-footprint polygons (source='xbd') are used for the
    building-damage component; spectral-source polygons are skipped here because the
    flood-extent component (flood_geom) handles road impact for flood/fire events.
    """
    weights = EVENT_DAMAGE_WEIGHTS.get(event_type, DEFAULT_DAMAGE_WEIGHTS)

    roads_buf = roads_utm.copy()
    roads_buf['geometry'] = roads_utm['_buf']
    roads_buf = roads_buf.reset_index(drop=True)

    # Damage score: only use xBD building footprints, not spectral pixel blobs
    damage_scores = {}
    if len(dmg_gdf) > 0 and 'damage_class' in dmg_gdf.columns:
        if 'source' in dmg_gdf.columns:
            dmg_clean = dmg_gdf[dmg_gdf['source'] == 'xbd'].copy()
        else:
            dmg_clean = dmg_gdf.copy()
        dmg_clean = dmg_clean[dmg_clean.geometry.notna() & ~dmg_clean.geometry.is_empty]
        if len(dmg_clean) > 0:
            joined = gpd.sjoin(roads_buf[['geometry']].reset_index(),
                               dmg_clean[['geometry', 'damage_class']],
                               how='left', predicate='intersects')
            joined['weight'] = joined['damage_class'].map(
                lambda dc: weights.get(int(dc), 0.0) if dc == dc else 0.0
            )
            damage_scores = joined.groupby('index')['weight'].max().to_dict()

    # Flood score: binary flag via spatial join
    flood_scores = {}
    if flood_geom is not None and not flood_geom.is_empty:
        flood_gdf_tmp = gpd.GeoDataFrame(geometry=[flood_geom], crs=roads_utm.crs)
        joined_f = gpd.sjoin(roads_buf[['geometry']].reset_index(),
                             flood_gdf_tmp,
                             how='left', predicate='intersects')
        flood_scores = {
            idx: FLOOD_SCORE
            for idx in joined_f[joined_f['index_right'].notna()]['index']
        }

    scores, statuses = [], []
    for i in range(len(roads_utm)):
        ds = damage_scores.get(i, 0.0)
        fs = flood_scores.get(i, 0.0)
        final = max(ds, fs)
        status = 'passable' if final < 0.40 else ('degraded' if final < 0.60 else 'impassable')
        scores.append(final)
        statuses.append(status)

    return scores, statuses


def score_aoi(aoi_id, bbox, epochs, base_dir, cache_dir, event_type=None):
    base = Path(base_dir)
    out_path = base / 'data' / 'annotations' / aoi_id / 'road_damage_scores.gpkg'

    G = _fetch_osm(bbox, cache_dir)
    if G is None:
        # Write empty GPKG so this AOI is not re-attempted and QA can count it
        empty = gpd.GeoDataFrame({'geometry': [], 'note': []}, crs='EPSG:4326')
        out_path.parent.mkdir(parents=True, exist_ok=True)
        empty.to_file(out_path, driver='GPKG')
        print(f'  NO-ROADS {aoi_id}: no OSM drive network, empty GPKG written')
        return str(out_path)

    roads = ox.graph_to_gdfs(G, nodes=False).to_crs('EPSG:4326')
    keep  = ['geometry', 'osmid', 'name', 'highway', 'length', 'oneway', 'maxspeed']
    roads = roads[[c for c in keep if c in roads.columns]].copy()
    roads = roads.reset_index(drop=True)

    utm = _utm_epsg(bbox)
    roads_utm = roads.to_crs(utm).copy()
    roads_utm['_buf'] = roads_utm.geometry.buffer(BUFFER_METERS)

    for ep in epochs:
        dmg_path = base / 'data' / 'annotations' / aoi_id / f'damage_polygons_T{ep}.geojson'
        fld_path = base / 'data' / 'annotations' / aoi_id / f'flood_extent_T{ep}.geojson'

        dmg_gdf = _read_geojson(dmg_path, utm) if dmg_path.exists() \
                  else gpd.GeoDataFrame(geometry=[], crs=utm)

        flood_geom = None
        if fld_path.exists():
            fld_gdf = _read_geojson(fld_path, utm)
            if len(fld_gdf) > 0:
                flood_geom = unary_union(fld_gdf.geometry)

        scores, statuses = _score_vectorised(roads_utm, dmg_gdf, flood_geom, event_type)
        roads[f'score_T{ep}']  = scores
        roads[f'status_T{ep}'] = statuses

    out_path.parent.mkdir(parents=True, exist_ok=True)
    roads.to_file(out_path, driver='GPKG')
    print(f'  OK {aoi_id}: {len(roads)} roads, epochs {epochs}')
    return str(out_path)


class Step4RoadDamagePipeline:
    def __init__(self, workers=2, base_dir='.'):
        self.workers   = workers
        self.base_dir  = str(Path(base_dir).resolve())
        self.cache_dir = str(Path(base_dir) / 'data' / 'osm_cache')
        self.aoi_list  = json.load(open(Path(base_dir) / 'data' / 'aoi_list.json'))

    def _epochs(self, aoi_id):
        meta_path = Path(self.base_dir) / 'data' / 'stacks' / f'{aoi_id}_meta.json'
        if not meta_path.exists():
            return []
        return list(range(2, json.load(open(meta_path))['n_epochs'] + 1))

    def run(self, force=False):
        if force:
            pending = self.aoi_list
        else:
            pending = [
                a for a in self.aoi_list
                if not (Path(self.base_dir) / 'data' / 'annotations' / a['aoi_id'] / 'road_damage_scores.gpkg').exists()
            ]
        done_count = len(self.aoi_list) - len(pending)
        print(f'Step 4: {len(pending)} AOIs to score ({done_count} already done)')

        completed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {
                ex.submit(
                    score_aoi,
                    a['aoi_id'], a['bbox'], self._epochs(a['aoi_id']),
                    self.base_dir, self.cache_dir, a.get('event_type')
                ): a['aoi_id']
                for a in pending
            }
            for fut in concurrent.futures.as_completed(futures):
                aoi_id = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    print(f'  ERROR {aoi_id}: {e}')
                completed += 1
                if completed % 50 == 0 or completed == len(pending):
                    print(f'Progress: {completed + done_count}/{len(self.aoi_list)}')

        print('Step 4 complete.')
