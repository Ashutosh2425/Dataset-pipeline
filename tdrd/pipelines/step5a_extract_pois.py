"""
step5a_extract_pois.py
----------------------
Extract population centroids (WorldPop rasters) and shelter POIs (OSM)
for each AOI.

Outputs per AOI (cached — skipped if already present):
  data/annotations/{aoi_id}/population_centroids.json
  data/annotations/{aoi_id}/shelter_pois.json
"""

import json
import time
import threading
import warnings
from pathlib import Path

import numpy as np
import rasterio
from rasterio.mask import mask as raster_mask
import osmnx as ox
from shapely.geometry import box as sbox, mapping

warnings.filterwarnings('ignore')

ox.settings.requests_timeout = 60
ox.settings.overpass_rate_limit = False

WORLDPOP_DIR = Path('data/worldpop')
OSM_CACHE_DIR = Path('data/osm_cache')

_SHELTER_TAGS = {
    'amenity': ['hospital', 'clinic', 'shelter'],
    'emergency': ['shelter', 'assembly_point'],
    'building':  ['hospital', 'school'],
}

_API_LOCK = threading.Lock()
_LAST_CALL = [0.0]


def _worldpop_raster(bbox):
    """Return the first worldpop raster whose bounds overlap bbox, or None."""
    for p in WORLDPOP_DIR.glob('*.tif'):
        with rasterio.open(p) as src:
            b = src.bounds
            if b.left <= bbox[2] and b.right >= bbox[0] and b.bottom <= bbox[3] and b.top >= bbox[1]:
                return p
    return None


def extract_population_centroids(bbox, n_centroids=5):
    """
    Top-N population density centroids within bbox from WorldPop.
    Falls back to bbox centre when no raster covers the AOI.
    """
    raster_path = _worldpop_raster(bbox)
    if raster_path is None:
        lon = (bbox[0] + bbox[2]) / 2
        lat = (bbox[1] + bbox[3]) / 2
        return [{'lat': lat, 'lon': lon, 'pop_density': 0.0, 'source': 'bbox_centre'}]

    try:
        with rasterio.open(raster_path) as src:
            pop_data, pop_transform = raster_mask(src, [mapping(sbox(*bbox))], crop=True)
        pop_data = pop_data[0].astype(float)
        pop_data[pop_data < 0] = 0
    except Exception:
        lon = (bbox[0] + bbox[2]) / 2
        lat = (bbox[1] + bbox[3]) / 2
        return [{'lat': lat, 'lon': lon, 'pop_density': 0.0, 'source': 'bbox_centre'}]

    r_px = max(1, int(500 / 100))  # 500m radius at 100m/px WorldPop resolution
    centroids = []
    pop_flat = pop_data.copy()

    for _ in range(n_centroids):
        if pop_flat.max() < 10:
            break
        row, col = np.unravel_index(np.argmax(pop_flat), pop_flat.shape)
        lon, lat = rasterio.transform.xy(pop_transform, row, col)
        centroids.append({
            'lat': float(lat), 'lon': float(lon),
            'pop_density': float(pop_flat[row, col]),
            'source': raster_path.name,
        })
        r0 = max(0, row - r_px); r1 = min(pop_flat.shape[0], row + r_px)
        c0 = max(0, col - r_px); c1 = min(pop_flat.shape[1], col + r_px)
        pop_flat[r0:r1, c0:c1] = 0

    if not centroids:
        lon = (bbox[0] + bbox[2]) / 2
        lat = (bbox[1] + bbox[3]) / 2
        centroids = [{'lat': lat, 'lon': lon, 'pop_density': 0.0, 'source': 'bbox_centre'}]

    return centroids


def _cache_key(bbox):
    return '_'.join(f'{v:.3f}' for v in bbox)


def _synthetic_shelter_points(bbox, n=5):
    """
    Fallback when OSM has no shelter POIs: pick high-degree road intersections
    from the already-cached GraphML. Never calls Overpass.
    """
    cache_path = OSM_CACHE_DIR / f'{_cache_key(bbox)}.graphml'
    if cache_path.exists():
        try:
            G = ox.load_graphml(cache_path)
            # Prefer nodes spread across the AOI: pick top-n by degree,
            # filtering for spatial diversity (> ~500m apart)
            nodes_by_degree = sorted(G.degree(), key=lambda x: -x[1])
            result = []
            for node_id, _ in nodes_by_degree:
                nd = G.nodes[node_id]
                lat, lon = float(nd['y']), float(nd['x'])
                # Ensure > ~0.005 degrees (~500m) from already-chosen shelters
                too_close = any(
                    abs(lat - s['lat']) < 0.005 and abs(lon - s['lon']) < 0.005
                    for s in result
                )
                if not too_close:
                    result.append({
                        'lat': lat, 'lon': lon,
                        'name': 'synthetic_intersection', 'amenity': 'intersection',
                    })
                if len(result) >= n:
                    break
            if result:
                return result
        except Exception:
            pass
    return _bbox_centre_shelter(bbox)


def _bbox_centre_shelter(bbox):
    return [{
        'lat': (bbox[1] + bbox[3]) / 2,
        'lon': (bbox[0] + bbox[2]) / 2,
        'name': 'bbox_centre', 'amenity': 'fallback',
    }]


def extract_shelters(bbox):
    """
    Fetch shelter POIs from OSM with polite pacing. Falls back to synthetic
    intersection points when OSM returns nothing.
    """
    with _API_LOCK:
        elapsed = time.time() - _LAST_CALL[0]
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        _LAST_CALL[0] = time.time()

    try:
        gdf = ox.features_from_bbox(
            north=bbox[3], south=bbox[1], east=bbox[2], west=bbox[0],
            tags=_SHELTER_TAGS
        )
        if len(gdf) == 0:
            return _synthetic_shelter_points(bbox)
        gdf = gdf.copy()
        gdf['geometry'] = gdf.geometry.centroid
        result = []
        for row in gdf.itertuples():
            result.append({
                'lat': float(row.geometry.y), 'lon': float(row.geometry.x),
                'name': str(getattr(row, 'name', 'unknown')),
                'amenity': str(getattr(row, 'amenity', 'unknown')),
            })
        return result
    except Exception:
        return _synthetic_shelter_points(bbox)


def extract_and_cache(aoi_id, bbox, out_dir):
    """
    Run extraction for one AOI. Writes centroids + shelters JSON files.
    Skips if both already exist.
    """
    out_dir = Path(out_dir)
    c_path = out_dir / 'population_centroids.json'
    s_path = out_dir / 'shelter_pois.json'

    if c_path.exists() and s_path.exists():
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    if not c_path.exists():
        centroids = extract_population_centroids(bbox)
        c_path.write_text(json.dumps(centroids, indent=2))

    if not s_path.exists():
        shelters = extract_shelters(bbox)
        s_path.write_text(json.dumps(shelters, indent=2))
