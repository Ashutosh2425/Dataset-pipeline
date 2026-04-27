"""
Step 3a — Building Damage Annotation Transfer
Transfers xBD labels to AOIs with overlap; uses spectral change (NDVI/NBR delta)
as fallback for AOIs without xBD coverage.

xBD folder names are discovered automatically from data/xbd/ — no hardcoding.
Event-to-folder matching uses keyword search on the event config name.

Output: data/annotations/{aoi_id}/damage_polygons_T{n}.geojson
"""

import json
import numpy as np
from pathlib import Path

import geopandas as gpd
import rasterio
import shapely
from shapely.geometry import box, shape, mapping
from shapely.ops import unary_union
import rasterio.features

AOI_LIST_PATH = Path("data/aoi_list.json")
STACKS_DIR    = Path("data/stacks")
XBD_DIR       = Path("data/xbd")
ANNOTATIONS   = Path("data/annotations")

# Keywords used to match event config name to an xBD folder name.
# Values are ordered by specificity — first match wins.
_EVENT_KEYWORDS = {
    'EVT001': ['hurricane-harvey', 'harvey'],
    'EVT003': ['santa-rosa-wildfire', 'santa-rosa'],
    'EVT005': ['midwest-flooding', 'midwest'],
    'EVT008': ['hurricane-michael', 'michael'],
    'EVT009': ['nepal-flooding', 'nepal'],
    'EVT010': ['mexico-earthquake', 'mexico'],
    'EVT012': ['hurricane-florence', 'florence'],
}

DAMAGE_MAP = {
    'no-damage':     0,
    'minor-damage':  1,
    'major-damage':  2,
    'destroyed':     3,
    'un-classified': 0,
}


def discover_xbd_event_map():
    """
    Scan data/xbd/ and return {event_id: [folder, ...]} mapping every event
    to ALL matching xBD subfolders (one event can span multiple disaster folders).
    """
    if not XBD_DIR.exists():
        return {}

    available = [
        d.name for d in XBD_DIR.iterdir()
        if d.is_dir() and (d / 'labels').exists()
    ]
    if not available:
        return {}

    result = {}
    for event_id, keywords in _EVENT_KEYWORDS.items():
        matches = [f for f in available
                   if any(kw in f.lower() for kw in keywords)]
        if matches:
            result[event_id] = matches

    return result


# ── xBD helpers ───────────────────────────────────────────────────────────────

def load_xbd_polygons(aoi_bbox, event_id, xbd_map):
    folders = xbd_map.get(event_id)  # now a list of folder names
    if not folders:
        return None

    from shapely import wkt as shapely_wkt
    aoi_geom = box(*aoi_bbox)
    rows = []

    for folder in folders:
        label_dir = XBD_DIR / folder / 'labels'
        if not label_dir.exists():
            continue

        for label_file in label_dir.glob('*_post_disaster.json'):
            try:
                with open(label_file) as f:
                    data = json.load(f)
            except Exception:
                continue

            for feat in data.get('features', {}).get('lng_lat', []):
                props = feat.get('properties', {})
                damage = DAMAGE_MAP.get(props.get('subtype', 'no-damage'), 0)
                wkt = feat.get('wkt')
                if not wkt:
                    continue
                try:
                    geom = shapely_wkt.loads(wkt)
                    if geom.intersects(aoi_geom):
                        rows.append({'geometry': geom, 'damage_class': damage,
                                     'source': 'xbd'})
                except Exception:
                    continue

    if not rows:
        return None

    gdf = gpd.GeoDataFrame(rows, crs='EPSG:4326')
    return gdf[gdf.intersects(aoi_geom)].reset_index(drop=True)


def scale_damage_for_epoch(gdf, epoch_idx, n_epochs):
    out = gdf.copy()
    if epoch_idx == 1:
        out['damage_class'] = out['damage_class'].clip(0, 2)
    elif epoch_idx == n_epochs - 1:
        out['damage_class'] = (out['damage_class'] * 0.7).astype(int)
    return out


# ── Spectral change fallback ───────────────────────────────────────────────────

def _read_band(stack_path, band_idx):
    with rasterio.open(stack_path) as src:
        return src.read(band_idx).astype(np.float32), src.transform, src.crs, src.meta


def _ndvi(red, nir):
    denom = nir + red
    denom = np.where(denom == 0, 1e-6, denom)
    return (nir - red) / denom


def spectral_change_damage(aoi_id, meta, stack_path):
    """
    Computes per-pixel damage class from NDVI change between pre-event (T0)
    and each post-event epoch. Returns a list of GeoDataFrames, one per epoch.

    Band layout per epoch (S2 only, 4 bands): B02, B03, B04, B08
    Band indices in stack (1-based): epoch * 4 - 3 to epoch * 4
    """
    n_ep   = meta['n_epochs']
    bpe    = meta['bands_per_epoch']
    results = []

    if not Path(stack_path).exists():
        return [None] * (n_ep - 1)

    pre_start    = 1
    pre_red_idx  = pre_start + 2   # B04
    pre_nir_idx  = pre_start + 3   # B08

    try:
        pre_red, transform, crs, _ = _read_band(stack_path, pre_red_idx)
        pre_nir, _, _, _            = _read_band(stack_path, pre_nir_idx)
        pre_ndvi = _ndvi(pre_red, pre_nir)
    except Exception:
        return [None] * (n_ep - 1)

    for t in range(1, n_ep):
        ep_start = sum(bpe[:t]) + 1   # 1-based

        try:
            post_red, _, _, _ = _read_band(stack_path, ep_start + 2)
            post_nir, _, _, _ = _read_band(stack_path, ep_start + 3)
            post_ndvi = _ndvi(post_red, post_nir)

            delta = pre_ndvi - post_ndvi   # positive = vegetation loss

            damage_raster = np.zeros_like(delta, dtype=np.uint8)
            damage_raster[delta > 0.1]  = 1
            damage_raster[delta > 0.25] = 2
            damage_raster[delta > 0.4]  = 3

            rows = []
            for geom_dict, val in rasterio.features.shapes(
                damage_raster, mask=(damage_raster > 0).astype(np.uint8),
                transform=transform
            ):
                rows.append({'geometry': shape(geom_dict),
                             'damage_class': int(val),
                             'source': 'spectral'})

            if rows:
                gdf = gpd.GeoDataFrame(rows, crs=crs).to_crs('EPSG:4326')
                results.append(gdf)
            else:
                results.append(None)

        except Exception as e:
            print(f"  [{aoi_id}] Spectral change failed epoch {t}: {e}")
            results.append(None)

    return results


# ── Main pipeline ─────────────────────────────────────────────────────────────

class Step3aDamagePipeline:

    def __init__(self):
        with open(AOI_LIST_PATH) as f:
            self.aois = json.load(f)

    def run(self, force_xbd=False):
        """
        force_xbd: if True, delete existing damage polygons for any AOI whose
                   event now has xBD coverage and re-annotate them from xBD.
        """
        xbd_map = discover_xbd_event_map()
        if xbd_map:
            print(f"[xBD] Found coverage for events: {sorted(xbd_map.keys())}")
            for eid, folders in sorted(xbd_map.items()):
                for folder in folders:
                    print(f"  {eid} -> data/xbd/{folder}/labels/")
        else:
            print("[xBD] data/xbd/ not found or empty — spectral fallback for all AOIs")

        xbd_events = set(xbd_map.keys())

        if force_xbd and xbd_events:
            print(f"\n[force-xbd] Clearing existing damage polygons for {len(xbd_events)} xBD events...")
            cleared = 0
            for aoi in self.aois:
                if aoi['event_id'] not in xbd_events:
                    continue
                aoi_id = aoi['aoi_id']
                meta_path = STACKS_DIR / f"{aoi_id}_meta.json"
                if not meta_path.exists():
                    continue
                meta = json.load(open(meta_path))
                for t in range(1, meta['n_epochs']):
                    p = ANNOTATIONS / aoi_id / f"damage_polygons_T{t+1}.geojson"
                    if p.exists():
                        try:
                            p.unlink()
                            cleared += 1
                        except PermissionError:
                            pass   # file locked by another process; skip
            print(f"  Cleared {cleared} files — will re-annotate from xBD.")

        print(f"\nStep 3a: Damage annotation for {len(self.aois)} AOIs...")
        xbd_count = spectral_count = skipped = 0

        for aoi in self.aois:
            aoi_id    = aoi['aoi_id']
            event_id  = aoi['event_id']
            meta_path = STACKS_DIR / f"{aoi_id}_meta.json"
            stack_path = STACKS_DIR / f"{aoi_id}_stack.tif"
            out_dir   = ANNOTATIONS / aoi_id
            out_dir.mkdir(parents=True, exist_ok=True)

            if not meta_path.exists():
                skipped += 1
                continue

            with open(meta_path) as f:
                meta = json.load(f)

            n_epochs = meta['n_epochs']

            all_done = all(
                (out_dir / f"damage_polygons_T{t+1}.geojson").exists()
                for t in range(1, n_epochs)
            )
            if all_done:
                skipped += 1
                continue

            # Try xBD if this event has a discovered folder
            xbd_gdf = None
            if event_id in xbd_events:
                xbd_gdf = load_xbd_polygons(aoi['bbox'], event_id, xbd_map)

            if xbd_gdf is not None and len(xbd_gdf) > 0:
                for t in range(1, n_epochs):
                    out_path = out_dir / f"damage_polygons_T{t+1}.geojson"
                    if out_path.exists():
                        continue
                    epoch_gdf = scale_damage_for_epoch(xbd_gdf, t, n_epochs)
                    epoch_gdf.to_file(out_path, driver='GeoJSON')
                xbd_count += 1

            else:
                spectral_gdfs = spectral_change_damage(aoi_id, meta, stack_path)
                for t, gdf in enumerate(spectral_gdfs):
                    out_path = out_dir / f"damage_polygons_T{t+2}.geojson"
                    if out_path.exists():
                        continue
                    if gdf is not None and len(gdf) > 0:
                        gdf.to_file(out_path, driver='GeoJSON')
                    else:
                        empty = gpd.GeoDataFrame(
                            columns=['geometry', 'damage_class', 'source'],
                            geometry='geometry'
                        ).set_crs('EPSG:4326')
                        empty.to_file(out_path, driver='GeoJSON')
                spectral_count += 1

        print(f"\n{'='*50}")
        print(f"[Step 3a Complete]")
        print(f"  xBD transfer   : {xbd_count}")
        print(f"  Spectral change: {spectral_count}")
        print(f"  Skipped        : {skipped}")
        if not xbd_map:
            print(f"\n  NOTE: No xBD data found. Load xBD into data/xbd/ then")
            print(f"  re-run: python main.py run-step3a --force-xbd")
        print(f"{'='*50}")
