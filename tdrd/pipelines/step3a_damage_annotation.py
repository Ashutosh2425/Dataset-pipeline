"""
Step 3a — Building Damage Annotation Transfer
Transfers xBD labels to AOIs with overlap; uses spectral change (NDVI/NBR delta)
as fallback for AOIs without xBD coverage.

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

EVENT_TO_XBD = {
    'EVT001': 'hurricane-harvey',
    'EVT003': 'santa-rosa-wildfire',
    'EVT005': 'midwest-flooding',
    'EVT006': 'beirut-explosion',
    'EVT008': 'tuscaloosa-tornado',
    'EVT010': 'turkey-earthquake-2023',
    'EVT011': 'hawaii-wildfire',
}

DAMAGE_MAP = {
    'no-damage':     0,
    'minor-damage':  1,
    'major-damage':  2,
    'destroyed':     3,
    'un-classified': 0,
}


# ── xBD helpers ───────────────────────────────────────────────────────────────

def load_xbd_polygons(aoi_bbox, event_id):
    xbd_name = EVENT_TO_XBD.get(event_id)
    if not xbd_name:
        return None

    label_dir = XBD_DIR / xbd_name / 'labels'
    if not label_dir.exists():
        return None

    aoi_geom = box(*aoi_bbox)
    rows = []

    for label_file in label_dir.glob('*_post_disaster.json'):
        try:
            with open(label_file) as f:
                data = json.load(f)
        except Exception:
            continue

        for feat in data.get('features', {}).get('xy', []):
            props = feat.get('properties', {})
            damage = DAMAGE_MAP.get(props.get('subtype', 'no-damage'), 0)
            wkt = feat.get('wkt')
            if not wkt:
                continue
            try:
                from shapely import wkt as shapely_wkt
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
    dates  = meta['dates']
    bpe    = meta['bands_per_epoch']   # bands per epoch list
    n_ep   = meta['n_epochs']
    results = []

    if not Path(stack_path).exists():
        return [None] * (n_ep - 1)

    # Pre-event band indices (epoch 0)
    pre_start = 1
    pre_red_idx  = pre_start + 2   # B04 = band 3 in 0-indexed → 1-based = 3
    pre_nir_idx  = pre_start + 3   # B08

    try:
        pre_red, transform, crs, meta_info = _read_band(stack_path, pre_red_idx)
        pre_nir, _, _, _                   = _read_band(stack_path, pre_nir_idx)
        pre_ndvi = _ndvi(pre_red, pre_nir)
    except Exception:
        return [None] * (n_ep - 1)

    cursor = sum(bpe[:1])  # skip epoch 0 bands

    for t in range(1, n_ep):
        ep_bands = bpe[t]
        ep_start = sum(bpe[:t]) + 1   # 1-based

        try:
            post_red, _, _, _ = _read_band(stack_path, ep_start + 2)
            post_nir, _, _, _ = _read_band(stack_path, ep_start + 3)
            post_ndvi = _ndvi(post_red, post_nir)

            delta = pre_ndvi - post_ndvi   # positive = vegetation loss = damage

            # Classify: 0=intact, 1=minor, 2=major, 3=destroyed
            damage_raster = np.zeros_like(delta, dtype=np.uint8)
            damage_raster[delta > 0.1]  = 1
            damage_raster[delta > 0.25] = 2
            damage_raster[delta > 0.4]  = 3

            # Vectorise
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

    def run(self):
        print(f"Step 3a: Damage annotation for {len(self.aois)} AOIs...")
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
            dates    = meta['dates']

            # Check if all outputs already exist
            all_done = all(
                (out_dir / f"damage_polygons_T{t+1}.geojson").exists()
                for t in range(1, n_epochs)
            )
            if all_done:
                skipped += 1
                continue

            # Try xBD first
            xbd_gdf = load_xbd_polygons(aoi['bbox'], event_id) if aoi.get('has_xbd_overlap') else None

            if xbd_gdf is not None and len(xbd_gdf) > 0:
                for t in range(1, n_epochs):
                    out_path = out_dir / f"damage_polygons_T{t+1}.geojson"
                    if out_path.exists():
                        continue
                    epoch_gdf = scale_damage_for_epoch(xbd_gdf, t, n_epochs)
                    epoch_gdf.to_file(out_path, driver='GeoJSON')
                xbd_count += 1

            else:
                # Spectral change fallback
                spectral_gdfs = spectral_change_damage(aoi_id, meta, stack_path)
                for t, gdf in enumerate(spectral_gdfs):
                    out_path = out_dir / f"damage_polygons_T{t+2}.geojson"
                    if out_path.exists():
                        continue
                    if gdf is not None and len(gdf) > 0:
                        gdf.to_file(out_path, driver='GeoJSON')
                    else:
                        # Write empty GeoJSON so file exists
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
        print(f"{'='*50}")
